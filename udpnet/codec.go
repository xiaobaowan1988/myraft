// Package udpnet is a raft.Transport over raw UDP.
//
// Raft's safety does not depend on the network, so plain datagrams are legal; this
// package makes them efficient and usable. Per directed pair of nodes it builds an
// ordered, de-duplicated message stream with fragmentation, selective
// retransmission (NAK), a bounded give-up on gaps, flow and congestion control,
// and per-packet authentication, all with a fixed number of goroutines.
package udpnet

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math"

	"github.com/xiaobaowan1988/myraft/raft"
)

// CommandCodec turns a log entry's command into bytes and back. Commands are
// opaque to Raft, so the transport needs to be told how to ship them.
type CommandCodec interface {
	Encode(cmd interface{}) ([]byte, error)
	Decode(b []byte) (interface{}, error)
}

// DefaultCodec handles nil, []byte, string, int, bool and float64 directly and
// falls back to gob for anything else (the type must be gob.Register-ed).
type DefaultCodec struct{}

const (
	tagNil byte = iota
	tagBytes
	tagString
	tagInt
	tagBool
	tagFloat
	tagGob
)

type gobBox struct{ V interface{} }

func (DefaultCodec) Encode(cmd interface{}) ([]byte, error) {
	switch v := cmd.(type) {
	case nil:
		return []byte{tagNil}, nil
	case []byte:
		return append([]byte{tagBytes}, v...), nil
	case string:
		return append([]byte{tagString}, v...), nil
	case int:
		return binary.AppendVarint([]byte{tagInt}, int64(v)), nil
	case bool:
		if v {
			return []byte{tagBool, 1}, nil
		}
		return []byte{tagBool, 0}, nil
	case float64:
		return binary.BigEndian.AppendUint64([]byte{tagFloat}, math.Float64bits(v)), nil
	}
	var buf bytes.Buffer
	buf.WriteByte(tagGob)
	if err := gob.NewEncoder(&buf).Encode(&gobBox{V: cmd}); err != nil {
		return nil, fmt.Errorf("udpnet: cannot encode command of type %T (gob.Register it?): %w", cmd, err)
	}
	return buf.Bytes(), nil
}

func (DefaultCodec) Decode(b []byte) (interface{}, error) {
	if len(b) == 0 {
		return nil, errors.New("udpnet: empty command")
	}
	body := b[1:]
	switch b[0] {
	case tagNil:
		return nil, nil
	case tagBytes:
		b := make([]byte, len(body)) // non-nil even when empty, so []byte{} round-trips
		copy(b, body)
		return b, nil
	case tagString:
		return string(body), nil
	case tagInt:
		v, n := binary.Varint(body)
		if n <= 0 || n != len(body) {
			return nil, errors.New("udpnet: bad int command")
		}
		return int(v), nil
	case tagBool:
		if len(body) != 1 || body[0] > 1 {
			return nil, errors.New("udpnet: bad bool command")
		}
		return body[0] == 1, nil
	case tagFloat:
		if len(body) != 8 {
			return nil, errors.New("udpnet: bad float command")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(body)), nil
	case tagGob:
		var box gobBox
		if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&box); err != nil {
			return nil, err
		}
		return box.V, nil
	}
	return nil, fmt.Errorf("udpnet: unknown command tag %d", b[0])
}

// ---- message encoding: compact varints ----

// encodeMessage serialises m. The result is the stream payload that gets
// fragmented into datagrams.
func encodeMessage(m raft.Message, c CommandCodec) ([]byte, error) {
	b := make([]byte, 0, 64)
	b = append(b, byte(m.Type))
	b = binary.AppendVarint(b, int64(m.From))
	b = binary.AppendVarint(b, int64(m.To))
	b = binary.AppendVarint(b, int64(m.Term))
	switch m.Type {
	case raft.MsgVote:
		b = binary.AppendVarint(b, int64(m.LastLogIndex))
		b = binary.AppendVarint(b, int64(m.LastLogTerm))
	case raft.MsgVoteResp:
		b = appendBool(b, m.Granted)
	case raft.MsgApp:
		b = binary.AppendUvarint(b, m.Seq)
		b = binary.AppendVarint(b, int64(m.PrevLogIndex))
		b = binary.AppendVarint(b, int64(m.PrevLogTerm))
		b = binary.AppendVarint(b, int64(m.LeaderCommit))
		b = binary.AppendUvarint(b, uint64(len(m.Entries)))
		for _, e := range m.Entries {
			cmd, err := c.Encode(e.Command)
			if err != nil {
				return nil, err
			}
			b = binary.AppendVarint(b, int64(e.Term))
			b = binary.AppendUvarint(b, uint64(len(cmd)))
			b = append(b, cmd...)
		}
	case raft.MsgAppResp:
		b = binary.AppendUvarint(b, m.Seq)
		b = appendBool(b, m.Success)
		b = binary.AppendVarint(b, int64(m.Match))
		b = binary.AppendVarint(b, int64(m.ConflictIndex))
		b = binary.AppendVarint(b, int64(m.ConflictTerm))
	default:
		return nil, fmt.Errorf("udpnet: unknown message type %d", m.Type)
	}
	return b, nil
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

var errShort = errors.New("udpnet: truncated or malformed message")

// reader decodes with a sticky error and never trusts a declared length.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail() { r.err = errShort }

func (r *reader) byte() byte {
	if r.err != nil || len(r.b) < 1 {
		r.fail()
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) varint() int {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 || v > math.MaxInt32 || v < math.MinInt32 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return int(v)
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bool() bool {
	v := r.byte()
	if v > 1 {
		r.fail()
	}
	return v == 1
}

func (r *reader) take(n uint64) []byte {
	if r.err != nil || n > uint64(len(r.b)) {
		r.fail()
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// decodeMessage parses a message. It rejects truncated or inconsistent input
// (including trailing bytes) and never allocates in proportion to a declared
// count or length rather than to the bytes actually present.
func decodeMessage(b []byte, c CommandCodec) (raft.Message, error) {
	r := &reader{b: b}
	var m raft.Message
	m.Type = raft.MsgType(r.byte())
	m.From = r.varint()
	m.To = r.varint()
	m.Term = r.varint()
	switch m.Type {
	case raft.MsgVote:
		m.LastLogIndex = r.varint()
		m.LastLogTerm = r.varint()
	case raft.MsgVoteResp:
		m.Granted = r.bool()
	case raft.MsgApp:
		m.Seq = r.uvarint()
		m.PrevLogIndex = r.varint()
		m.PrevLogTerm = r.varint()
		m.LeaderCommit = r.varint()
		n := r.uvarint()
		if r.err == nil && n > uint64(len(r.b)) { // each entry needs at least 2 bytes
			r.fail()
		}
		if r.err == nil && n > 0 {
			m.Entries = make([]raft.LogEntry, 0, n)
			for i := uint64(0); i < n && r.err == nil; i++ {
				term := r.varint()
				cmdLen := r.uvarint()
				cmd := r.take(cmdLen)
				if r.err != nil {
					break
				}
				v, err := c.Decode(cmd)
				if err != nil {
					return raft.Message{}, err
				}
				m.Entries = append(m.Entries, raft.LogEntry{Term: term, Command: v})
			}
		}
	case raft.MsgAppResp:
		m.Seq = r.uvarint()
		m.Success = r.bool()
		m.Match = r.varint()
		m.ConflictIndex = r.varint()
		m.ConflictTerm = r.varint()
	default:
		return raft.Message{}, fmt.Errorf("udpnet: unknown message type %d", m.Type)
	}
	if r.err != nil {
		return raft.Message{}, r.err
	}
	if len(r.b) != 0 {
		return raft.Message{}, errors.New("udpnet: trailing bytes after message")
	}
	return m, nil
}
