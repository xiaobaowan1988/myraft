// Package proto is the UDP wire format of the media driver.
//
// Every frame starts with the 32-byte header of a log buffer frame (little-endian):
//
//	length(4) | version(1) flags(1) type(2) | termOffset(4) | sessionID(4) | streamID(4) | termID(4) | reserved(8)
//
// so a data frame can leave the sender's log and enter the receiver's log byte for
// byte. A data datagram is a sequence of such frames. The control frames (Setup,
// Status, Nak, Heartbeat) are one per datagram, a header plus a small body.
package proto

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/xiaobaowan1988/myraft/driver/logbuf"
)

const (
	HeaderLength = logbuf.HeaderLength
	Version      = 1

	TypePad       = logbuf.TypePad
	TypeData      = logbuf.TypeData
	TypeSetup     = 0x10
	TypeStatus    = 0x11
	TypeNak       = 0x12
	TypeHeartbeat = 0x13

	setupLen  = HeaderLength + 12
	statusLen = HeaderLength + 4
	nakLen    = HeaderLength + 4
)

var le = binary.LittleEndian

// ErrMalformed is returned for datagrams that are not valid.
var ErrMalformed = errors.New("proto: malformed packet")

// Header is the common frame header.
type Header struct {
	Length     int32
	Version    uint8
	Flags      uint8
	Type       uint16
	TermOffset int32
	SessionID  int32
	StreamID   int32
	TermID     int32
}

func putHeader(b []byte, h Header) {
	le.PutUint32(b[0:], uint32(h.Length))
	b[4], b[5] = h.Version, h.Flags
	le.PutUint16(b[6:], h.Type)
	le.PutUint32(b[8:], uint32(h.TermOffset))
	le.PutUint32(b[12:], uint32(h.SessionID))
	le.PutUint32(b[16:], uint32(h.StreamID))
	le.PutUint32(b[20:], uint32(h.TermID))
	le.PutUint64(b[24:], 0)
}

// ParseHeader decodes the header at the start of b.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderLength {
		return Header{}, fmt.Errorf("%w: %d bytes is less than a header", ErrMalformed, len(b))
	}
	h := Header{
		Length:     int32(le.Uint32(b[0:])),
		Version:    b[4],
		Flags:      b[5],
		Type:       le.Uint16(b[6:]),
		TermOffset: int32(le.Uint32(b[8:])),
		SessionID:  int32(le.Uint32(b[12:])),
		StreamID:   int32(le.Uint32(b[16:])),
		TermID:     int32(le.Uint32(b[20:])),
	}
	if h.Version != Version {
		return h, fmt.Errorf("%w: version %d", ErrMalformed, h.Version)
	}
	return h, nil
}

// Setup announces a publication to a would-be receiver.
type Setup struct {
	SessionID, StreamID int32
	TermID              int32 // the sender's current term
	TermOffset          int32 // and offset in it: where a joining receiver starts
	InitialTermID       int32
	TermLength          int32
	MTU                 int32
}

func EncodeSetup(s Setup) []byte {
	b := make([]byte, setupLen)
	putHeader(b, Header{Length: setupLen, Version: Version, Type: TypeSetup, TermOffset: s.TermOffset, SessionID: s.SessionID, StreamID: s.StreamID, TermID: s.TermID})
	le.PutUint32(b[HeaderLength:], uint32(s.InitialTermID))
	le.PutUint32(b[HeaderLength+4:], uint32(s.TermLength))
	le.PutUint32(b[HeaderLength+8:], uint32(s.MTU))
	return b
}

func ParseSetup(b []byte) (Setup, error) {
	h, err := controlHeader(b, TypeSetup, setupLen)
	if err != nil {
		return Setup{}, err
	}
	return Setup{SessionID: h.SessionID, StreamID: h.StreamID, TermID: h.TermID, TermOffset: h.TermOffset,
		InitialTermID: int32(le.Uint32(b[HeaderLength:])), TermLength: int32(le.Uint32(b[HeaderLength+4:])),
		MTU: int32(le.Uint32(b[HeaderLength+8:]))}, nil
}

// Status is a receiver's report: how far it has consumed, and how much more it will take.
type Status struct {
	SessionID, StreamID int32
	TermID, TermOffset  int32 // the consumption position
	Window              int32 // bytes the sender may send beyond it
}

func EncodeStatus(s Status) []byte {
	b := make([]byte, statusLen)
	putHeader(b, Header{Length: statusLen, Version: Version, Type: TypeStatus, TermOffset: s.TermOffset, SessionID: s.SessionID, StreamID: s.StreamID, TermID: s.TermID})
	le.PutUint32(b[HeaderLength:], uint32(s.Window))
	return b
}

func ParseStatus(b []byte) (Status, error) {
	h, err := controlHeader(b, TypeStatus, statusLen)
	if err != nil {
		return Status{}, err
	}
	w := int32(le.Uint32(b[HeaderLength:]))
	if w < 0 {
		return Status{}, fmt.Errorf("%w: negative window", ErrMalformed)
	}
	return Status{SessionID: h.SessionID, StreamID: h.StreamID, TermID: h.TermID, TermOffset: h.TermOffset, Window: w}, nil
}

// Nak asks the sender to resend [TermID/TermOffset, +Length).
type Nak struct {
	SessionID, StreamID int32
	TermID, TermOffset  int32
	Length              int32
}

func EncodeNak(n Nak) []byte {
	b := make([]byte, nakLen)
	putHeader(b, Header{Length: nakLen, Version: Version, Type: TypeNak, TermOffset: n.TermOffset, SessionID: n.SessionID, StreamID: n.StreamID, TermID: n.TermID})
	le.PutUint32(b[HeaderLength:], uint32(n.Length))
	return b
}

func ParseNak(b []byte) (Nak, error) {
	h, err := controlHeader(b, TypeNak, nakLen)
	if err != nil {
		return Nak{}, err
	}
	l := int32(le.Uint32(b[HeaderLength:]))
	if l <= 0 {
		return Nak{}, fmt.Errorf("%w: NAK for %d bytes", ErrMalformed, l)
	}
	return Nak{SessionID: h.SessionID, StreamID: h.StreamID, TermID: h.TermID, TermOffset: h.TermOffset, Length: l}, nil
}

// Heartbeat tells the receiver where the sender is when there is nothing to send.
type Heartbeat struct {
	SessionID, StreamID int32
	TermID, TermOffset  int32
}

func EncodeHeartbeat(h Heartbeat) []byte {
	b := make([]byte, HeaderLength)
	putHeader(b, Header{Length: HeaderLength, Version: Version, Type: TypeHeartbeat, TermOffset: h.TermOffset, SessionID: h.SessionID, StreamID: h.StreamID, TermID: h.TermID})
	return b
}

func ParseHeartbeat(b []byte) (Heartbeat, error) {
	h, err := controlHeader(b, TypeHeartbeat, HeaderLength)
	if err != nil {
		return Heartbeat{}, err
	}
	return Heartbeat{SessionID: h.SessionID, StreamID: h.StreamID, TermID: h.TermID, TermOffset: h.TermOffset}, nil
}

// controlHeader checks a control datagram: right type, exactly the expected size,
// and a length field that agrees with it.
func controlHeader(b []byte, typ uint16, size int) (Header, error) {
	h, err := ParseHeader(b)
	if err != nil {
		return h, err
	}
	if h.Type != typ || len(b) != size || int(h.Length) != size {
		return h, fmt.Errorf("%w: type %#x, %d bytes (header says %d), want type %#x of %d bytes", ErrMalformed, h.Type, len(b), h.Length, typ, size)
	}
	return h, nil
}

// Kind returns the type of a datagram without decoding the rest.
func Kind(b []byte) (uint16, error) {
	h, err := ParseHeader(b)
	return h.Type, err
}

// CheckData validates a data datagram's framing and returns the first frame's header:
// a sequence of whole frames (a trailing padding frame may be only its header) whose
// sessions and streams agree. It does not know the receiver's term geometry; the
// receiving log checks that when it inserts the frames.
func CheckData(b []byte) (Header, error) {
	first, err := ParseHeader(b)
	if err != nil {
		return first, err
	}
	for cur := 0; cur < len(b); {
		h, err := ParseHeader(b[cur:])
		if err != nil {
			return first, err
		}
		if h.Type != TypeData && h.Type != TypePad {
			return first, fmt.Errorf("%w: type %#x inside a data datagram", ErrMalformed, h.Type)
		}
		if h.SessionID != first.SessionID || h.StreamID != first.StreamID {
			return first, fmt.Errorf("%w: frames of different streams in one datagram", ErrMalformed)
		}
		onWire := int(h.Length)
		if h.Type == TypePad {
			onWire = HeaderLength
		}
		if h.Length < HeaderLength || cur+onWire > len(b) {
			return first, fmt.Errorf("%w: frame of %d bytes with %d available", ErrMalformed, h.Length, len(b)-cur)
		}
		cur += (onWire + logbuf.FrameAlign - 1) &^ (logbuf.FrameAlign - 1)
	}
	return first, nil
}

// DataSpan is how far a data datagram advances a stream position: the aligned sizes
// of its frames, where a padding frame counts for the whole space it covers although
// only its header is present. b must have passed CheckData.
func DataSpan(b []byte) int {
	span := 0
	for cur := 0; cur < len(b); {
		h, _ := ParseHeader(b[cur:])
		onWire := int(h.Length)
		if h.Type == TypePad {
			onWire = HeaderLength
		}
		span += (int(h.Length) + logbuf.FrameAlign - 1) &^ (logbuf.FrameAlign - 1)
		cur += (onWire + logbuf.FrameAlign - 1) &^ (logbuf.FrameAlign - 1)
	}
	return span
}
