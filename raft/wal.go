package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Record kinds stored in the WAL.
const (
	recMeta   uint8 = iota + 1 // Term, VotedFor
	recAppend                  // log = log[:Index] + Entries
)

// Record is one WAL record.
type Record struct {
	Kind     uint8
	Term     int
	VotedFor int
	Index    int
	Entries  []LogEntry
}

// Storage durably stores Raft's persistent state as an append-only record stream.
// Append must not return until the records are durable.
type Storage interface {
	Append(recs []Record) error
	Load() ([]Record, error)
}

// replay rebuilds persistent state from records. The log always starts with a
// term-0 sentinel at index 0.
func replay(recs []Record) (term, votedFor int, log []LogEntry, err error) {
	votedFor = -1
	log = []LogEntry{{}}
	for _, rec := range recs {
		switch rec.Kind {
		case recMeta:
			term, votedFor = rec.Term, rec.VotedFor
		case recAppend:
			if rec.Index < 1 || rec.Index > len(log) {
				return 0, 0, nil, fmt.Errorf("raft: wal append at index %d with log length %d", rec.Index, len(log))
			}
			log = append(log[:rec.Index], rec.Entries...)
		default:
			return 0, 0, nil, fmt.Errorf("raft: unknown wal record kind %d", rec.Kind)
		}
	}
	return term, votedFor, log, nil
}

// MemStorage is an in-memory Storage; it survives a simulated node crash
// because tests keep it across Raft instances.
type MemStorage struct {
	mu   sync.Mutex
	recs []Record
}

func (m *MemStorage) Append(recs []Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, recs...)
	return nil
}

func (m *MemStorage) Load() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Record(nil), m.recs...), nil
}

// FileWAL is a file-backed Storage. Frame format: len(u32) | crc32(u32) | gob payload.
type FileWAL struct {
	mu sync.Mutex
	f  *os.File
}

// OpenFileWAL opens (creating if needed) the WAL file at path.
func OpenFileWAL(path string) (*FileWAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileWAL{f: f}, nil
}

func (w *FileWAL) Append(recs []Record) error {
	var buf bytes.Buffer
	for _, rec := range recs {
		var payload bytes.Buffer
		if err := gob.NewEncoder(&payload).Encode(rec); err != nil {
			return err
		}
		var hdr [8]byte
		binary.BigEndian.PutUint32(hdr[0:4], uint32(payload.Len()))
		binary.BigEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload.Bytes()))
		buf.Write(hdr[:])
		buf.Write(payload.Bytes())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return err
	}
	return w.f.Sync() // durable only once Sync succeeds
}

// Load reads all valid records. A torn or corrupt tail is dropped and the file
// is truncated to the last valid record so later appends stay well-formed.
func (w *FileWAL) Load() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(w.f)
	if err != nil {
		return nil, err
	}
	var recs []Record
	off := 0
	for off+8 <= len(data) {
		n := int(binary.BigEndian.Uint32(data[off : off+4]))
		sum := binary.BigEndian.Uint32(data[off+4 : off+8])
		if off+8+n > len(data) {
			break
		}
		payload := data[off+8 : off+8+n]
		if crc32.ChecksumIEEE(payload) != sum {
			break
		}
		var rec Record
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&rec); err != nil {
			break
		}
		recs = append(recs, rec)
		off += 8 + n
	}
	if off < len(data) {
		if err := w.f.Truncate(int64(off)); err != nil {
			return nil, err
		}
		if err := w.f.Sync(); err != nil {
			return nil, err
		}
	}
	return recs, nil
}

func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
