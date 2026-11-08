package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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

// frameMagic starts every frame. It lets Load tell a torn tail from damage in the
// middle of the file: after a bad frame it can scan ahead for a valid one.
const frameMagic uint32 = 0xD7A1B10C

const frameHdrLen = 12 // magic(4) | len(4) | crc32(4)

// CorruptError reports damage in the middle of a WAL: a bad frame at Offset with a
// valid frame still to be found at NextValid. Dropping everything after the bad
// frame would silently lose state that was acknowledged, so the log is refused.
type CorruptError struct {
	Offset    int // where the first bad frame starts
	NextValid int // where a valid frame was found after it
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("raft: wal corrupt at offset %d (valid data resumes at %d); refusing to drop it", e.Offset, e.NextValid)
}

// encodeFrames serialises records as frames: magic | len | crc32 | gob payload.
func encodeFrames(recs []Record) ([]byte, error) {
	var buf bytes.Buffer
	for _, rec := range recs {
		var payload bytes.Buffer
		if err := gob.NewEncoder(&payload).Encode(rec); err != nil {
			return nil, err
		}
		var hdr [frameHdrLen]byte
		binary.BigEndian.PutUint32(hdr[0:4], frameMagic)
		binary.BigEndian.PutUint32(hdr[4:8], uint32(payload.Len()))
		binary.BigEndian.PutUint32(hdr[8:12], crc32.ChecksumIEEE(payload.Bytes()))
		buf.Write(hdr[:])
		buf.Write(payload.Bytes())
	}
	return buf.Bytes(), nil
}

// parseFrame decodes the frame at the start of b, returning its record and size.
func parseFrame(b []byte) (Record, int, bool) {
	if len(b) < frameHdrLen || binary.BigEndian.Uint32(b[0:4]) != frameMagic {
		return Record{}, 0, false
	}
	n := int(binary.BigEndian.Uint32(b[4:8]))
	if n < 0 || frameHdrLen+n > len(b) {
		return Record{}, 0, false
	}
	payload := b[frameHdrLen : frameHdrLen+n]
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(b[8:12]) {
		return Record{}, 0, false
	}
	var rec Record
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&rec); err != nil {
		return Record{}, 0, false
	}
	return rec, frameHdrLen + n, true
}

// findFrame returns the offset of the first valid frame at or after from, or -1.
func findFrame(data []byte, from int) int {
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], frameMagic)
	for off := from; off+frameHdrLen <= len(data); off++ {
		i := bytes.Index(data[off:], magic[:])
		if i < 0 {
			return -1
		}
		off += i
		if _, _, ok := parseFrame(data[off:]); ok {
			return off
		}
	}
	return -1
}

// FileWAL is a file-backed Storage: an append-only file of frames, synced to disk
// before Append returns.
type FileWAL struct {
	mu  sync.Mutex
	f   *os.File
	err error // sticky: once a write fails the file may end in half a frame
}

// OpenFileWAL opens (creating if needed) the WAL file at path. A newly created file
// is made durable by syncing its directory too; otherwise a crash could lose the
// file itself, and with it votes and acknowledgements it had recorded.
func OpenFileWAL(path string) (*FileWAL, error) {
	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &FileWAL{f: f}, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) { // some filesystems cannot sync a directory
		return err
	}
	return nil
}

// Append writes and syncs the records. After a failed write or sync every later
// Append fails too: the file may end in half a frame, and appending behind it would
// bury damage in the middle of the log.
func (w *FileWAL) Append(recs []Record) error {
	buf, err := encodeFrames(recs)
	if err != nil {
		return err // nothing was written, so the log is still sound
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if _, err := w.f.Write(buf); err != nil {
		w.err = fmt.Errorf("raft: wal write failed, no further appends are safe: %w", err)
		return w.err
	}
	if err := w.f.Sync(); err != nil { // durable only once Sync succeeds
		w.err = fmt.Errorf("raft: wal sync failed, no further appends are safe: %w", err)
		return w.err
	}
	return nil
}

// Load reads all valid records. The file may end in a torn frame (a crash in the
// middle of an Append): it is dropped and the file truncated, so later appends stay
// well-formed. Damage anywhere else, a bad frame with valid data after it, is
// reported as *CorruptError and nothing is dropped.
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
	for off < len(data) {
		rec, n, ok := parseFrame(data[off:])
		if !ok {
			break
		}
		recs = append(recs, rec)
		off += n
	}
	if off < len(data) {
		if next := findFrame(data, off+1); next >= 0 {
			return nil, &CorruptError{Offset: off, NextValid: next}
		}
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
