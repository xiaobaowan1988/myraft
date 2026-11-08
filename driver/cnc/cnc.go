// Package cnc defines the command-and-control file: the one shared-memory file
// through which clients and the driver find each other, and the messages they
// exchange over it.
//
// Layout (all lengths are in the header, so a client needs no configuration):
//
//	| header | to-driver ring | to-clients broadcast | counters metadata | counters values |
//
// The driver creates the file and writes the header's magic last, so a client that
// sees the magic sees a complete file. The driver's liveness is the heartbeat in the
// to-driver ring's trailer: a client treats a stale heartbeat as a dead driver.
package cnc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/ring"
	"github.com/xiaobaowan1988/myraft/driver/shm"
)

// FileName is the CnC file's name inside the driver directory.
const FileName = "cnc.dat"

const (
	headerLength = 128

	offMagic          = 0
	offVersion        = 4
	offToDriverLen    = 8
	offToClientsLen   = 12
	offCountersMeta   = 16
	offCountersValues = 20
	offClientTimeout  = 24 // int64 ns
	offStart          = 32 // int64 ns
	offPID            = 40 // int64

	magic   = 0x434e4331 // "CNC1"
	version = 1
)

// ErrNotReady means the file exists but its header is not complete yet (the driver is still starting).
var ErrNotReady = errors.New("cnc: file not initialised yet")

// Layout sizes a new CnC file.
type Layout struct {
	ToDriverCapacity  int // power of two
	ToClientsCapacity int // power of two
	MaxCounters       int
	ClientTimeoutNs   int64
	StartNs           int64
	PID               int64
}

func align8(n int) int { return (n + 7) &^ 7 }

// File is a mapped CnC file with typed access to its sections.
type File struct {
	m      *shm.Mapping
	buf    shm.Buffer
	toDrv  shm.Buffer
	toCli  shm.Buffer
	meta   shm.Buffer
	values shm.Buffer
}

// Create makes a new CnC file in dir (which must exist) and initialises it.
func Create(dir string, l Layout) (*File, error) {
	meta, vals := counters.BufferLengths(l.MaxCounters)
	toDrv := l.ToDriverCapacity + ring.TrailerLength
	toCli := l.ToClientsCapacity + ring.BroadcastTrailerLength
	total := headerLength + align8(toDrv) + align8(toCli) + meta + vals
	m, err := shm.Create(filepath.Join(dir, FileName), total)
	if err != nil {
		return nil, err
	}
	f := &File{m: m, buf: m.Buffer()}
	f.carve(toDrv, toCli, meta, vals)
	h := f.buf
	h.PutInt32(offVersion, version)
	h.PutInt32(offToDriverLen, int32(toDrv))
	h.PutInt32(offToClientsLen, int32(toCli))
	h.PutInt32(offCountersMeta, int32(meta))
	h.PutInt32(offCountersValues, int32(vals))
	h.PutInt64(offClientTimeout, l.ClientTimeoutNs)
	h.PutInt64(offStart, l.StartNs)
	h.PutInt64(offPID, l.PID)
	return f, nil
}

// Publish marks the file complete; clients that open it from now on succeed. Call it
// after the heartbeat has been set.
func (f *File) Publish() { f.buf.PutInt32Ordered(offMagic, magic) }

// Open maps an existing CnC file in dir.
func Open(dir string) (*File, error) {
	m, err := shm.Open(filepath.Join(dir, FileName))
	if err != nil {
		return nil, err
	}
	f := &File{m: m, buf: m.Buffer()}
	if f.buf.Len() < headerLength || f.buf.Int32Volatile(offMagic) != magic {
		m.Close()
		return nil, ErrNotReady
	}
	if v := f.buf.Int32(offVersion); v != version {
		m.Close()
		return nil, fmt.Errorf("cnc: version %d, this build speaks %d", v, version)
	}
	toDrv, toCli := int(f.buf.Int32(offToDriverLen)), int(f.buf.Int32(offToClientsLen))
	meta, vals := int(f.buf.Int32(offCountersMeta)), int(f.buf.Int32(offCountersValues))
	if headerLength+align8(toDrv)+align8(toCli)+meta+vals != f.buf.Len() {
		m.Close()
		return nil, errors.New("cnc: header does not match the file size")
	}
	f.carve(toDrv, toCli, meta, vals)
	return f, nil
}

func (f *File) carve(toDrv, toCli, meta, vals int) {
	off := headerLength
	f.toDrv = f.buf.Slice(off, toDrv)
	off += align8(toDrv)
	f.toCli = f.buf.Slice(off, toCli)
	off += align8(toCli)
	f.meta = f.buf.Slice(off, meta)
	off += meta
	f.values = f.buf.Slice(off, vals)
}

func (f *File) ToDriver() (*ring.ManyToOne, error)         { return ring.NewManyToOne(f.toDrv) }
func (f *File) NewTransmitter() (*ring.Transmitter, error) { return ring.NewTransmitter(f.toCli) }
func (f *File) NewReceiver() (*ring.Receiver, error)       { return ring.NewReceiver(f.toCli) }
func (f *File) CountersManager() (*counters.Manager, error) {
	return counters.NewManager(f.meta, f.values)
}
func (f *File) CountersReader() (*counters.Reader, error) {
	return counters.NewReader(f.meta, f.values)
}

func (f *File) ClientTimeoutNs() int64 { return f.buf.Int64(offClientTimeout) }
func (f *File) StartNs() int64         { return f.buf.Int64(offStart) }
func (f *File) PID() int64             { return f.buf.Int64(offPID) }

// Close unmaps the file; nothing obtained from it may be used afterwards.
func (f *File) Close() error { return f.m.Close() }

// Remove deletes a driver directory's contents and the directory itself.
func Remove(dir string) error { return os.RemoveAll(dir) }
