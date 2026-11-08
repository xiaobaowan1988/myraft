// Package counters is a table of 64-bit counters in shared memory. The driver
// allocates them and updates them (positions, limits, statistics); clients map the
// same memory and read them, which is how a client learns, without a system call,
// how far a stream has got and how much more it may write.
package counters

import (
	"errors"
	"fmt"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

const (
	// Each counter has a 128-byte value slot (its own two cache lines, so counters
	// written by different threads do not share a line) and a 128-byte metadata slot.
	ValueSlot    = 128
	MetadataSlot = 128

	// metadata: state(4) | typeID(4) | registrationID(8) | labelLen(4) | label...
	mdState    = 0
	mdType     = 4
	mdRegID    = 8
	mdLabelLen = 16
	mdLabel    = 20
	MaxLabel   = MetadataSlot - mdLabel

	stateFree      int32 = 0
	stateAllocated int32 = 1
	stateReclaimed int32 = -1
)

// ErrFull is returned when every slot is in use.
var ErrFull = errors.New("counters: no free slot")

// BufferLengths returns the metadata and value area sizes for n counters.
func BufferLengths(n int) (metadata, values int) { return n * MetadataSlot, n * ValueSlot }

// Manager allocates counters. Only one goroutine (the driver's conductor) may call
// Allocate and Free.
type Manager struct {
	meta, vals shm.Buffer
	n          int
}

// NewManager wraps the two areas, which must describe the same number of counters.
func NewManager(meta, vals shm.Buffer) (*Manager, error) {
	if meta.Len()%MetadataSlot != 0 || vals.Len()%ValueSlot != 0 || meta.Len()/MetadataSlot != vals.Len()/ValueSlot {
		return nil, fmt.Errorf("counters: areas of %d and %d bytes do not describe the same number of counters", meta.Len(), vals.Len())
	}
	return &Manager{meta: meta, vals: vals, n: vals.Len() / ValueSlot}, nil
}

// Allocate takes a free slot, zeroes its value and describes it.
func (m *Manager) Allocate(typeID int32, registrationID int64, label string) (int, error) {
	if len(label) > MaxLabel {
		label = label[:MaxLabel]
	}
	for id := 0; id < m.n; id++ {
		off := id * MetadataSlot
		st := m.meta.Int32Volatile(off + mdState)
		if st != stateFree && st != stateReclaimed {
			continue
		}
		m.vals.PutInt64Ordered(id*ValueSlot, 0)
		m.meta.PutInt32(off+mdType, typeID)
		m.meta.PutInt64(off+mdRegID, registrationID)
		m.meta.PutInt32(off+mdLabelLen, int32(len(label)))
		m.meta.PutBytes(off+mdLabel, []byte(label))
		m.meta.PutInt32Ordered(off+mdState, stateAllocated) // publish last
		return id, nil
	}
	return -1, ErrFull
}

// Free releases a slot for reuse. Readers that still hold the id see it as gone.
func (m *Manager) Free(id int) {
	m.meta.PutInt32Ordered(id*MetadataSlot+mdState, stateReclaimed)
}

// Counter returns a handle on slot id.
func (m *Manager) Counter(id int) Counter { return Counter{buf: m.vals, off: id * ValueSlot} }

// Reader is the client side: read-only access to the same two areas.
type Reader struct{ m *Manager }

func NewReader(meta, vals shm.Buffer) (*Reader, error) {
	m, err := NewManager(meta, vals)
	if err != nil {
		return nil, err
	}
	return &Reader{m: m}, nil
}

// Counter returns a handle on slot id.
func (r *Reader) Counter(id int) Counter { return r.m.Counter(id) }

// Allocated reports whether slot id currently holds a counter registered as regID.
// A client uses it to notice that the counter it was given has been freed or reused.
func (r *Reader) Allocated(id int, regID int64) bool {
	if id < 0 || id >= r.m.n {
		return false
	}
	off := id * MetadataSlot
	return r.m.meta.Int32Volatile(off+mdState) == stateAllocated && r.m.meta.Int64(off+mdRegID) == regID
}

// Info describes one allocated counter.
type Info struct {
	ID             int
	TypeID         int32
	RegistrationID int64
	Label          string
	Value          int64
}

// ForEach calls f for every allocated counter.
func (r *Reader) ForEach(f func(Info)) {
	for id := 0; id < r.m.n; id++ {
		off := id * MetadataSlot
		if r.m.meta.Int32Volatile(off+mdState) != stateAllocated {
			continue
		}
		l := int(r.m.meta.Int32(off + mdLabelLen))
		if l < 0 || l > MaxLabel {
			l = 0
		}
		label := make([]byte, l)
		r.m.meta.GetBytes(off+mdLabel, label)
		f(Info{ID: id, TypeID: r.m.meta.Int32(off + mdType), RegistrationID: r.m.meta.Int64(off + mdRegID), Label: string(label), Value: r.m.vals.Int64Volatile(id * ValueSlot)})
	}
}

// Counter is a handle on one counter. Get is an atomic load. Set and Add are for the
// counter's single writer (see the package that defines the counter); concurrent
// writers should use Add, which is atomic.
type Counter struct {
	buf shm.Buffer
	off int
}

func (c Counter) Get() int64        { return c.buf.Int64Volatile(c.off) }
func (c Counter) Set(v int64)       { c.buf.PutInt64Ordered(c.off, v) }
func (c Counter) Add(d int64) int64 { return c.buf.AddInt64(c.off, d) + d }
func (c Counter) CompareAndSet(old, new int64) bool {
	return c.buf.CompareAndSetInt64(c.off, old, new)
}
