package ring

import (
	"fmt"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

// Broadcast trailer, after the data area.
const (
	bcTailIntentOff = 0
	bcTailOff       = 64
	bcLatestOff     = 128
	// BroadcastTrailerLength is the space a broadcast buffer needs after its data area.
	BroadcastTrailerLength = 192
)

// Transmitter is the single writer of a broadcast buffer. It never blocks and never
// waits for receivers: a receiver that falls a whole buffer behind is lapped and
// loses records (it is told).
type Transmitter struct {
	buf  shm.Buffer
	cap  int
	mask int64
}

func checkBroadcast(buf shm.Buffer) (int, error) {
	capacity := buf.Len() - BroadcastTrailerLength
	if capacity < 64 || capacity&(capacity-1) != 0 {
		return 0, fmt.Errorf("%w (got %d bytes of data area)", ErrBadCapacity, capacity)
	}
	return capacity, nil
}

// NewTransmitter wraps buf (capacity + BroadcastTrailerLength bytes).
func NewTransmitter(buf shm.Buffer) (*Transmitter, error) {
	c, err := checkBroadcast(buf)
	if err != nil {
		return nil, err
	}
	return &Transmitter{buf: buf, cap: c, mask: int64(c - 1)}, nil
}

// MaxMessageLength is the largest payload Transmit accepts.
func (t *Transmitter) MaxMessageLength() int { return t.cap/8 - recordHeaderLen }

// Transmit publishes a record to every receiver. msgType must be positive. Only one
// goroutine at a time may call it.
func (t *Transmitter) Transmit(msgType int32, src []byte) {
	if msgType < 1 {
		panic("ring: message type must be positive")
	}
	if len(src) > t.MaxMessageLength() {
		panic(fmt.Sprintf("ring: message of %d bytes exceeds the maximum %d", len(src), t.MaxMessageLength()))
	}
	tail := t.buf.Int64(t.cap + bcTailOff)
	recLen := len(src) + recordHeaderLen
	aligned := align(recLen)
	idx := int(tail & t.mask)
	toEnd := t.cap - idx
	if toEnd < aligned {
		// Declare the intent before touching memory so a receiver that is about to
		// read the area being overwritten can detect it.
		t.buf.PutInt64Ordered(t.cap+bcTailIntentOff, tail+int64(aligned+toEnd))
		t.buf.PutInt32(idx, int32(toEnd))
		t.buf.PutInt32(idx+4, paddingType)
		tail += int64(toEnd)
		idx = 0
	} else {
		t.buf.PutInt64Ordered(t.cap+bcTailIntentOff, tail+int64(aligned))
	}
	t.buf.PutInt32(idx, int32(recLen))
	t.buf.PutInt32(idx+4, msgType)
	t.buf.PutBytes(idx+recordHeaderLen, src)
	t.buf.PutInt64(t.cap+bcLatestOff, tail)
	t.buf.PutInt64Ordered(t.cap+bcTailOff, tail+int64(aligned))
}

// Receiver reads a broadcast buffer. Each receiver has its own cursor, so any number
// of them can read the same buffer, in any process. A new receiver starts at the most
// recent record, not at the beginning.
type Receiver struct {
	buf     shm.Buffer
	cap     int
	mask    int64
	cursor  int64
	scratch []byte
	lapped  int64
}

// NewReceiver wraps buf and positions the receiver at the latest record.
func NewReceiver(buf shm.Buffer) (*Receiver, error) {
	c, err := checkBroadcast(buf)
	if err != nil {
		return nil, err
	}
	r := &Receiver{buf: buf, cap: c, mask: int64(c - 1)}
	r.cursor = buf.Int64Volatile(c + bcLatestOff)
	return r, nil
}

// Lapped counts how many times this receiver fell behind and lost records.
func (r *Receiver) Lapped() int64 { return r.lapped }

// valid reports whether the data at position cursor may still be intact.
func (r *Receiver) valid(cursor int64) bool {
	return cursor+int64(r.cap) > r.buf.Int64Volatile(r.cap+bcTailIntentOff)
}

// Receive returns the next record, or ok=false if there is none. The payload is a
// copy owned by the receiver, valid until the next call. A record is only returned
// after the receiver has verified that the transmitter did not overwrite it while it
// was being copied.
func (r *Receiver) Receive() (msgType int32, payload []byte, ok bool) {
	for {
		tail := r.buf.Int64Volatile(r.cap + bcTailOff)
		if tail <= r.cursor {
			return 0, nil, false
		}
		cur := r.cursor
		if !r.valid(cur) {
			r.lapped++
			r.cursor = r.buf.Int64Volatile(r.cap + bcLatestOff)
			continue
		}
		idx := int(cur & r.mask)
		length := int(r.buf.Int32(idx))
		typ := r.buf.Int32(idx + 4)
		if length < recordHeaderLen || idx+length > r.cap {
			// Read torn memory: the transmitter is overwriting it. Resynchronise.
			r.lapped++
			r.cursor = r.buf.Int64Volatile(r.cap + bcLatestOff)
			continue
		}
		if typ != paddingType {
			n := length - recordHeaderLen
			if cap(r.scratch) < n {
				r.scratch = make([]byte, n)
			}
			r.scratch = r.scratch[:n]
			r.buf.GetBytes(idx+recordHeaderLen, r.scratch)
		}
		if !r.valid(cur) { // overwritten while we copied
			r.lapped++
			r.cursor = r.buf.Int64Volatile(r.cap + bcLatestOff)
			continue
		}
		r.cursor = cur + int64(align(length))
		if typ == paddingType {
			continue
		}
		return typ, r.scratch, true
	}
}
