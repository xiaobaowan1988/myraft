// Package ring has the two shared-memory message queues of the media driver:
// ManyToOne carries commands from any number of client threads or processes to the
// driver; Broadcast carries events from the driver to all clients.
package ring

import (
	"errors"
	"fmt"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

const (
	recordHeaderLen = 8 // length(4) | type(4)
	recordAlign     = 8
	paddingType     = -1

	// Trailer layout of ManyToOne, after the data area. Each hot field sits in its
	// own cache line so producers and the consumer do not fight over one.
	headCacheOff   = 0
	tailOff        = 64
	headOff        = 128
	correlationOff = 192
	heartbeatOff   = 256
	// TrailerLength is the space ManyToOne needs after its data area.
	TrailerLength = 320
)

func align(n int) int { return (n + recordAlign - 1) &^ (recordAlign - 1) }

// ErrBadCapacity is returned for a data area that is not a power of two.
var ErrBadCapacity = errors.New("ring: capacity must be a power of two")

// ManyToOne is a lock-free multi-producer single-consumer ring buffer in shared
// memory. Producers claim space with a compare-and-swap of the tail, mark the claim
// with a negative length, write the record, and publish it by storing the positive
// length last. The consumer only trusts records whose length is positive.
//
// A producer that dies between claiming and publishing would block the consumer
// forever; Unblock lets the consumer turn such a hole into padding.
type ManyToOne struct {
	buf  shm.Buffer
	cap  int
	mask int64
	// capacity is also the base offset of the trailer
}

// NewManyToOne wraps buf, whose length must be capacity+TrailerLength with capacity a power of two.
func NewManyToOne(buf shm.Buffer) (*ManyToOne, error) {
	capacity := buf.Len() - TrailerLength
	if capacity < 64 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("%w (got %d bytes of data area)", ErrBadCapacity, capacity)
	}
	return &ManyToOne{buf: buf, cap: capacity, mask: int64(capacity - 1)}, nil
}

// MaxMessageLength is the largest payload Write accepts.
func (r *ManyToOne) MaxMessageLength() int { return r.cap/8 - recordHeaderLen }

// Write appends a record. It returns false, without blocking, if the ring is full.
// msgType must be positive.
func (r *ManyToOne) Write(msgType int32, src []byte) bool {
	if msgType < 1 {
		panic("ring: message type must be positive")
	}
	if len(src) > r.MaxMessageLength() {
		panic(fmt.Sprintf("ring: message of %d bytes exceeds the maximum %d", len(src), r.MaxMessageLength()))
	}
	recLen := len(src) + recordHeaderLen
	idx := r.claim(recLen)
	if idx < 0 {
		return false
	}
	r.commit(idx, recLen, msgType, src)
	return true
}

// testBeforePublish, if set by a test, runs after the record is fully written and
// right before the store that makes it visible. It lets a test check, without any
// timing luck, that nothing is visible earlier.
var testBeforePublish func()

func (r *ManyToOne) commit(idx, recLen int, msgType int32, src []byte) {
	r.buf.PutInt32Ordered(idx, -int32(recLen)) // claim marker: "being written"
	r.buf.PutBytes(idx+recordHeaderLen, src)
	r.buf.PutInt32(idx+4, msgType)
	if testBeforePublish != nil {
		testBeforePublish()
	}
	r.buf.PutInt32Ordered(idx, int32(recLen)) // publish
}

// claim reserves aligned space for a record and returns its offset, or -1 if full.
// When the record does not fit before the end of the data area, the rest of the
// area is turned into a padding record and the record goes to offset 0.
func (r *ManyToOne) claim(recLen int) int {
	required := align(recLen)
	cap64 := int64(r.cap)
	head := r.buf.Int64Volatile(r.cap + headCacheOff)
	var tail int64
	padding := 0
	for {
		tail = r.buf.Int64Volatile(r.cap + tailOff)
		if int64(required) > cap64-(tail-head) {
			head = r.buf.Int64Volatile(r.cap + headOff)
			if int64(required) > cap64-(tail-head) {
				return -1
			}
			r.buf.PutInt64Ordered(r.cap+headCacheOff, head)
		}
		padding = 0
		tailIdx := int(tail & r.mask)
		toEnd := r.cap - tailIdx
		if required > toEnd {
			headIdx := int(head & r.mask)
			if required > headIdx { // the start of the ring is not free yet
				head = r.buf.Int64Volatile(r.cap + headOff)
				headIdx = int(head & r.mask)
				if required > headIdx {
					return -1
				}
				r.buf.PutInt64Ordered(r.cap+headCacheOff, head)
			}
			padding = toEnd
		}
		if r.buf.CompareAndSetInt64(r.cap+tailOff, tail, tail+int64(required+padding)) {
			break
		}
	}
	idx := int(tail & r.mask)
	if padding != 0 {
		r.buf.PutInt32Ordered(idx, -int32(padding))
		r.buf.PutInt32(idx+4, paddingType)
		r.buf.PutInt32Ordered(idx, int32(padding))
		idx = 0
	}
	return idx
}

// Read hands up to limit committed records to handler, oldest first, and frees
// their space. Only one goroutine (the consumer) may call Read and Unblock. The
// payload slice is valid only during the call.
func (r *ManyToOne) Read(handler func(msgType int32, payload []byte), limit int) int {
	head := r.buf.Int64(r.cap + headOff)
	headIdx := int(head & r.mask)
	contiguous := r.cap - headIdx
	bytesRead, count := 0, 0
	defer func() {
		if bytesRead != 0 {
			r.buf.Zero(headIdx, bytesRead) // before releasing the space to producers
			r.buf.PutInt64Ordered(r.cap+headOff, head+int64(bytesRead))
		}
	}()
	for bytesRead < contiguous && count < limit {
		idx := headIdx + bytesRead
		recLen := int(r.buf.Int32Volatile(idx))
		if recLen <= 0 {
			break // not published yet
		}
		if recLen < recordHeaderLen || idx+recLen > r.cap {
			panic(fmt.Sprintf("ring: corrupt record of length %d at %d", recLen, idx))
		}
		bytesRead += align(recLen)
		typ := r.buf.Int32(idx + 4)
		if typ == paddingType {
			continue
		}
		count++
		handler(typ, r.buf.Bytes(idx+recordHeaderLen, recLen-recordHeaderLen))
	}
	return count
}

// Size is the number of bytes currently in the ring (claimed, published or not).
func (r *ManyToOne) Size() int {
	for {
		head := r.buf.Int64Volatile(r.cap + headOff)
		tail := r.buf.Int64Volatile(r.cap + tailOff)
		if h2 := r.buf.Int64Volatile(r.cap + headOff); h2 == head {
			return int(tail - head)
		}
	}
}

// NextCorrelationID returns a number that no other caller, in any process, gets.
func (r *ManyToOne) NextCorrelationID() int64 {
	return r.buf.AddInt64(r.cap+correlationOff, 1) + 1
}

// SetConsumerHeartbeat records the consumer's liveness (a timestamp in ns).
func (r *ManyToOne) SetConsumerHeartbeat(t int64) { r.buf.PutInt64Ordered(r.cap+heartbeatOff, t) }

// ConsumerHeartbeat returns the last value SetConsumerHeartbeat stored.
func (r *ManyToOne) ConsumerHeartbeat() int64 { return r.buf.Int64Volatile(r.cap + heartbeatOff) }

// Unblock repairs a hole left by a producer that died between claiming space and
// publishing its record: the claimed range at the consumer's position is turned into
// padding so Read can move on. It returns true if it changed anything. Call it only
// when the consumer has been stuck for longer than any live producer could take.
func (r *ManyToOne) Unblock() bool {
	head := r.buf.Int64Volatile(r.cap + headOff)
	tail := r.buf.Int64Volatile(r.cap + tailOff)
	if head == tail {
		return false
	}
	consumerIdx := int(head & r.mask)
	producerIdx := int(tail & r.mask)
	length := r.buf.Int32Volatile(consumerIdx)
	if length < 0 { // claimed, never published
		r.buf.PutInt32(consumerIdx+4, paddingType)
		r.buf.PutInt32Ordered(consumerIdx, -length)
		return true
	}
	if length > 0 {
		return false
	}
	// Length 0: the claimer died before even writing its marker. Find the next
	// claimed record and pad up to it, if everything in between is untouched.
	limit := r.cap
	if producerIdx > consumerIdx {
		limit = producerIdx
	}
	for i := consumerIdx + recordAlign; i < limit; i += recordAlign {
		if r.buf.Int32Volatile(i) != 0 {
			for j := i - recordAlign; j > consumerIdx; j -= recordAlign {
				if r.buf.Int32Volatile(j) != 0 {
					return false
				}
			}
			r.buf.PutInt32(consumerIdx+4, paddingType)
			r.buf.PutInt32Ordered(consumerIdx, int32(i-consumerIdx))
			return true
		}
	}
	// Nothing claimed behind it either: the dead producer's claim reaches the producer
	// index (or the end of the data area), and it is all zero.
	if limit > consumerIdx {
		r.buf.PutInt32(consumerIdx+4, paddingType)
		r.buf.PutInt32Ordered(consumerIdx, int32(limit-consumerIdx))
		return true
	}
	return false
}
