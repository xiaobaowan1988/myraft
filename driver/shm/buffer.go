// Package shm is the shared-memory foundation of the media driver: memory-mapped
// files and a bounds-checked, alignment-checked view over a byte range that offers
// the atomic accesses the lock-free structures are built from.
//
// Go's sync/atomic operations are sequentially consistent, which is stronger than
// the release/acquire ordering Aeron relies on. So the usual publishing pattern is
// correct as it stands: write the payload with plain stores, then publish it with
// one atomic store; a reader that observes the atomic store observes the payload.
//
// The race detector does NOT track memory outside the Go heap, so code on top of
// this package gets no protection from `go test -race` once the buffer is mmap'd.
package shm

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Heap returns n bytes of ordinary Go memory aligned to 8 bytes, for tests and for
// structures that do not need to be shared between processes.
func Heap(n int) []byte {
	w := make([]uint64, (n+7)/8)
	if len(w) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&w[0])), n)
}

// Buffer is a view over a byte range. It is a small value: copy it freely. Every
// access is bounds-checked and every atomic access is alignment-checked; a bad
// offset panics, because it can only be a bug in the caller.
type Buffer struct {
	b []byte
}

// Wrap makes a Buffer over b. The first byte must be 8-byte aligned (mmap'd memory
// always is; use Heap for Go memory).
func Wrap(b []byte) Buffer {
	if len(b) > 0 && uintptr(unsafe.Pointer(&b[0]))%8 != 0 {
		panic("shm: buffer is not 8-byte aligned")
	}
	return Buffer{b: b}
}

func (a Buffer) Len() int { return len(a.b) }

// Slice is a view of n bytes starting at off. off must be a multiple of 8, so the
// view keeps the alignment guarantee.
func (a Buffer) Slice(off, n int) Buffer {
	a.check(off, n)
	if off%8 != 0 {
		panic(fmt.Sprintf("shm: slice offset %d is not 8-byte aligned", off))
	}
	return Buffer{b: a.b[off : off+n : off+n]}
}

// Bytes returns the bytes [off, off+n) without copying. The caller must respect the
// concurrency protocol of whatever structure lives there.
func (a Buffer) Bytes(off, n int) []byte {
	a.check(off, n)
	return a.b[off : off+n : off+n]
}

func (a Buffer) check(off, n int) {
	if off < 0 || n < 0 || off > len(a.b)-n {
		panic(fmt.Sprintf("shm: range [%d,%d) outside buffer of %d bytes", off, off+n, len(a.b)))
	}
}

func (a Buffer) ptr(off, size int) unsafe.Pointer {
	a.check(off, size)
	if off%size != 0 {
		panic(fmt.Sprintf("shm: offset %d is not %d-byte aligned", off, size))
	}
	return unsafe.Pointer(&a.b[off])
}

// 32-bit accesses. "Volatile"/"Ordered" are atomic loads/stores; the unmarked pair
// are plain accesses that are only safe where the protocol says a single thread
// owns the location, or where a later atomic store publishes them.

func (a Buffer) Int32(off int) int32       { return *(*int32)(a.ptr(off, 4)) }
func (a Buffer) PutInt32(off int, v int32) { *(*int32)(a.ptr(off, 4)) = v }
func (a Buffer) Int32Volatile(off int) int32 {
	return atomic.LoadInt32((*int32)(a.ptr(off, 4)))
}
func (a Buffer) PutInt32Ordered(off int, v int32) {
	atomic.StoreInt32((*int32)(a.ptr(off, 4)), v)
}
func (a Buffer) CompareAndSetInt32(off int, old, new int32) bool {
	return atomic.CompareAndSwapInt32((*int32)(a.ptr(off, 4)), old, new)
}
func (a Buffer) AddInt32(off int, d int32) int32 {
	return atomic.AddInt32((*int32)(a.ptr(off, 4)), d) - d // value before the add
}

// 64-bit accesses.

func (a Buffer) Int64(off int) int64       { return *(*int64)(a.ptr(off, 8)) }
func (a Buffer) PutInt64(off int, v int64) { *(*int64)(a.ptr(off, 8)) = v }
func (a Buffer) Int64Volatile(off int) int64 {
	return atomic.LoadInt64((*int64)(a.ptr(off, 8)))
}
func (a Buffer) PutInt64Ordered(off int, v int64) {
	atomic.StoreInt64((*int64)(a.ptr(off, 8)), v)
}
func (a Buffer) CompareAndSetInt64(off int, old, new int64) bool {
	return atomic.CompareAndSwapInt64((*int64)(a.ptr(off, 8)), old, new)
}

// AddInt64 adds d atomically and returns the value BEFORE the add (Aeron's getAndAdd).
func (a Buffer) AddInt64(off int, d int64) int64 {
	return atomic.AddInt64((*int64)(a.ptr(off, 8)), d) - d
}

// PutBytes copies src into the buffer at off.
func (a Buffer) PutBytes(off int, src []byte) {
	a.check(off, len(src))
	copy(a.b[off:], src)
}

// GetBytes copies len(dst) bytes at off into dst.
func (a Buffer) GetBytes(off int, dst []byte) {
	a.check(off, len(dst))
	copy(dst, a.b[off:])
}

// Zero clears [off, off+n).
func (a Buffer) Zero(off, n int) {
	a.check(off, n)
	clear(a.b[off : off+n])
}
