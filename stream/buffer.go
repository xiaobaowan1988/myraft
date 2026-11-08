// Package stream is a general-purpose reliable, ordered, flow-controlled message
// stream over UDP, in the spirit of Aeron's publication/subscription: reliability
// is infrastructure, so what runs on top (here, Raft) can assume that messages are
// not lost, duplicated or reordered for as long as a stream lives.
//
// Each node has, per peer, one outgoing and one incoming stream. A stream is a byte
// stream addressed by position; messages are length-prefixed inside it, so small
// messages share frames and large ones span many. The receiver NAKs gaps by
// position range and the sender retransmits exactly those bytes; nothing is ever
// skipped inside a stream. A stream ends only when its peer goes silent or
// restarts, and then the owner is told (Handler.OnPeerDown).
package stream

import "sort"

// seg is a run of bytes that arrived ahead of the contiguous prefix.
type seg struct {
	pos int64
	b   []byte
}

func (s seg) end() int64 { return s.pos + int64(len(s.b)) }

// recvBuf reassembles a byte stream from frames that may arrive out of order,
// duplicated or overlapping.
//
// data holds the contiguous bytes [base, contig); base is the first byte the
// application has not consumed yet. Bytes that arrived beyond contig wait in ooo,
// kept sorted, disjoint and non-adjacent.
type recvBuf struct {
	base     int64
	data     []byte
	contig   int64
	ooo      []seg
	oooBytes int
}

// insert adds a frame. Bytes at or beyond limit (the agreed right edge of the
// window) are dropped, as are bytes already present. It reports whether the frame
// contributed anything new.
func (r *recvBuf) insert(pos int64, payload []byte, limit int64) bool {
	end := pos + int64(len(payload))
	if end > limit {
		if pos >= limit {
			return false
		}
		payload = payload[:limit-pos]
		end = limit
	}
	if end <= r.contig || len(payload) == 0 {
		return false // entirely old
	}
	if pos < r.contig { // overlaps what we already hold: keep only the new tail
		payload = payload[r.contig-pos:]
		pos = r.contig
	}
	if pos == r.contig {
		r.data = append(r.data, payload...)
		r.contig = end
		r.absorb()
		return true
	}
	return r.addOOO(pos, payload)
}

// absorb moves out-of-order segments that have become contiguous into data.
func (r *recvBuf) absorb() {
	for len(r.ooo) > 0 && r.ooo[0].pos <= r.contig {
		s := r.ooo[0]
		if s.end() > r.contig {
			r.data = append(r.data, s.b[r.contig-s.pos:]...)
			r.contig = s.end()
		}
		r.oooBytes -= len(s.b)
		r.ooo[0] = seg{}
		r.ooo = r.ooo[1:]
	}
	if len(r.ooo) == 0 {
		r.ooo = nil
	}
}

// addOOO stores bytes ahead of the contiguous prefix, merging with any segment
// they overlap or touch.
func (r *recvBuf) addOOO(pos int64, b []byte) bool {
	end := pos + int64(len(b))
	// first segment that ends at or after pos (overlapping or adjacent on the left)
	i := sort.Search(len(r.ooo), func(k int) bool { return r.ooo[k].end() >= pos })
	j, start, stop := i, pos, end
	for j < len(r.ooo) && r.ooo[j].pos <= end {
		start = min(start, r.ooo[j].pos)
		stop = max(stop, r.ooo[j].end())
		j++
	}
	if j == i+1 && r.ooo[i].pos <= pos && r.ooo[i].end() >= end {
		return false // already covered by one existing segment
	}
	if j == i { // disjoint: insert a copy
		r.ooo = append(r.ooo, seg{})
		copy(r.ooo[i+1:], r.ooo[i:])
		r.ooo[i] = seg{pos, append([]byte(nil), b...)}
		r.oooBytes += len(b)
		return true
	}
	merged := make([]byte, stop-start)
	old := 0
	for k := i; k < j; k++ {
		copy(merged[r.ooo[k].pos-start:], r.ooo[k].b)
		old += len(r.ooo[k].b)
	}
	copy(merged[pos-start:], b)
	r.ooo[i] = seg{start, merged}
	r.ooo = append(r.ooo[:i+1], r.ooo[j:]...)
	r.oooBytes += len(merged) - old
	return true
}

// consume releases the first n bytes of data to the application.
func (r *recvBuf) consume(n int) {
	r.data = r.data[n:]
	r.base += int64(n)
	if len(r.data) == 0 {
		r.data = nil // let a large buffer go
	} else if cap(r.data) > 4*len(r.data)+(64<<10) {
		r.data = append([]byte(nil), r.data...)
	}
}

// buffered is the memory held for this stream, in bytes.
func (r *recvBuf) buffered() int { return len(r.data) + r.oooBytes }
