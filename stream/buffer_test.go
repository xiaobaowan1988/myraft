package stream

import (
	"bytes"
	"math/rand"
	"testing"
)

func checkInvariants(t *testing.T, r *recvBuf) {
	t.Helper()
	if int64(len(r.data)) != r.contig-r.base {
		t.Fatalf("data length %d != contig-base %d", len(r.data), r.contig-r.base)
	}
	total := 0
	prevEnd := r.contig
	for i, s := range r.ooo {
		if s.pos <= prevEnd && !(i == 0 && s.pos > r.contig) {
			if i == 0 && s.pos <= r.contig {
				t.Fatalf("out-of-order segment at %d is not ahead of contig %d", s.pos, r.contig)
			}
			t.Fatalf("segment %d at %d overlaps or touches the previous end %d", i, s.pos, prevEnd)
		}
		prevEnd = s.end()
		total += len(s.b)
	}
	if total != r.oooBytes {
		t.Fatalf("oooBytes = %d, recount = %d", r.oooBytes, total)
	}
}

// Frames of random size and position, shuffled, duplicated and overlapping, must
// always reassemble the original stream, and the buffer must stay well-formed.
func TestRecvBufReassemblesAnyArrivalOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 300; round++ {
		n := 1 + rng.Intn(20_000)
		want := make([]byte, n)
		rng.Read(want)

		type frame struct{ pos, end int }
		var frames []frame
		for pos := 0; pos < n; {
			l := 1 + rng.Intn(1500)
			end := min(pos+l, n)
			frames = append(frames, frame{pos, end})
			pos = end
		}
		// extra overlapping frames at random places, and duplicates
		tiled := len(frames) // the first set tiles [0,n); the rest are extras
		for i := 0; i < tiled; i++ {
			a := rng.Intn(n)
			b := min(n, a+1+rng.Intn(3000))
			frames = append(frames, frame{a, b})
			if rng.Intn(3) == 0 {
				frames = append(frames, frames[rng.Intn(len(frames))])
			}
		}
		rng.Shuffle(len(frames), func(i, j int) { frames[i], frames[j] = frames[j], frames[i] })

		var r recvBuf
		limit := int64(n) + 100
		for _, f := range frames {
			r.insert(int64(f.pos), want[f.pos:f.end], limit)
			checkInvariants(t, &r)
		}
		// the shuffled set covers [0,n) because the first set tiles it
		if r.contig != int64(n) || !bytes.Equal(r.data, want) {
			t.Fatalf("round %d: contig=%d (want %d), data equal=%v", round, r.contig, n, bytes.Equal(r.data, want))
		}
		if len(r.ooo) != 0 || r.oooBytes != 0 {
			t.Fatalf("round %d: %d segments left after the stream completed", round, len(r.ooo))
		}
	}
}

func TestRecvBufRespectsTheWindowEdge(t *testing.T) {
	var r recvBuf
	data := bytes.Repeat([]byte{7}, 100)
	if r.insert(0, data, 40) != true || r.contig != 40 {
		t.Fatalf("a frame crossing the edge should be cut at it: contig=%d", r.contig)
	}
	if r.insert(40, data, 40) {
		t.Error("a frame starting at the edge was accepted")
	}
	if r.insert(500, data, 40) {
		t.Error("a frame beyond the edge was accepted")
	}
	checkInvariants(t, &r)
}

func TestRecvBufIgnoresOldAndDuplicateData(t *testing.T) {
	var r recvBuf
	r.insert(0, []byte("hello"), 1000)
	if r.insert(0, []byte("hello"), 1000) || r.insert(2, []byte("llo"), 1000) {
		t.Error("data already held was reported as new")
	}
	r.insert(10, []byte("zz"), 1000)
	if r.insert(10, []byte("zz"), 1000) || r.insert(10, []byte("z"), 1000) {
		t.Error("an out-of-order segment already held was reported as new")
	}
	if !r.insert(5, []byte("12345"), 1000) {
		t.Fatal("the filling frame was not accepted")
	}
	if string(r.data) != "hello12345zz" || r.contig != 12 {
		t.Fatalf("data = %q contig = %d", r.data, r.contig)
	}
	checkInvariants(t, &r)
}

func TestRecvBufConsumeAdvancesBase(t *testing.T) {
	var r recvBuf
	r.insert(0, []byte("abcdef"), 1000)
	r.consume(4)
	if r.base != 4 || string(r.data) != "ef" || r.contig != 6 {
		t.Fatalf("base=%d data=%q contig=%d", r.base, r.data, r.contig)
	}
	r.insert(6, []byte("gh"), 1000)
	if string(r.data) != "efgh" || r.contig != 8 {
		t.Fatalf("data=%q contig=%d", r.data, r.contig)
	}
}
