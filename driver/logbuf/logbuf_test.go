package logbuf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

const noLimit = int64(1) << 60

func newLog(t testing.TB, termLength, mtu int) *LogBuffer {
	t.Helper()
	buf := shm.Wrap(shm.Heap(FileLength(termLength)))
	l, err := Create(buf, Params{TermLength: termLength, MTU: mtu, InitialTermID: 7, SessionID: 11, StreamID: 13})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// offer retries through term rotations.
func offer(t testing.TB, l *LogBuffer, msg []byte, limit int64) (int64, Status) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		pos, st := l.Offer(msg, limit)
		if st != AdminAction {
			return pos, st
		}
	}
	t.Fatal("Offer kept asking for admin action")
	return 0, 0
}

// reader polls a log and reassembles messages.
type reader struct {
	l    *LogBuffer
	pos  int64
	msgs [][]byte
	asm  *Assembler
}

func newReader(l *LogBuffer) *reader {
	r := &reader{l: l}
	r.asm = NewAssembler(func(p []byte, _ FrameInfo) { r.msgs = append(r.msgs, append([]byte(nil), p...)) })
	return r
}

func (r *reader) poll(t testing.TB) int {
	t.Helper()
	pos, n, err := r.l.Poll(r.pos, r.asm.OnFragment, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r.pos = pos
	return n
}

// drain polls until the reader's position stops moving (a poll that only skips
// padding delivers nothing but still advances).
func (r *reader) drain(t testing.TB) {
	t.Helper()
	for {
		before := r.pos
		r.poll(t)
		if r.pos == before {
			return
		}
	}
}

func TestCreateValidatesParameters(t *testing.T) {
	good := Params{TermLength: 4096, MTU: 256}
	for name, p := range map[string]Params{
		"term not a power of two": {TermLength: 5000, MTU: 256},
		"term too small":          {TermLength: 2048, MTU: 64},
		"mtu not aligned":         {TermLength: 4096, MTU: 100},
		"mtu too small":           {TermLength: 4096, MTU: 32},
		"mtu above term/8":        {TermLength: 4096, MTU: 1024},
	} {
		if _, err := Create(shm.Wrap(shm.Heap(FileLength(4096))), p); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := Create(shm.Wrap(shm.Heap(FileLength(4096)-8)), good); err == nil {
		t.Error("a buffer of the wrong size was accepted")
	}
	if _, err := Create(shm.Wrap(shm.Heap(FileLength(4096))), good); err != nil {
		t.Errorf("good parameters rejected: %v", err)
	}
}

func TestWrapSeesWhatCreateWrote(t *testing.T) {
	buf := shm.Wrap(shm.Heap(FileLength(8192)))
	if _, err := Wrap(buf); err == nil {
		t.Fatal("Wrap accepted an uninitialised buffer")
	}
	a, err := Create(buf, Params{TermLength: 8192, MTU: 512, InitialTermID: -5, SessionID: 3, StreamID: 4})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Wrap(buf)
	if err != nil {
		t.Fatal(err)
	}
	if b.TermLength() != 8192 || b.MTU() != 512 || b.InitialTermID() != -5 || b.SessionID() != 3 || b.StreamID() != 4 {
		t.Fatalf("Wrap read wrong parameters: %d %d %d %d %d", b.TermLength(), b.MTU(), b.InitialTermID(), b.SessionID(), b.StreamID())
	}
	a.Offer([]byte("hello"), noLimit)
	r := newReader(b)
	if r.poll(t) != 1 || string(r.msgs[0]) != "hello" {
		t.Fatal("a message offered through one handle is not visible through the other")
	}
}

func TestOfferPollRoundTripAndPositions(t *testing.T) {
	l := newLog(t, 4096, 256)
	if l.Position() != 0 {
		t.Fatal("a new log is not at position 0")
	}
	r := newReader(l)
	var want int64
	for i, n := range []int{0, 1, 31, 32, 33, 100, 224} { // 224 = max payload with MTU 256
		msg := bytes.Repeat([]byte{byte('a' + i)}, n)
		pos, st := offer(t, l, msg, noLimit)
		if st != Ok {
			t.Fatalf("offer of %d bytes: %v", n, st)
		}
		want += int64(align(HeaderLength + n))
		if pos != want || l.Position() != want {
			t.Fatalf("after %d bytes: Offer returned %d, Position %d, want %d", n, pos, l.Position(), want)
		}
		if got := r.poll(t); got != 1 || !bytes.Equal(r.msgs[len(r.msgs)-1], msg) {
			t.Fatalf("message of %d bytes did not come back (polled %d)", n, got)
		}
		if r.pos != want {
			t.Fatalf("reader at %d, want %d", r.pos, want)
		}
	}
	if r.poll(t) != 0 {
		t.Fatal("polled something from an empty log")
	}
}

func TestFrameHeaderFields(t *testing.T) {
	l := newLog(t, 4096, 256)
	l.Offer([]byte("x"), noLimit)
	l.Offer(bytes.Repeat([]byte("y"), 500), noLimit) // 3 fragments
	var infos []FrameInfo
	_, n, err := l.Poll(0, func(_ []byte, i FrameInfo) { infos = append(infos, i) }, 100)
	if err != nil || n != 4 {
		t.Fatalf("polled %d frames, err=%v", n, err)
	}
	wantFlags := []uint8{FlagUnfragmented, FlagBegin, 0, FlagEnd}
	wantOff := []int{0, 64, 64 + 256, 64 + 512}
	for i, in := range infos {
		if in.Flags != wantFlags[i] || in.TermOffset != wantOff[i] || in.SessionID != 11 || in.StreamID != 13 || in.TermID != 7 {
			t.Errorf("frame %d: %+v, want flags %#x offset %d session 11 stream 13 term 7", i, in, wantFlags[i], wantOff[i])
		}
	}
}

func TestFragmentationBoundaries(t *testing.T) {
	l := newLog(t, 1<<16, 256)
	r := newReader(l)
	mp := l.MaxPayload()
	sizes := []int{mp - 1, mp, mp + 1, 2 * mp, 2*mp + 1, 5*mp - 3, 8 * 1024}
	for _, n := range sizes {
		msg := make([]byte, n)
		for i := range msg {
			msg[i] = byte(i*7 + n)
		}
		before := l.Position()
		pos, st := offer(t, l, msg, noLimit)
		if st != Ok {
			t.Fatalf("size %d: %v", n, st)
		}
		if pos-before != int64(l.framedLength(n)) {
			t.Fatalf("size %d: advanced %d, want %d", n, pos-before, l.framedLength(n))
		}
		r.msgs = nil
		r.poll(t)
		if len(r.msgs) != 1 || !bytes.Equal(r.msgs[0], msg) {
			t.Fatalf("size %d: reassembled message differs (got %d messages)", n, len(r.msgs))
		}
	}
	if _, st := l.Offer(make([]byte, l.MaxMessageLength()+1), noLimit); st != TooBig {
		t.Fatalf("an oversize message gave %v, want TooBig", st)
	}
	if _, st := offer(t, l, make([]byte, l.MaxMessageLength()), noLimit); st != Ok {
		t.Fatalf("a maximum-size message gave %v", st)
	}
}

func TestBackPressureAtLimit(t *testing.T) {
	l := newLog(t, 4096, 256)
	if _, st := l.Offer([]byte("a"), 128); st != Ok { // each message takes 64 bytes
		t.Fatal(st)
	}
	if _, st := l.Offer([]byte("b"), 128); st != Ok { // position 64 < 128
		t.Fatal(st)
	}
	if _, st := l.Offer([]byte("c"), 128); st != BackPressured { // position 128 >= 128
		t.Fatalf("got %v at the limit, want BackPressured", st)
	}
	if _, st := l.Offer([]byte("c"), 256); st != Ok {
		t.Fatalf("a raised limit did not release the publisher: %v", st)
	}
}

// Writing through many terms: the position keeps growing by exactly the framed
// length plus padding, padding is never delivered, and nothing is lost or garbled.
func TestRotationAcrossManyTerms(t *testing.T) {
	l := newLog(t, 4096, 256)
	r := newReader(l)
	const total = 3000
	for i := 0; i < total; i++ {
		msg := make([]byte, (i*53)%400)
		for j := range msg {
			msg[j] = byte(i + j)
		}
		if _, st := offer(t, l, msg, noLimit); st != Ok {
			t.Fatalf("message %d: %v", i, st)
		}
		r.poll(t)
	}
	if len(r.msgs) != total {
		t.Fatalf("got %d messages, want %d", len(r.msgs), total)
	}
	for i, m := range r.msgs {
		if len(m) != (i*53)%400 {
			t.Fatalf("message %d has length %d", i, len(m))
		}
		for j := range m {
			if m[j] != byte(i+j) {
				t.Fatalf("message %d garbled at byte %d", i, j)
			}
		}
	}
	if r.pos != l.Position() {
		t.Fatalf("reader at %d, writer at %d", r.pos, l.Position())
	}
	if l.Position() < int64(10*4096) {
		t.Fatalf("only reached position %d: the test did not cross enough terms", l.Position())
	}
	if l.Position()>>l.termShift < 10 {
		t.Fatal("fewer than 10 term rotations")
	}
}

// The tail of a term that a message does not fit in becomes one padding frame, and the
// message starts the next term.
// (The reader polls as it goes: a log may only be written a bit less than a term ahead of its reader.)
func TestPaddingAtEndOfTerm(t *testing.T) {
	l := newLog(t, 4096, 512)
	r := newReader(l)
	for l.Position() < 4096-512 {
		offer(t, l, make([]byte, 480), noLimit) // 512-byte frames
		r.poll(t)
	}
	pos, _ := offer(t, l, make([]byte, 480), noLimit) // exactly fills the term
	r.poll(t)
	if pos != 4096 {
		t.Fatalf("filled term to %d, want 4096", pos)
	}
	// A 100-byte message in a term with 0 bytes left: no padding frame, straight to the next term.
	pos, _ = offer(t, l, make([]byte, 100), noLimit)
	r.poll(t)
	if pos != 4096+160 {
		t.Fatalf("first message of term 1 ended at %d, want %d", pos, 4096+160)
	}
	// Leave 96 bytes at the end of term 1 and offer something bigger.
	for l.Position() < 2*4096-96 {
		offer(t, l, make([]byte, 64), noLimit) // 96-byte frames
		r.poll(t)
	}
	if l.Position() != 2*4096-96 {
		t.Fatalf("setup went wrong: %d", l.Position())
	}
	pos, _ = offer(t, l, make([]byte, 200), noLimit)
	if pos != 2*4096+256 { // 96 padding, then 232 -> 256 in the next term
		t.Fatalf("message after padding ended at %d, want %d", pos, 2*4096+256)
	}
	r.drain(t)
	if r.pos != l.Position() || r.msgs[len(r.msgs)-1] == nil || len(r.msgs[len(r.msgs)-1]) != 200 {
		t.Fatalf("reader at %d (writer %d), last message %d bytes", r.pos, l.Position(), len(r.msgs[len(r.msgs)-1]))
	}
}

// Nothing becomes visible before it is complete, not even the first frame of a
// fragmented message.
func TestMessageInvisibleUntilPublished(t *testing.T) {
	for _, size := range []int{10, 1000} {
		l := newLog(t, 1<<14, 256)
		seen, marker := -1, int32(0)
		testBeforePublish = func() {
			_, seen, _ = l.Poll(0, func([]byte, FrameInfo) {}, 100)
			marker = l.terms[0].Int32Volatile(offLength)
		}
		l.Offer(make([]byte, size), noLimit)
		testBeforePublish = nil
		if want := -int32(l.framedLength(size)); marker != want {
			t.Fatalf("size %d: while being written the first frame's length field is %d, want the negated claim size %d", size, marker, want)
		}
		if seen != 0 {
			t.Fatalf("size %d: the reader saw %d frames before the message was published", size, seen)
		}
		r := newReader(l)
		if r.poll(t) == 0 || len(r.msgs) != 1 || len(r.msgs[0]) != size {
			t.Fatalf("size %d: the message did not appear after publishing", size)
		}
	}
}

// A partition is reused every third term. Frames left in it from the earlier use
// carry the old term id and must never be read as current data.
func TestPollIgnoresFramesOfAnEarlierTerm(t *testing.T) {
	l := newLog(t, 4096, 256)
	l.Offer([]byte("old"), noLimit) // term 0, partition 0
	if _, n, _ := l.Poll(3*4096, func([]byte, FrameInfo) {}, 10); n != 0 {
		t.Fatalf("Poll delivered %d frames of term 0 as if they were term 3", n)
	}
	if f, adv := l.Scan(3*4096, 1<<20); f != nil || adv != 0 {
		t.Fatal("Scan returned frames of term 0 for term 3")
	}
}

func TestScan(t *testing.T) {
	l := newLog(t, 4096, 256)
	if f, adv := l.Scan(0, 1000); f != nil || adv != 0 {
		t.Fatal("scan of an empty log returned data")
	}
	for i := 0; i < 5; i++ {
		l.Offer(bytes.Repeat([]byte{byte(i)}, 100), noLimit) // 160-byte frames
	}
	f, adv := l.Scan(0, 500) // three frames fit in 500
	if len(f) != 480 || adv != 480 {
		t.Fatalf("scan returned %d bytes, advance %d; want 480", len(f), adv)
	}
	if f[HeaderLength] != 0 || f[160+HeaderLength] != 1 || f[320+HeaderLength] != 2 {
		t.Fatal("scan returned frames in the wrong order")
	}
	f, adv = l.Scan(480, 1<<20)
	if len(f) != 320 || adv != 320 {
		t.Fatalf("second scan: %d bytes, advance %d, want 320", len(f), adv)
	}
	if f, _ := l.Scan(0, 100); f != nil {
		t.Fatal("scan returned a frame bigger than maxLength")
	}
	// padding: only its header goes out, the position advances over all of it
	for i := 0; i < 33; i++ { // 33 frames of 96 bytes after the 800 already there: 128 bytes left in the term
		offer(t, l, make([]byte, 64), noLimit)
	}
	start := l.Position()
	pad := int(4096 - start)
	if pad != 128 {
		t.Fatalf("setup went wrong: %d bytes left in the term, want 128", pad)
	}
	offer(t, l, make([]byte, 300), noLimit) // does not fit: pads the rest of the term and rotates
	f, adv = l.Scan(start, 1<<20)
	if len(f) != HeaderLength || adv != pad {
		t.Fatalf("scan at the padding: %d bytes, advance %d; want 32 and %d", len(f), adv, pad)
	}
	if got := int(binary.LittleEndian.Uint32(f[0:4])); got != pad {
		t.Fatalf("padding header says length %d, want %d", got, pad)
	}
	// data before the padding is returned without it
	f, adv = l.Scan(start-96, 1<<20)
	if len(f) != 96 || adv != 96 {
		t.Fatalf("scan before the padding: %d bytes advance %d, want 96 (the padding left for the next scan)", len(f), adv)
	}
}

// Several publishers, one reader that is held back by the limit like the driver's
// flow control would: term-sized limits, many rotations. Every publisher's messages
// arrive in order, complete and exactly once.
func TestConcurrentPublishersAcrossRotations(t *testing.T) {
	const publishers, perPublisher = 4, 6000
	l := newLog(t, 1<<16, 256)
	var consumed atomic.Int64
	window := int64(l.TermLength() / 2)
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for s := 0; s < perPublisher; s++ {
				msg := makeMsg(p, s)
				for {
					_, st := l.Offer(msg, consumed.Load()+window)
					if st == Ok {
						break
					}
					if st == TooBig {
						t.Errorf("message of %d bytes is too big", len(msg))
						return
					}
					time.Sleep(time.Microsecond)
				}
			}
		}(p)
	}
	next := make([]int, publishers)
	total := 0
	asm := NewAssembler(func(m []byte, _ FrameInfo) { verifyMsg(t, m, next); total++ })
	var pos int64
	deadline := time.Now().Add(120 * time.Second)
	for total < publishers*perPublisher {
		np, n, err := l.Poll(pos, asm.OnFragment, 64)
		if err != nil {
			t.Fatal(err)
		}
		pos = np
		consumed.Store(pos)
		if n == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("stuck at %d of %d messages (position %d, producer %d)", total, publishers*perPublisher, pos, l.Position())
			}
			time.Sleep(5 * time.Microsecond)
		}
	}
	wg.Wait()
	for p, n := range next {
		if n != perPublisher {
			t.Errorf("publisher %d: %d messages, want %d", p, n, perPublisher)
		}
	}
	t.Logf("%d messages through %d terms", total, l.Position()>>l.termShift)
}

func makeMsg(p, s int) []byte {
	size := 12 + (p*131+s*17)%500 // up to 3 fragments with MTU 256
	m := make([]byte, size)
	binary.BigEndian.PutUint32(m[0:], uint32(p))
	binary.BigEndian.PutUint32(m[4:], uint32(s))
	binary.BigEndian.PutUint32(m[8:], uint32(size))
	for i := 12; i < size; i++ {
		m[i] = byte(p + s + i)
	}
	return m
}

func verifyMsg(t testing.TB, m []byte, next []int) {
	t.Helper()
	if len(m) < 12 {
		t.Fatalf("garbled message of %d bytes", len(m))
	}
	p, s, size := int(binary.BigEndian.Uint32(m[0:])), int(binary.BigEndian.Uint32(m[4:])), int(binary.BigEndian.Uint32(m[8:]))
	if len(m) != size || p >= len(next) {
		t.Fatalf("garbled message: len %d, header says %d, publisher %d", len(m), size, p)
	}
	if s != next[p] {
		t.Fatalf("publisher %d: got sequence %d, want %d", p, s, next[p])
	}
	for i := 12; i < size; i++ {
		if m[i] != byte(p+s+i) {
			t.Fatalf("publisher %d seq %d: corrupt byte %d", p, s, i)
		}
	}
	next[p]++
}

// --- holes left by dead publishers --------------------------------------------

// deadClaim claims space like Offer does and then "dies": nothing, or only the
// marker, is written.
func deadClaim(t *testing.T, l *LogBuffer, size int, marker bool) {
	t.Helper()
	termCount := l.meta.Int32Volatile(metaActive)
	part := l.partitionOf(termCount)
	raw := l.meta.Int64Volatile(l.tailOff(part))
	required := l.framedLength(size)
	off, st := l.claim(termCount, part, int32(raw>>32), required)
	if st != Ok {
		t.Fatalf("setup: claim gave %v", st)
	}
	if marker {
		l.terms[part].PutInt32Ordered(off+offLength, -int32(required))
	}
}

func TestUnblockHoles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int
		marker bool
	}{
		{"marker, single frame", 40, true},
		{"marker, fragmented message", 700, true},
		{"nothing written", 40, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLog(t, 1<<14, 256)
			r := newReader(l)
			l.Offer([]byte("before"), noLimit)
			deadClaim(t, l, tc.size, tc.marker)
			l.Offer([]byte("after"), noLimit)
			r.poll(t)
			if len(r.msgs) != 1 || string(r.msgs[0]) != "before" {
				t.Fatalf("got %q", r.msgs)
			}
			if r.poll(t) != 0 {
				t.Fatal("the reader got past the hole")
			}
			if !l.Unblock(r.pos) {
				t.Fatal("Unblock did nothing")
			}
			r.poll(t)
			if len(r.msgs) != 2 || string(r.msgs[1]) != "after" {
				t.Fatalf("after Unblock got %q", r.msgs)
			}
			if r.pos != l.Position() {
				t.Fatalf("reader at %d, writer %d", r.pos, l.Position())
			}
		})
	}
}

func TestUnblockLeavesLiveDataAlone(t *testing.T) {
	l := newLog(t, 4096, 256)
	if l.Unblock(0) {
		t.Fatal("Unblock changed an empty log")
	}
	l.Offer([]byte("x"), noLimit)
	if l.Unblock(0) {
		t.Fatal("Unblock touched a published frame")
	}
	r := newReader(l)
	if r.poll(t) != 1 {
		t.Fatal("published frame was damaged")
	}
	if l.Unblock(r.pos) {
		t.Fatal("Unblock changed a log with nothing in front of the reader")
	}
}

// A publisher that overshot the end of the term died before padding it, and before
// rotating. The reader is stuck at the start of the hole.
func TestUnblockHoleAtTheEndOfATermThatWasNeverRotated(t *testing.T) {
	l := newLog(t, 4096, 256)
	r := newReader(l)
	for l.Position() < 4096-96 {
		offer(t, l, make([]byte, 64), noLimit)
	}
	r.poll(t)
	// the dying publisher: claims 300 bytes with 96 left, then dies before writing padding or rotating
	termCount := l.meta.Int32Volatile(metaActive)
	part := l.partitionOf(termCount)
	l.meta.AddInt64(l.tailOff(part), int64(l.framedLength(300)))
	if r.poll(t) != 0 {
		t.Fatal("reader moved past the hole")
	}
	if !l.Unblock(r.pos) {
		t.Fatal("Unblock did nothing")
	}
	// Unblock itself finished the rotation the dead publisher never did (nobody else
	// has called Offer, so it cannot have been anyone else).
	if got := l.meta.Int32Volatile(metaActive); got != 1 {
		t.Fatalf("active term count is %d after Unblock, want 1", got)
	}
	// the log works again: a new message goes through into the next term
	pos, st := offer(t, l, []byte("fresh"), noLimit)
	if st != Ok || pos <= 4096 {
		t.Fatalf("offer after Unblock: %v at %d", st, pos)
	}
	r.drain(t)
	if len(r.msgs) == 0 || string(r.msgs[len(r.msgs)-1]) != "fresh" {
		t.Fatalf("the reader did not get the message after the hole: %q", r.msgs[len(r.msgs)-1:])
	}
}

// --- other processes ----------------------------------------------------------

func TestMain(m *testing.M) {
	if os.Getenv("LOGBUF_ROLE") == "publisher" {
		os.Exit(childPublisher())
	}
	os.Exit(m.Run())
}

func childPublisher() int {
	mp, err := shm.Open(os.Getenv("LOGBUF_FILE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer mp.Close()
	l, err := Wrap(mp.Buffer())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	var p, n int
	fmt.Sscan(os.Getenv("LOGBUF_ID"), &p)
	fmt.Sscan(os.Getenv("LOGBUF_N"), &n)
	ctl := mp.Buffer() // the consumer's position is published in the spare tail of the metadata
	window := int64(l.TermLength() / 2)
	for s := 0; s < n; s++ {
		msg := makeMsg(p, s)
		deadline := time.Now().Add(60 * time.Second)
		for {
			consumed := ctl.Int64Volatile(ctl.Len() - 8)
			_, st := l.Offer(msg, consumed+window)
			if st == Ok {
				break
			}
			if time.Now().After(deadline) {
				return 3
			}
			time.Sleep(time.Microsecond)
		}
	}
	return 0
}

func TestPublishersInOtherProcesses(t *testing.T) {
	const publishers, perPublisher = 3, 5000
	path := filepath.Join(t.TempDir(), "log")
	mp, err := shm.Create(path, FileLength(1<<14))
	if err != nil {
		t.Fatal(err)
	}
	defer mp.Close()
	l, err := Create(mp.Buffer(), Params{TermLength: 1 << 14, MTU: 256, InitialTermID: 100, SessionID: 1, StreamID: 2})
	if err != nil {
		t.Fatal(err)
	}
	var cmds []*exec.Cmd
	for p := 0; p < publishers; p++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), "LOGBUF_ROLE=publisher", "LOGBUF_FILE="+path, fmt.Sprintf("LOGBUF_ID=%d", p), fmt.Sprintf("LOGBUF_N=%d", perPublisher))
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	next := make([]int, publishers)
	total := 0
	asm := NewAssembler(func(m []byte, _ FrameInfo) { verifyMsg(t, m, next); total++ })
	var pos int64
	whole := mp.Buffer()
	deadline := time.Now().Add(120 * time.Second)
	for total < publishers*perPublisher {
		np, n, err := l.Poll(pos, asm.OnFragment, 64)
		if err != nil {
			t.Fatal(err)
		}
		pos = np
		whole.PutInt64Ordered(whole.Len()-8, pos)
		if n == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("stuck at %d of %d", total, publishers*perPublisher)
			}
			time.Sleep(5 * time.Microsecond)
		}
	}
	for i, c := range cmds {
		if err := c.Wait(); err != nil {
			t.Errorf("publisher process %d: %v", i, err)
		}
	}
	for p, n := range next {
		if n != perPublisher {
			t.Errorf("process %d: %d messages, want %d", p, n, perPublisher)
		}
	}
	t.Logf("%d messages from %d processes through %d terms", total, publishers, l.Position()>>l.termShift)
}

// A publisher whose view of the active term is stale (it was descheduled between
// looking and claiming, long enough for its partition to be recycled) must fail its
// claim without touching the tail of a term that is not its own.
func TestStaleClaimDoesNotDisturbARecycledTerm(t *testing.T) {
	l := newLog(t, 4096, 256)
	r := newReader(l)
	for l.Position() < 3*4096+1000 { // term 3 is being written, in the partition term 0 used
		offer(t, l, make([]byte, 200), noLimit)
		r.poll(t)
	}
	staleCount := int32(0) // term 0 lived in partition 0, which now holds a later term
	part := l.partitionOf(staleCount)
	before := l.meta.Int64Volatile(l.tailOff(part))
	if _, st := l.claim(staleCount, part, l.initialTermID+staleCount, 256); st != AdminAction {
		t.Fatalf("a stale claim returned %v, want AdminAction", st)
	}
	if after := l.meta.Int64Volatile(l.tailOff(part)); after != before {
		t.Fatalf("a stale claim changed the tail of a term that was not its own: %#x -> %#x", before, after)
	}
}

func TestAssembler(t *testing.T) {
	var got []string
	a := NewAssembler(func(p []byte, _ FrameInfo) { got = append(got, string(p)) })
	frag := func(flags uint8, s string) { a.OnFragment([]byte(s), FrameInfo{Flags: flags}) }
	frag(FlagUnfragmented, "whole")
	frag(FlagBegin, "a")
	frag(0, "b")
	frag(FlagEnd, "c")
	frag(0, "orphan-middle") // its beginning was lost
	frag(FlagEnd, "orphan-end")
	frag(FlagBegin, "x") // an unfinished message ...
	frag(FlagBegin, "y") // ... is abandoned when a new one starts
	frag(FlagEnd, "z")
	frag(FlagBegin, "p")
	frag(FlagUnfragmented, "interleaved") // a whole message ends a half-built one
	frag(FlagEnd, "q")
	want := []string{"whole", "abc", "yz", "interleaved"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestConnectedFlagIsSharedThroughTheMetadata(t *testing.T) {
	buf := shm.Wrap(shm.Heap(FileLength(4096)))
	a, _ := Create(buf, Params{TermLength: 4096, MTU: 256})
	b, _ := Wrap(buf)
	if a.IsConnected() || b.IsConnected() {
		t.Fatal("a new log is connected")
	}
	a.SetConnected(true)
	if !b.IsConnected() {
		t.Fatal("the flag set through one handle is not seen through the other")
	}
	a.SetConnected(false)
	if b.IsConnected() {
		t.Fatal("the flag was not cleared")
	}
}

// --- the receiving side ---------------------------------------------------------

func newPair(t *testing.T, termLength, mtu int) (pub, img *LogBuffer) {
	t.Helper()
	pub = newLog(t, termLength, mtu)
	img = newLog(t, termLength, mtu) // same initial term id, session and stream (see newLog)
	return
}

// transfer scans everything the publisher has from position `from` and returns the
// datagrams a sender would put on the wire.
func datagrams(pub *LogBuffer, from int64, maxLen int) (out []struct {
	pos  int64
	data []byte
}) {
	for {
		frames, adv := pub.Scan(from, maxLen)
		if adv == 0 {
			return
		}
		out = append(out, struct {
			pos  int64
			data []byte
		}{from, append([]byte(nil), frames...)})
		from += int64(adv)
	}
}

func TestInsertReproducesThePublishersLog(t *testing.T) {
	pub, img := newPair(t, 1<<14, 256)
	rd := newReader(img)
	var sent int
	for pub.Position() < 5*(1<<14) { // several terms, with padding at the ends
		offer(t, pub, makeMsg(0, sent), noLimit)
		sent++
		// move what is there to the image, and let the image's reader keep up
		if sent%7 == 0 {
			for _, d := range datagrams(pub, rd.pos, 256) {
				if _, err := img.Insert(d.pos, d.data); err != nil {
					t.Fatal(err)
				}
			}
			rd.drain(t)
			// what the driver's receiver does: once the reader is in term t, term t+1's
			// partition (last used by term t-2) is cleaned for reuse
			img.CleanTerm(int32(rd.pos>>img.termShift) + 1)
		}
	}
	for _, d := range datagrams(pub, rd.pos, 256) {
		img.Insert(d.pos, d.data)
	}
	rd.drain(t)
	next := make([]int, 1)
	for _, m := range rd.msgs {
		verifyMsg(t, m, next)
	}
	if next[0] != sent {
		t.Fatalf("the image delivered %d of %d messages", next[0], sent)
	}
	if rd.pos != pub.Position() {
		t.Fatalf("image reader at %d, publisher at %d", rd.pos, pub.Position())
	}
}

func TestInsertHandlesReorderingAndDuplicates(t *testing.T) {
	pub, img := newPair(t, 1<<16, 256)
	for i := 0; i < 60; i++ {
		offer(t, pub, makeMsg(0, i), noLimit)
	}
	ds := datagrams(pub, 0, 256)
	if len(ds) < 20 {
		t.Fatalf("only %d datagrams", len(ds))
	}
	rd := newReader(img)
	// deliver in a scrambled order with every datagram twice; the reader must see
	// nothing past the first gap and everything once the gaps are filled
	order := []int{}
	for i := len(ds) - 1; i >= 1; i-- {
		order = append(order, i, i)
	}
	for _, i := range order {
		if _, err := img.Insert(ds[i].pos, ds[i].data); err != nil {
			t.Fatal(err)
		}
	}
	if rd.poll(t) != 0 {
		t.Fatal("the reader got data although the first datagram is missing")
	}
	img.Insert(ds[0].pos, ds[0].data)
	img.Insert(ds[0].pos, ds[0].data)
	rd.drain(t)
	next := make([]int, 1)
	for _, m := range rd.msgs {
		verifyMsg(t, m, next)
	}
	if next[0] != 60 {
		t.Fatalf("delivered %d of 60 messages", next[0])
	}
}

func TestInsertRejectsBadFrames(t *testing.T) {
	pub, img := newPair(t, 4096, 256)
	offer(t, pub, []byte("hello"), noLimit)
	good, _ := pub.Scan(0, 256)
	good = append([]byte(nil), good...)

	check := func(name string, pos int64, data []byte) {
		t.Helper()
		before := img.ContiguousEnd(0, 1<<20)
		if _, err := img.Insert(pos, data); !errors.Is(err, ErrBadFrame) {
			t.Errorf("%s: got %v, want ErrBadFrame", name, err)
		}
		if img.ContiguousEnd(0, 1<<20) != before {
			t.Errorf("%s: a rejected datagram left a frame behind", name)
		}
	}
	check("misaligned position", 8, good)
	check("truncated header", 0, good[:20])
	check("truncated payload", 0, good[:HeaderLength+2])
	bad := append([]byte(nil), good...)
	bad[0], bad[1], bad[2], bad[3] = 8, 0, 0, 0 // length below a header
	check("length below the header size", 0, bad)
	bad = append([]byte(nil), good...)
	bad[20]++ // term id
	check("wrong term id", 0, bad)
	check("header says another offset", 64, good)
	bad = append([]byte(nil), good...)
	bad[0], bad[1], bad[2], bad[3] = 0, 0x10, 0, 0 // 4096 bytes: past the end of the term from offset 0 + more than present
	check("length beyond the data", 0, bad)
	if _, err := img.Insert(0, good); err != nil {
		t.Fatalf("the good frame was rejected: %v", err)
	}
	if got := img.ContiguousEnd(0, 1<<20); got != 64 {
		t.Fatalf("contiguous end %d, want 64", got)
	}
}

// A padding frame arrives as just its header; the receiver's reader must skip the
// whole padded space, including across the term boundary.
func TestInsertPaddingFrame(t *testing.T) {
	pub, img := newPair(t, 4096, 256)
	rd := newReader(img)
	for i := 0; i < 15; i++ {
		offer(t, pub, make([]byte, 200), noLimit) // 232 -> 256 each: 3840
	}
	offer(t, pub, make([]byte, 100), noLimit) // 160: 4000, leaves 96
	offer(t, pub, make([]byte, 150), noLimit) // 192 > 96 left: pads 96, goes to term 1
	var padHeaderOnly bool
	for _, d := range datagrams(pub, 0, 1<<12) {
		if len(d.data) == HeaderLength && int(binary.LittleEndian.Uint32(d.data[0:4])) == 96 {
			padHeaderOnly = true
		}
		if _, err := img.Insert(d.pos, d.data); err != nil {
			t.Fatal(err)
		}
	}
	if !padHeaderOnly {
		t.Fatal("the padding frame did not travel as a header with the full padded length")
	}
	rd.drain(t)
	if rd.pos != pub.Position() {
		t.Fatalf("image reader at %d, publisher at %d", rd.pos, pub.Position())
	}
	if len(rd.msgs) != 17 {
		t.Fatalf("got %d messages, want 17", len(rd.msgs))
	}
}

func TestContiguousEndAndNextFrameAfter(t *testing.T) {
	pub, img := newPair(t, 1<<14, 256)
	for i := 0; i < 8; i++ {
		offer(t, pub, make([]byte, 100), noLimit) // 160 bytes each
	}
	ds := datagrams(pub, 0, 160)
	if len(ds) != 8 {
		t.Fatalf("%d datagrams", len(ds))
	}
	for _, i := range []int{0, 1, 3, 4, 7} {
		img.Insert(ds[i].pos, ds[i].data)
	}
	if got := img.ContiguousEnd(0, 8*160); got != 2*160 {
		t.Fatalf("contiguous end %d, want %d", got, 2*160)
	}
	if got := img.NextFrameAfter(2*160, 8*160); got != 3*160 {
		t.Fatalf("next frame after the first gap at %d, want %d", got, 3*160)
	}
	if got := img.ContiguousEnd(0, 3*160); got != 2*160 {
		t.Fatalf("contiguous end with a limit: %d", got)
	}
	img.Insert(ds[2].pos, ds[2].data)
	if got := img.ContiguousEnd(0, 8*160); got != 5*160 {
		t.Fatalf("after filling the gap: %d, want %d", got, 5*160)
	}
	if got := img.NextFrameAfter(5*160, 8*160); got != 7*160 {
		t.Fatalf("next frame after the second gap at %d, want %d", got, 7*160)
	}
	if got := img.NextFrameAfter(5*160, 6*160); got != 6*160 {
		t.Fatalf("with no frame before the limit it should return the limit, got %d", got)
	}
}

func TestCleanTerm(t *testing.T) {
	l := newLog(t, 4096, 256)
	l.Offer([]byte("x"), noLimit)
	l.CleanTerm(0)
	if _, n, _ := l.Poll(0, func([]byte, FrameInfo) {}, 10); n != 0 {
		t.Fatal("a cleaned term still holds a frame")
	}
	// (term 3 uses the same partition as term 0)
	l.Offer([]byte("y"), noLimit)
	l.CleanTerm(3)
	if _, n, _ := l.Poll(0, func([]byte, FrameInfo) {}, 10); n != 0 {
		t.Fatal("CleanTerm(3) did not clean the partition term 0 uses")
	}
}
