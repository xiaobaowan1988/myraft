package ring

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

func newBroadcast(t testing.TB, capacity int) (*Transmitter, shm.Buffer) {
	t.Helper()
	b := shm.Wrap(shm.Heap(capacity + BroadcastTrailerLength))
	tx, err := NewTransmitter(b)
	if err != nil {
		t.Fatal(err)
	}
	return tx, b
}

func TestBroadcastDeliversToEveryReceiverInOrder(t *testing.T) {
	tx, b := newBroadcast(t, 1024)
	var rs []*Receiver
	for i := 0; i < 3; i++ {
		r, err := NewReceiver(b)
		if err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	for i := 0; i < 10; i++ {
		tx.Transmit(int32(1+i%3), []byte(fmt.Sprintf("event-%d", i)))
	}
	for n, r := range rs {
		for i := 0; i < 10; i++ {
			typ, p, ok := r.Receive()
			if !ok || typ != int32(1+i%3) || string(p) != fmt.Sprintf("event-%d", i) {
				t.Fatalf("receiver %d, event %d: got %d %q ok=%v", n, i, typ, p, ok)
			}
		}
		if _, _, ok := r.Receive(); ok {
			t.Fatalf("receiver %d got an extra record", n)
		}
		if r.Lapped() != 0 {
			t.Fatalf("receiver %d was lapped without falling behind", n)
		}
	}
}

func TestBroadcastNewReceiverStartsAtLatest(t *testing.T) {
	tx, b := newBroadcast(t, 1024)
	tx.Transmit(1, []byte("old"))
	tx.Transmit(1, []byte("older"))
	tx.Transmit(1, []byte("latest"))
	r, _ := NewReceiver(b)
	_, p, ok := r.Receive()
	if !ok || string(p) != "latest" {
		t.Fatalf("a new receiver should begin at the latest record, got %q ok=%v", p, ok)
	}
	if _, _, ok := r.Receive(); ok {
		t.Fatal("more records than the latest")
	}
	tx.Transmit(1, []byte("next"))
	if _, p, ok := r.Receive(); !ok || string(p) != "next" {
		t.Fatalf("got %q", p)
	}
}

// A receiver that stops reading while the transmitter goes round the buffer several
// times is lapped: it is told, loses records, and resumes at a record boundary.
func TestBroadcastSlowReceiverIsLapped(t *testing.T) {
	tx, b := newBroadcast(t, 256)
	r, _ := NewReceiver(b)
	for i := 0; i < 200; i++ {
		tx.Transmit(1, []byte(fmt.Sprintf("event-%03d", i)))
	}
	typ, p, ok := r.Receive()
	if !ok || typ != 1 {
		t.Fatal("a lapped receiver got nothing")
	}
	if r.Lapped() == 0 {
		t.Fatal("receiver lost records without being told")
	}
	var n int
	fmt.Sscanf(string(p), "event-%d", &n)
	if n < 100 {
		t.Fatalf("after being lapped it resumed at event %d; expected a recent one", n)
	}
	for {
		_, p, ok := r.Receive()
		if !ok {
			break
		}
		var m int
		fmt.Sscanf(string(p), "event-%d", &m)
		if m != n+1 {
			t.Fatalf("after resync: event %d followed %d", m, n)
		}
		n = m
	}
	if n != 199 {
		t.Fatalf("ended at event %d, want 199", n)
	}
}

// The transmitter never waits. Receivers reading concurrently must never see a torn
// record: anything they return is exactly what was transmitted, in increasing order.
//
// The buffer is an mmap'd file on purpose. A broadcast receiver copies a record while
// the transmitter may be overwriting it and validates afterwards (a seqlock); that is
// a deliberate data race which the race detector would flag on Go heap memory. In
// production the memory is always mmap'd, which the detector does not track, so this
// is also the faithful setup.
func TestBroadcastConcurrentReceiversNeverSeeTornRecords(t *testing.T) {
	m, err := shm.Create(filepath.Join(t.TempDir(), "bc"), 512+BroadcastTrailerLength)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	b := m.Buffer()
	tx, err := NewTransmitter(b)
	if err != nil {
		t.Fatal(err)
	}
	const total = 200000
	var done atomic.Bool
	var wg sync.WaitGroup
	var lappedAny atomic.Int64
	for i := 0; i < 4; i++ {
		r, _ := NewReceiver(b)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			last, got := -1, 0
			for !done.Load() || got == 0 {
				_, p, ok := r.Receive()
				if !ok {
					time.Sleep(time.Microsecond)
					continue
				}
				seq, ok2 := parseEvent(p)
				if !ok2 {
					t.Errorf("receiver %d got a torn record %q", id, p)
					return
				}
				if seq <= last {
					t.Errorf("receiver %d: sequence went %d -> %d", id, last, seq)
					return
				}
				last = seq
				got++
				if done.Load() && last == total-1 {
					break
				}
			}
			lappedAny.Add(r.Lapped())
		}(i)
	}
	for s := 0; s < total; s++ {
		tx.Transmit(1, makeEvent(s))
	}
	done.Store(true)
	wg.Wait()
	t.Logf("receivers were lapped %d times in total", lappedAny.Load())
}

func makeEvent(s int) []byte {
	size := 24 + s%30
	p := make([]byte, size)
	copy(p, fmt.Sprintf("%016d", s))
	for i := 16; i < size; i++ {
		p[i] = byte(s)
	}
	return p
}

func parseEvent(p []byte) (int, bool) {
	if len(p) < 24 {
		return 0, false
	}
	var s int
	if _, err := fmt.Sscanf(string(p[:16]), "%016d", &s); err != nil {
		return 0, false
	}
	if len(p) != 24+s%30 {
		return 0, false
	}
	for i := 16; i < len(p); i++ {
		if p[i] != byte(s) {
			return 0, false
		}
	}
	return s, true
}

func TestBroadcastRejectsBadInput(t *testing.T) {
	if _, err := NewTransmitter(shm.Wrap(shm.Heap(1000 + BroadcastTrailerLength))); err == nil {
		t.Fatal("bad capacity accepted")
	}
	tx, _ := newBroadcast(t, 1024)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("oversize message accepted")
			}
		}()
		tx.Transmit(1, make([]byte, tx.MaxMessageLength()+1))
	}()
}
