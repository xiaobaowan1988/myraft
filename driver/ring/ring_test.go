package ring

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

func newRing(t testing.TB, capacity int) (*ManyToOne, shm.Buffer) {
	t.Helper()
	b := shm.Wrap(shm.Heap(capacity + TrailerLength))
	r, err := NewManyToOne(b)
	if err != nil {
		t.Fatal(err)
	}
	return r, b
}

func TestManyToOneRejectsBadCapacity(t *testing.T) {
	if _, err := NewManyToOne(shm.Wrap(shm.Heap(1000 + TrailerLength))); err == nil {
		t.Fatal("a capacity that is not a power of two was accepted")
	}
	if _, err := NewManyToOne(shm.Wrap(shm.Heap(TrailerLength))); err == nil {
		t.Fatal("a zero capacity was accepted")
	}
}

func TestManyToOneRoundTripAndFull(t *testing.T) {
	r, _ := newRing(t, 1024)
	var got []string
	read := func() int {
		return r.Read(func(typ int32, p []byte) { got = append(got, fmt.Sprintf("%d:%s", typ, p)) }, 100)
	}
	if read() != 0 {
		t.Fatal("read from an empty ring returned something")
	}
	for i := 0; i < 5; i++ {
		if !r.Write(int32(i+1), []byte(fmt.Sprintf("m%d", i))) {
			t.Fatal("write to an empty ring failed")
		}
	}
	if read() != 5 || fmt.Sprint(got) != "[1:m0 2:m1 3:m2 4:m3 5:m4]" {
		t.Fatalf("got %v", got)
	}
	if r.Size() != 0 {
		t.Fatalf("ring size %d after draining", r.Size())
	}
	// Fill it: a full ring refuses instead of overwriting.
	payload := make([]byte, 100)
	n := 0
	for r.Write(1, payload) {
		n++
	}
	if n == 0 || n*align(100+recordHeaderLen) > 1024 {
		t.Fatalf("wrote %d records of 108 bytes into a 1024-byte ring", n)
	}
	got = nil
	if c := read(); c != n {
		t.Fatalf("read %d of %d records", c, n)
	}
	if !r.Write(1, payload) {
		t.Fatal("ring did not free its space after the read")
	}
}

func TestManyToOneRejectsOversizeAndBadType(t *testing.T) {
	r, _ := newRing(t, 1024)
	mustPanic := func(what string, f func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", what)
			}
		}()
		f()
	}
	mustPanic("oversize", func() { r.Write(1, make([]byte, r.MaxMessageLength()+1)) })
	mustPanic("type 0", func() { r.Write(0, nil) })
	if !r.Write(1, make([]byte, r.MaxMessageLength())) {
		t.Fatal("a maximum-size message was refused")
	}
}

// Records that do not fit before the end of the data area wrap via a padding record;
// run the ring around many times with varying sizes and check content and order.
func TestManyToOneWrapAround(t *testing.T) {
	r, _ := newRing(t, 512)
	next, want := 0, 0
	for round := 0; round < 3000; round++ {
		size := 4 + (round*37)%(r.MaxMessageLength()-3)
		p := make([]byte, size)
		binary.BigEndian.PutUint32(p[:4], uint32(next))
		for i := 4; i < size; i++ {
			p[i] = byte(next)
		}
		if !r.Write(int32(1+size%5), p) {
			// full: drain, then it must succeed
			r.Read(func(typ int32, q []byte) { checkRecord(t, q, &want) }, 1000)
			if !r.Write(int32(1+size%5), p) {
				t.Fatalf("write of %d bytes failed on an empty ring", size)
			}
		}
		next++
		if round%3 == 0 {
			r.Read(func(typ int32, q []byte) { checkRecord(t, q, &want) }, 2)
		}
	}
	r.Read(func(typ int32, q []byte) { checkRecord(t, q, &want) }, 1<<30)
	if want != next {
		t.Fatalf("read %d records, wrote %d", want, next)
	}
}

func checkRecord(t testing.TB, q []byte, want *int) {
	t.Helper()
	if len(q) >= 4 {
		if got := int(binary.BigEndian.Uint32(q[:4])); got != *want {
			t.Fatalf("record %d came out as %d", *want, got)
		}
		for i := 4; i < len(q); i++ {
			if q[i] != byte(*want) {
				t.Fatalf("record %d has a corrupt byte at %d", *want, i)
			}
		}
	}
	*want++
}

// Many producers, one consumer, a small ring so it wraps and fills constantly.
// Each message carries (producer, sequence, checksum); per producer the order must
// be preserved and nothing lost or duplicated.
func TestManyToOneConcurrentProducers(t *testing.T) {
	const producers, perProducer = 8, 20000
	r, _ := newRing(t, 4096)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for s := 0; s < perProducer; s++ {
				msg := makeMsg(p, s)
				for !r.Write(1, msg) {
					time.Sleep(time.Microsecond)
				}
			}
		}(p)
	}
	next := make([]int, producers)
	total := 0
	deadline := time.Now().Add(60 * time.Second)
	for total < producers*perProducer {
		n := r.Read(func(_ int32, m []byte) { verifyMsg(t, m, next) }, 256)
		total += n
		if n == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("stuck after %d of %d messages", total, producers*perProducer)
			}
			time.Sleep(10 * time.Microsecond)
		}
	}
	wg.Wait()
	for p, n := range next {
		if n != perProducer {
			t.Errorf("producer %d: %d messages arrived, want %d", p, n, perProducer)
		}
	}
}

func makeMsg(p, s int) []byte {
	size := 16 + (p*7+s)%80
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
	p, s, size := int(binary.BigEndian.Uint32(m[0:])), int(binary.BigEndian.Uint32(m[4:])), int(binary.BigEndian.Uint32(m[8:]))
	if len(m) != size || p >= len(next) {
		t.Fatalf("garbled message: len %d, header says %d, producer %d", len(m), size, p)
	}
	if s != next[p] {
		t.Fatalf("producer %d: got sequence %d, want %d", p, s, next[p])
	}
	for i := 12; i < size; i++ {
		if m[i] != byte(p+s+i) {
			t.Fatalf("producer %d seq %d: corrupt byte %d", p, s, i)
		}
	}
	next[p]++
}

func TestCorrelationIDsAreUnique(t *testing.T) {
	r, _ := newRing(t, 1024)
	const goroutines, each = 8, 5000
	seen := make([][]int64, goroutines)
	var wg sync.WaitGroup
	for g := range seen {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				seen[g] = append(seen[g], r.NextCorrelationID())
			}
		}(g)
	}
	wg.Wait()
	all := map[int64]bool{}
	for _, s := range seen {
		for _, id := range s {
			if all[id] {
				t.Fatalf("correlation id %d handed out twice", id)
			}
			all[id] = true
		}
	}
}

func TestConsumerHeartbeat(t *testing.T) {
	r, _ := newRing(t, 1024)
	r.SetConsumerHeartbeat(12345)
	if r.ConsumerHeartbeat() != 12345 {
		t.Fatal("heartbeat not stored")
	}
}

// A producer that dies after claiming space leaves a record that never becomes
// readable. Without Unblock the consumer is stuck behind it for ever.
func TestUnblockAfterProducerDiedMidWrite(t *testing.T) {
	for _, stage := range []string{"claimed-with-marker", "claimed-nothing-written"} {
		t.Run(stage, func(t *testing.T) {
			r, b := newRing(t, 1024)
			var got []string
			read := func() int {
				return r.Read(func(_ int32, p []byte) { got = append(got, string(p)) }, 100)
			}
			r.Write(1, []byte("before"))
			idx := r.claim(20 + recordHeaderLen) // the "dead" producer's space
			if stage == "claimed-with-marker" {
				b.PutInt32Ordered(idx, -int32(20+recordHeaderLen))
			}
			r.Write(1, []byte("after")) // a live producer behind it
			if read() != 1 || got[0] != "before" {
				t.Fatalf("got %v", got)
			}
			if read() != 0 {
				t.Fatal("the consumer read past an unpublished record")
			}
			if !r.Unblock() {
				t.Fatal("Unblock did nothing")
			}
			if read() != 1 || got[len(got)-1] != "after" {
				t.Fatalf("after Unblock got %v", got)
			}
			if r.Unblock() {
				t.Fatal("Unblock changed an empty ring")
			}
		})
	}
}

func TestUnblockLeavesLiveRecordsAlone(t *testing.T) {
	r, _ := newRing(t, 1024)
	if r.Unblock() {
		t.Fatal("Unblock on an empty ring reported a change")
	}
	r.Write(1, []byte("x"))
	if r.Unblock() {
		t.Fatal("Unblock touched a published record")
	}
	n := r.Read(func(int32, []byte) {}, 10)
	if n != 1 {
		t.Fatal("the published record was damaged")
	}
}

// Real separate processes writing into one mmap'd file.
func TestMain(m *testing.M) {
	if os.Getenv("RING_ROLE") == "producer" {
		os.Exit(childProducer())
	}
	os.Exit(m.Run())
}

func childProducer() int {
	m, err := shm.Open(os.Getenv("RING_FILE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer m.Close()
	r, err := NewManyToOne(m.Buffer())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	var p, n int
	fmt.Sscan(os.Getenv("RING_ID"), &p)
	fmt.Sscan(os.Getenv("RING_N"), &n)
	for s := 0; s < n; s++ {
		msg := makeMsg(p, s)
		deadline := time.Now().Add(60 * time.Second)
		for !r.Write(1, msg) {
			if time.Now().After(deadline) {
				return 3
			}
			time.Sleep(time.Microsecond)
		}
	}
	return 0
}

func TestManyToOneAcrossProcesses(t *testing.T) {
	const producers, perProducer = 4, 15000
	path := filepath.Join(t.TempDir(), "ring")
	m, err := shm.Create(path, 4096+TrailerLength)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	r, err := NewManyToOne(m.Buffer())
	if err != nil {
		t.Fatal(err)
	}
	var cmds []*exec.Cmd
	for p := 0; p < producers; p++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), "RING_ROLE=producer", "RING_FILE="+path, fmt.Sprintf("RING_ID=%d", p), fmt.Sprintf("RING_N=%d", perProducer))
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	next := make([]int, producers)
	total := 0
	deadline := time.Now().Add(90 * time.Second)
	for total < producers*perProducer {
		n := r.Read(func(_ int32, m []byte) { verifyMsg(t, m, next) }, 256)
		total += n
		if n == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("stuck after %d of %d messages", total, producers*perProducer)
			}
			time.Sleep(10 * time.Microsecond)
		}
	}
	for i, c := range cmds {
		if err := c.Wait(); err != nil {
			t.Errorf("producer process %d: %v", i, err)
		}
	}
	for p, n := range next {
		if n != perProducer {
			t.Errorf("process %d: %d messages arrived, want %d", p, n, perProducer)
		}
	}
}

// A record must become visible only once it is complete: before the publishing
// store the consumer sees nothing, after it the whole record.
func TestRecordIsInvisibleUntilPublished(t *testing.T) {
	r, _ := newRing(t, 1024)
	seen := -1
	testBeforePublish = func() {
		seen = r.Read(func(int32, []byte) {}, 10)
	}
	defer func() { testBeforePublish = nil }()
	r.Write(1, []byte("payload"))
	testBeforePublish = nil
	if seen != 0 {
		t.Fatalf("the consumer saw %d records before the record was published", seen)
	}
	var got string
	if r.Read(func(_ int32, p []byte) { got = string(p) }, 10) != 1 || got != "payload" {
		t.Fatalf("after publishing got %q", got)
	}
}
