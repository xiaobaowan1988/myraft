package counters

import (
	"strings"
	"sync"
	"testing"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

func newMgr(t *testing.T, n int) (*Manager, *Reader) {
	t.Helper()
	ml, vl := BufferLengths(n)
	meta, vals := shm.Wrap(shm.Heap(ml)), shm.Wrap(shm.Heap(vl))
	m, err := NewManager(meta, vals)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := NewReader(meta, vals)
	return m, r
}

func TestAllocateReadFree(t *testing.T) {
	m, r := newMgr(t, 4)
	a, _ := m.Allocate(1, 100, "pub-pos")
	b, _ := m.Allocate(2, 200, "pub-lmt")
	m.Counter(a).Set(10)
	m.Counter(b).Add(5)
	if r.Counter(a).Get() != 10 || r.Counter(b).Get() != 5 {
		t.Fatal("reader does not see the writer's values")
	}
	var infos []Info
	r.ForEach(func(i Info) { infos = append(infos, i) })
	if len(infos) != 2 || infos[0].Label != "pub-pos" || infos[1].RegistrationID != 200 || infos[1].Value != 5 {
		t.Fatalf("ForEach gave %+v", infos)
	}
	if !r.Allocated(a, 100) || r.Allocated(a, 999) || r.Allocated(99, 100) {
		t.Fatal("Allocated gave a wrong answer")
	}
	m.Free(a)
	if r.Allocated(a, 100) {
		t.Fatal("a freed counter still looks allocated")
	}
	c, _ := m.Allocate(3, 300, "reused")
	if c != a || r.Counter(c).Get() != 0 || r.Allocated(a, 100) || !r.Allocated(a, 300) {
		t.Fatalf("slot %d was not reused cleanly (value %d)", c, r.Counter(c).Get())
	}
}

func TestFullAndLongLabel(t *testing.T) {
	m, r := newMgr(t, 2)
	m.Allocate(1, 1, strings.Repeat("x", 500))
	m.Allocate(1, 2, "b")
	if _, err := m.Allocate(1, 3, "c"); err != ErrFull {
		t.Fatalf("got %v, want ErrFull", err)
	}
	r.ForEach(func(i Info) {
		if i.RegistrationID == 1 && len(i.Label) != MaxLabel {
			t.Fatalf("label length %d, want it cut to %d", len(i.Label), MaxLabel)
		}
	})
}

func TestMismatchedAreasRejected(t *testing.T) {
	ml, _ := BufferLengths(4)
	_, vl := BufferLengths(3)
	if _, err := NewManager(shm.Wrap(shm.Heap(ml)), shm.Wrap(shm.Heap(vl))); err == nil {
		t.Fatal("mismatched areas accepted")
	}
}

func TestConcurrentAdd(t *testing.T) {
	m, _ := newMgr(t, 1)
	id, _ := m.Allocate(1, 1, "n")
	c := m.Counter(id)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				c.Add(1)
			}
		}()
	}
	wg.Wait()
	if c.Get() != 80000 {
		t.Fatalf("counter is %d, want 80000", c.Get())
	}
}
