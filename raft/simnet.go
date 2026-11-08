package raft

import (
	"container/heap"
	"math/rand"
	"sync"
	"time"
)

// SimStreamBuf bounds the messages in flight per (from, to) pair, like etcd's
// streamBufSize: past it Send drops the message and reports the peer unreachable.
var SimStreamBuf = 4096

// SimNet is an in-process message network with fault injection: random loss,
// duplication, random delay (hence reordering), partitions and node outages.
//
// It is a Transport reference implementation. Send is non-blocking and starts no
// goroutine: messages go into a heap ordered by delivery time, and ONE scheduler
// goroutine for the whole network hands them to the destination's Step.
type SimNet struct {
	mu      sync.Mutex
	rng     *rand.Rand
	eps     []Endpoint
	up      []bool
	group   []int    // nodes can talk iff same group
	queued  [][]int  // [from][to] messages in flight
	unreach [][]bool // [from][to] an unreachable report is already pending

	drop, dup float64
	maxDelay  time.Duration
	longRate  float64
	longDelay time.Duration

	q    simHeap
	seq  uint64
	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

type simItem struct {
	at       time.Time
	seq      uint64
	unreach  bool // report from->to as unreachable instead of delivering msg
	from, to int
	msg      Message
}

type simHeap []simItem

func (h simHeap) Len() int { return len(h) }
func (h simHeap) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}
func (h simHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *simHeap) Push(x any)   { *h = append(*h, x.(simItem)) }
func (h *simHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func NewSimNet(n int, seed int64) *SimNet {
	s := &SimNet{
		rng:     rand.New(rand.NewSource(seed)),
		eps:     make([]Endpoint, n),
		up:      make([]bool, n),
		group:   make([]int, n),
		queued:  make([][]int, n),
		unreach: make([][]bool, n),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	for i := range s.up {
		s.up[i] = true
		s.queued[i] = make([]int, n)
		s.unreach[i] = make([]bool, n)
	}
	go s.run()
	return s
}

// Close stops the scheduler goroutine. Undelivered messages are discarded.
func (s *SimNet) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// Register installs (or replaces, after a restart) node id's endpoint.
func (s *SimNet) Register(id int, ep Endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eps[id] = ep
}

// SetUp turns a node's network presence on or off (crash/restart).
func (s *SimNet) SetUp(id int, up bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.up[id] = up
}

// Partition splits the cluster; nodes in no listed group are isolated together.
func (s *SimNet) Partition(groups ...[]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.group {
		s.group[i] = 0
	}
	for g, members := range groups {
		for _, id := range members {
			s.group[id] = g + 1
		}
	}
}

func (s *SimNet) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.group {
		s.group[i] = 0
	}
}

// SetChaos configures loss probability, max regular delay, and the
// probability/length of occasional long delays.
func (s *SimNet) SetChaos(drop float64, maxDelay time.Duration, longRate float64, longDelay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drop, s.maxDelay, s.longRate, s.longDelay = drop, maxDelay, longRate, longDelay
}

// SetDup sets the probability that a sent message is delivered twice.
func (s *SimNet) SetDup(p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dup = p
}

// Endpoint returns the Transport used by node from.
func (s *SimNet) Endpoint(from int) Transport { return &simTransport{s: s, from: from} }

type simTransport struct {
	s    *SimNet
	from int
}

func (t *simTransport) Send(m Message) { t.s.send(t.from, m) }

func (s *SimNet) reachableLocked(a, b int) bool {
	return s.up[a] && s.up[b] && s.group[a] == s.group[b]
}

func (s *SimNet) delayLocked() time.Duration {
	var d time.Duration
	if s.maxDelay > 0 {
		d = time.Duration(s.rng.Int63n(int64(s.maxDelay)))
	}
	if s.longRate > 0 && s.rng.Float64() < s.longRate {
		d += time.Duration(s.rng.Int63n(int64(s.longDelay) + 1))
	}
	return d
}

func (s *SimNet) pushLocked(it simItem) {
	it.seq = s.seq
	s.seq++
	heap.Push(&s.q, it)
}

// reportLocked schedules an unreachable report for from->to unless one is pending.
func (s *SimNet) reportLocked(from, to int) {
	if s.unreach[from][to] {
		return
	}
	s.unreach[from][to] = true
	s.pushLocked(simItem{at: time.Now(), unreach: true, from: from, to: to})
}

// send decides the fate of m at the sender, like a stream write: an unreachable
// peer or a full queue is an error reported back; random loss is silent.
func (s *SimNet) send(from int, m Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	to := m.To
	if to < 0 || to >= len(s.eps) || to == from || !s.up[from] {
		return
	}
	if !s.reachableLocked(from, to) || s.queued[from][to] >= SimStreamBuf {
		s.reportLocked(from, to)
		s.wakeLocked()
		return
	}
	if s.rng.Float64() < s.drop {
		return
	}
	copies := 1
	if s.dup > 0 && s.rng.Float64() < s.dup {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		s.queued[from][to]++
		s.pushLocked(simItem{at: time.Now().Add(s.delayLocked()), from: from, to: to, msg: m})
	}
	s.wakeLocked()
}

func (s *SimNet) wakeLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run is the single scheduler goroutine.
func (s *SimNet) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		now := time.Now()
		var due []simItem
		for len(s.q) > 0 && !s.q[0].at.After(now) {
			due = append(due, heap.Pop(&s.q).(simItem))
		}
		wait := time.Duration(-1)
		if len(s.q) > 0 {
			wait = s.q[0].at.Sub(now)
		}
		s.mu.Unlock()

		for _, it := range due {
			s.deliver(it)
		}
		if len(due) > 0 {
			continue
		}
		if wait < 0 {
			select {
			case <-s.wake:
			case <-s.stop:
				return
			}
			continue
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-s.wake:
			t.Stop()
		case <-s.stop:
			t.Stop()
			return
		}
	}
}

// deliver hands one due item to its endpoint, outside the lock (Step may block
// until the destination's event loop takes the message).
func (s *SimNet) deliver(it simItem) {
	s.mu.Lock()
	var ep Endpoint
	ok := false
	if it.unreach {
		s.unreach[it.from][it.to] = false
		ep, ok = s.eps[it.from], s.eps[it.from] != nil
	} else {
		s.queued[it.from][it.to]--
		ep = s.eps[it.to]
		// A message already sent is not recalled if its sender crashes meanwhile.
		ok = ep != nil && s.up[it.to] && s.group[it.from] == s.group[it.to]
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	if it.unreach {
		ep.ReportUnreachable(it.to)
		return
	}
	ep.Step(it.msg)
}
