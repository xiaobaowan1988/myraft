package raft

import (
	"math/rand"
	"sync"
	"time"
)

// SimNet is an in-process network with fault injection: random loss, random
// delay, partitions and node outages. Requests and replies are each subject
// to loss/delay independently.
type SimNet struct {
	mu       sync.Mutex
	rng      *rand.Rand
	handlers []Handler
	up       []bool
	group    []int // nodes can talk iff same group

	drop      float64
	maxDelay  time.Duration
	longRate  float64
	longDelay time.Duration
}

func NewSimNet(n int, seed int64) *SimNet {
	s := &SimNet{
		rng:      rand.New(rand.NewSource(seed)),
		handlers: make([]Handler, n),
		up:       make([]bool, n),
		group:    make([]int, n),
	}
	for i := range s.up {
		s.up[i] = true
	}
	return s
}

// Register installs (or replaces, after a restart) node id's handler.
func (s *SimNet) Register(id int, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[id] = h
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

// Endpoint returns the Transport used by node from.
func (s *SimNet) Endpoint(from int) Transport { return &endpoint{s: s, from: from} }

func (s *SimNet) reachableLocked(a, b int) bool {
	return s.up[a] && s.up[b] && s.group[a] == s.group[b]
}

// pass reports whether a packet a->b gets through right now.
func (s *SimNet) pass(a, b int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reachableLocked(a, b) && s.rng.Float64() >= s.drop
}

func (s *SimNet) delay() {
	s.mu.Lock()
	var d time.Duration
	if s.maxDelay > 0 {
		d = time.Duration(s.rng.Int63n(int64(s.maxDelay)))
	}
	if s.longRate > 0 && s.rng.Float64() < s.longRate {
		d += time.Duration(s.rng.Int63n(int64(s.longDelay) + 1))
	}
	s.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
}

func (s *SimNet) handler(a, b int) (Handler, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.handlers[b]
	return h, h != nil && s.reachableLocked(a, b)
}

// roundTrip models request travel, remote handling, and reply travel.
func (s *SimNet) roundTrip(from, to int, call func(Handler) interface{}) (interface{}, bool) {
	if !s.pass(from, to) {
		return nil, false
	}
	s.delay()
	h, ok := s.handler(from, to)
	if !ok {
		return nil, false
	}
	res := call(h)
	if !s.pass(to, from) {
		return nil, false
	}
	s.delay()
	s.mu.Lock()
	ok = s.reachableLocked(to, from)
	s.mu.Unlock()
	return res, ok
}

type endpoint struct {
	s    *SimNet
	from int
}

func (e *endpoint) RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	res, ok := e.s.roundTrip(e.from, to, func(h Handler) interface{} {
		a := *args
		return h.RequestVote(&a)
	})
	if !ok {
		return nil, false
	}
	return res.(*RequestVoteReply), true
}

func (e *endpoint) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	res, ok := e.s.roundTrip(e.from, to, func(h Handler) interface{} {
		a := *args
		a.Entries = append([]LogEntry(nil), args.Entries...)
		return h.AppendEntries(&a)
	})
	if !ok {
		return nil, false
	}
	return res.(*AppendEntriesReply), true
}
