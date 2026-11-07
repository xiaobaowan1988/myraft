package raft

// Deterministic simulation: the Raft state machines run on a single goroutine
// under a virtual clock. Message loss, duplication, delay/reordering,
// partitions, crashes and client proposals are all drawn from ONE seeded RNG,
// so a (seed -> execution) mapping is exact and a failing seed replays
// bit-for-bit. Nothing here iterates over a map in a way that affects
// behaviour.

import (
	"container/heap"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand"
	"strings"
	"testing"
	"time"
)

var (
	flagSeed  = flag.Int64("seed", 0, "run only this seed of TestDeterministicReplay (0 = sweep)")
	flagSeeds = flag.Int("seeds", 100, "number of seeds in the sweep")
	flagTrace = flag.Bool("trace", false, "dump the full event trace of the -seed run")
)

// ---- event queue ----

type simEvent struct {
	at   time.Duration
	seq  uint64
	desc string
	fn   func()
}

type eventHeap []*simEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*simEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// ---- simulator ----

type detSim struct {
	seed int64
	rng  *rand.Rand
	n    int
	now  time.Duration
	base time.Time
	q    eventHeap
	seq  uint64

	nodes  []*core
	stores []*MemStorage
	up     []bool
	epoch  []int // bumped on crash/restart; invalidates in-flight events
	group  []int // nodes talk iff same group

	drop, dup float64
	crashRate float64 // per non-empty flush, at each of the two persistence-boundary crash points
	maxDelay  time.Duration
	longRate  float64
	longDelay time.Duration

	h        hash.Hash64
	recent   []string
	trace    []string
	dumpAll  bool
	events   int
	failures []string

	// invariant bookkeeping
	leaderOf    map[int]int // term -> leader id
	committed   []LogEntry  // index -> first entry seen committed (Term 0 = unknown)
	commitTerm  []int       // index -> term in which it was first committed (the committing leader's term)
	seenCommit  []int
	maxTerm     []int
	applied     []map[int]interface{}
	appliedAny  map[int]interface{}
	expectApply []int
	nextCmd     int
	slotAt      [][]time.Duration // [leader][peer]: when the current in-flight AppendEntries was sent
	slotSeq     [][]uint64
}

func newDetSim(seed int64, dump bool) *detSim {
	const n = 5
	s := &detSim{
		seed: seed, rng: rand.New(rand.NewSource(seed)), n: n,
		base:   time.Unix(1_700_000_000, 0),
		nodes:  make([]*core, n),
		stores: make([]*MemStorage, n),
		up:     make([]bool, n), epoch: make([]int, n), group: make([]int, n),
		h: fnv.New64a(), dumpAll: dump,
		leaderOf: map[int]int{}, seenCommit: make([]int, n), maxTerm: make([]int, n),
		applied: make([]map[int]interface{}, n), appliedAny: map[int]interface{}{},
		expectApply: make([]int, n), nextCmd: 1000,
		slotAt: make([][]time.Duration, n), slotSeq: make([][]uint64, n),
	}
	for i := 0; i < n; i++ {
		s.slotAt[i], s.slotSeq[i] = make([]time.Duration, n), make([]uint64, n)
		s.stores[i] = &MemStorage{}
		s.applied[i] = map[int]interface{}{}
		s.boot(i)
	}
	return s
}

func (s *detSim) fail(format string, args ...any) {
	if len(s.failures) < 10 {
		s.failures = append(s.failures, fmt.Sprintf("[t=%v ev=%d] ", s.now, s.events)+fmt.Sprintf(format, args...))
	}
}

func (s *detSim) after(d time.Duration, desc string, fn func()) {
	heap.Push(&s.q, &simEvent{at: s.now + d, seq: s.seq, desc: desc, fn: fn})
	s.seq++
}

func (s *detSim) reach(a, b int) bool { return s.up[a] && s.up[b] && s.group[a] == s.group[b] }

// boot (re)starts node i from its stored state.
func (s *detSim) boot(i int) {
	s.epoch[i]++
	ep := s.epoch[i]
	r, err := restoreCore(i, s.n, s.stores[i], s.seed*7919+int64(i)*131+int64(ep), func() time.Time { return s.base.Add(s.now) })
	if err != nil {
		s.fail("boot %d: %v", i, err)
		return
	}
	s.nodes[i], s.up[i] = r, true
	s.expectApply[i], s.seenCommit[i] = 1, 0
	s.tickLoop(i, ep)
}

func (s *detSim) crash(i int) {
	s.up[i] = false
	s.epoch[i]++
}

func (s *detSim) tickLoop(i, ep int) {
	s.after(tickInterval, fmt.Sprintf("tick %d", i), func() {
		if s.epoch[i] != ep || !s.up[i] {
			return
		}
		s.nodes[i].onTick()
		if !s.flush(i) {
			return
		}
		s.tickLoop(i, ep)
	})
}

func (s *detSim) delay() time.Duration {
	var d time.Duration
	if s.maxDelay > 0 {
		d = time.Duration(s.rng.Int63n(int64(s.maxDelay)))
	}
	if s.longRate > 0 && s.rng.Float64() < s.longRate {
		d += time.Duration(s.rng.Int63n(int64(s.longDelay) + 1))
	}
	return d
}

// rpc models one request/reply exchange. handle runs on the destination when
// the request arrives and returns the continuation to run on the source when
// the reply arrives. Requests may be lost, delayed, reordered or duplicated;
// so may replies. Like the real shell, the caller gives up after RPCTimeout: a
// reply that arrives later is discarded (the request itself may still be
// processed by the destination). If onLost is non-nil it fires at the timeout,
// mirroring Raft.sendAppend's loss event.
func (s *detSim) rpc(from, to int, name string, handle func(dst *core) func(src *core), onLost func(src *core), canDup bool) {
	fromEpoch := s.epoch[from]
	deadline := s.now + RPCTimeout
	if onLost != nil {
		s.after(RPCTimeout, fmt.Sprintf("%s timeout %d->%d", name, from, to), func() {
			if s.epoch[from] != fromEpoch || !s.up[from] {
				return
			}
			onLost(s.nodes[from])
			s.flush(from)
		})
	}
	if !s.reach(from, to) || s.rng.Float64() < s.drop {
		return
	}
	if canDup && s.rng.Float64() < s.dup {
		s.rpc(from, to, name, handle, nil, false) // a second copy of the same request
	}
	s.after(s.delay(), fmt.Sprintf("%s req %d->%d", name, from, to), func() {
		if !s.reach(from, to) {
			return
		}
		onReply := handle(s.nodes[to])
		if !s.flush(to) {
			return // crashed at a persistence boundary: the reply never left
		}
		if s.rng.Float64() < s.drop {
			return
		}
		deliverReply := func() {
			if s.now > deadline || s.epoch[from] != fromEpoch || !s.reach(to, from) {
				return // caller already gave up, or crashed, or unreachable
			}
			onReply(s.nodes[from])
			s.flush(from)
		}
		if canDup && s.rng.Float64() < s.dup {
			s.after(s.delay(), fmt.Sprintf("%s rep(dup) %d->%d", name, to, from), deliverReply)
		}
		s.after(s.delay(), fmt.Sprintf("%s rep %d->%d", name, to, from), deliverReply)
	})
}

// flush carries out node i's Ready in the same order as Raft.flush:
// persist -> send -> apply. Crash points are injected at the two persistence
// boundaries; it returns false if the node crashed (the rest of the Ready is
// then lost, exactly like a real process death).
func (s *detSim) flush(i int) bool {
	c := s.nodes[i]
	rd := c.takeReady()
	if rd.empty() {
		return true
	}
	if s.crashAt(i, "before persist") {
		return false // nothing durable, nothing sent, no reply
	}
	if len(rd.Persist) > 0 {
		if err := s.stores[i].Append(rd.Persist); err != nil {
			s.fail("node %d persist: %v", i, err)
		}
	}
	if s.crashAt(i, "after persist, before send") {
		return false // durable, but requests/replies/apply never left
	}
	for _, v := range rd.Votes {
		v := v
		s.rpc(i, v.peer, "vote", func(dst *core) func(*core) {
			reply := &RequestVoteReply{}
			dst.handleRequestVote(v.args, reply)
			return func(src *core) { src.onVoteReply(v.peer, v.args.Term, reply) }
		}, nil, true)
	}
	for _, a := range rd.Appends {
		a := a
		s.slotAt[i][a.peer], s.slotSeq[i][a.peer] = s.now, a.seq
		s.rpc(i, a.peer, "append", func(dst *core) func(*core) {
			reply := &AppendEntriesReply{}
			dst.handleAppendEntries(a.args, reply)
			return func(src *core) { src.onAppendReply(a.peer, a.ni, a.seq, a.args, reply) }
		}, func(src *core) { src.onAppendLost(a.peer, a.seq) }, true)
	}
	s.onApply(i, rd.Apply)
	return true
}

func (s *detSim) crashAt(i int, where string) bool {
	if s.crashRate > 0 && s.up[i] && s.downCount() < 2 && s.rng.Float64() < s.crashRate {
		s.crash(i)
		s.trace1(fmt.Sprintf("FAULT crash %d %s", i, where))
		return true
	}
	return false
}

func (s *detSim) onApply(i int, msgs []ApplyMsg) {
	for _, m := range msgs {
		if m.CommandIndex != s.expectApply[i] {
			s.fail("node %d applied index %d, expected %d", i, m.CommandIndex, s.expectApply[i])
		}
		s.expectApply[i] = m.CommandIndex + 1
		if !m.CommandValid {
			continue
		}
		if prev, ok := s.applied[i][m.CommandIndex]; ok && prev != m.Command {
			s.fail("node %d re-applied index %d as %v, was %v", i, m.CommandIndex, m.Command, prev)
		}
		if prev, ok := s.appliedAny[m.CommandIndex]; ok && prev != m.Command {
			s.fail("DIVERGENCE: index %d applied as %v on node %d but %v elsewhere", m.CommandIndex, m.Command, i, prev)
		}
		s.applied[i][m.CommandIndex] = m.Command
		s.appliedAny[m.CommandIndex] = m.Command
	}
}

// ---- workload and faults ----

func (s *detSim) clientLoop(until time.Duration) {
	s.after(25*time.Millisecond, "client", func() {
		if s.now > until {
			return
		}
		if i := s.rng.Intn(s.n); s.up[i] {
			if _, _, ok := s.nodes[i].propose(s.nextCmd); ok {
				s.nextCmd++
			}
			s.flush(i)
		}
		s.clientLoop(until)
	})
}

func (s *detSim) faultLoop(until time.Duration) {
	s.after(time.Duration(200+s.rng.Intn(400))*time.Millisecond, "fault", func() {
		if s.now >= until {
			return
		}
		s.fault()
		s.faultLoop(until)
	})
}

func (s *detSim) downCount() int {
	c := 0
	for _, u := range s.up {
		if !u {
			c++
		}
	}
	return c
}

func (s *detSim) fault() {
	switch s.rng.Intn(4) {
	case 0: // random 2|3 partition
		perm := s.rng.Perm(s.n)
		for i := range s.group {
			s.group[i] = 0
		}
		s.group[perm[0]], s.group[perm[1]] = 1, 1
		s.trace1(fmt.Sprintf("FAULT partition {%d,%d}", perm[0], perm[1]))
	case 1:
		for i := range s.group {
			s.group[i] = 0
		}
		s.trace1("FAULT heal")
	case 2: // crash, keeping a majority alive
		if i := s.rng.Intn(s.n); s.up[i] && s.downCount() < 2 {
			s.crash(i)
			s.trace1(fmt.Sprintf("FAULT crash %d", i))
		}
	case 3:
		for i := range s.up {
			if !s.up[i] {
				s.boot(i)
				s.trace1(fmt.Sprintf("FAULT restart %d", i))
				break
			}
		}
	}
}

func (s *detSim) healAll() {
	for i := range s.group {
		s.group[i] = 0
	}
	s.drop, s.dup, s.maxDelay, s.longRate, s.crashRate = 0, 0, 5*time.Millisecond, 0, 0
	for i := range s.up {
		if !s.up[i] {
			s.boot(i)
		}
	}
	s.trace1("FAULT heal-all")
}

// ---- running and tracing ----

func (s *detSim) trace1(line string) {
	fmt.Fprintf(s.h, "%d|%s\n", s.now, line)
	if s.dumpAll {
		s.trace = append(s.trace, fmt.Sprintf("%12v %s", s.now, line))
	}
	s.recent = append(s.recent, fmt.Sprintf("%12v %s", s.now, line))
	if len(s.recent) > 40 {
		s.recent = s.recent[1:]
	}
}

func (s *detSim) runUntil(end time.Duration) {
	for len(s.q) > 0 && s.q[0].at <= end {
		ev := heap.Pop(&s.q).(*simEvent)
		s.now = ev.at
		s.events++
		if !strings.HasPrefix(ev.desc, "tick") {
			s.trace1(ev.desc)
		}
		ev.fn()
		s.checkLight()
		if s.events%50 == 0 {
			s.checkHeavy()
		}
		if len(s.failures) > 0 {
			return
		}
	}
	s.now = end
}

// checkLight runs after every event.
func (s *detSim) checkLight() {
	for i := 0; i < s.n; i++ {
		if !s.up[i] {
			continue
		}
		r := s.nodes[i]
		if r.currentTerm < s.maxTerm[i] {
			s.fail("node %d term went back %d -> %d", i, s.maxTerm[i], r.currentTerm)
		}
		s.maxTerm[i] = r.currentTerm
		if r.commitIndex >= len(r.log) {
			s.fail("node %d commitIndex %d >= log length %d", i, r.commitIndex, len(r.log))
			continue
		}
		if r.commitIndex < s.seenCommit[i] {
			s.fail("node %d commitIndex went back %d -> %d", i, s.seenCommit[i], r.commitIndex)
		}
		if r.state == Leader {
			// An in-flight slot must be freed by a reply or the timeout event; one
			// that outlives RPCTimeout would starve that follower of heartbeats.
			for p := 0; p < s.n; p++ {
				if r.inflight[p] && r.inflightSeq[p] == s.slotSeq[i][p] && s.now-s.slotAt[i][p] > RPCTimeout {
					s.fail("STUCK SLOT: leader %d has had an AppendEntries to %d in flight for %v (> RPCTimeout)", i, p, s.now-s.slotAt[i][p])
				}
			}
			if prev, ok := s.leaderOf[r.currentTerm]; ok && prev != i {
				s.fail("ELECTION SAFETY: nodes %d and %d both leader in term %d", prev, i, r.currentTerm)
			}
			s.leaderOf[r.currentTerm] = i
		}
		for idx := s.seenCommit[i] + 1; idx <= r.commitIndex; idx++ {
			e := r.log[idx]
			for len(s.committed) <= idx {
				s.committed = append(s.committed, LogEntry{})
				s.commitTerm = append(s.commitTerm, 0)
			}
			if c := s.committed[idx]; c.Term == 0 {
				// The first observer is the leader that just advanced its
				// commitIndex (a follower learns of a commit only afterwards).
				s.committed[idx], s.commitTerm[idx] = e, r.currentTerm
			} else if c != e {
				s.fail("COMMIT DIVERGENCE at index %d: node %d has %+v, earlier committed %+v", idx, i, e, c)
			}
		}
		if r.commitIndex > s.seenCommit[i] {
			s.seenCommit[i] = r.commitIndex
		}
	}
}

// checkHeavy runs periodically and at the end.
func (s *detSim) checkHeavy() {
	for i := 0; i < s.n; i++ {
		if !s.up[i] || s.nodes[i].state != Leader {
			continue
		}
		r := s.nodes[i]
		for idx := 1; idx < len(s.committed); idx++ {
			// Leader Completeness: a leader of term T holds every entry committed
			// in a term < T. A stale (partitioned) leader legitimately lacks
			// entries committed in later terms.
			if c := s.committed[idx]; c.Term != 0 && s.commitTerm[idx] < r.currentTerm && (idx >= len(r.log) || r.log[idx] != c) {
				s.fail("LEADER COMPLETENESS: leader %d (term %d) lacks committed entry %d %+v", i, r.currentTerm, idx, c)
				break
			}
		}
	}
	// Log Matching: same (index, term) => same command and identical prefix.
	for a := 0; a < s.n; a++ {
		for b := a + 1; b < s.n; b++ {
			if !s.up[a] || !s.up[b] {
				continue
			}
			la, lb := s.nodes[a].log, s.nodes[b].log
			m := len(la)
			if len(lb) < m {
				m = len(lb)
			}
			for idx := m - 1; idx >= 1; idx-- {
				if la[idx].Term != lb[idx].Term {
					continue
				}
				for j := idx; j >= 1; j-- {
					if la[j] != lb[j] {
						s.fail("LOG MATCHING: nodes %d,%d agree on term at %d but differ at %d: %+v vs %+v", a, b, idx, j, la[j], lb[j])
						break
					}
				}
				break
			}
		}
	}
}

type simResult struct {
	hash       uint64
	failures   []string
	recent     []string
	trace      []string
	finalIndex int
	events     int
}

func runSim(seed int64, dump bool) simResult {
	s := newDetSim(seed, dump)
	s.drop, s.dup, s.maxDelay, s.longRate, s.longDelay = 0.15, 0.05, 20*time.Millisecond, 0.05, 500*time.Millisecond
	s.crashRate = 0.006
	const chaosEnd, clientEnd, end = 12 * time.Second, 18 * time.Second, 21 * time.Second
	s.clientLoop(clientEnd)
	s.faultLoop(chaosEnd)
	s.after(chaosEnd, "heal-all", s.healAll)
	s.runUntil(end)
	if len(s.failures) == 0 {
		s.checkHeavy()
		s.finalChecks()
	}
	for i := 0; i < s.n; i++ {
		if s.up[i] {
			r := s.nodes[i]
			fmt.Fprintf(s.h, "node %d term=%d vote=%d state=%d commit=%d log=%v\n", i, r.currentTerm, r.votedFor, r.state, r.commitIndex, r.log)
		}
	}
	res := simResult{hash: s.h.Sum64(), failures: s.failures, recent: s.recent, trace: s.trace, events: s.events}
	if len(s.committed) > 0 {
		res.finalIndex = len(s.committed) - 1
	}
	return res
}

func (s *detSim) finalChecks() {
	leaders := 0
	c0 := s.nodes[0].commitIndex
	for i := 0; i < s.n; i++ {
		r := s.nodes[i]
		if !s.up[i] {
			s.fail("node %d still down after heal", i)
			return
		}
		if r.state == Leader {
			leaders++
		}
		if r.commitIndex != c0 {
			s.fail("no convergence: node %d commitIndex %d, node 0 has %d", i, r.commitIndex, c0)
		}
		for idx := 1; idx < len(s.committed); idx++ {
			if c := s.committed[idx]; c.Term != 0 && (idx >= len(r.log) || r.log[idx] != c) {
				s.fail("node %d lost committed entry %d %+v", i, idx, c)
				break
			}
		}
		for idx := 1; idx <= c0; idx++ {
			if s.applied[i][idx] != s.applied[0][idx] {
				s.fail("node %d applied[%d]=%v, node 0 has %v", i, idx, s.applied[i][idx], s.applied[0][idx])
				break
			}
		}
	}
	if leaders != 1 {
		s.fail("expected exactly 1 leader after heal, got %d", leaders)
	}
	if c0 < 10 {
		s.fail("no progress after heal: commitIndex %d", c0)
	}
}

// TestDeterministicReplay sweeps seeds; each seed is run twice and the two
// event-trace hashes must match, proving the run is exactly reproducible.
//
// Reproduce a failure:  go test ./raft -run TestDeterministicReplay -seed=N -trace
func TestDeterministicReplay(t *testing.T) {
	var seeds []int64
	if *flagSeed != 0 {
		seeds = []int64{*flagSeed}
	} else {
		for i := 1; i <= *flagSeeds; i++ {
			seeds = append(seeds, int64(i))
		}
	}
	hashes := map[uint64]int64{}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			a := runSim(seed, *flagTrace)
			if *flagTrace {
				t.Log("\n" + strings.Join(a.trace, "\n"))
			}
			if len(a.failures) > 0 {
				t.Fatalf("invariant violated (reproduce: go test ./raft -run TestDeterministicReplay -seed=%d -trace)\n%s\n--- last events ---\n%s",
					seed, strings.Join(a.failures, "\n"), strings.Join(a.recent, "\n"))
			}
			b := runSim(seed, false)
			if a.hash != b.hash || a.events != b.events {
				t.Fatalf("NOT DETERMINISTIC: seed %d hash %x (%d events) vs %x (%d events)", seed, a.hash, a.events, b.hash, b.events)
			}
			t.Logf("events=%d finalCommitIndex=%d hash=%x", a.events, a.finalIndex, a.hash)
			if other, dup := hashes[a.hash]; dup {
				t.Errorf("seeds %d and %d produced the same trace hash; hash is not sensitive", other, seed)
			}
			hashes[a.hash] = seed
		})
	}
}
