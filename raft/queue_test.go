package raft

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakyTransport models node 0 of a 3-node cluster: votes are granted by peer 1;
// AppendEntries succeed while the gate is open and block until their timeout
// while it is closed. It records the most calls in progress at once per peer.
type flakyTransport struct {
	open   atomic.Bool
	cur    [3]atomic.Int32
	maxCur [3]atomic.Int32
	calls  [3]atomic.Int32

	mu   sync.Mutex
	seen []appendRec // AppendEntries calls to peer 1, in order
}

type appendRec struct{ prev, entries int }

func (f *flakyTransport) RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	if to == 1 {
		return &RequestVoteReply{Term: args.Term, VoteGranted: true}, true
	}
	<-ctx.Done()
	return nil, false
}

func (f *flakyTransport) AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	f.calls[to].Add(1)
	if to == 1 {
		f.mu.Lock()
		f.seen = append(f.seen, appendRec{args.PrevLogIndex, len(args.Entries)})
		f.mu.Unlock()
	}
	n := f.cur[to].Add(1)
	for {
		m := f.maxCur[to].Load()
		if n <= m || f.maxCur[to].CompareAndSwap(m, n) {
			break
		}
	}
	defer f.cur[to].Add(-1)
	if f.open.Load() {
		return &AppendEntriesReply{Term: args.Term, Success: true}, true
	}
	<-ctx.Done()
	return nil, false
}

// With a window larger than workers + queue, a stalled follower makes requests
// queue and then get dropped as lost; concurrency never exceeds the worker count,
// goroutines stay fixed, and once the follower answers again everything commits.
func TestQueueFullDropsAndRecovers(t *testing.T) {
	oldW, oldK, oldQ, oldU := MaxInflightAppends, AppendWorkers, AppendQueueSize, MaxUncommittedEntries
	MaxInflightAppends, AppendWorkers, AppendQueueSize, MaxUncommittedEntries = 16, 1, 2, 0
	defer func() {
		MaxInflightAppends, AppendWorkers, AppendQueueSize, MaxUncommittedEntries = oldW, oldK, oldQ, oldU
	}()

	tr := &flakyTransport{}
	tr.open.Store(true) // let the first probe succeed so the follower enters the replicating state
	applyCh := make(chan ApplyMsg, 256)
	r, err := Make(0, 3, tr, &MemStorage{}, applyCh)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Kill()
	becomeLeader(t, r)
	time.Sleep(200 * time.Millisecond) // both followers confirmed and replicating
	tr.open.Store(false)               // from now on every AppendEntries stalls until its timeout
	base := runtime.NumGoroutine()

	const n = 60
	for i := 1; i <= n; i++ {
		if _, _, err := r.Propose(i); err != nil {
			t.Fatalf("Propose(%d): %v", i, err)
		}
		time.Sleep(2 * time.Millisecond) // each proposal tries to send a batch: pending outruns workers+queue
	}
	if g := runtime.NumGoroutine() - base; g > 4 {
		t.Errorf("goroutines grew by %d while follower requests piled up", g)
	}
	for p := 1; p <= 2; p++ {
		if m := tr.maxCur[p].Load(); m > 1 {
			t.Errorf("peer %d: %d calls in progress at once, but there is only 1 worker", p, m)
		}
	}

	tr.open.Store(true) // the follower answers again
	got := 0
	deadline := time.After(10 * time.Second)
	for got < n {
		select {
		case m := <-applyCh:
			if m.CommandValid {
				got++
			}
		case <-deadline:
			t.Fatalf("only %d/%d entries committed after the follower recovered", got, n)
		}
	}
}

// A pipeline reset (here: a timeout) makes the core forget every outstanding
// request. Requests it had already queued must be skipped, not sent: they would
// delay the fresh probe and carry optimistic positions the follower never confirmed.
func TestStaleQueuedRequestsSkipped(t *testing.T) {
	oldW, oldK, oldQ, oldU := MaxInflightAppends, AppendWorkers, AppendQueueSize, MaxUncommittedEntries
	MaxInflightAppends, AppendWorkers, AppendQueueSize, MaxUncommittedEntries = 16, 1, 64, 0
	defer func() {
		MaxInflightAppends, AppendWorkers, AppendQueueSize, MaxUncommittedEntries = oldW, oldK, oldQ, oldU
	}()

	tr := &flakyTransport{}
	tr.open.Store(true)
	r, err := Make(0, 3, tr, &MemStorage{}, make(chan ApplyMsg, 64))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Kill()
	becomeLeader(t, r)
	time.Sleep(200 * time.Millisecond) // followers confirmed: replicating, nothing outstanding

	tr.open.Store(false) // every call now stalls until its 80ms timeout
	tr.mu.Lock()
	mark := len(tr.seen)
	tr.mu.Unlock()
	for i := 1; i <= 6; i++ { // back to back: one worker is stuck on the first, five queue behind it
		if _, _, err := r.Propose(i); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(700 * time.Millisecond) // several timeouts and resets

	tr.mu.Lock()
	calls := append([]appendRec(nil), tr.seen[mark:]...)
	tr.mu.Unlock()
	first := -1
	for _, c := range calls {
		if c.entries > 0 {
			first = c.prev
			break
		}
	}
	if first < 0 {
		t.Fatal("no AppendEntries with entries reached the transport")
	}
	for i, c := range calls {
		if c.entries > 0 && c.prev > first {
			t.Fatalf("call %d (prev=%d) is a request queued before the reset (first optimistic prev=%d); it should have been skipped", i, c.prev, first)
		}
	}
}

// A follower that is slow but never fails: with few workers and a tiny queue the
// leader's requests overflow and are dropped, over and over. Replication must keep
// going and everything must commit. (Reporting each dropped request as lost lets
// the core resend at once; without it the dropped entries are only resent after
// the follower rejects a later request for the gap, so that report is an
// optimisation, not a liveness requirement, and this test does not depend on it.)
func TestQueueOverflowWithSlowFollowerKeepsReplicating(t *testing.T) {
	oldW, oldK, oldQ := MaxInflightAppends, AppendWorkers, AppendQueueSize
	MaxInflightAppends, AppendWorkers, AppendQueueSize = 16, 1, 2
	defer func() { MaxInflightAppends, AppendWorkers, AppendQueueSize = oldW, oldK, oldQ }()

	took, _ := timeToCommit(t, MaxInflightAppends, AppendWorkers, 10, 200)
	if took > 8*time.Second {
		t.Fatalf("200 entries took %v with a slow-but-healthy follower; replication stalled after queue overflow", took)
	}
	t.Logf("200 entries committed in %v (workers=1, queue=2, window=16, RTT 30ms)", took.Round(time.Millisecond))
}
