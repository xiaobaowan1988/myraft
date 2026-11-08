package raft

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimingConstraint(t *testing.T) {
	if RPCTimeout+HeartbeatInterval >= ElectionTimeoutMin {
		t.Fatalf("RPCTimeout(%v)+HeartbeatInterval(%v) must be < ElectionTimeoutMin(%v)", RPCTimeout, HeartbeatInterval, ElectionTimeoutMin)
	}
}

// stuckTransport models a 3-node cluster from node 0's point of view: peer 1
// grants votes (so node 0 becomes leader) and every AppendEntries blocks.
type stuckTransport struct {
	honorCtx bool
	release  chan struct{} // closed at test end to free callers that ignore ctx

	calls   [3]atomic.Int32 // AppendEntries calls started per peer
	cur     [3]atomic.Int32 // currently blocked per peer
	maxCur  [3]atomic.Int32
	votesIn atomic.Int32 // RequestVote calls currently blocked
	maxVote atomic.Int32
}

func (s *stuckTransport) RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	if to == 1 {
		return &RequestVoteReply{Term: args.Term, VoteGranted: true}, true
	}
	n := s.votesIn.Add(1)
	for {
		m := s.maxVote.Load()
		if n <= m || s.maxVote.CompareAndSwap(m, n) {
			break
		}
	}
	defer s.votesIn.Add(-1)
	<-ctx.Done() // the vote RPC always honors the timeout here
	return nil, false
}

func (s *stuckTransport) AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	s.calls[to].Add(1)
	n := s.cur[to].Add(1)
	for {
		m := s.maxCur[to].Load()
		if n <= m || s.maxCur[to].CompareAndSwap(m, n) {
			break
		}
	}
	defer s.cur[to].Add(-1)
	if s.honorCtx {
		<-ctx.Done()
	} else {
		<-s.release
	}
	return nil, false
}

type stuckResult struct {
	tr                      *stuckTransport
	base, early, late, peak int
}

func runStuck(t *testing.T, honorCtx bool) stuckResult {
	tr := &stuckTransport{honorCtx: honorCtx, release: make(chan struct{})}
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	r, err := Make(0, 3, tr, &MemStorage{}, make(chan ApplyMsg, 1))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { close(tr.release); r.Kill() }) }
	t.Cleanup(stop)

	becomeLeader(t, r)
	time.Sleep(300 * time.Millisecond) // let every worker start and the first timeouts pass
	res := stuckResult{tr: tr, base: base}
	sample := func(d time.Duration) int {
		peak := 0
		for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if n := runtime.NumGoroutine(); n > peak {
				peak = n
			}
		}
		return peak
	}
	res.early = sample(time.Second)
	res.late = sample(time.Second)
	res.peak = max(res.early, res.late)
	return res
}

// With peers that never answer, the goroutine count must equal the fixed
// resident set (it does not grow with the number of requests or elections), and
// no peer may ever have more than one AppendEntries outstanding (probing).
func TestInFlightBounded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		honorCtx bool
	}{{"transport honors ctx", true}, {"transport ignores ctx", false}} {
		t.Run(tc.name, func(t *testing.T) {
			old := MaxInflightAppends
			MaxInflightAppends = 4
			defer func() { MaxInflightAppends = old }()
			res := runStuck(t, tc.honorCtx)
			tr := res.tr
			const n, workers = 3, 4
			// 2 (run loop, applyPump) + per follower (workers + 1 vote worker), plus a few
			// short-lived goroutines the runtime starts when a context deadline fires.
			resident := 2 + (n-1)*(workers+1)
			if growth := res.peak - res.base; growth > resident+4 {
				t.Errorf("goroutines grew by %d (base %d, peak %d); expected at most %d resident + 4 transient", growth, res.base, res.peak, resident)
			}
			// Fixed pool: nothing may accumulate over time (a per-request design would).
			if res.late > res.early+2 {
				t.Errorf("goroutine count grew while running: first second peak %d, second second peak %d", res.early, res.late)
			}
			for p := 1; p <= 2; p++ {
				if m := tr.maxCur[p].Load(); m > 1 {
					t.Errorf("peer %d had %d AppendEntries in flight at once", p, m)
				}
			}
			if tc.honorCtx {
				// Timeouts must free the slot so the leader keeps retrying.
				for p := 1; p <= 2; p++ {
					if c := tr.calls[p].Load(); c < 5 {
						t.Errorf("peer %d got only %d AppendEntries in 2s: slot not released on timeout", p, c)
					}
				}
			} else {
				// A caller that never returns permanently occupies exactly one slot.
				for p := 1; p <= 2; p++ {
					if c := tr.calls[p].Load(); c != 1 {
						t.Errorf("peer %d got %d AppendEntries, want exactly 1 while the first is stuck", p, c)
					}
				}
			}
			t.Logf("goroutines base=%d early=%d late=%d (resident bound %d)", res.base, res.early, res.late, resident)
		})
	}
}
