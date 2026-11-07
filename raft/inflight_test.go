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

func runStuck(t *testing.T, honorCtx bool) (*stuckTransport, int, int) {
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

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, isL := r.State(); isL {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, isL := r.State(); !isL {
		t.Fatal("node 0 never became leader")
	}
	peak := 0
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if n := runtime.NumGoroutine(); n > peak {
			peak = n
		}
	}
	return tr, base, peak
}

// With peers that never answer, goroutines must stay bounded and no peer may
// ever have more than one AppendEntries outstanding.
func TestInFlightBounded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		honorCtx bool
	}{{"transport honors ctx", true}, {"transport ignores ctx", false}} {
		t.Run(tc.name, func(t *testing.T) {
			tr, base, peak := runStuck(t, tc.honorCtx)
			// run loop + applyPump + <=2 appends + <=2 votes (+ slack for test/timers).
			if growth := peak - base; growth > 10 {
				t.Errorf("goroutines grew by %d (base %d, peak %d): not bounded", growth, base, peak)
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
			t.Logf("goroutines base=%d peak=%d, votes blocked at most %d", base, peak, tr.maxVote.Load())
		})
	}
}
