package raft

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// latencyTransport models node 0 of a 3-node cluster whose peers ack every
// AppendEntries successfully after a fixed one-way latency each way.
type latencyTransport struct{ oneWay time.Duration }

func (l *latencyTransport) sleep(ctx context.Context) bool {
	t := time.NewTimer(l.oneWay)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *latencyTransport) RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	if to == 1 {
		return &RequestVoteReply{Term: args.Term, VoteGranted: true}, true
	}
	<-ctx.Done()
	return nil, false
}

func (l *latencyTransport) AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	if !l.sleep(ctx) || !l.sleep(ctx) { // request leg + reply leg
		return nil, false
	}
	return &AppendEntriesReply{Term: args.Term, Success: true}, true
}

// timeToCommit proposes n entries at once and returns how long until all are
// applied, and the most goroutines above the post-election level seen meanwhile.
func timeToCommit(t *testing.T, window, workers, batch, n int) (time.Duration, int) {
	oldW, oldK, oldB, oldU := MaxInflightAppends, AppendWorkers, MaxAppendEntries, MaxUncommittedEntries
	MaxInflightAppends, AppendWorkers, MaxAppendEntries, MaxUncommittedEntries = window, workers, batch, 0
	defer func() {
		MaxInflightAppends, AppendWorkers, MaxAppendEntries, MaxUncommittedEntries = oldW, oldK, oldB, oldU
	}()

	applyCh := make(chan ApplyMsg, n+16)
	r, err := Make(0, 3, &latencyTransport{oneWay: 15 * time.Millisecond}, &MemStorage{}, applyCh)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Kill()
	becomeLeader(t, r)
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine() // workers are already running
	peak := base

	start := time.Now()
	for i := 1; i <= n; i++ {
		if _, _, err := r.Propose(i); err != nil {
			t.Fatalf("Propose(%d): %v", i, err)
		}
	}
	got := 0
	deadline := time.After(20 * time.Second)
	for got < n {
		select {
		case m := <-applyCh:
			if g := runtime.NumGoroutine(); g > peak {
				peak = g
			}
			if m.CommandValid {
				got++
			}
		case <-deadline:
			t.Fatalf("window=%d: only %d/%d entries applied", window, got, n)
		}
	}
	return time.Since(start), peak - base
}

// A bigger window must raise throughput on a link with real latency: with
// window 1 each batch costs a full round trip; with more in flight they overlap.
// Goroutines come from fixed worker pools, so they must not grow with the load.
func TestPipelineThroughput(t *testing.T) {
	const n, batch = 600, 10
	slow, slowG := timeToCommit(t, 1, 1, batch, n)
	// Window == workers: every in-flight request has a worker.
	fast, fastG := timeToCommit(t, 8, 8, batch, n)
	// etcd-style: more requests allowed in flight than workers; the rest queue.
	queued, queuedG := timeToCommit(t, 32, 4, batch, n)
	t.Logf("%d entries, batch %d, RTT 30ms: window=1 %v | window=8,workers=8 %v (%.1fx) | window=32,workers=4 %v (%.1fx)",
		n, batch, slow.Round(time.Millisecond), fast.Round(time.Millisecond), float64(slow)/float64(fast),
		queued.Round(time.Millisecond), float64(slow)/float64(queued))
	for name, g := range map[string]int{"window 1": slowG, "window 8": fastG, "window 32 / 4 workers": queuedG} {
		if g > 4 {
			t.Errorf("goroutines grew with load (%s): +%d while sending hundreds of requests", name, g)
		}
	}
	if slow < 3*fast {
		t.Errorf("window=8,workers=8 (%v) is not at least 3x faster than window=1 (%v)", fast, slow)
	}
	// 4 workers bound concurrency to 4 calls, so expect ~4x; require a clear gain.
	if slow < 2*queued {
		t.Errorf("window=32,workers=4 (%v) is not at least 2x faster than window=1 (%v)", queued, slow)
	}
}
