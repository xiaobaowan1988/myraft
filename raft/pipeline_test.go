package raft

import (
	"testing"
	"time"
)

// timeToCommit proposes n entries at once against followers that acknowledge
// every MsgApp after rtt, and returns how long until all are applied.
func timeToCommit(t *testing.T, window, batch, n int, rtt time.Duration) time.Duration {
	oldW, oldB, oldU := MaxInflightAppends, MaxAppendEntries, MaxUncommittedEntries
	MaxInflightAppends, MaxAppendEntries, MaxUncommittedEntries = window, batch, 0
	defer func() { MaxInflightAppends, MaxAppendEntries, MaxUncommittedEntries = oldW, oldB, oldU }()

	f := &fakePeers{latency: rtt, handle: func(m Message) []Message {
		if r := voteFromPeer1(m); r != nil {
			return r
		}
		return ackApp(m)
	}}
	applyCh := make(chan ApplyMsg, n+16)
	r := startNode(t, f, applyCh)
	becomeLeader(t, r)
	time.Sleep(100 * time.Millisecond)

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
			if m.CommandValid {
				got++
			}
		case <-deadline:
			t.Fatalf("window=%d: only %d/%d entries applied", window, got, n)
		}
	}
	return time.Since(start)
}

// A bigger window must raise throughput on a link with real latency: with
// window 1 each batch costs a full round trip; with more in flight they overlap.
func TestPipelineThroughput(t *testing.T) {
	const n, batch = 600, 10
	const rtt = 30 * time.Millisecond
	slow := timeToCommit(t, 1, batch, n, rtt)
	fast := timeToCommit(t, 8, batch, n, rtt)
	wide := timeToCommit(t, 32, batch, n, rtt)
	t.Logf("%d entries, batch %d, RTT %v: window=1 %v | window=8 %v (%.1fx) | window=32 %v (%.1fx)",
		n, batch, rtt, slow.Round(time.Millisecond), fast.Round(time.Millisecond), float64(slow)/float64(fast),
		wide.Round(time.Millisecond), float64(slow)/float64(wide))
	if slow < 3*fast {
		t.Errorf("window=8 (%v) is not at least 3x faster than window=1 (%v)", fast, slow)
	}
	if wide > fast+fast/2 {
		t.Errorf("window=32 (%v) is clearly slower than window=8 (%v)", wide, fast)
	}
}
