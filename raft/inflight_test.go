package raft

import (
	"runtime"
	"testing"
	"time"
)

func TestTimingConstraint(t *testing.T) {
	if AppendTimeout+tickInterval+HeartbeatInterval >= ElectionTimeoutMin {
		t.Fatalf("AppendTimeout(%v)+tick(%v)+HeartbeatInterval(%v) must be < ElectionTimeoutMin(%v)",
			AppendTimeout, tickInterval, HeartbeatInterval, ElectionTimeoutMin)
	}
}

func peakGoroutines(d time.Duration) int {
	peak := 0
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if n := runtime.NumGoroutine(); n > peak {
			peak = n
		}
	}
	return peak
}

// The node starts exactly two goroutines (event loop and applyPump) and none per
// message, election or proposal; sending belongs to the transport. Here the
// followers never answer a MsgApp, which is the worst case for anything that
// would pile up per request.
func TestNodeStartsFixedGoroutines(t *testing.T) {
	setMaxUncommitted(t, 0)
	f := &fakePeers{handle: voteFromPeer1}
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	r := startNode(t, f, make(chan ApplyMsg, 1))
	becomeLeader(t, r)
	time.Sleep(300 * time.Millisecond) // the vote answer's timer goroutine has finished

	early := peakGoroutines(time.Second)
	for i := 1; i <= 2000; i++ { // each proposal tries to send; nothing is ever answered
		if _, _, err := r.Propose(i); err != nil {
			t.Fatal(err)
		}
	}
	late := peakGoroutines(time.Second)
	t.Logf("goroutines: base=%d idle=%d after 2000 proposals=%d", base, early, late)
	if early-base != 2 || late-base != 2 {
		t.Errorf("node should add exactly 2 goroutines, got +%d idle and +%d under load", early-base, late-base)
	}
	if n := len(f.sentTo(1, MsgApp)) + len(f.sentTo(2, MsgApp)); n == 0 {
		t.Error("no MsgApp was sent; the test did not exercise sending")
	}
}

// With followers that never answer, a leader probing one request at a time must
// wait for AppendTimeout before it resends to the same follower, and must keep
// retrying (a lost message never wedges replication).
func TestUnansweredAppendsBounded(t *testing.T) {
	f := &fakePeers{handle: voteFromPeer1}
	r := startNode(t, f, make(chan ApplyMsg, 1))
	becomeLeader(t, r)
	time.Sleep(2 * time.Second)

	for peer := 1; peer <= 2; peer++ {
		sent := f.sentTo(peer, MsgApp)
		if len(sent) < 5 {
			t.Errorf("peer %d got only %d MsgApp in ~2s: retries stopped", peer, len(sent))
		}
		for i := 1; i < len(sent); i++ {
			if gap := sent[i].at.Sub(sent[i-1].at); gap < AppendTimeout-2*time.Millisecond {
				t.Errorf("peer %d: MsgApp %d followed the previous one after %v, before AppendTimeout (%v): more than one outstanding while probing", peer, i, gap, AppendTimeout)
			}
		}
	}
}
