package raft

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// A leader that cannot commit must stop accepting proposals at the limit, so
// its log stays bounded; once commits resume it accepts again.
func TestProposalBackpressure(t *testing.T) {
	const limit = 10
	setMaxUncommitted(t, limit)
	var open atomic.Bool // while closed, followers never answer MsgApp
	f := &fakePeers{handle: func(m Message) []Message {
		if r := voteFromPeer1(m); r != nil {
			return r
		}
		if open.Load() {
			return ackApp(m)
		}
		return nil
	}}
	r := startNode(t, f, make(chan ApplyMsg, 1024))
	becomeLeader(t, r)

	// The election no-op is uncommitted and counts, so exactly limit-1 fit.
	accepted, dropped := 0, 0
	for i := 1; i <= 50; i++ {
		_, _, err := r.Propose(i)
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrProposalDropped):
			dropped++
		default:
			t.Fatalf("Propose(%d): unexpected error %v", i, err)
		}
	}
	if accepted != limit-1 || dropped != 50-(limit-1) {
		t.Fatalf("accepted=%d dropped=%d, want accepted=%d dropped=%d", accepted, dropped, limit-1, 50-(limit-1))
	}

	// Let replication succeed: entries commit, room opens up, proposals work again.
	open.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, _, err := r.Propose("after-recovery"); err == nil {
			return
		} else if !errors.Is(err, ErrProposalDropped) {
			t.Fatalf("unexpected error after recovery: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("proposals were still dropped after replication resumed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProposeErrors(t *testing.T) {
	r, err := Make(0, 3, &fakePeers{}, &MemStorage{}, make(chan ApplyMsg, 8))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Propose(nil); !errors.Is(err, ErrNilCommand) {
		t.Errorf("nil command: got %v, want ErrNilCommand", err)
	}
	// Follower at start (no election yet within a few ms).
	if _, _, err := r.Propose(1); !errors.Is(err, ErrNotLeader) {
		t.Errorf("follower: got %v, want ErrNotLeader", err)
	}
	r.Kill()
	if _, _, err := r.Propose(1); !errors.Is(err, ErrStopped) {
		t.Errorf("killed node: got %v, want ErrStopped", err)
	}
}
