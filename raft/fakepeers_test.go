package raft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sentMsg struct {
	at time.Time
	m  Message
}

// fakePeers plays the other two nodes of a 3-node cluster for a single Raft
// node 0. It is the node's Transport: every message sent is recorded, and
// handle (if set) scripts the answers, which arrive after latency through Step.
type fakePeers struct {
	node    atomic.Pointer[Raft]
	latency time.Duration
	handle  func(m Message) []Message

	mu   sync.Mutex
	sent []sentMsg
}

func (f *fakePeers) Send(m Message) {
	f.mu.Lock()
	f.sent = append(f.sent, sentMsg{time.Now(), m})
	f.mu.Unlock()
	if f.handle == nil {
		return
	}
	resps := f.handle(m)
	r := f.node.Load()
	if len(resps) == 0 || r == nil {
		return
	}
	time.AfterFunc(f.latency, func() {
		for _, x := range resps {
			r.Step(x)
		}
	})
}

// sentTo returns the messages of type t sent to peer, in order.
func (f *fakePeers) sentTo(peer int, t MsgType) []sentMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []sentMsg
	for _, s := range f.sent {
		if s.m.To == peer && s.m.Type == t {
			out = append(out, s)
		}
	}
	return out
}

// voteFromPeer1 makes peer 1 grant votes, so node 0 wins every election.
func voteFromPeer1(m Message) []Message {
	if m.Type == MsgVote && m.To == 1 {
		return []Message{{Type: MsgVoteResp, From: 1, To: 0, Term: m.Term, Granted: true}}
	}
	return nil
}

// ackApp answers a MsgApp as a follower holding exactly the leader's log would.
func ackApp(m Message) []Message {
	if m.Type == MsgApp {
		return []Message{{Type: MsgAppResp, From: m.To, To: 0, Term: m.Term, Seq: m.Seq, Success: true, Match: m.PrevLogIndex + len(m.Entries)}}
	}
	return nil
}

func startNode(t *testing.T, f *fakePeers, applyCh chan ApplyMsg) *Raft {
	t.Helper()
	return startNodeWith(t, f, &MemStorage{}, applyCh)
}

func startNodeWith(t *testing.T, f *fakePeers, st Storage, applyCh chan ApplyMsg) *Raft {
	t.Helper()
	r, err := Make(0, 3, f, st, applyCh)
	if err != nil {
		t.Fatal(err)
	}
	f.node.Store(r)
	t.Cleanup(r.Kill)
	return r
}

func becomeLeader(t *testing.T, r *Raft) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, isL := r.State(); isL {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("never became leader")
}

func setMaxUncommitted(t *testing.T, n int) {
	old := MaxUncommittedEntries
	MaxUncommittedEntries = n
	t.Cleanup(func() { MaxUncommittedEntries = old })
}
