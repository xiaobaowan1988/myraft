package raft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowStore is a MemStorage whose every Append takes delay, like a slow disk.
type slowStore struct {
	MemStorage
	delay   time.Duration // fixed per-write delay
	extra   atomic.Int64  // extra delay in ns, adjustable while running
	appends atomic.Int32
}

func (s *slowStore) Append(r []Record) error {
	s.appends.Add(1)
	time.Sleep(s.delay + time.Duration(s.extra.Load()))
	return s.MemStorage.Append(r)
}

func (s *slowStore) has(match func(Record) bool) bool {
	recs, _ := s.Load()
	for _, r := range recs {
		if match(r) {
			return true
		}
	}
	return false
}

// An answer must not leave the node before the state it reveals is on disk. Here
// a follower grants a vote on a slow disk: the grant must appear only after the
// (term, votedFor) record is durable, and the node must stay responsive meanwhile.
func TestPersistBeforeSend(t *testing.T) {
	st := &slowStore{delay: 200 * time.Millisecond}
	var durableAtSend atomic.Bool
	f := &fakePeers{}
	f.handle = func(m Message) []Message {
		if m.Type == MsgVoteResp && m.Granted {
			durableAtSend.Store(st.has(func(r Record) bool { return r.Kind == recMeta && r.Term == 5 && r.VotedFor == 1 }))
		}
		return nil
	}
	r := startNodeWith(t, f, st, make(chan ApplyMsg, 1))

	r.Step(Message{Type: MsgVote, From: 1, To: 0, Term: 5})
	time.Sleep(60 * time.Millisecond) // the Ready has been taken and the write is under way
	if n := len(f.sentTo(1, MsgVoteResp)); n != 0 {
		t.Fatalf("the vote answer left the node while its record was still being written (%d sent)", n)
	}
	// The write is slow, but the node loop must not be: it already runs at term 5.
	start := time.Now()
	term, _ := r.State()
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("State() took %v while the disk was busy: the node loop waited for the write", d)
	}
	if term != 5 {
		t.Errorf("term = %d, want 5 (events must keep being processed during the write)", term)
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(f.sentTo(1, MsgVoteResp)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no vote answer after the write finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	resp := f.sentTo(1, MsgVoteResp)[0].m
	if !resp.Granted || resp.Term != 5 {
		t.Fatalf("answer = %+v, want a grant in term 5", resp)
	}
	if !durableAtSend.Load() {
		t.Fatal("the vote answer was sent before the vote was durable")
	}
}

// While one Ready is being written, new events keep being processed and their
// output waits for the next Ready, so concurrent proposals share fsyncs.
func TestBatchesWritesWhileDiskIsSlow(t *testing.T) {
	setMaxUncommitted(t, 0)
	st := &slowStore{delay: 20 * time.Millisecond}
	f := &fakePeers{handle: func(m Message) []Message {
		if r := voteFromPeer1(m); r != nil {
			return r
		}
		return ackApp(m)
	}}
	applyCh := make(chan ApplyMsg, 1024)
	r := startNodeWith(t, f, st, applyCh)
	becomeLeader(t, r)
	time.Sleep(100 * time.Millisecond)
	before := st.appends.Load()

	const workers, each = 40, 5
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, _, err := r.Propose(i); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	writes := int(st.appends.Load() - before)
	t.Logf("%d concurrent proposals -> %d storage writes", workers*each, writes)
	if writes > workers*each/4 {
		t.Errorf("%d proposals needed %d writes; expected them to share writes (<= %d)", workers*each, writes, workers*each/4)
	}
	got := 0
	deadline := time.After(10 * time.Second)
	for got < workers*each {
		select {
		case m := <-applyCh:
			if m.CommandValid {
				got++
			}
		case <-deadline:
			t.Fatalf("only %d/%d proposals committed", got, workers*each)
		}
	}
}

// Propose waits for durability but must give up cleanly when the node stops.
func TestProposeReturnsWhenStopped(t *testing.T) {
	st := &slowStore{} // fast while the node wins its election
	f := &fakePeers{handle: voteFromPeer1}
	r := startNodeWith(t, f, st, make(chan ApplyMsg, 8))
	becomeLeader(t, r)
	time.Sleep(100 * time.Millisecond)
	st.extra.Store(int64(300 * time.Millisecond)) // now the disk stalls

	errc := make(chan error, 1)
	go func() { _, _, err := r.Propose(1); errc <- err }()
	time.Sleep(50 * time.Millisecond) // the proposal is staged and its write is in progress
	r.Kill()
	select {
	case err := <-errc:
		if err != nil && err != ErrStopped {
			t.Fatalf("unexpected error %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propose never returned after Kill")
	}
}
