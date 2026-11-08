package raft

import (
	"fmt"
	"sync"
	"time"
)

// Timing and flow-control parameters (variables so tests may tune them).
var (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	tickInterval       = 10 * time.Millisecond

	// AppendTimeout is how long a leader waits for the answer to a MsgApp before
	// treating it as lost and restarting replication to that follower from a
	// known-good point. Keep AppendTimeout + tickInterval + HeartbeatInterval <
	// ElectionTimeoutMin, or one lost message can let a follower time out.
	AppendTimeout = 80 * time.Millisecond

	// MaxUncommittedEntries bounds the leader's log entries awaiting commit; past
	// it, Propose fails with ErrProposalDropped. <= 0 disables the limit.
	MaxUncommittedEntries = 1024

	// MaxInflightAppends is how many MsgApp may be unanswered per follower once its
	// log match is confirmed (1 while probing); etcd's MaxInflightMsgs.
	MaxInflightAppends = 32

	// MaxAppendEntries caps the entries in one MsgApp (<= 0: no cap).
	MaxAppendEntries = 128
)

// Raft is the I/O shell around the pure state machine (core). One goroutine,
// run(), owns the core and performs all I/O on its behalf:
//
//   - Outside code (Step, ReportUnreachable, Propose, State) never touches core
//     state; it posts a closure to events and the loop runs it.
//   - After every event the loop calls flush, which persists first and only
//     then sends messages and delivers committed entries.
//   - Code on the loop must never block, nor post to events and wait for it
//     (that would deadlock the loop).
//
// The node starts exactly two goroutines (run and applyPump) and none per message:
// sending, queueing and connection handling belong to the Transport.
//
// A given Storage must be used by at most one Raft at a time; Kill waits for
// the loop to exit so a dead instance can no longer write.
type Raft struct {
	c  *core
	st Storage
	tr Transport

	events    chan func()
	applyFeed chan []ApplyMsg
	applyCh   chan ApplyMsg
	done      chan struct{}
	stopped   chan struct{} // closed when run() has exited
	killOnce  sync.Once

	// after holds actions to run once the current event's Ready has been
	// persisted (releasing Propose/State callers). Loop only.
	after []func()
}

// Raft receives messages from its transport.
var _ Endpoint = (*Raft)(nil)

// restoreCore loads persistent state from st and builds the state machine.
// It is shared by Make and the deterministic simulator.
func restoreCore(me, n int, st Storage, seed int64, now func() time.Time) (*core, error) {
	recs, err := st.Load()
	if err != nil {
		return nil, err
	}
	term, votedFor, log, err := replay(recs)
	if err != nil {
		return nil, err
	}
	return newCore(me, n, term, votedFor, log, seed, now), nil
}

// Make starts node me of an n-node cluster, restoring state from st. The
// transport must deliver this node's incoming messages to the returned Raft's
// Step (it is an Endpoint).
func Make(me, n int, tr Transport, st Storage, applyCh chan ApplyMsg) (*Raft, error) {
	c, err := restoreCore(me, n, st, time.Now().UnixNano()+int64(me)*7919, time.Now)
	if err != nil {
		return nil, err
	}
	r := &Raft{
		c: c, st: st, tr: tr, applyCh: applyCh,
		events:    make(chan func()), // unbuffered: a successful post means the loop took it and will run it
		applyFeed: make(chan []ApplyMsg, 16),
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	go r.run()
	go r.applyPump()
	return r, nil
}

// Kill stops the node and waits for the event loop to exit, so after Kill
// returns the node performs no further writes to storage.
func (r *Raft) Kill() {
	r.killOnce.Do(func() { close(r.done) })
	<-r.stopped
}

// post schedules f on the event loop. It reports false if the node is dead,
// in which case f will never run. Once post returns true, f is guaranteed to
// run to completion (and its flush too).
func (r *Raft) post(f func()) bool {
	select {
	case r.events <- f:
		return true
	case <-r.done:
		return false
	}
}

// release schedules fn to run after this event's persistence. Loop only.
func (r *Raft) release(fn func()) { r.after = append(r.after, fn) }

// Step delivers an incoming message (called by the transport). It returns once
// the event loop has taken the message, or at once if the node is stopped.
func (r *Raft) Step(m Message) {
	r.post(func() { r.c.step(m) })
}

// ReportUnreachable tells the node that sending to peer failed, so replication
// to it restarts from a safe point (called by the transport).
func (r *Raft) ReportUnreachable(peer int) {
	r.post(func() { r.c.onUnreachable(peer) })
}

// State returns the current term and whether this node believes it is leader.
func (r *Raft) State() (term int, isLeader bool) {
	resp := make(chan struct{})
	if !r.post(func() {
		term, isLeader = r.c.currentTerm, r.c.state == Leader
		r.release(func() { close(resp) })
	}) {
		return 0, false
	}
	<-resp
	return term, isLeader
}

// Propose appends cmd (must be non-nil) to the log if this node is the leader
// and not overloaded. It returns the index and term the command will have if it
// is ever committed, only after the proposal is durable on this node. It never
// blocks on replication: errors are ErrNotLeader, ErrProposalDropped (too many
// uncommitted entries; retry later), ErrStopped and ErrNilCommand.
func (r *Raft) Propose(cmd interface{}) (index, term int, err error) {
	if cmd == nil {
		return -1, -1, ErrNilCommand
	}
	resp := make(chan struct{})
	if !r.post(func() {
		index, term, err = r.c.propose(cmd)
		r.release(func() { close(resp) })
	}) {
		return -1, -1, ErrStopped
	}
	<-resp
	return index, term, err
}

// Start is Propose with a boolean result: ok is false if the command was not
// accepted for any reason (not leader, overloaded, stopped).
func (r *Raft) Start(cmd interface{}) (index, term int, ok bool) {
	index, term, err := r.Propose(cmd)
	return index, term, err == nil
}

// ---- event loop ----

func (r *Raft) run() {
	defer close(r.stopped)
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case f := <-r.events:
			f()
			r.flush()
		case <-t.C:
			r.c.onTick()
			r.flush()
		}
	}
}

// flush carries out the core's Ready in the only safe order:
// persist -> send messages -> deliver apply -> release waiting callers.
func (r *Raft) flush() {
	rd := r.c.takeReady()
	if len(rd.Persist) > 0 {
		if err := r.st.Append(rd.Persist); err != nil {
			// Continuing without durability would violate Raft's safety.
			panic(fmt.Sprintf("raft %d: wal write failed: %v", r.c.me, err))
		}
	}
	for _, m := range rd.Messages {
		r.tr.Send(m) // must not block
	}
	if len(rd.Apply) > 0 {
		select {
		case r.applyFeed <- rd.Apply: // applyPump always drains promptly, so this does not stall the loop
		case <-r.done:
		}
	}
	for _, fn := range r.after {
		fn()
	}
	r.after = r.after[:0]
}

// applyPump is an unbounded, lock-free queue between the event loop and the
// (possibly slow) consumer of applyCh.
func (r *Raft) applyPump() {
	var queue []ApplyMsg
	for {
		var out chan ApplyMsg
		var head ApplyMsg
		if len(queue) > 0 {
			out, head = r.applyCh, queue[0]
		}
		select {
		case batch := <-r.applyFeed:
			queue = append(queue, batch...)
		case out <- head:
			queue = queue[1:]
		case <-r.done:
			return
		}
	}
}
