package raft

import (
	"fmt"
	"sync"
	"time"
)

// Timing parameters (variables so tests may tune them).
var (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

// Raft is the I/O shell around the pure state machine (core). One goroutine,
// run(), owns the core and performs all I/O on its behalf:
//
//   - Outside code (RPC handlers, Start, State, RPC reply goroutines) never
//     touches core state; it posts a closure to events and the loop runs it.
//   - After every event the loop calls flush, which persists first and only
//     then sends messages, delivers committed entries and releases waiting
//     RPC callers.
//   - Code on the loop must never block on the network, nor post to events
//     and wait for it (that would deadlock the loop).
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
	// persisted (releasing RPC callers). Loop only.
	after []func()
}

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

// Make starts node me of an n-node cluster, restoring state from st.
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

// Start proposes cmd (must be non-nil). It returns the index and term the
// command will have if it is ever committed, and whether this node is leader.
// It returns only after the proposal is durable on this node.
func (r *Raft) Start(cmd interface{}) (index, term int, isLeader bool) {
	resp := make(chan struct{})
	if cmd == nil || !r.post(func() {
		index, term, isLeader = r.c.propose(cmd)
		r.release(func() { close(resp) })
	}) {
		return -1, -1, false
	}
	<-resp
	return index, term, isLeader
}

// RequestVote is the RPC entry point. The reply is released only after any
// state it depends on (vote, term) is durable.
func (r *Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	reply := &RequestVoteReply{}
	resp := make(chan struct{})
	if !r.post(func() {
		r.c.handleRequestVote(args, reply)
		r.release(func() { close(resp) })
	}) {
		return reply
	}
	<-resp
	return reply
}

// AppendEntries is the RPC entry point. The reply is released only after the
// appended entries are durable.
func (r *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{}
	resp := make(chan struct{})
	if !r.post(func() {
		r.c.handleAppendEntries(args, reply)
		r.release(func() { close(resp) })
	}) {
		return reply
	}
	<-resp
	return reply
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
// persist -> send requests -> deliver apply -> release waiting RPC callers.
func (r *Raft) flush() {
	rd := r.c.takeReady()
	if len(rd.Persist) > 0 {
		if err := r.st.Append(rd.Persist); err != nil {
			// Continuing without durability would violate Raft's safety.
			panic(fmt.Sprintf("raft %d: wal write failed: %v", r.c.me, err))
		}
	}
	for _, v := range rd.Votes {
		r.sendVote(v.peer, v.args)
	}
	for _, a := range rd.Appends {
		r.sendAppend(a.peer, a.ni, a.args)
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

// sendVote and sendAppend send from helper goroutines (the loop never blocks
// on the network) and post the reply back as an event.
func (r *Raft) sendVote(peer int, args *RequestVoteArgs) {
	go func() {
		reply, ok := r.tr.RequestVote(peer, args)
		if !ok {
			return
		}
		r.post(func() { r.c.onVoteReply(peer, args.Term, reply) })
	}()
}

func (r *Raft) sendAppend(peer, ni int, args *AppendEntriesArgs) {
	go func() {
		reply, ok := r.tr.AppendEntries(peer, args)
		if !ok {
			return
		}
		r.post(func() { r.c.onAppendReply(peer, ni, args, reply) })
	}()
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
