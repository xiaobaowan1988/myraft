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

	// MaxAppendBytes caps the estimated size of one MsgApp's entries (<= 0: no cap),
	// but a batch always carries at least one entry. Datagram transports want this
	// small (a few KiB): every fragment of a message must arrive for it to count.
	MaxAppendBytes = 64 << 10

	// EntrySize estimates the bytes a command takes on the wire; it is only used to
	// cap batches, so it need not be exact. Replace it if commands are not []byte
	// or string.
	EntrySize = func(cmd interface{}) int {
		switch v := cmd.(type) {
		case nil:
			return 0
		case []byte:
			return len(v)
		case string:
			return len(v)
		}
		return 16
	}
)

// entryOverhead is the estimated per-entry cost (term, length) added to EntrySize.
const entryOverhead = 8

// Raft is the I/O shell around the pure state machine (core), built like etcd's
// raft.Node plus the application loop that drives it, with two goroutines:
//
//   - run, the node loop, is the only goroutine that touches the core. It handles
//     events (incoming messages, ticks, proposals) and lets the core accumulate its
//     output. When the core has output and no Ready is outstanding it hands the
//     output over as one Ready on readyc, then waits for advancec. It never waits for
//     the disk: while a Ready is being written, events keep being processed and
//     their output piles up into the next Ready (so writes batch naturally).
//   - readyLoop takes each Ready and processes it in the only safe order: persist,
//     then send messages, then deliver committed entries, then release callers
//     waiting for durability; then it sends on advancec (etcd's Advance). Readys are
//     strictly sequential, so nothing from a later Ready can become visible before
//     an earlier one is durable.
//
// Outside code (Step, ReportUnreachable, Propose, State) never touches core
// state; it posts a closure to events and the node loop runs it. Code on the node
// loop must never block, nor post to events and wait for it.
//
// The node starts three goroutines (run, readyLoop, applyPump) and none per
// message: sending, queueing and connection handling belong to the Transport.
//
// A given Storage must be used by at most one Raft at a time; Kill waits for both
// loops to exit so a dead instance can no longer write.
type Raft struct {
	c  *core
	st Storage
	tr Transport

	events    chan func()
	readyc    chan readyBatch
	advancec  chan struct{}
	applyFeed chan []ApplyMsg
	applyCh   chan ApplyMsg
	done      chan struct{}
	stopped   chan struct{} // closed when run() has exited
	rstopped  chan struct{} // closed when readyLoop() has exited
	killOnce  sync.Once

	// after holds actions to run once the output of the events handled so far has
	// been persisted (releasing Propose callers). Node loop only; it travels with
	// the next Ready that is handed out.
	after []func()
}

// readyBatch is one Ready plus the callers to release once it is durable.
type readyBatch struct {
	Ready
	release []func()
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
		readyc:    make(chan readyBatch),
		advancec:  make(chan struct{}),
		applyFeed: make(chan []ApplyMsg, 16),
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
		rstopped:  make(chan struct{}),
	}
	go r.run()
	go r.readyLoop()
	go r.applyPump()
	return r, nil
}

// Kill stops the node and waits for both loops to exit (a Ready being written is
// finished first), so after Kill returns the node performs no further writes.
func (r *Raft) Kill() {
	r.killOnce.Do(func() { close(r.done) })
	<-r.stopped
	<-r.rstopped
}

// post schedules f on the node loop. It reports false if the node is dead, in
// which case f will never run. Once post returns true, f is guaranteed to run to
// completion (its output is handed over in a later Ready, unless the node is
// killed first).
func (r *Raft) post(f func()) bool {
	select {
	case r.events <- f:
		return true
	case <-r.done:
		return false
	}
}

// release schedules fn to run once the output of the events handled so far is
// durable. Node loop only.
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

// State returns the current term and whether this node believes it is leader. It
// answers at once, without waiting for any disk write (it is only an observation).
func (r *Raft) State() (term int, isLeader bool) {
	type res struct {
		term     int
		isLeader bool
	}
	ch := make(chan res, 1)
	if !r.post(func() { ch <- res{r.c.currentTerm, r.c.state == Leader} }) {
		return 0, false
	}
	v := <-ch
	return v.term, v.isLeader
}

// Propose appends cmd (must be non-nil) to the log if this node is the leader
// and not overloaded. It returns the index and term the command will have if it
// is ever committed, only after the proposal is durable on this node. It never
// blocks on replication: errors are ErrNotLeader, ErrProposalDropped (too many
// uncommitted entries; retry later), ErrStopped and ErrNilCommand. If the node
// is stopped while waiting, the result is ErrStopped and the outcome is unknown.
func (r *Raft) Propose(cmd interface{}) (index, term int, err error) {
	if cmd == nil {
		return -1, -1, ErrNilCommand
	}
	type res struct {
		index, term int
		err         error
	}
	ch := make(chan res, 1)
	if !r.post(func() {
		i, t, e := r.c.propose(cmd)
		if e != nil {
			ch <- res{i, t, e} // nothing was staged: no need to wait for the disk
			return
		}
		r.release(func() { ch <- res{i, t, nil} })
	}) {
		return -1, -1, ErrStopped
	}
	select {
	case v := <-ch:
		return v.index, v.term, v.err
	case <-r.done:
		return -1, -1, ErrStopped
	}
}

// Start is Propose with a boolean result: ok is false if the command was not
// accepted for any reason (not leader, overloaded, stopped).
func (r *Raft) Start(cmd interface{}) (index, term int, ok bool) {
	index, term, err := r.Propose(cmd)
	return index, term, err == nil
}

// ---- node loop and ready loop ----

// run is the node loop (see Raft). It follows etcd's node.run: arm readyc only when
// there is output and no Ready is outstanding.
func (r *Raft) run() {
	defer close(r.stopped)
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	var (
		readyc      chan readyBatch // non-nil while a Ready is waiting to be taken
		rd          readyBatch
		outstanding bool // a Ready has been handed over and not yet advanced
	)
	for {
		if !outstanding && readyc == nil && (!r.c.rd.empty() || len(r.after) > 0) {
			rd = readyBatch{Ready: r.c.takeReady(), release: r.after}
			r.after = nil
			readyc = r.readyc
		}
		select {
		case <-r.done:
			return
		case f := <-r.events:
			f()
		case <-t.C:
			r.c.onTick()
		case readyc <- rd:
			readyc, outstanding = nil, true
		case <-r.advancec:
			outstanding = false
		}
	}
}

// readyLoop is the application side: it carries out each Ready, one at a time.
func (r *Raft) readyLoop() {
	defer close(r.rstopped)
	for {
		select {
		case <-r.done:
			return
		case rd := <-r.readyc:
			r.process(rd)
			select {
			case r.advancec <- struct{}{}:
			case <-r.done:
				return
			}
		}
	}
}

// process carries out a Ready in the only safe order:
// persist -> send messages -> deliver apply -> release waiting callers.
func (r *Raft) process(rd readyBatch) {
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
		case r.applyFeed <- rd.Apply: // applyPump always drains promptly, so this does not stall
		case <-r.done:
		}
	}
	for _, fn := range rd.release {
		fn()
	}
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
