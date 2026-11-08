package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Timing parameters (variables so tests may tune them).
var (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	tickInterval       = 10 * time.Millisecond

	// RPCTimeout bounds every outgoing RPC, and so bounds the goroutines blocked
	// in them. Keep RPCTimeout + HeartbeatInterval < ElectionTimeoutMin, or one
	// lost heartbeat can let a follower time out.
	RPCTimeout = 80 * time.Millisecond

	// MaxUncommittedEntries bounds the leader's log entries awaiting commit; past
	// it, Propose fails with ErrProposalDropped. <= 0 disables the limit.
	MaxUncommittedEntries = 1024

	// MaxInflightAppends is how many AppendEntries may be outstanding per follower
	// once its log match is confirmed (1 while probing). It may exceed AppendWorkers:
	// the extra requests wait in the follower's queue.
	// Like etcd's rafthttp pipeline: fixed workers drain a bounded queue.
	//
	// AppendWorkers is the number of fixed worker goroutines per follower, started
	// once in Make. Together with one vote worker per follower they set the resident
	// goroutine count, 2 + (n-1)*(AppendWorkers+1), independent of load and window.
	AppendWorkers = 4

	// AppendQueueSize is the capacity of each follower's AppendEntries queue
	// (etcd's pipelineBufSize). A full queue drops the request and reports it lost.
	AppendQueueSize    = 64
	MaxInflightAppends = 32

	// MaxAppendEntries caps the entries in one AppendEntries (<= 0: no cap).
	MaxAppendEntries = 128
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

	// Fixed worker pools, one per follower, started in Make: the number of
	// goroutines blocked in RPCs never depends on load.
	peers []*peerQueues
}

// peerQueues feeds one follower's fixed workers. RPCTimeout runs from the moment
// a worker starts the call, not from enqueueing (as in etcd's pipeline), so a
// request's lifetime is its queue wait plus its call.
type peerQueues struct {
	appends chan appendJob
	votes   chan voteJob
	// gen is the newest request generation enqueued for this follower (written
	// only by the event loop). Queued jobs of an older generation were made
	// before the core reset this follower's pipeline and are skipped.
	gen atomic.Uint64
}

type appendJob struct {
	seq  uint64
	gen  uint64
	args *AppendEntriesArgs
}

type voteJob struct {
	args *RequestVoteArgs
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
		peers:     make([]*peerQueues, n),
	}
	for p := 0; p < n; p++ {
		if p == me {
			continue
		}
		q := &peerQueues{appends: make(chan appendJob, max(AppendQueueSize, 1)), votes: make(chan voteJob, 2)}
		r.peers[p] = q
		for i := 0; i < max(AppendWorkers, 1); i++ {
			go r.appendWorker(p, q)
		}
		go r.voteWorker(p, q)
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
	// Publish each follower's newest request generation to its queue first, so
	// workers can tell queued requests made before a pipeline reset are stale.
	if r.c.gen != nil {
		for p, q := range r.peers {
			if q != nil {
				q.gen.Store(r.c.gen[p])
			}
		}
	}
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
		r.sendAppend(a)
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

// sendVote and sendAppend only enqueue; the peer's fixed workers do the RPC.
// They run on the event loop and never block.
func (r *Raft) sendVote(peer int, args *RequestVoteArgs) {
	select {
	case r.peers[peer].votes <- voteJob{args}:
	default:
		// Queue full (the vote worker is stuck): drop it. Raft tolerates lost
		// vote requests; the next election retries.
	}
}

func (r *Raft) sendAppend(a appendOut) {
	q := r.peers[a.peer]
	select {
	case q.appends <- appendJob{a.seq, a.gen, a.args}:
	default:
		// Queue full: drop and report the request lost, which makes the core fall
		// back to probing (etcd: drop + ReportUnreachable).
		r.c.onAppendLost(a.peer, a.seq)
	}
}

func (r *Raft) appendWorker(peer int, q *peerQueues) {
	for {
		select {
		case <-r.done:
			return
		case j := <-q.appends:
			if j.gen < q.gen.Load() {
				continue // made before a pipeline reset; the core has forgotten it
			}
			// The timeout starts now, when the call starts, not when it was queued.
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			reply, ok := r.tr.AppendEntries(ctx, peer, j.args)
			cancel()
			if !ok {
				// Free the in-flight slot so the next heartbeat can retry. Wait until the
				// loop has handled it: the loss resets the pipeline and bumps the
				// generation, and the next queued job must see that to be skipped.
				handled := make(chan struct{})
				if r.post(func() {
					r.c.onAppendLost(peer, j.seq)
					r.release(func() { close(handled) })
				}) {
					<-handled
				}
				continue
			}
			r.post(func() { r.c.onAppendReply(peer, j.seq, j.args, reply) })
		}
	}
}

func (r *Raft) voteWorker(peer int, q *peerQueues) {
	for {
		select {
		case <-r.done:
			return
		case j := <-q.votes:
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			reply, ok := r.tr.RequestVote(ctx, peer, j.args)
			cancel()
			if ok {
				r.post(func() { r.c.onVoteReply(peer, j.args.Term, reply) })
			}
		}
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
