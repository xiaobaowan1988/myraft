package raft

import (
	"fmt"
	"math/rand"
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

// Raft is one consensus node, built as a single-goroutine event loop.
//
// Concurrency rules (see PLAN.md):
//   - Every field below the "loop-owned" marker is read and written ONLY by the
//     run() goroutine. There is no mutex.
//   - Outside code (RPC handlers, Start, State, RPC reply goroutines) never
//     touches that state; it posts a closure to events and the loop runs it.
//   - Code running inside the loop must never block on the network, nor post to
//     events and wait for it (that would deadlock the loop).
type Raft struct {
	me int
	n  int
	tr Transport
	st Storage

	events    chan func()
	applyFeed chan []ApplyMsg
	applyCh   chan ApplyMsg
	done      chan struct{}
	stopped   chan struct{} // closed when run() has exited
	killOnce  sync.Once

	// ---- loop-owned state ----

	// Persistent (written to st before replying to any RPC).
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a term-0 sentinel

	// Volatile.
	commitIndex int
	lastApplied int // highest index handed to applyPump
	state       int

	// Leader only.
	nextIndex  []int
	matchIndex []int

	electionDeadline time.Time
	lastBroadcast    time.Time
	rng              *rand.Rand
}

// Make starts node me of an n-node cluster, restoring state from st.
func Make(me, n int, tr Transport, st Storage, applyCh chan ApplyMsg) (*Raft, error) {
	recs, err := st.Load()
	if err != nil {
		return nil, err
	}
	term, votedFor, log, err := replay(recs)
	if err != nil {
		return nil, err
	}
	r := &Raft{
		me: me, n: n, tr: tr, st: st, applyCh: applyCh,
		events:      make(chan func()), // unbuffered: a successful post means the loop took it and will run it
		applyFeed:   make(chan []ApplyMsg, 16),
		done:        make(chan struct{}),
		stopped:     make(chan struct{}),
		currentTerm: term, votedFor: votedFor, log: log,
		state: Follower,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*7919)),
	}
	r.resetElection()
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
// run to completion.
func (r *Raft) post(f func()) bool {
	select {
	case r.events <- f:
		return true
	case <-r.done:
		return false
	}
}

// State returns the current term and whether this node believes it is leader.
func (r *Raft) State() (term int, isLeader bool) {
	resp := make(chan struct{})
	if !r.post(func() {
		term, isLeader = r.currentTerm, r.state == Leader
		close(resp)
	}) {
		return 0, false
	}
	<-resp
	return term, isLeader
}

// Start proposes cmd (must be non-nil). It returns the index and term the
// command will have if it is ever committed, and whether this node is leader.
func (r *Raft) Start(cmd interface{}) (index, term int, isLeader bool) {
	index, term = -1, -1
	resp := make(chan struct{})
	if cmd == nil || !r.post(func() {
		defer close(resp)
		if r.state != Leader {
			return
		}
		r.appendLocal(LogEntry{Term: r.currentTerm, Command: cmd})
		index, term, isLeader = len(r.log)-1, r.currentTerm, true
		r.broadcast()
	}) {
		return -1, -1, false
	}
	<-resp
	return index, term, isLeader
}

// RequestVote is the RPC entry point; it runs the handler on the event loop.
func (r *Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	reply := &RequestVoteReply{}
	resp := make(chan struct{})
	if !r.post(func() { r.handleRequestVote(args, reply); close(resp) }) {
		return reply
	}
	<-resp
	return reply
}

// AppendEntries is the RPC entry point; it runs the handler on the event loop.
func (r *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{}
	resp := make(chan struct{})
	if !r.post(func() { r.handleAppendEntries(args, reply); close(resp) }) {
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
		case <-t.C:
			r.onTick()
		}
	}
}

func (r *Raft) onTick() {
	if r.state == Leader {
		if time.Since(r.lastBroadcast) >= HeartbeatInterval {
			r.broadcast()
		}
	} else if time.Now().After(r.electionDeadline) {
		r.startElection()
	}
}

// ---- helpers (loop only) ----

func (r *Raft) persist(recs ...Record) {
	if err := r.st.Append(recs); err != nil {
		// Continuing without durability would violate Raft's safety.
		panic(fmt.Sprintf("raft %d: wal write failed: %v", r.me, err))
	}
}

func (r *Raft) persistMeta() {
	r.persist(Record{Kind: recMeta, Term: r.currentTerm, VotedFor: r.votedFor})
}

func (r *Raft) resetElection() {
	span := ElectionTimeoutMax - ElectionTimeoutMin
	r.electionDeadline = time.Now().Add(ElectionTimeoutMin + time.Duration(r.rng.Int63n(int64(span))))
}

// becomeFollower steps down; a higher term clears the vote and is persisted.
func (r *Raft) becomeFollower(term int) {
	r.state = Follower
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = -1
		r.persistMeta()
	}
}

// appendLocal appends a leader-created entry and persists it.
func (r *Raft) appendLocal(e LogEntry) {
	r.log = append(r.log, e)
	idx := len(r.log) - 1
	r.persist(Record{Kind: recAppend, Index: idx, Entries: []LogEntry{e}})
	r.matchIndex[r.me] = idx
	r.nextIndex[r.me] = idx + 1
	if r.n == 1 {
		r.advanceCommit()
	}
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.nextIndex = make([]int, r.n)
	r.matchIndex = make([]int, r.n)
	for i := range r.nextIndex {
		r.nextIndex[i] = len(r.log)
	}
	// No-op so entries from earlier terms can be committed indirectly.
	r.appendLocal(LogEntry{Term: r.currentTerm})
	r.broadcast()
}

func (r *Raft) advanceCommit() {
	for idx := len(r.log) - 1; idx > r.commitIndex; idx-- {
		if r.log[idx].Term != r.currentTerm {
			break // only count replicas for current-term entries
		}
		cnt := 0
		for _, m := range r.matchIndex {
			if m >= idx {
				cnt++
			}
		}
		if cnt > r.n/2 {
			r.setCommit(idx)
			break
		}
	}
}

// setCommit raises commitIndex and hands newly committed entries to applyPump.
func (r *Raft) setCommit(idx int) {
	if idx <= r.commitIndex {
		return
	}
	r.commitIndex = idx
	var msgs []ApplyMsg
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		e := r.log[r.lastApplied]
		msgs = append(msgs, ApplyMsg{CommandValid: e.Command != nil, Command: e.Command, CommandIndex: r.lastApplied})
	}
	select {
	case r.applyFeed <- msgs: // pump always drains promptly, so this does not stall the loop
	case <-r.done:
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

// ---- election ----

func (r *Raft) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.me
	r.persistMeta()
	r.resetElection()
	term := r.currentTerm
	if r.n == 1 {
		r.becomeLeader()
		return
	}
	lastIdx := len(r.log) - 1
	args := &RequestVoteArgs{Term: term, CandidateID: r.me, LastLogIndex: lastIdx, LastLogTerm: r.log[lastIdx].Term}
	votes := 1 // only touched by loop closures
	for peer := 0; peer < r.n; peer++ {
		if peer == r.me {
			continue
		}
		go func(peer int) {
			reply, ok := r.tr.RequestVote(peer, args)
			if !ok {
				return
			}
			r.post(func() {
				if reply.Term > r.currentTerm {
					r.becomeFollower(reply.Term)
					return
				}
				if r.state != Candidate || r.currentTerm != term {
					return // stale reply
				}
				if reply.VoteGranted {
					votes++
					if votes > r.n/2 {
						r.becomeLeader()
					}
				}
			})
		}(peer)
	}
}

func (r *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return
	}
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
		reply.Term = r.currentTerm
	}
	lastIdx := len(r.log) - 1
	lastTerm := r.log[lastIdx].Term
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	if (r.votedFor == -1 || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		r.persistMeta() // durable before the vote is revealed
		r.resetElection()
		reply.VoteGranted = true
	}
}

// ---- replication ----

func (r *Raft) broadcast() {
	r.lastBroadcast = time.Now()
	for peer := 0; peer < r.n; peer++ {
		if peer != r.me {
			r.replicate(peer)
		}
	}
}

// replicate builds the request on the loop, sends it from a helper goroutine,
// and processes the reply back on the loop.
func (r *Raft) replicate(peer int) {
	if r.state != Leader {
		return
	}
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	if ni > len(r.log) {
		ni = len(r.log)
	}
	prev := ni - 1
	args := &AppendEntriesArgs{
		Term: r.currentTerm, LeaderID: r.me,
		PrevLogIndex: prev, PrevLogTerm: r.log[prev].Term,
		Entries:      append([]LogEntry(nil), r.log[ni:]...),
		LeaderCommit: r.commitIndex,
	}
	go func() {
		reply, ok := r.tr.AppendEntries(peer, args)
		if !ok {
			return
		}
		r.post(func() { r.onAppendReply(peer, ni, args, reply) })
	}()
}

func (r *Raft) onAppendReply(peer, ni int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		r.resetElection()
		return
	}
	if r.state != Leader || r.currentTerm != args.Term {
		return // stale reply
	}
	prev := args.PrevLogIndex
	if reply.Success {
		match := prev + len(args.Entries)
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		if r.matchIndex[peer]+1 > r.nextIndex[peer] {
			r.nextIndex[peer] = r.matchIndex[peer] + 1
		}
		r.advanceCommit()
		return
	}
	if r.nextIndex[peer] != ni {
		return // another reply already moved nextIndex
	}
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		for i := len(r.log) - 1; i >= 1; i-- {
			if r.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
			if r.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}
	if next >= ni {
		next = ni - 1
	}
	if next < r.matchIndex[peer]+1 {
		next = r.matchIndex[peer] + 1
	}
	if next < 1 {
		next = 1
	}
	r.nextIndex[peer] = next
	if next < ni {
		r.replicate(peer) // retry immediately with the backed-off index
	}
}

func (r *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return // stale leader
	}
	r.becomeFollower(args.Term) // also demotes a same-term candidate
	reply.Term = r.currentTerm
	r.resetElection()

	if args.PrevLogIndex >= len(r.log) {
		reply.ConflictIndex, reply.ConflictTerm = len(r.log), -1
		return
	}
	if t := r.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && r.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Truncate only on a real conflict; matching entries (duplicates or
	// reordered RPCs) are left alone.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(r.log) {
			if r.log[idx].Term == e.Term {
				continue
			}
			r.log = r.log[:idx]
		}
		rest := append([]LogEntry(nil), args.Entries[i:]...)
		r.log = append(r.log, rest...)
		r.persist(Record{Kind: recAppend, Index: idx, Entries: rest})
		break
	}

	lastNew := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > r.commitIndex {
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		r.setCommit(c)
	}
	reply.Success = true
}
