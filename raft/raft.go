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

// Raft is one consensus node. All mutable state is guarded by mu; mu is never
// held across a network call or an applyCh send.
type Raft struct {
	mu   sync.Mutex
	me   int
	n    int
	tr   Transport
	st   Storage
	dead bool
	done chan struct{}

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Persistent (written to st before replying to any RPC).
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a term-0 sentinel

	// Volatile.
	commitIndex int
	lastApplied int
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
		done:        make(chan struct{}),
		currentTerm: term, votedFor: votedFor, log: log,
		state: Follower,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*7919)),
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.resetElectionLocked()
	go r.ticker()
	go r.applier()
	return r, nil
}

// Kill stops the node. After Kill returns it performs no further writes.
func (r *Raft) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead {
		return
	}
	r.dead = true
	close(r.done)
	r.applyCond.Broadcast()
}

// State returns the current term and whether this node believes it is leader.
func (r *Raft) State() (term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state == Leader && !r.dead
}

// Start proposes cmd (must be non-nil). It returns the index and term the
// command will have if it is ever committed, and whether this node is leader.
func (r *Raft) Start(cmd interface{}) (index, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead || r.state != Leader || cmd == nil {
		return -1, -1, false
	}
	e := LogEntry{Term: r.currentTerm, Command: cmd}
	r.appendLocal(e)
	idx := len(r.log) - 1
	r.broadcastLocked()
	return idx, r.currentTerm, true
}

// ---- helpers (lock held) ----

func (r *Raft) persist(recs ...Record) {
	if err := r.st.Append(recs); err != nil {
		// Continuing without durability would violate Raft's safety.
		panic(fmt.Sprintf("raft %d: wal write failed: %v", r.me, err))
	}
}

func (r *Raft) persistMeta() {
	r.persist(Record{Kind: recMeta, Term: r.currentTerm, VotedFor: r.votedFor})
}

func (r *Raft) resetElectionLocked() {
	span := ElectionTimeoutMax - ElectionTimeoutMin
	r.electionDeadline = time.Now().Add(ElectionTimeoutMin + time.Duration(r.rng.Int63n(int64(span))))
}

// becomeFollowerLocked steps down; a higher term clears the vote and is persisted.
func (r *Raft) becomeFollowerLocked(term int) {
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
		r.advanceCommitLocked()
	}
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.nextIndex = make([]int, r.n)
	r.matchIndex = make([]int, r.n)
	for i := range r.nextIndex {
		r.nextIndex[i] = len(r.log)
	}
	// No-op so entries from earlier terms can be committed indirectly.
	r.appendLocal(LogEntry{Term: r.currentTerm})
	r.broadcastLocked()
}

func (r *Raft) advanceCommitLocked() {
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
			r.commitIndex = idx
			r.applyCond.Broadcast()
			break
		}
	}
}

// ---- ticker / election ----

func (r *Raft) ticker() {
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-t.C:
		}
		r.mu.Lock()
		if r.dead {
			r.mu.Unlock()
			return
		}
		if r.state == Leader {
			if time.Since(r.lastBroadcast) >= HeartbeatInterval {
				r.broadcastLocked()
			}
		} else if time.Now().After(r.electionDeadline) {
			r.startElectionLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startElectionLocked() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.me
	r.persistMeta()
	r.resetElectionLocked()
	term := r.currentTerm
	if r.n == 1 {
		r.becomeLeaderLocked()
		return
	}
	lastIdx := len(r.log) - 1
	args := &RequestVoteArgs{Term: term, CandidateID: r.me, LastLogIndex: lastIdx, LastLogTerm: r.log[lastIdx].Term}
	votes := 1
	for peer := 0; peer < r.n; peer++ {
		if peer == r.me {
			continue
		}
		go func(peer int) {
			reply, ok := r.tr.RequestVote(peer, args)
			if !ok {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.dead {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if r.state != Candidate || r.currentTerm != term {
				return // stale reply
			}
			if reply.VoteGranted {
				votes++
				if votes > r.n/2 {
					r.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

// RequestVote handles an incoming vote request.
func (r *Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &RequestVoteReply{Term: r.currentTerm}
	if r.dead || args.Term < r.currentTerm {
		return reply
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
		reply.Term = r.currentTerm
	}
	lastIdx := len(r.log) - 1
	lastTerm := r.log[lastIdx].Term
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	if (r.votedFor == -1 || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		r.persistMeta() // durable before the vote is revealed
		r.resetElectionLocked()
		reply.VoteGranted = true
	}
	return reply
}

// ---- replication ----

func (r *Raft) broadcastLocked() {
	r.lastBroadcast = time.Now()
	for peer := 0; peer < r.n; peer++ {
		if peer != r.me {
			go r.replicate(peer)
		}
	}
}

func (r *Raft) replicate(peer int) {
	r.mu.Lock()
	if r.dead || r.state != Leader {
		r.mu.Unlock()
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
	r.mu.Unlock()

	reply, ok := r.tr.AppendEntries(peer, args)
	if !ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead {
		return
	}
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		r.resetElectionLocked()
		return
	}
	if r.state != Leader || r.currentTerm != args.Term {
		return // stale reply
	}
	if reply.Success {
		match := prev + len(args.Entries)
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		if r.matchIndex[peer]+1 > r.nextIndex[peer] {
			r.nextIndex[peer] = r.matchIndex[peer] + 1
		}
		r.advanceCommitLocked()
		return
	}
	if r.nextIndex[peer] != ni {
		return // another reply already moved nextIndex
	}
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		next = reply.ConflictIndex
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
		go r.replicate(peer) // retry immediately with the backed-off index
	}
}

// AppendEntries handles log replication / heartbeats from a leader.
func (r *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &AppendEntriesReply{Term: r.currentTerm}
	if r.dead || args.Term < r.currentTerm {
		return reply // stale leader
	}
	r.becomeFollowerLocked(args.Term) // also demotes a same-term candidate
	reply.Term = r.currentTerm
	r.resetElectionLocked()

	if args.PrevLogIndex >= len(r.log) {
		reply.ConflictIndex, reply.ConflictTerm = len(r.log), -1
		return reply
	}
	if t := r.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && r.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		return reply
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
		if c > r.commitIndex {
			r.commitIndex = c
			r.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply
}

// ---- apply ----

func (r *Raft) applier() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		for !r.dead && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.dead {
			return
		}
		var msgs []ApplyMsg
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			e := r.log[r.lastApplied]
			msgs = append(msgs, ApplyMsg{CommandValid: e.Command != nil, Command: e.Command, CommandIndex: r.lastApplied})
		}
		r.mu.Unlock()
		for _, m := range msgs {
			select {
			case r.applyCh <- m:
			case <-r.done:
				r.mu.Lock()
				return
			}
		}
		r.mu.Lock()
	}
}
