package raft

import (
	"math/rand"
	"time"
)

// core is the Raft state machine. It performs NO I/O: it never touches
// storage, the network, channels, goroutines or the wall clock. Callers drive
// it with events (tick, incoming RPCs, RPC replies, proposals); everything it
// wants done in response accumulates in c.rd and is collected with takeReady.
//
// The driver MUST process a Ready in this order: persist Persist records
// (durably) first, and only then send messages, deliver Apply entries, and
// answer the RPC that triggered the event. That ordering is what makes the
// state safe to reveal; it is enforced in one place (Raft.flush and the
// simulator's flush) instead of inside every handler.
//
// core is not safe for concurrent use: exactly one goroutine may call it.
type core struct {
	me int
	n  int

	// now is the only clock the state machine reads.
	now func() time.Time
	rng *rand.Rand

	// Persistent: changes are staged into rd.Persist.
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a term-0 sentinel

	// Volatile.
	commitIndex int
	lastApplied int // highest index handed out through rd.Apply
	state       int

	// Leader only.
	nextIndex  []int
	matchIndex []int

	granted          []bool // granted[p]: peer p voted for us in the current election (dedups duplicate replies)
	electionDeadline time.Time
	lastBroadcast    time.Time

	rd Ready // output accumulated since the last takeReady
}

type voteOut struct {
	peer int
	args *RequestVoteArgs
}

type appendOut struct {
	peer, ni int
	args     *AppendEntriesArgs
}

// Ready is what the state machine asks its driver to do.
type Ready struct {
	Persist []Record // make durable BEFORE anything below becomes visible
	Votes   []voteOut
	Appends []appendOut
	Apply   []ApplyMsg
}

func (rd *Ready) empty() bool {
	return len(rd.Persist) == 0 && len(rd.Votes) == 0 && len(rd.Appends) == 0 && len(rd.Apply) == 0
}

// takeReady returns and clears the accumulated output.
func (c *core) takeReady() Ready {
	rd := c.rd
	c.rd = Ready{}
	return rd
}

// newCore builds a state machine from recovered persistent state.
func newCore(me, n, term, votedFor int, log []LogEntry, seed int64, now func() time.Time) *core {
	c := &core{
		me: me, n: n, now: now,
		currentTerm: term, votedFor: votedFor, log: log,
		state: Follower,
		rng:   rand.New(rand.NewSource(seed)),
	}
	c.resetElection()
	return c
}

// ---- helpers ----

// stage queues records for durable storage.
func (c *core) stage(recs ...Record) { c.rd.Persist = append(c.rd.Persist, recs...) }

func (c *core) stageMeta() {
	c.stage(Record{Kind: recMeta, Term: c.currentTerm, VotedFor: c.votedFor})
}

func (c *core) resetElection() {
	span := ElectionTimeoutMax - ElectionTimeoutMin
	c.electionDeadline = c.now().Add(ElectionTimeoutMin + time.Duration(c.rng.Int63n(int64(span))))
}

// becomeFollower steps down; a higher term clears the vote and is staged.
func (c *core) becomeFollower(term int) {
	c.state = Follower
	if term > c.currentTerm {
		c.currentTerm = term
		c.votedFor = -1
		c.stageMeta()
	}
}

// appendLocal appends a leader-created entry and stages it.
func (c *core) appendLocal(e LogEntry) {
	c.log = append(c.log, e)
	idx := len(c.log) - 1
	c.stage(Record{Kind: recAppend, Index: idx, Entries: []LogEntry{e}})
	c.matchIndex[c.me] = idx
	c.nextIndex[c.me] = idx + 1
	if c.n == 1 {
		c.advanceCommit()
	}
}

func (c *core) becomeLeader() {
	c.state = Leader
	c.nextIndex = make([]int, c.n)
	c.matchIndex = make([]int, c.n)
	for i := range c.nextIndex {
		c.nextIndex[i] = len(c.log)
	}
	// No-op so entries from earlier terms can be committed indirectly.
	c.appendLocal(LogEntry{Term: c.currentTerm})
	c.broadcast()
}

func (c *core) advanceCommit() {
	for idx := len(c.log) - 1; idx > c.commitIndex; idx-- {
		if c.log[idx].Term != c.currentTerm {
			break // only count replicas for current-term entries
		}
		cnt := 0
		for _, m := range c.matchIndex {
			if m >= idx {
				cnt++
			}
		}
		if cnt > c.n/2 {
			c.setCommit(idx)
			break
		}
	}
}

// setCommit raises commitIndex and queues newly committed entries for apply.
func (c *core) setCommit(idx int) {
	if idx <= c.commitIndex {
		return
	}
	c.commitIndex = idx
	for c.lastApplied < c.commitIndex {
		c.lastApplied++
		e := c.log[c.lastApplied]
		c.rd.Apply = append(c.rd.Apply, ApplyMsg{CommandValid: e.Command != nil, Command: e.Command, CommandIndex: c.lastApplied})
	}
}

// propose appends cmd if this node is leader.
func (c *core) propose(cmd interface{}) (index, term int, ok bool) {
	if c.state != Leader {
		return -1, -1, false
	}
	c.appendLocal(LogEntry{Term: c.currentTerm, Command: cmd})
	c.broadcast()
	return len(c.log) - 1, c.currentTerm, true
}

// ---- time ----

func (c *core) onTick() {
	if c.state == Leader {
		if c.now().Sub(c.lastBroadcast) >= HeartbeatInterval {
			c.broadcast()
		}
	} else if c.now().After(c.electionDeadline) {
		c.startElection()
	}
}

// ---- election ----

func (c *core) startElection() {
	c.state = Candidate
	c.currentTerm++
	c.votedFor = c.me
	c.stageMeta()
	c.resetElection()
	term := c.currentTerm
	if c.n == 1 {
		c.becomeLeader()
		return
	}
	lastIdx := len(c.log) - 1
	args := &RequestVoteArgs{Term: term, CandidateID: c.me, LastLogIndex: lastIdx, LastLogTerm: c.log[lastIdx].Term}
	c.granted = make([]bool, c.n)
	c.granted[c.me] = true
	for peer := 0; peer < c.n; peer++ {
		if peer != c.me {
			c.rd.Votes = append(c.rd.Votes, voteOut{peer, args})
		}
	}
}

func (c *core) onVoteReply(peer, term int, reply *RequestVoteReply) {
	if reply.Term > c.currentTerm {
		c.becomeFollower(reply.Term)
		return
	}
	if c.state != Candidate || c.currentTerm != term {
		return // stale reply
	}
	if reply.VoteGranted && !c.granted[peer] {
		c.granted[peer] = true
		votes := 0
		for _, g := range c.granted {
			if g {
				votes++
			}
		}
		if votes > c.n/2 {
			c.becomeLeader()
		}
	}
}

func (c *core) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = c.currentTerm
	if args.Term < c.currentTerm {
		return
	}
	if args.Term > c.currentTerm {
		c.becomeFollower(args.Term)
		reply.Term = c.currentTerm
	}
	lastIdx := len(c.log) - 1
	lastTerm := c.log[lastIdx].Term
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	if (c.votedFor == -1 || c.votedFor == args.CandidateID) && upToDate {
		c.votedFor = args.CandidateID
		c.stageMeta() // the driver persists this before the reply can leave
		c.resetElection()
		reply.VoteGranted = true
	}
}

// ---- replication ----

func (c *core) broadcast() {
	c.lastBroadcast = c.now()
	for peer := 0; peer < c.n; peer++ {
		if peer != c.me {
			c.replicate(peer)
		}
	}
}

// replicate queues one AppendEntries request for peer.
func (c *core) replicate(peer int) {
	if c.state != Leader {
		return
	}
	ni := c.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	if ni > len(c.log) {
		ni = len(c.log)
	}
	prev := ni - 1
	args := &AppendEntriesArgs{
		Term: c.currentTerm, LeaderID: c.me,
		PrevLogIndex: prev, PrevLogTerm: c.log[prev].Term,
		Entries:      append([]LogEntry(nil), c.log[ni:]...),
		LeaderCommit: c.commitIndex,
	}
	c.rd.Appends = append(c.rd.Appends, appendOut{peer, ni, args})
}

func (c *core) onAppendReply(peer, ni int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > c.currentTerm {
		c.becomeFollower(reply.Term)
		c.resetElection()
		return
	}
	if c.state != Leader || c.currentTerm != args.Term {
		return // stale reply
	}
	prev := args.PrevLogIndex
	if reply.Success {
		match := prev + len(args.Entries)
		if match > c.matchIndex[peer] {
			c.matchIndex[peer] = match
		}
		if c.matchIndex[peer]+1 > c.nextIndex[peer] {
			c.nextIndex[peer] = c.matchIndex[peer] + 1
		}
		c.advanceCommit()
		return
	}
	if c.nextIndex[peer] != ni {
		return // another reply already moved nextIndex
	}
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		for i := len(c.log) - 1; i >= 1; i-- {
			if c.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
			if c.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}
	if next >= ni {
		next = ni - 1
	}
	if next < c.matchIndex[peer]+1 {
		next = c.matchIndex[peer] + 1
	}
	if next < 1 {
		next = 1
	}
	c.nextIndex[peer] = next
	if next < ni {
		c.replicate(peer) // retry immediately with the backed-off index
	}
}

func (c *core) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = c.currentTerm
	if args.Term < c.currentTerm {
		return // stale leader
	}
	c.becomeFollower(args.Term) // also demotes a same-term candidate
	reply.Term = c.currentTerm
	c.resetElection()

	if args.PrevLogIndex >= len(c.log) {
		reply.ConflictIndex, reply.ConflictTerm = len(c.log), -1
		return
	}
	if t := c.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && c.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Truncate only on a real conflict; matching entries (duplicates or
	// reordered RPCs) are left alone.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(c.log) {
			if c.log[idx].Term == e.Term {
				continue
			}
			c.log = c.log[:idx]
		}
		rest := append([]LogEntry(nil), args.Entries[i:]...)
		c.log = append(c.log, rest...)
		c.stage(Record{Kind: recAppend, Index: idx, Entries: rest})
		break
	}

	lastNew := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > c.commitIndex {
		cm := args.LeaderCommit
		if lastNew < cm {
			cm = lastNew
		}
		c.setCommit(cm)
	}
	reply.Success = true
}
