package raft

import (
	"math/rand"
	"time"
)

// core is the Raft state machine. It performs NO I/O: it never touches
// storage, the network, channels, goroutines or the wall clock. Callers drive
// it with events (tick, incoming messages, unreachable reports, proposals);
// everything it wants done in response accumulates in c.rd and is collected
// with takeReady.
//
// The driver MUST process a Ready in this order: persist Persist records
// (durably) first, and only then send Messages and deliver Apply entries. That
// ordering is what makes the state safe to reveal - including answers to
// incoming messages, which are just entries in Messages - and it is enforced in
// one place (Raft.flush and the simulator's flush) instead of inside every handler.
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

	// Per-follower replication progress (like etcd/raft's Progress).
	//   probing[p]: nextIndex is unconfirmed, so only one request may be outstanding.
	//   otherwise (replicating): nextIndex is advanced optimistically when sending and up
	//   to window requests may be outstanding.
	// pending[p] holds the outstanding MsgApp requests, oldest first. An answer only
	// counts as "current" if its Seq is still in pending[p]; any reset to probing
	// empties it, which makes older answers stale. A request that stays unanswered
	// for AppendTimeout is treated as lost and also resets to probing.
	probing []bool
	pending [][]pendReq
	seq     uint64
	window  int // MaxInflightAppends, fixed at construction

	granted          []bool // granted[p]: peer p voted for us in the current election (dedups duplicate answers)
	electionDeadline time.Time
	lastBroadcast    time.Time

	rd Ready // output accumulated since the last takeReady
}

// pendReq is a MsgApp the leader has sent and not yet seen answered.
type pendReq struct {
	seq  uint64
	at   time.Time
	prev int // the request's PrevLogIndex
}

// Ready is what the state machine asks its driver to do.
type Ready struct {
	Persist  []Record  // make durable BEFORE anything below becomes visible
	Messages []Message // to send, in order
	Apply    []ApplyMsg
}

func (rd *Ready) empty() bool {
	return len(rd.Persist) == 0 && len(rd.Messages) == 0 && len(rd.Apply) == 0
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
		state:  Follower,
		rng:    rand.New(rand.NewSource(seed)),
		window: max(MaxInflightAppends, 1),
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

// send queues a message; the driver sends it after persisting.
func (c *core) send(m Message) {
	m.From = c.me
	c.rd.Messages = append(c.rd.Messages, m)
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
	c.probing = make([]bool, c.n)
	c.pending = make([][]pendReq, c.n)
	for i := range c.probing {
		c.probing[i] = true
	}
	for i := range c.nextIndex {
		c.nextIndex[i] = len(c.log)
	}
	// No-op so entries from earlier terms can be committed indirectly.
	c.appendLocal(LogEntry{Term: c.currentTerm})
	c.broadcast(true)
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

// uncommitted is the number of log entries the leader has not yet committed.
func (c *core) uncommitted() int { return len(c.log) - 1 - c.commitIndex }

// propose appends cmd if this node is leader and not overloaded. It never blocks:
// when too many entries are waiting to commit it rejects with ErrProposalDropped
// so a stalled leader's log cannot grow without bound.
func (c *core) propose(cmd interface{}) (index, term int, err error) {
	if c.state != Leader {
		return -1, -1, ErrNotLeader
	}
	if MaxUncommittedEntries > 0 && c.uncommitted() >= MaxUncommittedEntries {
		return -1, -1, ErrProposalDropped
	}
	c.appendLocal(LogEntry{Term: c.currentTerm, Command: cmd})
	c.broadcast(false)
	return len(c.log) - 1, c.currentTerm, nil
}

// ---- time ----

func (c *core) onTick() {
	if c.state == Leader {
		// A request unanswered for AppendTimeout is lost: the pipeline can no longer
		// be trusted, so restart probing from the known match. pending is oldest first.
		for p := 0; p < c.n; p++ {
			if p != c.me && len(c.pending[p]) > 0 && c.now().Sub(c.pending[p][0].at) > AppendTimeout {
				c.resetToProbe(p, c.matchIndex[p]+1)
			}
		}
		if c.now().Sub(c.lastBroadcast) >= HeartbeatInterval {
			c.broadcast(true)
		}
	} else if c.now().After(c.electionDeadline) {
		c.startElection()
	}
}

// onUnreachable is the transport reporting that sending to peer failed (a broken
// connection). Like a timeout, it makes replication to peer restart from a safe point.
func (c *core) onUnreachable(peer int) {
	if c.state == Leader && peer >= 0 && peer < c.n && peer != c.me {
		c.resetToProbe(peer, c.matchIndex[peer]+1)
	}
}

// ---- messages ----

// step handles one incoming message.
func (c *core) step(m Message) {
	if m.To != c.me || m.From < 0 || m.From >= c.n || m.From == c.me {
		return // not for us, or from nobody we know
	}
	switch m.Type {
	case MsgVote:
		c.handleVote(m)
	case MsgVoteResp:
		c.onVoteResp(m)
	case MsgApp:
		c.handleApp(m)
	case MsgAppResp:
		c.onAppResp(m)
	}
}

// ---- election ----

func (c *core) startElection() {
	c.state = Candidate
	c.currentTerm++
	c.votedFor = c.me
	c.stageMeta()
	c.resetElection()
	if c.n == 1 {
		c.becomeLeader()
		return
	}
	lastIdx := len(c.log) - 1
	c.granted = make([]bool, c.n)
	c.granted[c.me] = true
	for peer := 0; peer < c.n; peer++ {
		if peer != c.me {
			c.send(Message{Type: MsgVote, To: peer, Term: c.currentTerm, LastLogIndex: lastIdx, LastLogTerm: c.log[lastIdx].Term})
		}
	}
}

func (c *core) onVoteResp(m Message) {
	if m.Term > c.currentTerm {
		c.becomeFollower(m.Term)
		return
	}
	if c.state != Candidate || c.currentTerm != m.Term {
		return // stale answer
	}
	if m.Granted && !c.granted[m.From] {
		c.granted[m.From] = true
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

func (c *core) handleVote(m Message) {
	resp := Message{Type: MsgVoteResp, To: m.From, Term: c.currentTerm}
	if m.Term < c.currentTerm {
		c.send(resp)
		return
	}
	if m.Term > c.currentTerm {
		c.becomeFollower(m.Term)
		resp.Term = c.currentTerm
	}
	lastIdx := len(c.log) - 1
	lastTerm := c.log[lastIdx].Term
	upToDate := m.LastLogTerm > lastTerm || (m.LastLogTerm == lastTerm && m.LastLogIndex >= lastIdx)
	if (c.votedFor == -1 || c.votedFor == m.From) && upToDate {
		c.votedFor = m.From
		c.stageMeta() // the driver persists this before the answer can leave
		c.resetElection()
		resp.Granted = true
	}
	c.send(resp)
}

// ---- replication ----

// broadcast sends to every follower. With heartbeat set it also restarts the
// heartbeat timer and guarantees each follower with window room gets a message.
func (c *core) broadcast(heartbeat bool) {
	if heartbeat {
		c.lastBroadcast = c.now()
	}
	for peer := 0; peer < c.n; peer++ {
		if peer != c.me {
			c.replicate(peer, heartbeat)
		}
	}
}

// canSend reports whether peer's window has room: one request while probing,
// up to window while replicating.
func (c *core) canSend(peer int) bool {
	if c.probing[peer] {
		return len(c.pending[peer]) == 0
	}
	return len(c.pending[peer]) < c.window
}

// replicate sends peer as many batches of new entries as the window allows. If
// heartbeat is set and nothing was sent, it sends one empty heartbeat instead.
func (c *core) replicate(peer int, heartbeat bool) {
	if c.state != Leader {
		return
	}
	sent := false
	for c.canSend(peer) && c.nextIndex[peer] < len(c.log) {
		c.sendBatch(peer)
		sent = true
	}
	if heartbeat && !sent && c.canSend(peer) {
		c.sendHeartbeat(peer)
	}
}

func (c *core) newRequest(peer int, prev int, entries []LogEntry) {
	c.seq++
	c.pending[peer] = append(c.pending[peer], pendReq{seq: c.seq, at: c.now(), prev: prev})
	c.send(Message{
		Type: MsgApp, To: peer, Term: c.currentTerm, Seq: c.seq,
		PrevLogIndex: prev, PrevLogTerm: c.log[prev].Term,
		Entries:      entries,
		LeaderCommit: c.commitIndex,
	})
}

// sendBatch sends the next batch starting at nextIndex. While replicating it
// optimistically advances nextIndex past the batch so the next one can follow
// without waiting for its answer.
func (c *core) sendBatch(peer int) {
	ni := c.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	// Take entries until the count or byte cap is reached, but always at least one.
	end, size := ni, 0
	for end < len(c.log) && (MaxAppendEntries <= 0 || end-ni < MaxAppendEntries) {
		sz := entryOverhead + EntrySize(c.log[end].Command)
		if end > ni && MaxAppendBytes > 0 && size+sz > MaxAppendBytes {
			break
		}
		size += sz
		end++
	}
	c.newRequest(peer, ni-1, append([]LogEntry(nil), c.log[ni:end]...))
	if !c.probing[peer] {
		c.nextIndex[peer] = end
	}
}

// sendHeartbeat sends an empty MsgApp anchored at matchIndex, a position already
// known to match, so reordering or in-flight batches cannot make it fail.
func (c *core) sendHeartbeat(peer int) {
	c.newRequest(peer, c.matchIndex[peer], nil)
}

// release removes seq from peer's outstanding set and returns the request and
// whether it was there, i.e. whether the answer is current rather than stale.
func (c *core) release(peer int, seq uint64) (pendReq, bool) {
	if c.state != Leader {
		return pendReq{}, false
	}
	p := c.pending[peer]
	for i, r := range p {
		if r.seq == seq {
			c.pending[peer] = append(p[:i:i], p[i+1:]...)
			return r, true
		}
	}
	return pendReq{}, false
}

// resetToProbe drops all outstanding requests of peer (their answers become
// stale) and returns it to one-at-a-time probing from next.
func (c *core) resetToProbe(peer, next int) {
	c.probing[peer] = true
	c.pending[peer] = nil
	if next < c.matchIndex[peer]+1 {
		next = c.matchIndex[peer] + 1
	}
	if next < 1 {
		next = 1
	}
	c.nextIndex[peer] = next
}

func (c *core) onAppResp(m Message) {
	if m.Term > c.currentTerm {
		c.becomeFollower(m.Term)
		c.resetElection()
		return
	}
	if c.state != Leader || c.currentTerm != m.Term {
		return // stale answer
	}
	peer := m.From
	pr, current := c.release(peer, m.Seq)
	if m.Success {
		// Valid even for a stale answer: within this term the follower really
		// holds the leader's log up to Match.
		match := m.Match
		if match >= len(c.log) {
			return // cannot match beyond our own log: malformed
		}
		if match > c.matchIndex[peer] {
			c.matchIndex[peer] = match
		}
		if c.nextIndex[peer] < c.matchIndex[peer]+1 {
			c.nextIndex[peer] = c.matchIndex[peer] + 1
		}
		if current && c.probing[peer] && match+1 >= c.nextIndex[peer] {
			c.probing[peer] = false // log match confirmed: start pipelining
		}
		c.advanceCommit()
		c.replicate(peer, false) // refill the window
		return
	}
	if !current {
		return // a rejection from before the last reset says nothing new
	}
	ni := pr.prev + 1
	next := m.ConflictIndex
	if m.ConflictTerm != -1 {
		for i := len(c.log) - 1; i >= 1; i-- {
			if c.log[i].Term == m.ConflictTerm {
				next = i + 1
				break
			}
			if c.log[i].Term < m.ConflictTerm {
				break
			}
		}
	}
	if next >= ni {
		next = ni - 1
	}
	c.resetToProbe(peer, next)
	if c.nextIndex[peer] < ni {
		c.replicate(peer, false) // retry immediately with the backed-off index
	}
}

func (c *core) handleApp(m Message) {
	resp := Message{Type: MsgAppResp, To: m.From, Seq: m.Seq, Term: c.currentTerm}
	if m.Term < c.currentTerm || m.PrevLogIndex < 0 {
		c.send(resp) // stale leader (it learns our term) or malformed
		return
	}
	c.becomeFollower(m.Term) // also demotes a same-term candidate
	resp.Term = c.currentTerm
	c.resetElection()

	if m.PrevLogIndex >= len(c.log) {
		resp.ConflictIndex, resp.ConflictTerm = len(c.log), -1
		c.send(resp)
		return
	}
	if t := c.log[m.PrevLogIndex].Term; t != m.PrevLogTerm {
		resp.ConflictTerm = t
		i := m.PrevLogIndex
		for i > 1 && c.log[i-1].Term == t {
			i--
		}
		resp.ConflictIndex = i
		c.send(resp)
		return
	}

	// Truncate only on a real conflict; matching entries (duplicates or
	// reordered messages) are left alone.
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + 1 + i
		if idx < len(c.log) {
			if c.log[idx].Term == e.Term {
				continue
			}
			c.log = c.log[:idx]
		}
		rest := append([]LogEntry(nil), m.Entries[i:]...)
		c.log = append(c.log, rest...)
		c.stage(Record{Kind: recAppend, Index: idx, Entries: rest})
		break
	}

	lastNew := m.PrevLogIndex + len(m.Entries)
	if m.LeaderCommit > c.commitIndex {
		cm := m.LeaderCommit
		if lastNew < cm {
			cm = lastNew
		}
		c.setCommit(cm)
	}
	resp.Success = true
	resp.Match = lastNew
	c.send(resp)
}
