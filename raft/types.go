// Package raft is a from-scratch implementation of the Raft consensus protocol.
package raft

import (
	"errors"
)

// Errors returned by Propose.
var (
	ErrNotLeader       = errors.New("raft: not the leader")
	ErrProposalDropped = errors.New("raft: too many uncommitted entries, proposal dropped")
	ErrStopped         = errors.New("raft: node stopped")
	ErrNilCommand      = errors.New("raft: nil command is reserved for the leader no-op")
)

// Node roles.
const (
	Follower = iota
	Candidate
	Leader
)

// LogEntry is one replicated log record. Command == nil is reserved for the
// no-op entry a new leader appends; it is never delivered as a valid command.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg is delivered to the upper state machine, in log order.
// CommandValid is false for leader no-op entries.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// MsgType identifies a Message. Replication and voting are one-way messages;
// an answer is a separate message travelling the other way, like etcd's streams.
type MsgType uint8

const (
	MsgVote     MsgType = iota + 1 // candidate -> peer: request a vote
	MsgVoteResp                    // peer -> candidate
	MsgApp                         // leader -> follower: entries and/or heartbeat
	MsgAppResp                     // follower -> leader
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "vote"
	case MsgVoteResp:
		return "voteResp"
	case MsgApp:
		return "app"
	case MsgAppResp:
		return "appResp"
	}
	return "?"
}

// Message is the single wire message. Which fields matter depends on Type.
// Messages are immutable once created and may be shared between copies.
type Message struct {
	Type     MsgType
	From, To int
	Term     int // sender's term

	// MsgVote
	LastLogIndex int
	LastLogTerm  int

	// MsgApp (Seq is echoed in the matching MsgAppResp)
	Seq          uint64
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int

	// MsgVoteResp
	Granted bool

	// MsgAppResp. On success Match is the highest index now known to match the
	// leader's log; on failure ConflictIndex/ConflictTerm let the leader back up
	// quickly (ConflictTerm == -1 means the follower's log is too short).
	Success       bool
	Match         int
	ConflictIndex int
	ConflictTerm  int
}

// Transport carries messages to peers, like etcd's streams: Send is one-way and
// asynchronous. It must not block (it is called on the node's event loop),
// must bound its own queues and goroutines, and may drop messages (a full queue,
// a broken connection) - Raft tolerates loss, duplication and reordering. When a
// connection to a peer breaks the transport should tell the node through
// Endpoint.ReportUnreachable so replication to it restarts from a safe point.
type Transport interface {
	Send(m Message)
}

// Endpoint is the receiving side of a node, which a transport calls into.
// *Raft implements it.
type Endpoint interface {
	// Step delivers an incoming message to the node.
	Step(m Message)
	// ReportUnreachable tells the node that sending to peer failed.
	ReportUnreachable(peer int)
}
