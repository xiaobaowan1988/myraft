// Package raft is a from-scratch implementation of the Raft consensus protocol.
package raft

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

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast-backtrack hints, valid when Success is false.
	// ConflictTerm == -1 means the follower's log is too short.
	ConflictIndex int
	ConflictTerm  int
}

// Transport sends RPCs to peers. ok == false means the call was lost or timed out.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (reply *RequestVoteReply, ok bool)
	AppendEntries(to int, args *AppendEntriesArgs) (reply *AppendEntriesReply, ok bool)
}

// Handler is the server side of the RPCs; *Raft implements it.
type Handler interface {
	RequestVote(args *RequestVoteArgs) *RequestVoteReply
	AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}
