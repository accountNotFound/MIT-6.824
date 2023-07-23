package raft

import "fmt"

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{index=%d, term=%d, cmd=%v}", e.Index, e.Term, e.Command)
}

type LogEntries []LogEntry

func (log *LogEntries) Size() int {
	buffer := ([]LogEntry)(*log)
	return buffer[0].Index + len(buffer)
}

func (log *LogEntries) At(index int) *LogEntry {
	if index < 0 {
		index += log.Size()
	}
	buffer := ([]LogEntry)(*log)
	return &buffer[index-buffer[0].Index]
}

func (log *LogEntries) Head() *LogEntry {
	buffer := ([]LogEntry)(*log)
	return &buffer[0]
}

func (log *LogEntries) Slice(begin, end int) LogEntries {
	if begin < 0 {
		begin += log.Size()
	}
	if end < 0 {
		end += log.Size()
	}
	if begin >= end {
		return nil
	}
	buffer := ([]LogEntry)(*log)
	return buffer[begin-buffer[0].Index : end-buffer[0].Index]
}

func (log *LogEntries) Append(entry LogEntry) {
	buffer := ([]LogEntry)(*log)
	*log = LogEntries(append(buffer, entry))
}

func (log *LogEntries) Extend(entries []LogEntry) {
	buffer := ([]LogEntry)(*log)
	*log = LogEntries(append(buffer, entries...))
}

// drop all entries after given index, including this one
func (log *LogEntries) Truncate(index int) {
	if index < 0 {
		index += log.Size()
	}
	buffer := ([]LogEntry)(*log)
	*log = LogEntries(buffer[0 : index-buffer[0].Index])
}

// drop all entries before given index, not include this one
func (log *LogEntries) Trim(index int) {
	if index < 0 {
		index += log.Size()
	}
	buffer := ([]LogEntry)(*log)
	*log = LogEntries(buffer[index-buffer[0].Index:])
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s *RaftState) String() string {
	switch *s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unkown"
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ExpectNextIndex int
}

// install snapshot in just on rpc so that the implementation is more easily
type InstallSnapshotArgs struct {
	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}
