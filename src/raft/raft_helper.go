package raft

import "fmt"

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{index=%d, term=%d, cmd=%v}", e.Index, e.Term, e.Term)
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

func (log *LogEntries) Truncate(index int) {
	if index < 0 {
		index += log.Size()
	}
	buffer := ([]LogEntry)(*log)
	*log = LogEntries(buffer[0 : index-buffer[0].Index])
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
