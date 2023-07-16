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

type Log struct {
	buffer []LogEntry
}

func MakeLog(entries []LogEntry) Log {
	res := Log{}
	if entries == nil {
		res.buffer = make([]LogEntry, 1)
		res.buffer[0] = LogEntry{
			Term:    0,
			Index:   0,
			Command: nil,
		}
	} else {
		res.buffer = entries
	}
	return res
}

func (log *Log) Size() int {
	return log.buffer[0].Index + len(log.buffer)
}

func (log *Log) At(index int) *LogEntry {
	if index < 0 {
		index += log.Size()
	}
	return &log.buffer[index-log.buffer[0].Index]
}

func (log *Log) Slice(begin, end int) []LogEntry {
	if begin < 0 {
		begin += log.Size()
	}
	if end < 0 {
		end += log.Size()
	}
	return log.buffer[begin-log.buffer[0].Index : end-log.buffer[0].Index]
}

func (log *Log) Append(entry LogEntry) {
	log.buffer = append(log.buffer, entry)
}

func (log *Log) Extend(entries []LogEntry) {
	log.buffer = append(log.buffer, entries...)
}

func (log *Log) Truncate(index int) {
	if index < 0 {
		index += log.Size()
	}
	log.buffer = log.buffer[0 : index-log.buffer[0].Index]
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
