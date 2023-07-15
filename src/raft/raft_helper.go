package raft

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Log struct {
	Buffer []LogEntry
	Offset int
}

func (log *Log) Size() int {
	return log.Offset + len(log.Buffer)
}

func (log *Log) At(index int) *LogEntry {
	if index < 0 {
		index += len(log.Buffer)
	}
	return &log.Buffer[index-log.Offset]
}

func (log *Log) Slice(begin, end int) []LogEntry {
	if begin < 0 {
		begin += len(log.Buffer)
	}
	if end < 0 {
		end += len(log.Buffer)
	}
	return log.Buffer[begin-log.Offset : end-log.Offset]
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
