package kvraft

const (
	NoErr          = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutOfDate   = "ErrOutOfDate"
	ErrTimeout     = "ErrTimeout"
)

const (
	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

type Err string

type Header struct {
	ClientId  int
	RequestId int
}

// Put or Append
type PutAppendArgs struct {
	Header
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Header
	Err Err
}

type GetArgs struct {
	Header
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Header
	Err   Err
	Value string
}
