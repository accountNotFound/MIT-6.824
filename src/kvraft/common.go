package kvraft

import (
	"fmt"
	"time"
)

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

// the timeout control for network package
const TTL = time.Duration(200) * time.Millisecond

type Err string

type OpType string

type Header struct {
	ClientId int64
	ServerId int64 // for debug
	SeqNum   int64
	CreateAt time.Time
}

type Request struct {
	Header
	Op    OpType
	Key   string
	Value string
}

type Response struct {
	Header
	Value string
	Error Err
}

func (req *Request) String() string {
	return fmt.Sprintf("{seq=%v op=%v key='%v' value='%v'}", req.SeqNum, req.Op, req.Key, req.Value)
}

func (rsp *Response) String() string {
	return fmt.Sprintf("{value='%v', err=%v}", rsp.Value, rsp.Error)
}
