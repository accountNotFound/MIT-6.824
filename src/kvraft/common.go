package kvraft

import (
	"fmt"
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

type Err string

type OpType string

type Header struct {
	ClientId int64
	ServerId int64 // for debug
	SeqNum   int64
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
	if req.Op == OpGet {
		return fmt.Sprintf("{seq=%v op=%v key='%v'}", req.SeqNum, req.Op, req.Key)
	}
	return fmt.Sprintf("{seq=%v op=%v key='%v' value='%v'}", req.SeqNum, req.Op, req.Key, req.Value)
}

func (rsp *Response) String() string {
	return fmt.Sprintf("{value='%v', err=%v, server=%d}", rsp.Value, rsp.Error, rsp.ServerId)
}
