package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

const RequestRetryInterval = time.Duration(100) * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int64
	SeqNum   int64

	// NOTE: the LeaderId is just the ClientEnd's index and may not eqaul to KVServer.me !!!
	LeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:  servers,
		ClientId: nrand(),
		SeqNum:   0,
		LeaderId: 0,
	}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	req := &Request{
		Header: Header{
			ClientId: ck.ClientId,
			SeqNum:   ck.SeqNum,
			CreateAt: time.Now(),
		},
		Key: key,
		Op:  OpGet,
	}
	rsp := &Response{}
	return ck.request(req, rsp)
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := &Request{
		Header: Header{
			ClientId: ck.ClientId,
			SeqNum:   ck.SeqNum,
			CreateAt: time.Now(),
		},
		Key:   key,
		Value: value,
		Op:    OpType(op),
	}
	rsp := &Response{}
	ck.request(req, rsp)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) request(req *Request, rsp *Response) string {
	for {
		DPrintf("client [%d]: send req=%s", ck.ClientId, req.String())
		if ok := ck.servers[ck.LeaderId].Call("KVServer.Access", req, rsp); !ok {
			time.Sleep(RequestRetryInterval)
			continue
		}
		DPrintf("client [%d]: recv rsp=%s, server=%d, req=%s", ck.ClientId, rsp.String(), rsp.ServerId, req.String())
		switch rsp.Error {
		case NoErr:
			ck.SeqNum++
			return rsp.Value
		case ErrNoKey:
			ck.SeqNum++
			return ""
		case ErrWrongLeader:
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			time.Sleep(RequestRetryInterval)
		case ErrOutOfDate:
			return ""
		case ErrTimeout:
		default:
		}
	}
}
