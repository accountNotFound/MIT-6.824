package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seqNum   int64

	// NOTE: the leaderId is just the ClientEnd's index and may not eqaul to KVServer.me !!!
	leaderId int
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
		clientId: nrand(),
		seqNum:   0,
		leaderId: 0,
	}
	DPrintf("client [%d] created", ck.clientId)
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
	return ck.request(key, "", OpGet)
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
	ck.request(key, value, OpType(op))
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) request(key, value string, op OpType) string {
	leaderId := ck.leaderId
	for {
		req := &Request{
			Header: Header{
				ClientId: ck.clientId,
				SeqNum:   ck.seqNum,
			},
			Op:    op,
			Key:   key,
			Value: value,
		}
		rsp := &Response{}
		DPrintf("client [%d]: send to (%d), req=%s", ck.clientId, ck.leaderId, req.String())
		if ok := ck.servers[ck.leaderId].Call("KVServer.Access", req, rsp); !ok {
			DPrintf("client [%d]: call (%d) fail", ck.clientId, ck.leaderId)
			if ck.leaderId = (ck.leaderId + 1) % len(ck.servers); ck.leaderId == leaderId {
				time.Sleep(300 * time.Millisecond)
			}
			continue
		}
		DPrintf("client [%d]: recv from (%d), rsp=%s, req=%s", ck.clientId, ck.leaderId, rsp.String(), req.String())
		if ck.leaderId != int(rsp.ServerId) {
			// for clear log, the index of servers doesn't equal to server.me
			ck.servers[ck.leaderId], ck.servers[int(rsp.ServerId)] =
				ck.servers[int(rsp.ServerId)], ck.servers[ck.leaderId]
			ck.leaderId = (ck.leaderId - 1 + len(ck.servers)) % len(ck.servers)
		}
		switch rsp.Error {
		case NoErr:
			ck.seqNum++
			return rsp.Value
		case ErrNoKey:
			ck.seqNum++
			return ""
		case ErrWrongLeader, ErrTimeout, ErrOutOfDate:
			if ck.leaderId = (ck.leaderId + 1) % len(ck.servers); ck.leaderId == leaderId {
				time.Sleep(300 * time.Millisecond)
			}
		default:
			panic("unkown error")
		}
	}
}
