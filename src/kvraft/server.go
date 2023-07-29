package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) Access(req *Request, rsp *Response) {
	rsp.Header = Header{
		ClientId: req.ClientId,
		ServerId: int64(kv.me),
		SeqNum:   req.SeqNum,
	}
	if kv.check(req, rsp) {
		return
	}
	conn, term, index := kv.submit(req)
	if index == -1 {
		rsp.Error = ErrWrongLeader
		return
	}
	select {
	case result := <-conn:
		if currentTerm, _ := kv.rf.GetState(); currentTerm == term {
			rsp.Value, rsp.Error = result.Value, result.Error
		} else {
			rsp.Error = ErrWrongLeader
		}
	case <-time.After(3 * time.Second):
		rsp.Error = ErrTimeout
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.connections, int64(index))
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database    map[string]string
	connections map[int64]chan Response
	retrycache  map[int64]Response
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Header{})
	labgob.Register(Request{})
	labgob.Register(Response{})

	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 16),
		dead:         0,
		maxraftstate: maxraftstate,
		database:     make(map[string]string),
		connections:  make(map[int64]chan Response),
		retrycache:   make(map[int64]Response),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh
			req := applyMsg.Command.(Request)
			rsp := Response{
				Header: Header{
					ClientId: req.ClientId,
					ServerId: int64(kv.me),
					SeqNum:   req.SeqNum,
				},
			}
			if ok := kv.check(&req, &rsp); !ok {
				kv.apply(&req, &rsp)
			}
			kv.send(int64(applyMsg.CommandIndex), &rsp)
		}
	}()
	return kv
}

func (kv *KVServer) check(req *Request, rsp *Response) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cache, ok := kv.retrycache[req.ClientId]; !ok {
		return false
	} else if req.SeqNum < cache.SeqNum {
		rsp.Error = ErrOutOfDate
		return true
	} else if req.SeqNum == cache.SeqNum {
		rsp.Value, rsp.Error = cache.Value, cache.Error
		return true
	} else {
		return false
	}
}

func (kv *KVServer) submit(req *Request) (chan Response, int, int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if index, term, isLeader := kv.rf.Start(*req); !isLeader {
		return nil, -1, -1
	} else {
		DPrintf("server [%d]: submit req=%s", kv.me, req.String())
		kv.connections[int64(index)] = make(chan Response)
		return kv.connections[int64(index)], term, int64(index)
	}
}

func (kv *KVServer) apply(req *Request, rsp *Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch req.Op {
	case OpGet:
		if val, ok := kv.database[req.Key]; ok {
			rsp.Value, rsp.Error = val, NoErr
		} else {
			rsp.Error = ErrNoKey
		}
	case OpPut:
		kv.database[req.Key] = req.Value
		rsp.Error = NoErr
	case OpAppend:
		kv.database[req.Key] += req.Value
		rsp.Error = NoErr
	}
	kv.retrycache[req.ClientId] = *rsp
	if _, isLeader := kv.rf.GetState(); isLeader {
		DPrintf("server [%d]: apply req=%s", kv.me, req.String())
	}
}

func (kv *KVServer) send(index int64, rsp *Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if conn, ok := kv.connections[index]; ok {
		conn <- *rsp
	}
}
