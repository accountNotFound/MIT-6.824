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

type Model struct {
	Header
	Body interface{} // it's type should be GetReply or PutAppendReply
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
	connections map[int]chan Model
	retrycache  map[int]Model
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	req := Model{
		Header: args.Header,
		Body:   args,
	}
	success := func(res interface{}) {
		*reply = res.(GetReply)
	}
	fail := func(err Err) {
		reply.Err = err
	}
	kv.handle(req, success, fail)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	req := Model{
		Header: args.Header,
		Body:   args,
	}
	success := func(res interface{}) {
		*reply = res.(PutAppendReply)
	}
	fail := func(err Err) {
		reply.Err = err
	}
	kv.handle(req, success, fail)
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
	labgob.Register(Model{})

	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 16),
		dead:         0,
		maxraftstate: maxraftstate,
		database:     make(map[string]string),
		connections:  make(map[int]chan Model),
		retrycache:   make(map[int]Model),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh
			kv.apply(&applyMsg)
		}
	}()

	return kv
}

func (kv *KVServer) check(req Model, success func(interface{}), fail func(Err)) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if rsp, ok := kv.retrycache[req.ClientId]; !ok {
		return false
	} else if req.RequestId < rsp.RequestId {
		fail(ErrOutOfDate)
		return true
	} else if req.RequestId == rsp.RequestId {
		success(rsp.Body)
		return true
	} else {
		return false
	}

}

func (kv *KVServer) submit(req Model) (chan Model, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if index, _, isLeader := kv.rf.Start(req); !isLeader {
		return nil, -1
	} else {
		kv.connections[index] = make(chan Model)
		return kv.connections[index], index
	}
}

func (kv *KVServer) handle(req Model, success func(interface{}), fail func(Err)) {
	if kv.check(req, success, fail) {
		return
	}
	conn, index := kv.submit(req)
	if index == -1 {
		return
	}
	select {
	case rsp := <-conn:
		success(rsp.Body)
	case <-time.After(time.Duration(10) * time.Second):
		fail(ErrTimeout)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.connections, index)
}

func (kv *KVServer) apply(applyMsg *raft.ApplyMsg) {
	req := applyMsg.Command.(Model)
	switch args := req.Body.(type) {
	case PutAppendArgs:
		success := func(res interface{}) {
			if conn, ok := kv.connections[applyMsg.CommandIndex]; ok {
				conn <- Model{
					Header: req.Header,
					Body:   res.(PutAppendReply),
				}
			}
		}
		fail := func(err Err) {
			if conn, ok := kv.connections[applyMsg.CommandIndex]; ok {
				conn <- Model{
					Header: req.Header,
					Body:   PutAppendReply{Err: err},
				}
			}
		}
		if kv.check(req, success, fail) {
			return
		}
		kv.mu.Lock()
		kv.write(&args, success, fail)
		kv.mu.Unlock()
	case GetArgs:
		success := func(res interface{}) {
			if conn, ok := kv.connections[applyMsg.CommandIndex]; ok {
				conn <- Model{
					Header: req.Header,
					Body:   res.(GetReply),
				}
			}
		}
		fail := func(err Err) {
			if conn, ok := kv.connections[applyMsg.CommandIndex]; ok {
				conn <- Model{
					Header: req.Header,
					Body:   GetReply{Err: err},
				}
			}
		}
		if kv.check(req, success, fail) {
			return
		}
		kv.mu.Lock()
		kv.read(&args, success, fail)
		kv.mu.Unlock()
	}
}

// call with lock
func (kv *KVServer) write(args *PutAppendArgs, success func(interface{}), fail func(Err)) {
	if _, ok := kv.database[args.Key]; !ok || args.Op == OpPut {
		kv.database[args.Key] = args.Value
	} else {
		kv.database[args.Key] = args.Value
	}
	success(PutAppendReply{
		Err: NoErr,
	})
}

// call with lock
func (kv *KVServer) read(args *GetArgs, success func(interface{}), fail func(Err)) {
	if val, ok := kv.database[args.Key]; !ok {
		fail(ErrNoKey)
	} else {
		success(GetReply{
			Value: val,
		})
	}
}
