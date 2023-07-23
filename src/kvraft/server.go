package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database    map[string]string
	connections map[int]chan interface{}
	retrycache  map[int]interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if rsp := kv.check(args.ClientId); reply != nil {
		if rsp.(GetReply).RequestId < args.RequestId {
			reply.Header, reply.Err = args.Header, ErrOutOfDate
			return
		}
		*reply = rsp.(GetReply)
		return
	}
	conn := kv.submit(*args)
	if conn == nil {
		reply.Header, reply.Err = args.Header, ErrWrongLeader
		return
	}
	*reply = (<-conn).(GetReply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if rsp := kv.check(args.ClientId); reply != nil {
		if rsp.(GetReply).RequestId < args.RequestId {
			reply.Header, reply.Err = args.Header, ErrOutOfDate
			return
		}
		*reply = rsp.(PutAppendReply)
		return
	}
	conn := kv.submit(*args)
	if conn == nil {
		reply.Header, reply.Err = args.Header, ErrWrongLeader
		return
	}
	*reply = (<-conn).(PutAppendReply)
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

	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 16),
		dead:         0,
		maxraftstate: maxraftstate,
		database:     make(map[string]string),
		connections:  make(map[int]chan interface{}),
		retrycache:   make(map[int]interface{}),
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

func (kv *KVServer) check(clientId int) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply, ok := kv.retrycache[clientId]; !ok {
		return nil
	} else {
		return reply
	}

}

func (kv *KVServer) submit(args interface{}) chan interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		return nil
	}
	kv.connections[index] = make(chan interface{})
	return kv.connections[index]
}

func (kv *KVServer) apply(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	conn, ok := kv.connections[applyMsg.CommandIndex]
	if !ok {
		return
	}

	switch args := applyMsg.Command.(type) {
	case PutAppendArgs:
		if _, ok := kv.database[args.Key]; !ok || args.Op == OpPut {
			kv.database[args.Key] = args.Value
		} else {
			kv.database[args.Key] += args.Value
		}
		kv.retrycache[args.ClientId] = PutAppendReply{
			Header: args.Header,
			Err:    NoErr,
		}
		conn <- kv.retrycache[args.ClientId]
	case GetArgs:
		if val, ok := kv.database[args.Key]; ok {
			kv.retrycache[args.ClientId] = GetReply{
				Header: args.Header,
				Err:    NoErr,
				Value:  val,
			}
		} else {
			kv.retrycache[args.ClientId] = GetReply{
				Header: args.Header,
				Err:    ErrNoKey,
			}
		}
		conn <- kv.retrycache[args.ClientId]
	}
}
