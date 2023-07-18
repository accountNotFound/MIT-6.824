package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	log         LogEntries

	commitIndex      int
	lastApplied      int
	state            RaftState
	voteGrantedCount int
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf("[%d] persist, log=%v", rf.me, rf.log)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.voteFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("load rf state failed: %v")
	}
	DPrintf("[%d] reload, log=%v", rf.me, rf.log)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ExpectNextIndex int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	location := "RequestVote"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm || rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if args.LastLogTerm > rf.log.At(-1).Term ||
			args.LastLogTerm == rf.log.At(-1).Term && args.LastLogIndex >= rf.log.At(-1).Index {
			rf.useUpdate(func() string {
				rf.setTerm(args.Term)
				rf.voteFor = args.CandidateId
				return location
			})
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			return
		}
	}
	if args.Term > rf.currentTerm {
		rf.useUpdate(func() string {
			rf.setTerm(args.Term)
			return location
		})
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	location := "AppendEntries"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.useUpdate(func() string {
			rf.setTerm(args.Term)
			return location
		})
	}
	rf.resetElectionTimer()
	if args.PrevLogIndex >= rf.log.Size() {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ExpectNextIndex = rf.log.Size()
		return
	}
	if rf.log.At(args.PrevLogIndex).Term != args.PrevLogTerm {
		prevTermIndex := args.PrevLogIndex
		for prevTermIndex >= 0 && rf.log.At(prevTermIndex).Term == rf.log.At(args.PrevLogIndex).Term {
			prevTermIndex--
		}
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ExpectNextIndex = prevTermIndex + 1
		return
	}
	if args.Entries != nil {
		diffTermIndex := args.PrevLogIndex + 1
		for ; diffTermIndex < rf.log.Size() && diffTermIndex < args.Entries.Size(); diffTermIndex++ {
			if rf.log.At(diffTermIndex).Term != args.Entries.At(diffTermIndex).Term {
				break
			}
		}
		rf.useUpdate(func() string {
			rf.log.Truncate(diffTermIndex)
			rf.log.Extend(args.Entries.Slice(diffTermIndex, args.Entries.Size()))
			return location
		}, true)
	}
	rf.commitIndex = Max(rf.commitIndex, args.LeaderCommit)
	reply.Term, reply.Success = rf.currentTerm, true
	reply.ExpectNextIndex = rf.log.Size()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	location := "sendRequestVote"
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.useUpdate(func() string {
			rf.setTerm(reply.Term)
			return location
		})
		return
	}
	if reply.VoteGranted {
		DPrintf("[%d] get vote from [%d]", rf.me, server)
		rf.voteGrantedCount++
		if rf.voteGrantedCount > len(rf.peers)/2 && rf.state != Leader {
			rf.useUpdate(func() string {
				rf.state = Leader
				// see figure 8
				rf.log.Append(LogEntry{
					Term:    rf.currentTerm,
					Index:   rf.log.At(-1).Index + 1,
					Command: nil,
				})
				for i := range rf.nextIndex {
					rf.nextIndex[i] = Min(rf.nextIndex[i], rf.log.Size())
				}
				return location
			})
			rf.startHeartbeat()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	location := "sendAppendEntries"
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.useUpdate(func() string {
			rf.setTerm(reply.Term)
			return location
		})
	}
	debug := fmt.Sprintf("[%d] update next[%d]: %d->%d",
		rf.me, server, rf.nextIndex[server], reply.ExpectNextIndex)
	rf.nextIndex[server] = reply.ExpectNextIndex
	if reply.Success {
		median := Median(rf.nextIndex)
		// see figure 8
		if median-1 > rf.commitIndex && rf.log.At(median-1).Term == rf.currentTerm {
			debug = fmt.Sprintf("%s, update commit: %d->%d", debug, rf.commitIndex, median-1)
			rf.commitIndex = median - 1
		}
	}
	DPrintf(debug)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	rf.useUpdate(func() string {
		rf.log.Append(LogEntry{
			Term:    rf.currentTerm,
			Index:   rf.log.At(-1).Index + 1,
			Command: command,
		})
		return "Start"
	}, true)
	DPrintf("[%d] ----------- start %s", rf.me, rf.log.At(-1).String())
	return rf.log.At(-1).Index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("[%d] ----------- crash", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// call with lock
func (rf *Raft) startHeartbeat() {
	DPrintf("[%d] startHeartbeat", rf.me)
	rf.resetHeartbeatTimer()
	for i := range rf.peers {
		if i == rf.me {
			rf.nextIndex[rf.me] = rf.log.Size()
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.log.At(rf.nextIndex[i] - 1).Index,
			PrevLogTerm:  rf.log.At(rf.nextIndex[i] - 1).Term,
			Entries:      rf.log.Slice(rf.nextIndex[i], rf.log.Size()),
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		go rf.sendAppendEntries(i, args, reply)
	}
}

// call with lock
func (rf *Raft) startElection() {
	DPrintf("[%d] startElection", rf.me)
	location := "startElection"
	rf.resetElectionTimer()
	rf.useUpdate(func() string {
		rf.currentTerm++
		rf.state = Candidate
		return location
	})
	rf.voteFor = rf.me
	rf.voteGrantedCount = 1
	for i := range rf.peers {
		if i == rf.me {
			DPrintf("[%d] vote for itself", rf.me)
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log.At(-1).Index,
			LastLogTerm:  rf.log.At(-1).Term,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		currentTerm:      0,
		voteFor:          0, // set to -1 below to avoid labgob warning
		log:              make(LogEntries, 0),
		commitIndex:      0,
		lastApplied:      0,
		state:            Follower,
		voteGrantedCount: 0,
		electionTimer:    time.NewTimer(1000 * time.Second),
		heartbeatTimer:   time.NewTimer(1000 * time.Second),
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
	}

	if rf.persister.RaftStateSize() == 0 {
		rf.voteFor = -1
		rf.log.Append(LogEntry{
			Term:    0,
			Index:   0,
			Command: nil,
		})
	} else {
		// initialize from state persisted before a crash
		rf.readPersist(persister.ReadRaftState())
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.Size()
	}

	// start ticker goroutine to start elections
	go func() {
		rf.resetElectionTimer()
		for !rf.killed() {
			<-rf.electionTimer.C
			rf.mu.Lock()
			if rf.state != Leader {
				rf.startElection()
			}
			rf.mu.Unlock()
		}
	}()
	go func() {
		rf.resetHeartbeatTimer()
		for !rf.killed() {
			<-rf.heartbeatTimer.C
			rf.mu.Lock()
			if rf.state == Leader {
				rf.startHeartbeat()
			}
			rf.mu.Unlock()
		}
	}()
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			rf.mu.Unlock()

			if rf.lastApplied == commitIndex {
				time.Sleep(time.Duration(50) * time.Millisecond)
				continue
			}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log.At(i).Command,
					CommandIndex: rf.log.At(i).Index,
					// TODO: 2D snapshot
				}
				DPrintf("[%d] ----------- apply %s", rf.me, rf.log.At(-1))
			}
			rf.lastApplied = commitIndex
		}
	}()

	return rf
}

func (rf *Raft) useUpdate(convert func() string, disableLogging ...bool) {
	if len(disableLogging) > 0 && disableLogging[0] {
		convert()
		rf.persist()
		return
	}
	before := rf.String()
	location := convert()
	after := rf.String()
	DPrintf("[%d] %s -> %s in %s", rf.me, before, after, location)
	rf.persist()
}

func (rf *Raft) setTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.voteFor = -1
		rf.voteGrantedCount = 0
		rf.resetElectionTimer()
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	d := 300 + rand.Int63n(150)
	rf.electionTimer.Reset(time.Duration(d) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	d := 120
	rf.heartbeatTimer.Reset(time.Duration(d) * time.Millisecond)
}

func (rf *Raft) String() string {
	return fmt.Sprintf("{state=%s, term=%d}", rf.state.String(), rf.currentTerm)
}
