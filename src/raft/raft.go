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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Command
type Command struct {
	K  string
	OP string
	V  interface{}
}

type LogEntry struct {
	Command Command
	Term    int
}

type Role uint16

const (
	Leader    Role = 1
	Folower   Role = 2
	Candidate Role = 3
	Nobody    Role = 0

	minTimeout = 150
	maxTimeout = 300

	HeartBeatInterval = 100
)

func randTimeOut() time.Duration {
	return time.Duration(rand.Float32()*(maxTimeout-minTimeout)+minTimeout) * time.Millisecond
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// electionChan chan interface{}
	roleChangeChan chan Role
	// 所有服务器上持久存在的
	currentTerm int // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int // 在当前获得选票的候选人的 Id
	log         []LogEntry

	// 所有服务器经常变的
	commitIndex int //已知的最大的已经被提交的日志条目的索引值
	lastApplied int //最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	//在领导人里经常改变的 （选举后重新初始化）
	nextIndex  []int //对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int //对于每一个服务器，已经复制给他的日志的最高索引值

	wg               sync.WaitGroup
	heartBeatTime    time.Time
	heartBeatTimeOut time.Duration
}

// GetState is
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateID  int // 请求选票的候选人的 Id
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

// RequestVoteReply is
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// RequestVote is
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.heartBeatTime = time.Now()
	rf.mu.Unlock()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID && args.LastLogIndex >= rf.lastApplied) || args.Term >= rf.currentTerm {
		// 候选人的日志至少和自己一样新，那么就投票给他
		if !rf.isFollower() {
			rf.changeRole(Folower)
		}
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.mu.Unlock()
		// rf.changeRole(Folower)
		reply.Term = args.Term
		reply.VoteGranted = true
		DPrintf("Raft Node %d vote for: %d", rf.me, args.CandidateID)
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ProcessVote() Role {
	if !rf.isCandicate() {
		return rf.role
	}
	DPrintf("Raft Node %d start request votes", rf.me)
	rf.mu.Lock()
	rf.currentTerm++
	DPrintf("Raft Node %d become term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	// 发起选举请求
	args := &RequestVoteArgs{}
	args.CandidateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.lastApplied
	if len(rf.log) == 0 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.log[rf.lastApplied].Term
	}
	reply := &RequestVoteReply{}
	// 为自己投票
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.mu.Unlock()
	voteCount := 1
	for i := range rf.peers {
		if i != rf.me {
			rf.wg.Add(1)
			go func(peerIndex int) {
				defer rf.wg.Done()
				ok := rf.sendRequestVote(peerIndex, args, reply)
				if ok {
					DPrintf("Raft Node %d requestVote to node %d res: %v", rf.me, peerIndex, reply)
					if reply.VoteGranted {
						voteCount++
					}
				} else {
					DPrintf("Raft Node %d requestVote to node %d timeout", rf.me, peerIndex)
				}
			}(i)
		}
	}
	rf.wg.Wait()
	DPrintf("Raft Node %d received %d votes, total: %d votes", rf.me, voteCount, len(rf.peers))
	if voteCount*2 > len(rf.peers) {
		DPrintf("Raft Node %d will become leader", rf.me)
		rf.changeRole(Leader)
		return Leader
	}
	DPrintf("Raft Node %d voted failed, will be %d", rf.me, rf.role)
	return rf.role
}

type AppendEntriesArgs struct {
	Term         int        //领导人的任期号
	LeaderID     int        //领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int        //新的日志条目紧随之前的索引值
	PrevLogTerm  int        //prevLogIndex 条目的任期号
	Entries      []LogEntry //准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        //领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号，以便于候选人去更新自己的任期号
	Success bool // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
}

// AppendEntries is
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	DPrintf("Raft Node %d received RPC: AppendEntries at %v", rf.me, now)
	rf.mu.Lock()
	rf.heartBeatTime = now
	rf.mu.Unlock()
	DPrintf("Raft Node %d heartbeat time change to %v", rf.me, rf.heartBeatTime)
	if args.Term != rf.currentTerm {
		if rf.currentTerm > args.Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			// rf.changeRole(Candidate)
			return
		}
		if !rf.isFollower() {
			rf.changeRole(Folower)
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
	reply.Term = args.Term
	reply.Success = true
	return
	// heartbeat
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Raft Node %d start send RPC:sendAppendEntries for node %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ProcessAppendEntries is
// 发送附加日志请求
func (rf *Raft) ProcessAppendEntries(entry []LogEntry) {
	DPrintf("Raft Node %d start AppendEntries", rf.me)
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.Entries = entry

	reply := &AppendEntriesReply{}
	succeedCount := 0
	for i := range rf.peers {
		if i != rf.me {
			rf.wg.Add(1)
			go func(peerIndex int) {
				defer rf.wg.Done()
				ok := rf.sendAppendEntries(peerIndex, args, reply)
				if ok {
					DPrintf("Raft Node %d AppendEntries to %d res: %v", rf.me, peerIndex, reply)
					if reply.Success {
						succeedCount++
					} else {
						if reply.Term > rf.currentTerm {
							rf.changeRole(Folower)
						}
					}
				} else {
					DPrintf("Raft Node %d AppendEntries to %d timeout", rf.me, peerIndex)
				}
			}(i)
		}
	}
	rf.wg.Wait()
	DPrintf("Raft Node %d received %d ack, total: %d request", rf.me, succeedCount, len(rf.peers)-1)
	if succeedCount*2 > len(rf.peers) {
		// TODO: commit
		DPrintf("Raft Node %d ProcessAppendEntries succeed", rf.me)
	} else {
		DPrintf("Raft Node %d ProcessAppendEntries failed", rf.me)
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.changeRole(Nobody)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	if rf.role == Leader {
		return true
	}
	return false
}

func (rf *Raft) isCandicate() bool {
	if rf.role == Candidate {
		return true
	}
	return false
}

func (rf *Raft) isFollower() bool {
	if rf.role == Folower {
		return true
	}
	return false
}

func (rf *Raft) changeRole(r Role) {
	DPrintf("Raft Node %d change to %v", rf.me, r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = r
	rf.roleChangeChan <- r
}

func (rf *Raft) detectHeartBeatTimeOut() bool {
	for {
		if !rf.isFollower() {
			return false
		}
		now := time.Now()
		t := now.Sub(rf.heartBeatTime)
		DPrintf("Raft Folower Node %d heartbeat cost %v", rf.me, t)
		if t > rf.heartBeatTimeOut {
			DPrintf("Raft Folower Node %d received heartbeat timeout", rf.me)
			// rf.changeRole(Candidate)
			return true
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func (rf *Raft) resetHeartBeatTimeOut() {
	now := time.Now()
	DPrintf("Raft Node %d set heartbeat %v", rf.me, now)
	rf.mu.Lock()
	rf.heartBeatTime = now
	rf.mu.Unlock()
}

func (rf *Raft) electionLoop() {
	for {
		time.Sleep(randTimeOut())
		var r Role
		go func() {
			r = rf.ProcessVote()
			DPrintf("Raft Node %d process vote done: %v, %v", rf.me, rf.role, r)
		}()
		if r != Candidate {
			return
		}
	}
}

func (rf *Raft) heartBeatLoop() {
	for {
		if rf.role != Leader {
			return
		}
		rf.ProcessAppendEntries([]LogEntry{{Command{}, rf.currentTerm}})
		time.Sleep(HeartBeatInterval * time.Millisecond)
	}
}

func (rf *Raft) changeRoleEventHandler() {
	select {
	case i := <-rf.roleChangeChan:
		switch i {
		case Candidate:
			go func() {
				rf.electionLoop()
			}()
		case Leader:
			go func() {
				rf.heartBeatLoop()
			}()
			// DPrintf("Raft Node %d change to %v", rf.me, i)
		case Folower:
			rf.resetHeartBeatTimeOut()
			go func() {
				isTimeOut := rf.detectHeartBeatTimeOut()
				if isTimeOut {
					rf.changeRole(Candidate)
				}
				return
			}()
		default:
			return
		}
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("Start Raft node: %d", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// rf.log = []Command{}
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.role = Folower
	// rf.electionChan = make(chan struct{})
	rf.roleChangeChan = make(chan Role, 1)
	rf.heartBeatTime = time.Now()
	rf.heartBeatTimeOut = randTimeOut()
	DPrintf("Raft Node %d heartbeat timeout is %v", rf.me, rf.heartBeatTimeOut)

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// rf.currentTerm = rf.log[-1].term

	// 选举倒计时循环
	rf.changeRole(Folower)

	go func() {
		for rf.dead != 1 {
			rf.changeRoleEventHandler()
		}
	}()
	return rf
}
