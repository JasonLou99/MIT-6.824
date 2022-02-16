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
// 每次向日志提交一个新条目时，
// 每个Raft peer都应该向同一服务器中的服务(或测试者)发送一个ApplyMsg。

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// log条目成功提交时，每个peer通过发送ApplyMsg到同一个服务器上的上层服务中,这通过Make()函数中的applyCh通道
// 就是将command apply到上层服务
// 将CommandValid设置为true表示ApplyMsg包含一个新提交的日志条目。
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
// 2D会要求发送另一种消息（快照）
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

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int

	// 当前term内投票选定的候选者id，没有则为-1
	votedFor    int
	currentTerm int
	log         []Log
	// isLeader    bool
	// leaderId int

	// 已提交的最大日志index
	commitIndex int
	// 最后被执行的日志index
	lastApplied int
	// applyCh     chan ApplyMsg

	voteCount int

	// leader专有的变量
	// 每个服务器的下一个log index
	nextIndex []int
	// 每个服务器已经复制到的日志最高索引
	matchIndex []int

	// 选举超时时间
	// electionTimeOut int
	// 心跳频率
	heartbeatTime int
	// 最近一次心跳时间
	// lastHeartBeat time.Time

	// 用来触发事件的管道
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool
	chanApplyLog  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前的term，和该server是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

type AppendEntriesArgs struct {
	Term int
	// 可以让follower将client的请求重定向到leader
	LeaderId int
	// 新日志项之前日志的index
	PrevLogIndex int
	// 新日志项之前日志的term
	PrevLogTerm int
	// 存储的日志，如果是心跳rpc，则这里为空
	Entries []Log
	// leader commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// 用于leader更新自己的term（follower的term可能更大）
	Term int
	// follower匹配了PrevLog的index和Term则返回true
	Success bool
	// 用于更新下次检查的日志
	RetryIndex int
}

// AppendEntries RPC Handler
// 添加日志或者心跳通信
// term < currentTerm返回 false
// 自己在prevLogIndex处的日志的term与prevLogTerm不匹配时，返回 false
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Leader更新自己的Term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		// 更新term，根据日志情况考虑
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	// 重置心跳时间
	rf.chanHeartbeat <- true
	reply.Term = rf.currentTerm

	// 对日志进行检查、追加
	// rf的日志太少了
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.RetryIndex = rf.getLastLogIndex() + 1
		reply.Success = false
		return
	}
	// rf的日志足够 检查term是否一致
	// PrevLogIndex不能低于rf的最小的index，否则会越界
	if args.PrevLogIndex >= rf.log[0].Index && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 日志不一致，需要将PrevLog向前移动比较，这里直接越过了与PrevLog同term的一批日志
		term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex - 1; i >= rf.log[0].Index; i-- {
			if rf.log[i].Term != term {
				reply.RetryIndex = i + 1
				break
			}
		}
		// reply.RetryIndex = rf.log[args.PrevLogIndex].Index - 1
		reply.Success = false
		// return
	} else if args.PrevLogIndex >= rf.log[0].Index-1 {
		// PrevLog一致
		// 开始对Entries处理
		// if len(args.Entries) == 0 {
		// 	reply.Success = true
		// 	// return
		// }
		// 追加日志
		if len(args.Entries) > 0 {
			DPrintf("[%d](term %d, state %d) append logs", rf.me, rf.currentTerm, rf.state)
		}
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		reply.Success = true
		reply.RetryIndex = args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > rf.commitIndex {
			// 说明rf提交的日志太少了，需要commit log，日志一旦提交就需要apply
			DPrintf("[%d](term %d, state %d) update commitIndex", rf.me, rf.currentTerm, rf.state)
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			rf.chanApplyLog <- true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 网络不通、rf状态改变等直接返回ok
	// 无效心跳信息
	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		return ok
	}
	// reply的term更大，说明需要更新状态
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return ok
	}

	if !reply.Success {
		// 说明日志冲突（前一条日志不一致）
		// 更新nextIndex
		rf.nextIndex[server] = min(reply.RetryIndex, rf.getLastLogIndex())
		// return ok
	} else {
		// 日志追加成功
		// match过半 --> leader commit --> leader apply
		// matchIndex刷新到已追加日志的最后一条
		if len(args.Entries) > 0 {
			rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}

	// 返回失败，也有可能需要leader apply日志
	for i := rf.getLastLogIndex(); i > rf.commitIndex && rf.log[i].Term == rf.currentTerm; i-- {
		// 从后往前找到过半数节点已经提交的日志 如果找到了直接把这个之前的所有日志（直到上次apply的日志）全部apply
		// 然后就可以直接退出
		count := 0
		// 统计第i条log
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			rf.chanApplyLog <- true
			break
		}
	}

	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// RequestVote RPC参数结构，变量名首字母要大写
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// candidate的term
	Term int
	// candidate自己的Id
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// candidate可能需要自我更新（voter的term更大，那candidate就要自我更新）
	Term int
	// true表示同意投票
	VoteGranted bool
}

//
// example RequestVote RPC handler.
// 对RequestVote RPC的处理方法
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// DPrintf("%d starts RequestVote handler for %d", rf.me, args.CandidateId)
	// Your code here (2A, 2B).
	// 目标服务器调用该方法，处理投票请求
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// reject request with stale term number
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// become follower and update current term
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Election restriction
	// 比较两者日志新的程度
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		DPrintf("[%d](term %d, state %d) vote to [%d]", rf.me, rf.currentTerm, rf.state, args.CandidateId)
		// vote for the candidate
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}

	// if rf.currentTerm < args.Term {
	// 	// Candidate -> Follower
	// 	// Follower -> Follower
	// 	// term刷新
	// 	// 首先自己肯定不能再参与选举了
	// 	// 但是也不一定就投票给他，因为日志可能滞后，但是2A先不处理
	// 	rf.currentTerm = args.Term
	// 	rf.state = Follower
	// 	rf.votedFor = -1
	// }

	// reply.Term = rf.currentTerm
	// reply.VoteGranted = false

	// //Reply false if term < currentTerm or  voted in this term (§5.1)
	// if rf.currentTerm > args.Term || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
	// 	// 状态不改变，candidate或者follower
	// 	return
	// }

	// if rf.log[rf.commitIndex].Term > args.LastLogTerm || (args.LastLogTerm == rf.log[rf.commitIndex].Term && rf.commitIndex > args.LastLogIndex) {
	// 	// 本地日志更新，不投票 直接return
	// 	return
	// }

	// // 都没有问题，则可以投票
	// rf.votedFor = args.CandidateId
	// // rf.lastHeartBeat = time.Now()
	// // 通知投票成功，可能触发状态改变：Candidate->Follower
	// rf.chanGrantVote <- true
	// DPrintf("%d(term %d) vote to %d(term %d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	// // reply.Term = args.Term
	// reply.VoteGranted = true

}

// 检查candidate的日志是否更新
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[]. server是rf.peers[]内目标服务器的下标
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
// 传递给Call()的args和reply的类型必须与handler函数中声明的参数的类型相同(包括它们是否为指针)

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
// labrpc包模拟了一个有损耗的网络，在这个网络中，服务器可能是不可达的，
// 请求和响应可能会丢失。Call()发送请求并等待应答。如果在超时时间内收到回复，
// Call()返回true;否则Call()返回false。因此，Call()可能在一段时间内不会返回。
// 错误的返回可能是由于服务器死亡、无法访问的活动服务器、丢失的请求或丢失的应答。
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
// Call()保证返回(可能在延迟之后)除非服务器端的处理函数没有返回。
// 因此，没有必要在Call()周围实现您自己的超时机制。
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// reply传递要传地址&
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[%d](term %d, state %d) send RequestVote RPC to [%d]", rf.me, rf.currentTerm, rf.state, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 成功发送
	if ok {
		// 状态被修改，那么该票作废（*）
		if rf.state != Candidate || rf.currentTerm != args.Term {
			// invalid request
			return ok
		}
		if rf.currentTerm < reply.Term {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}
		if reply.VoteGranted {
			rf.voteCount++
			DPrintf("[%d](term %d, state %d) voteCount is %d", rf.me, rf.currentTerm, rf.state, rf.voteCount)
			if rf.voteCount > len(rf.peers)/2 {
				// win the election
				rf.state = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				// 选为新的Leader 需要创建nextIndex（Leader的专有变量） 每个都为自己的最新日志的后一条
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.chanWinElect <- true
			}
		}
	}

	return ok
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
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
// 使用Raft的服务想在下条命令start agreement（达成协议）以追加Raft的日志。若该服务器不是leader，
// 返回false。否则就会达成协议然后返回。不会保证这条命令一定写入日志，因为leader可能失败或者lose一场选举
// 甚至如果Raft的实例已经被kill，这个函数也应该返回。
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// 如果命令被提交，第一个返回值是command将会追加的位置索引。
// 第二个返回值是现在的term
// 如果server自己是leader，则第三个返回值为true
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// leader增加日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 不是Leader 直接返回false
	if rf.state != Leader {
		isLeader = false
	} else {
		log := Log{
			Command: command,
			Index:   rf.getLastLogIndex() + 1,
			Term:    rf.currentTerm,
		}
		DPrintf("[%d](term %d, state %d) get a new log {Command: %s, Index: %d, Term: %d}", rf.me, rf.currentTerm, rf.state, log.Command, log.Index, log.Term)
		rf.log = append(rf.log, log)
		rf.matchIndex[rf.me] = log.Index
		index = log.Index
		term = log.Term
		isLeader = true
	}

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// term, index := -1, -1
	// isLeader := (rf.state == Leader)

	// if isLeader {
	// 	term = rf.currentTerm
	// 	index = rf.getLastLogIndex() + 1
	// 	DPrintf("Leader %d(term %d) get a new log", rf.me, rf.currentTerm)
	// 	rf.log = append(rf.log, Log{Index: index, Term: term, Command: command})
	// 	// rf.persist()
	// }
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// tester不会暂停创造Raft的goroutine，但是确实会调用Kill方法。
// 你可以使用Killed()去检查Kill()是否已经调用。
// 这需要用锁
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	DPrintf("[%d] is killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// 清空所有值
	// rf = &Raft{}

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) election() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	// args.LastLogIndex = rf.commitIndex
	// args.LastLogTerm = rf.log[rf.commitIndex].Term
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for index := range rf.peers {
		if index != rf.me && rf.state == Candidate {
			go rf.sendRequestVote(index, args, &RequestVoteReply{})
		}
	}
	// 记录获得的选票数
	// count := 0
	// // 回收票数
	// finished := 0
	// DPrintf("%d(term %d) starts election", rf.me, rf.currentTerm)
	// // 如果自己不是Candidate（收到新leader的心跳，自己就会变回follower），退出选举
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// args := RequestVoteArgs{
	// 	CandidateId:  rf.me,
	// 	Term:         rf.currentTerm,
	// 	LastLogTerm:  rf.log[rf.lastApplied].Term,
	// 	LastLogIndex: rf.lastApplied,
	// }
	// // 对count和finished的锁
	// var mu1 sync.Mutex
	// cond1 := sync.NewCond(&mu1)
	// for index, _ := range rf.peers {
	// 	// 同时开启多个投票进程
	// 	// 这里参数要显式传递
	// 	if rf.state == Candidate {
	// 		go func(i int, argss *RequestVoteArgs) {
	// 			reply := RequestVoteReply{}
	// 			rf.sendRequestVote(i, argss, &reply)
	// 			mu1.Lock()
	// 			if reply.VoteGranted {
	// 				count++
	// 				DPrintf("%d(term %d)'s count is %d", rf.me, rf.currentTerm, count)
	// 			} else {
	// 				rf.mu.Lock()
	// 				if reply.Term > rf.currentTerm {
	// 					rf.currentTerm = reply.Term
	// 					rf.becomeFollower()
	// 					DPrintf("%d(term(new) %d) is fail to be leader", rf.me, rf.currentTerm)
	// 				}
	// 				rf.mu.Unlock()
	// 			}
	// 			finished++
	// 			cond1.Broadcast()
	// 			mu1.Unlock()
	// 		}(index, &args)
	// 	}
	// }
	// mu1.Lock()
	// // 同意票未到半数且没回收所有票，就一直等待
	// for count < (len(rf.peers)/2)+1 && finished != len(rf.peers) {
	// 	cond1.Wait()
	// }
	// if count >= (len(rf.peers)/2)+1 {
	// 	// 已经收到半数及以上投票
	// 	// 自己变为新的leader，并向所有peer发送心跳rpc，告知新leader已经产生
	// 	rf.becomeLeader()
	// 	rf.chanWinElect <- true
	// }
	// // 拉票失败，不用操作，Run()内会等待时间再次选举。
	// mu1.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// func (rf *Raft) ticker() {
// 	for !rf.killed() {
// 		// Your code here to check if a leader election should
// 		// be started and to randomize sleeping time using
// 		// time.Sleep().
// 		time.Sleep(time.Duration(rf.electionTimeOut) * time.Millisecond)
// 		if time.Since(rf.lastHeartBeat) > time.Duration(rf.electionTimeOut)*time.Millisecond {
// 			// 超时需要开始选举
// 			// 这里需要并行ticker和选举
// 			// 为了防止开启多个选举线程，只允许follower开始选举
// 			DPrintf("[%d](term %d, state %d) elect timeout", rf.me, rf.currentTerm, rf.state)
// 			// 这里是阻塞
// 			rf.election()
// 		}
// 	}
// }

// 既可以是发送心跳，也可以是发送日志
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == Leader {
			if rf.nextIndex[server] > rf.log[0].Index {
				DPrintf("[%d](term %d, state %d) send heartbeat to [%d]", rf.me, rf.currentTerm, rf.state, server)
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1
				// 正常情况不可能会小于baseIndex
				if args.PrevLogIndex >= rf.log[0].Index {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}
				// 只有这种情况需要追加日志
				// 因为server的日志更少（心跳RPC会检测到日志不一致，然后让nextIndex[server]自减）
				DPrintf("[%d](term %d, state %d)'s nextIndex is %d", server, rf.currentTerm, 0, rf.nextIndex[server])
				if rf.nextIndex[server] <= rf.getLastLogIndex() {
					DPrintf("[%d](term %d, state %d) send logs(from index %d) to [%d]", rf.me, rf.currentTerm, rf.state, rf.nextIndex[server], server)
					args.Entries = rf.log[rf.nextIndex[server]:]
				}
				args.LeaderCommit = rf.commitIndex

				go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
			}
		}
	}
}

func (rf *Raft) Run() {
	DPrintf("[%d] server is running", rf.me)
	for !rf.killed() {
		switch rf.state {
		case Follower:
			select {
			// 进行投票，刷新选举超时
			case <-rf.chanGrantVote:
				DPrintf("[%d](term %d, state %d) write to chanGrantVote to refresh election timeout", rf.me, rf.currentTerm, rf.state)
			// 收到正常心跳，刷新选举超时
			case <-rf.chanHeartbeat:
			// 心跳超时，触发自身状态变化
			// 下次case进入Candidate
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				DPrintf("[%d](term %d, state %d) becomes Candidate, election timeout", rf.me, rf.currentTerm, rf.state)
				rf.state = Candidate
			}
		case Leader:
			// 只需要发送心跳
			// 这里只发送一轮
			go rf.sendHeartBeat()
			// 心跳频率
			// 一个candidate变成leader发送心跳，其它candidate收到心跳的时间，不能超过重新选举的时间
			time.Sleep(60 * time.Millisecond)
		case Candidate:
			// 进行选举
			rf.mu.Lock()
			rf.currentTerm++
			// 给自己投票
			// 避免了一种情况：Candidate投票给相同term的其它节点的情况（这个符合paper 图2）
			DPrintf("[%d](term %d, state %d) vote to [%d]", rf.me, rf.currentTerm, rf.state, rf.me)
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()
			go rf.election()
			select {
			// 收到他人心跳（认可的心跳），修改状态，退出选举
			case <-rf.chanHeartbeat:
				DPrintf("[%d](term %d, state %d) become Follower (get new Leader RPC)", rf.me, rf.currentTerm, rf.state)
				rf.state = Follower
			// 赢得选举，修改状态，结束选举
			case <-rf.chanWinElect:
			// 平分选票，则上面管道不会触发，会随机等待时间（200~500ms）循环选举
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				DPrintf("集体竞选失败，重新选举")
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 所有服务器端口都在peers[]内。本服务器端口是peers[me]，所有服务器的
// peers[]都是一样的顺序。Persister是此服务器保存其持久状态的地方，
// 并且最初还保存最近保存的状态(如果有的话)。applyCh是一个通道，
// 测试者或服务希望Raft在该通道上发送ApplyMsg消息。Make()必须快速返回，
// 因此它应该为任何长时间运行的工作启动goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.state = Follower
	// rf.isLeader = false

	// 超时选举时间设置为300ms
	// rf.electionTimeOut = 300
	rf.heartbeatTime = 60
	// rf.lastHeartBeat = time.Now()
	// log要求index从1开始，所以初始化先写入一条log
	rf.log = append(rf.log, Log{"initial", 0, 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanApplyLog = make(chan bool, 100)
	go rf.Run()
	go rf.applyToService(applyCh)

	// start send heartbeat
	// go rf.sendHeartBeat()
	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}

func (rf *Raft) applyToService(applyCh chan ApplyMsg) {
	// 监听rf.applyCh
	for range rf.chanApplyLog {
		for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {
			applyMsg := ApplyMsg{
				Command:      rf.log[i].Command,
				CommandValid: true,
				CommandIndex: i,
			}
			DPrintf("[%d](term %d, state %d) apply command", rf.me, rf.currentTerm, rf.state)
			applyCh <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
}
