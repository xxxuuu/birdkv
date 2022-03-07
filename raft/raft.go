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
	"birdkv/net"
	"birdkv/persist"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term    int
	Command interface{}
}

// 一些参数配置 见论文 5.6 节
const (
	// 时间设置的比论文要大一点 因为测试脚本限定1秒只能进行10次心跳
	// ElectionTimeOut 选举超时时间
	ElectionTimeOut time.Duration = 300
	// BroadcastTimeOut 广播时间
	BroadcastTimeOut time.Duration = 100
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Locker       // Lock to protect shared access to this peer's state
	peers     []net.ClientEnd   // RPC end points of all peers
	persister persist.Persister // Object to hold this peer's persisted state
	me        int               // this peer's index into peers[]
	dead      int32             // set by Kill()

	// 选举超时标记
	timeout bool
	// 当前节点身份
	state NodeState
	// 存储日志被复制到多少个节点上
	logReplicateNum map[int]map[int]struct{}
	// 日志提交channel
	applyCh chan ApplyMsg
	// 心跳channel
	heartbeatCh chan struct{}
	// 上次心跳时间
	lastHeartbeatTime int64
	// 快照表示的最后一个位置和任期
	snapshotLastIndex int
	snapshotLastTerm  int
	// 快照数据
	snapshot []byte

	// Raft 状态 同论文P2
	// 持久化状态
	CurrentTerm int
	VotedFor    int
	Logs        []Log
	// 易失性状态
	commitIndex int
	lastApplied int
	// leader 易失性状态
	nextIndex  []int
	matchIndex []int
}

// GetState 返回当前任期和是否 leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.CurrentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.snapshot
}

func (rf *Raft) GetCommittedLogs() ([]Log, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapshotLastIndex == rf.commitIndex {
		return []Log{}, rf.commitIndex
	}
	return rf.sliceLogR(rf.commitIndex + 1), rf.commitIndex
}

// 保存应该持久化的状态到持久化存储中
func (rf *Raft) persist() {
	DPrintf("%d[%d] 进行持久化...\n", rf.me, rf.CurrentTerm)

	w := new(bytes.Buffer)
	e := net.NewGobEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// 读取之前持久化的状态并恢复
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := net.NewGobDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	var snapshotLastIndex int
	var snapshotLastTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotLastIndex) != nil ||
		d.Decode(&snapshotLastTerm) != nil {
		DPrintf("%d 持久化失败\n", rf.me)
		return
	}
	DPrintf("%d 从持久化数据中恢复... %v %v %v %v %v\n", rf.me, currentTerm, votedFor, logs, snapshotLastIndex, snapshotLastTerm)
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Logs = logs
	rf.snapshotLastIndex = snapshotLastIndex
	if rf.commitIndex < rf.snapshotLastIndex {
		rf.commitIndex = rf.snapshotLastIndex
	}
	rf.snapshotLastTerm = snapshotLastTerm
	rf.snapshot = rf.persister.ReadSnapshot()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 收到InstallSnapshot RPC后，快照被返回给上层服务
	// 然后上层服务再调用CondInstallSnapshot来确认是否安装快照
	// 如果这段时间内因为AppendEntries RPC导致commitIndex超过快照，就不必安装
	if lastIncludedIndex <= rf.snapshotLastIndex || rf.commitIndex >= lastIncludedIndex {
		return false
	}
	DPrintf("%d[%d]确认安装快照 %v %v", rf.me, rf.CurrentTerm, lastIncludedIndex, lastIncludedTerm)
	if rf.updateSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot) {
		rf.persist()
		return true
	}
	return false
}

// 上层服务创建了一个到 index 的 snapshot
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%d[%d]创建快照 %v", rf.me, rf.CurrentTerm, index)
		if rf.updateSnapshotByIndex(index, snapshot) {
			rf.persist()
		}
	}()
}

func (rf *Raft) updateSnapshot(index int, term int, snapshot []byte) bool {
	if index <= rf.snapshotLastIndex {
		return false
	}

	if rf.isLogArrayOutOfRange(index + 1) {
		rf.Logs = []Log{}
	} else {
		rf.Logs = rf.sliceLogL(index + 1)
	}
	rf.snapshotLastTerm = term
	rf.snapshotLastIndex = index
	rf.snapshot = snapshot
	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	DPrintf("%d[%d]更新快照到%d commitIndex:%v lastIndex:%v lastTerm:%v logindex:%v nextlogindex:%v\n",
		rf.me, rf.CurrentTerm, index, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, rf.getLogArrayIndex(index), rf.getLogArrayIndex(rf.getLogLen()+1))

	return true
}

func (rf *Raft) updateSnapshotByIndex(index int, snapshot []byte) bool {
	if index <= rf.snapshotLastIndex || rf.isLogArrayOutOfRange(index) {
		return false
	}

	term := rf.getLog(index).Term
	return rf.updateSnapshot(index, term, snapshot)
}

func (rf *Raft) updateTerm(term int) {
	rf.CurrentTerm = term
	rf.state = Follower
	rf.VotedFor = -1
}

func (rf *Raft) commitLog(commitIndex int) {
	if commitIndex <= rf.commitIndex {
		return
	}
	applyLen := commitIndex - rf.commitIndex
	applyMsgs := make([]ApplyMsg, 0, applyLen)
	for i := rf.commitIndex + 1; i <= commitIndex; i++ {
		applyMsgs = append(applyMsgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.getLog(i).Command,
			CommandIndex: i,
			CommandTerm:  rf.CurrentTerm,
		})
		DPrintf("%d提交日志%d:%v\n", rf.me, i, rf.getLog(i))
	}

	go func() {
		for _, msg := range applyMsgs {
			rf.applyCh <- msg
		}
		// TODO 这个锁开销貌似很大 在3A中SpeedTest如果加上-race就会导致超时
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
		}
		DPrintf("%d正式提交日志%v", rf.me, applyMsgs)
	}()
}

func (rf *Raft) getLogLen() int {
	return rf.snapshotLastIndex + len(rf.Logs)
}

// 给定log数组的位置（index），返回真实的log index
func (rf *Raft) getRealLogIndex(arrayIndex int) int {
	return arrayIndex + rf.snapshotLastIndex + 1
}

// 给定真实的log index，返回log数组的位置（index）
func (rf *Raft) getLogArrayIndex(realIndex int) int {
	return realIndex - rf.snapshotLastIndex - 1
}

func (rf *Raft) isLogArrayOutOfRange(realIndex int) bool {
	return rf.getLogArrayIndex(realIndex) >= rf.getLogArrayIndex(rf.getLogLen()+1) ||
		rf.getLogArrayIndex(realIndex) < 0
}

func (rf *Raft) getLog(index int) Log {
	return rf.Logs[rf.getLogArrayIndex(index)]
}

func (rf *Raft) setLog(log Log, index int) {
	rf.Logs[rf.getLogArrayIndex(index)] = log
}

func (rf *Raft) sliceLog(start int, end int) []Log {
	return rf.Logs[rf.getLogArrayIndex(start):rf.getLogArrayIndex(end)]
}

func (rf *Raft) sliceLogL(start int) []Log {
	return rf.Logs[rf.getLogArrayIndex(start):]
}

func (rf *Raft) sliceLogR(end int) []Log {
	return rf.Logs[:rf.getLogArrayIndex(end)]
}

// RequestVoteArgs 请求投票RPC参数
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply 请求投票RPC响应
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote 候选人请求投票RPC接口
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	vote := false

	if args.Term < rf.CurrentTerm {
		// 拒绝旧任期
		vote = false
	} else {
		if args.Term == rf.CurrentTerm {
			// 任期一样 看有没有投票给别人
			vote = rf.VotedFor == -1 || rf.VotedFor == args.CandidateId
		} else {
			vote = true
		}
		// 任期大的日志新 任期一样看索引位置 拒绝更旧的日志
		if vote {
			lastIdx := rf.getLogLen()
			lastTerm := 0
			if lastIdx > 0 {
				if lastIdx == rf.snapshotLastIndex {
					lastTerm = rf.snapshotLastTerm
				} else {
					lastTerm = rf.getLog(lastIdx).Term
				}
			}

			if args.LastLogTerm == lastTerm {
				vote = args.LastLogIndex >= lastIdx
			} else {
				vote = args.LastLogTerm > lastTerm
			}
		}
	}

	DPrintf("%d请求投票[任期%d]，%d当前投票给了%d[任期%d] 是否投票%v\n",
		args.CandidateId, args.Term, rf.me, rf.VotedFor, rf.CurrentTerm, vote)
	if args.Term > rf.CurrentTerm {
		rf.updateTerm(args.Term)
	}
	if vote {
		rf.VotedFor = args.CandidateId
		rf.timeout = false
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.persist()
	reply.Term = rf.CurrentTerm
	return nil
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

// AppendEntriesArgs 追加日志RPC参数
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// AppendEntriesReply 追加日志RPC响应
type AppendEntriesReply struct {
	Term           int
	Success        bool
	LastMatchIndex int
}

// AppendEntries 追加日志RPC接口
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d[任期%d]->%d[任期%d]心跳被接收 %v\n", args.LeaderId, args.Term, rf.me, rf.CurrentTerm, args)
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return nil
	} else {
		if args.Term > rf.CurrentTerm {
			rf.updateTerm(args.Term)
		}
		rf.state = Follower
		rf.timeout = false
	}
	// 一致性检查 日志匹配
	var logMatch bool
	if args.PrevLogIndex == 0 {
		logMatch = true
	} else {
		if args.PrevLogIndex == rf.snapshotLastIndex {
			logMatch = args.PrevLogTerm == rf.snapshotLastTerm
		} else {
			logMatch = rf.getLogLen() >= args.PrevLogIndex && rf.getLogArrayIndex(args.PrevLogIndex) >= 0 && rf.getLog(args.PrevLogIndex).Term == args.PrevLogTerm
		}
	}
	if logMatch {
		// 丢弃冲突日志 追加新日志
		// PS：这里调了我半天！！！，见论文 P2 AppendEntries RPC 的 Receiver implementation 第四项
		// 应该判断下，只追加新日志，因为 RPC 可能由于网络原因顺序不一致
		// 例如 leader 发送了两个追加日志，后一个的日志更长更新，但由于网络原因前一个更晚到达，导致新日志被丢弃
		newLogIdx := -1
		for i := 0; i < len(args.Entries); i++ {
			logIdx := args.PrevLogIndex + i + 1
			if logIdx > rf.getLogLen() || rf.getLog(logIdx) != args.Entries[i] {
				if rf.getLogLen() < logIdx {
					logIdx = rf.getLogLen() + 1
				}
				rf.Logs = rf.sliceLogR(logIdx)
				newLogIdx = i
				break
			}
		}
		if newLogIdx != -1 {
			appendIdx := rf.getLogLen()
			rf.Logs = append(rf.Logs, args.Entries[newLogIdx:]...)
			DPrintf("%v在%d追加日志%v", rf.me, appendIdx+1, args.Entries[newLogIdx:])
		}
		// 更新commit日志索引
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := args.LeaderCommit
			if newCommitIndex >= rf.getLogLen() {
				newCommitIndex = rf.getLogLen()
			}
			rf.commitLog(newCommitIndex)
		}
		rf.persist()
		reply.Success = true
		reply.Term = rf.CurrentTerm
	} else {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		// 一个优化 直接返回冲突日志的任期的最小日志索引 减少被拒绝的RPC次数
		reply.LastMatchIndex = args.PrevLogIndex
		if rf.getLogLen() < args.PrevLogIndex {
			reply.LastMatchIndex = rf.getLogLen() + 1
		} else if rf.getLogArrayIndex(args.PrevLogIndex) >= 0 {
			term := rf.getLog(args.PrevLogIndex).Term
			for i := args.PrevLogIndex; rf.getLogArrayIndex(i) >= 0; i-- {
				if rf.getLog(i).Term != term {
					break
				}
				reply.LastMatchIndex = i
			}
		}
		DPrintf("%d[任期%d]->%d[任期%d]心跳日志不匹配 返回%v\n", args.LeaderId, args.Term, rf.me, rf.CurrentTerm, reply.LastMatchIndex)
	}
	return nil
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// InstallSnapshotArgs 安装快照 RPC 参数
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// 暂不实现分块
	//offset            int
	//done              bool
}

// InstallSnapshotReply 安装快照 RPC 响应
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot 安装快照 RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		rf.updateTerm(args.Term)
	} else if args.Term < rf.CurrentTerm || args.LastIncludedIndex < rf.snapshotLastIndex {
		reply.Term = rf.CurrentTerm
		return nil
	}
	rf.state = Follower
	rf.timeout = false
	reply.Term = rf.CurrentTerm

	DPrintf("%v[%v]接收%v快照安装请求 %v %v", rf.me, rf.CurrentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	// 返回快照给上层服务 不会在这里直接切换快照
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
	return nil
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()

	me := rf.me
	currentTerm := rf.CurrentTerm

	DPrintf("%d[%d]对%d发送快照 lastIndex:%v lastTerm:%v", me, currentTerm, server, rf.snapshotLastIndex, rf.snapshotLastTerm)

	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotLastIndex,
		LastIncludedTerm:  rf.snapshotLastTerm,
		Data:              rf.snapshot,
	}
	reply := &InstallSnapshotReply{}

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		DPrintf("%d[%d]对%d发送快照失败", me, currentTerm, server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d[%d]对%d发送快照成功", me, currentTerm, server)
	if reply.Term > rf.CurrentTerm {
		DPrintf("%d[%d]任期比leader %d[%d]新，下台\n", server, reply.Term, me, currentTerm)
		rf.updateTerm(reply.Term)
	}
	// TODO: 更新nextIndex 总感觉有隐患
	if rf.nextIndex[server] <= rf.snapshotLastIndex {
		rf.nextIndex[server] = rf.snapshotLastIndex + 1
		rf.matchIndex[server] = rf.snapshotLastIndex
		DPrintf("%d的matchIndex更新至%v", server, rf.matchIndex[server])
	} else {
		DPrintf("%d的matchIndex：%v nextIndex：%v  snapshotindex：%v", server, rf.matchIndex[server], rf.nextIndex[server], rf.snapshotLastIndex)
	}
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.getLogLen() + 1
	term := rf.CurrentTerm
	isLeader := rf.state == Leader

	// 追加日志
	if isLeader {
		DPrintf("leader{%d[%d]}收到客户端在%v追加日志%v\n", rf.me, rf.CurrentTerm, index, command)
		rf.Logs = append(rf.Logs, Log{
			Term:    term,
			Command: command,
		})
		// TODO 一个优化 可以并行复制日志和写入硬盘
		rf.persist()
		// 一个优化 收到请求直接发送日志 跳过等待
		go func() {
			rf.heartbeatCh <- struct{}{}
		}()
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 一次心跳
func (rf *Raft) heartbeat(id int) {
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	me := rf.me
	state := rf.state
	leaderCommit := rf.commitIndex
	snapshotLastIndex := rf.snapshotLastIndex
	reply := &AppendEntriesReply{}
	// 根据nextIndex给每个节点发送日志条目
	sendLogIdx := rf.nextIndex[id]

	if state != Leader {
		rf.mu.Unlock()
		return
	}

	var logs []Log
	if sendLogIdx <= snapshotLastIndex {
		DPrintf("%d[%d]对%d发送快照 lastIndex:%v sendIdx:%v",
			me, currentTerm, id, rf.snapshotLastIndex, sendLogIdx)
		// 如果将发送的日志已经被快照 就直接发快照过去
		go rf.sendInstallSnapshot(id)
		rf.mu.Unlock()
		return
	} else if rf.getLogLen() >= sendLogIdx {
		// deep copy 避免发送RPC序列化时指向同一内存导致并发问题
		tmpLog := rf.sliceLogL(sendLogIdx)
		//DPrintf("realIndex:%v logindex:%v snapshotindex:%v templog:%v log:%v", sendLogIdx, rf.getLogArrayIndex(sendLogIdx), rf.snapshotLastIndex, tmpLog, rf.Logs)
		logs = make([]Log, len(tmpLog))
		copy(logs, tmpLog)
	}
	prevLogIndex := sendLogIdx - 1
	prevLogTerm := 0
	if prevLogIndex == rf.snapshotLastIndex {
		prevLogTerm = rf.snapshotLastTerm
	} else if rf.getLogArrayIndex(prevLogIndex) >= 0 {
		prevLogTerm = rf.getLog(prevLogIndex).Term
	}

	DPrintf("%d[%d]对%d发送心跳，nextIndex:%d prevLogIndex:%d 日志长度:%d\n", me, currentTerm, id, sendLogIdx, prevLogIndex, len(logs))
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(id, &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: leaderCommit,
	}, reply)

	if !ok {
		DPrintf("leader %d[%d]对%d的心跳丢失\n", me, currentTerm, id)
		return
	}

	rf.mu.Lock()
	if currentTerm != rf.CurrentTerm || rf.state != Leader { // 响应时世界都变天了
		DPrintf("leader %v[%v][%v]对[%v]的心跳返回已过时", me, currentTerm, rf.CurrentTerm, id)
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.CurrentTerm {
		DPrintf("%d[%d]任期比leader %d[%d]新，下台\n", id, reply.Term, me, currentTerm)
		// 如果接收者的任期更新 就同步任期并下台
		rf.updateTerm(reply.Term)
	} else if !reply.Success && reply.Term == rf.CurrentTerm {
		// 被拒绝但任期一致 说明一致性检查失败 减小nextIndex
		DPrintf("%d[%d]对leader %d[%d]的心跳一致性检查失败\n", id, reply.Term, me, currentTerm)
		rf.nextIndex[id] = reply.LastMatchIndex
	} else if reply.Success {
		// 成功 更新matchIndex
		DPrintf("leader %d[%d]对%d[%d]的心跳成功\n", me, currentTerm, id, reply.Term)
		// 如果当前任期的一个日志被复制到超过半数节点上 就可以提交它
		newCommitIndex := rf.commitIndex
		for idx, l := range logs {
			logIdx := sendLogIdx + idx
			if _, ok := rf.logReplicateNum[logIdx]; !ok {
				rf.logReplicateNum[logIdx] = make(map[int]struct{})
			}
			rf.logReplicateNum[logIdx][id] = struct{}{}
			if l.Term == rf.CurrentTerm && len(rf.logReplicateNum[logIdx])+1 > (len(rf.peers)/2) {
				if logIdx > newCommitIndex {
					//DPrintf("%d更新newCommitIndex从%v到%v %v", me, newCommitIndex, logIdx, rf.logReplicateNum)
					newCommitIndex = logIdx
				}
			}
		}
		// 通知日志已提交
		if newCommitIndex > rf.getLogLen() {
			newCommitIndex = rf.getLogLen()
		}
		rf.commitLog(newCommitIndex)
		rf.nextIndex[id] = sendLogIdx + len(logs)
		rf.matchIndex[id] = rf.nextIndex[id] - 1
	}
	rf.persist()
	rf.mu.Unlock()
}

// 广播定时器
func (rf *Raft) broadcastTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		// 因额外的发送（收到请求后立即发送）导致间隔太短，继续休眠
		interval := time.Duration(time.Now().UnixNano() - rf.lastHeartbeatTime)
		if interval < (time.Millisecond * BroadcastTimeOut) {
			time.Sleep(time.Millisecond*BroadcastTimeOut - interval)
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		go func() {
			rf.heartbeatCh <- struct{}{}
		}()
		time.Sleep(time.Millisecond * BroadcastTimeOut)
	}
}

// 返回一个选举超时时间
func (rf *Raft) randElectionTimeOut() time.Duration {
	// 选举超时时间加随机值，避免选票瓜分导致活锁
	return time.Millisecond * (ElectionTimeOut + time.Duration(rand.Intn(int(ElectionTimeOut)/2)))
}

// switch2Leader 转换为leader
func (rf *Raft) switch2Leader() {
	rf.mu.Lock()
	rf.state = Leader
	// leader 易失性状态重新初始化
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLogLen() + 1
		rf.matchIndex[i] = 0
	}
	rf.logReplicateNum = make(map[int]map[int]struct{})
	rf.mu.Unlock()

	// 启动广播定时器发送心跳
	go rf.broadcastTicker()

	// TODO 优化 可以刚上任就马上提交空日志 快速commit之前任期的日志
}

// 发送心跳
func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		<-rf.heartbeatCh

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.lastHeartbeatTime = time.Now().UnixNano()
		rf.mu.Unlock()

		for i := range rf.peers {
			id := i
			rf.mu.Lock()
			if i == rf.me {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			go rf.heartbeat(id)
		}
	}
}

// 选举超时定时器
func (rf *Raft) electionTicker() {
	re := false
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			time.Sleep(rf.randElectionTimeOut())
			continue
		}
		rf.timeout = true
		rf.mu.Unlock()

		if !re {
			time.Sleep(rf.randElectionTimeOut())
		}
		re = false

		rf.mu.Lock()
		if rf.timeout == false {
			rf.mu.Unlock()
			continue
		}

		timeoutCh := make(chan struct{}, 1)
		voteCh := make(chan int, 1)
		go func() {
			time.Sleep(rf.randElectionTimeOut())
			timeoutCh <- struct{}{}
		}()
		go func() {
			// 选举超时 转变为candidate开启一轮新选举
			rf.state = Candidate
			rf.CurrentTerm += 1
			rf.VotedFor = rf.me
			me := rf.me
			currentTerm := rf.CurrentTerm
			nodeNum := len(rf.peers)
			lastLogIndex := rf.getLogLen()
			lastLogTerm := 0
			if lastLogIndex == rf.snapshotLastIndex {
				lastLogTerm = rf.snapshotLastTerm
			} else if lastLogIndex >= 1 {
				lastLogTerm = rf.getLog(lastLogIndex).Term
			}
			rf.persist()

			DPrintf("%d转变为candidate 开启选举[任期%d] lastLogIndex:%d lastLogTerm:%d\n", rf.me, rf.CurrentTerm, lastLogIndex, lastLogTerm)
			rf.mu.Unlock()

			var voteCnt int32 = 1
			for i := 0; i < nodeNum; i++ {
				if i == me {
					continue
				}
				id := i
				go func() {
					reply := &RequestVoteReply{}
					DPrintf("%d给%d发送投票请求[%d]\n", me, id, currentTerm)
					ok := rf.sendRequestVote(id, &RequestVoteArgs{
						Term:         currentTerm,
						CandidateId:  me,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}, reply)
					if !ok {
						DPrintf("%d给%d发送投票请求[%d] 发送失败\n", me, id, currentTerm)
					} else {
						DPrintf("%d给%d发送投票请求[%d] 得到响应\n", me, id, currentTerm)
					}
					if ok && reply.VoteGranted {
						atomic.AddInt32(&voteCnt, 1)
						// 不需要等全部结束 选票过半直接成为leader
						if atomic.LoadInt32(&voteCnt) > int32(nodeNum/2) {
							voteCh <- currentTerm
						}
					}
				}()
			}
		}()
		select {
		case voteTerm := <-voteCh:
			// 成为leader
			rf.mu.Lock()
			currentTerm := rf.CurrentTerm
			me := rf.me
			nodeState := rf.state
			rf.mu.Unlock()

			if voteTerm == currentTerm && nodeState == Candidate { // 保证这期间没有出现新leader
				DPrintf("%d成为leader\n", me)
				rf.switch2Leader()
			}
		case <-timeoutCh:
			// 选举超时 重新开始选举
			rf.mu.Lock()
			if rf.state == Candidate {
				DPrintf("%d选举超时 再次发起选举\n", rf.me)
				re = true
			}
			rf.mu.Unlock()
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
//
func Make(peers []net.ClientEnd, me int,
	persister persist.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:              &sync.Mutex{},
		peers:           peers,
		persister:       persister,
		me:              me,
		timeout:         false,
		state:           Follower,
		applyCh:         applyCh,
		heartbeatCh:     make(chan struct{}, 1),
		dead:            0,
		CurrentTerm:     0,
		VotedFor:        -1,
		Logs:            []Log{},
		logReplicateNum: make(map[int]map[int]struct{}),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
	}
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}

	// 从持久化中崩溃恢复
	rf.readPersist(persister.ReadRaftState())

	// 启动选举超时定时器
	go rf.electionTicker()
	// 启动心跳发送器
	go rf.sendHeartbeat()

	return rf
}
