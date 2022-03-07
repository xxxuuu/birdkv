package kvraft

import (
	"birdkv/net"
	"birdkv/persist"
	"birdkv/raft"
	"birdkv/test-tool/labgob"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("[KV] "+format, a...)
	}
	return
}

// OpRaftID 操作传递到raft层使用的ID，格式为该请求的ClientID-OpSeriesID
// 主要是为了在接收applyMsg时还能进行去重和一致性判断
type OpRaftID string

func generateOpRaftID(cid ClientID, oid OpSeriesID) OpRaftID {
	return OpRaftID(fmt.Sprintf("%d-%d", cid, oid))
}

func splitOpRaftID(opRaftID OpRaftID) (ClientID, OpSeriesID) {
	tmp := strings.Split(string(opRaftID), "-")
	cid, _ := strconv.Atoi(tmp[0])
	oid, _ := strconv.Atoi(tmp[1])
	return ClientID(cid), OpSeriesID(oid)
}

// ChanID 接收raft层applyMsg的channel的ID，格式为该日志的index-term
// 因客户端可能会重发，ClientID-OpSeriesID可能会有一样的，而index-term是唯一的
type ChanID string

func generateChanID(index int, term int) ChanID {
	return ChanID(fmt.Sprintf("%d-%d", index, term))
}

type Op struct {
	Id     OpRaftID
	OpType OpType
	Key    string
	Value  string
}

type OpRaftRes struct {
	Id  OpRaftID
	Res string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	Rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data map[string]string
	// 记录每个客户端最新请求
	idMap map[ClientID]OpSeriesID
	// 存储每个接收raft层applyMsg的channel
	notifyCh map[ChanID]chan OpRaftRes
	// 最后应用的日志索引
	lastApplied int
}

func (kv *KVServer) StartRaft(op Op) OpRaftRes {
	// 超时
	timeoutCh := make(chan struct{}, 1)
	go func() {
		// 4个选举超时时间
		time.Sleep(raft.ElectionTimeOut * time.Millisecond * 4)
		timeoutCh <- struct{}{}
	}()

	index, term, isleader := kv.Rf.Start(op)
	if !isleader {
		return OpRaftRes{Id: op.Id, Err: ErrWrongLeader}
	}

	// TODO 一个隐患是 start后raft光速执行完毕 这里channel还没创建 无法收到完成消息
	chId := generateChanID(index, term)

	kv.mu.Lock()
	kv.notifyCh[chId] = make(chan OpRaftRes, 1)
	ch, _ := kv.notifyCh[chId]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, chId)
		kv.mu.Unlock()
	}()

	DPrintf("%d 发送消息到raft层：%v，notifyCh: %v", kv.me, op, chId)
	select {
	case res := <-ch:
		DPrintf("%v 接收到ch %v响应 %v", kv.me, chId, res)
		if _, isleader := kv.Rf.GetState(); !isleader {
			// TODO 优化 不是leader可以直接重定向到leader raft需要再存个leaderId
			return OpRaftRes{Id: op.Id, Err: ErrWrongLeader}
		}
		return res
	case <-timeoutCh:
		DPrintf("%v %v 超时", kv.me, op)
		return OpRaftRes{Id: op.Id, Err: ErrTimeout}
	}
}

func (kv *KVServer) Get(args *OpArgs, reply *OpReply) error {
	kv.mu.Lock()
	DPrintf("[%s] %d收到%v", "Get", kv.me, args)
	kv.mu.Unlock()

	// 通过空日志交换心跳 确保自己还是leader以避免脏数据
	id := generateOpRaftID(args.ClientID, args.OpSeriesID)
	res := kv.StartRaft(Op{
		Id:     id,
		OpType: GET,
		Key:    args.Key,
	})
	if res.Err != OK {
		reply.Err = res.Err
		return nil
	}
	// 可能出现leader下台 追加的日志还未提交就被覆盖，之后提交了这个覆盖的日志，所以要判断下还是不是一开始的操作
	if res.Id != id {
		reply.Err = ErrFail
		return nil
	}

	reply.Err = res.Err
	reply.Value = res.Res
	return nil
}

func (kv *KVServer) PutAppend(args *OpArgs, reply *OpReply) error {
	kv.mu.Lock()
	DPrintf("[%s] %d收到%v", args.OpType, kv.me, args)

	// 重复消息
	if id, ok := kv.idMap[args.ClientID]; ok && id >= args.OpSeriesID {
		reply.Err = OK
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	id := generateOpRaftID(args.ClientID, args.OpSeriesID)
	res := kv.StartRaft(Op{
		Id:     id,
		OpType: args.OpType,
		Key:    args.Key,
		Value:  args.Value,
	})
	if res.Err != OK {
		reply.Err = res.Err
		return nil
	}
	if res.Id != id {
		reply.Err = ErrFail
		return nil
	}

	reply.Err = OK
	reply.Value = res.Res
	return nil
}

func (kv *KVServer) receiveCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := applyMsg.Command.(Op)
	res := OpRaftRes{
		Id:  op.Id,
		Err: OK,
	}

	// 旧日志
	if kv.lastApplied >= applyMsg.CommandIndex {
		return
	}
	// TODO 乱序?
	//if applyMsg.CommandIndex > kv.lastApplied+1 {
	//	go func() {
	//		kv.applyCh <- applyMsg
	//	}()
	//	return
	//}

	if op.OpType == GET {
		DPrintf("%v %d读取数据 %v", op, kv.me, kv.data)
		if v, ok := kv.data[op.Key]; ok {
			res.Res = v
		} else {
			res.Err = ErrNoKey
		}
	} else {
		// 排除重复消息
		cid, oid := splitOpRaftID(op.Id)
		id, ok := kv.idMap[cid]
		if !ok || (ok && id < oid) {
			if op.OpType == PUT {
				kv.data[op.Key] = op.Value
			} else if op.OpType == APPEND {
				kv.data[op.Key] += op.Value
			}
			res.Res = kv.data[op.Key]
			kv.idMap[cid] = oid
			DPrintf("%v %d写入数据 %v", op, kv.me, kv.data)
		}
	}
	kv.lastApplied = applyMsg.CommandIndex

	chId := generateChanID(applyMsg.CommandIndex, applyMsg.CommandTerm)
	if _, ok := kv.notifyCh[chId]; ok {
		DPrintf("通过ch %v通知apply %v", chId, applyMsg)
		// 可能会在删除前发送前消息，因为没有消费者，就会阻塞，通过select避免这种情况
		select {
		case kv.notifyCh[chId] <- res:
		default:
			// 已经没有消费者了 就不管它
		}
	}

	kv.checkExceedSize()
}

func (kv *KVServer) receiveSnapshot(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.Rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
		if applyMsg.SnapshotIndex > kv.lastApplied {
			kv.installSnapshot(applyMsg.Snapshot)
		}
	}
}

func (kv *KVServer) checkExceedSize() {
	if kv.maxraftstate == -1 {
		return
	}
	// 如果大小超限 就生成快照
	DPrintf("%v 大小超限 生成快照", kv.me)
	if kv.Rf.GetRaftStateSize() >= kv.maxraftstate {
		kv.Rf.Snapshot(kv.lastApplied, kv.generateSnapshot())
	}
}

func (kv *KVServer) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.idMap)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) receiveApplyMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		DPrintf("%d收到apply %v", kv.me, applyMsg)
		if applyMsg.CommandValid {
			kv.receiveCommand(applyMsg)
		} else if applyMsg.SnapshotValid {
			kv.receiveSnapshot(applyMsg)
		}
	}
}

func (kv *KVServer) installSnapshot(snapshot []byte) bool {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.lastApplied) != nil || d.Decode(&kv.idMap) != nil || d.Decode(&kv.data) != nil {
		DPrintf("recover fail %v", d.Decode(&kv.lastApplied))
		return false
	}
	DPrintf("%v 从快照中恢复数据 %v %v", kv.me, kv.lastApplied, kv.data)
	return true
}

func (kv *KVServer) crashRecover() {
	snapshot := kv.Rf.GetSnapshot()
	if len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	// 重放已提交日志
	logs, commitIdx := kv.Rf.GetCommittedLogs()
	for i := range logs {
		op := logs[i].Command.(Op)
		DPrintf("%d 重放日志 %v", kv.me, op)
		switch op.OpType {
		case GET:
			continue
		case PUT:
			kv.data[op.Key] = op.Value
		case APPEND:
			kv.data[op.Key] += op.Value
		}
	}
	kv.lastApplied = commitIdx

	DPrintf("%v 恢复数据 %v %v", kv.me, kv.lastApplied, kv.data)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.Rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []net.ClientEnd, me int, persister persist.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg, 1)
	rf := raft.Make(servers, me, persister, applyCh)
	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		Rf:           rf,
		applyCh:      applyCh,
		maxraftstate: maxraftstate,
		data:         make(map[string]string),
		idMap:        make(map[ClientID]OpSeriesID),
		notifyCh:     make(map[ChanID]chan OpRaftRes),
		lastApplied:  0,
	}
	kv.crashRecover()
	go kv.receiveApplyMsg()

	return kv
}
