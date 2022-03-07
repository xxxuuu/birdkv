package kvraft

import (
	"birdkv/net"
	"birdkv/raft"
	"crypto/rand"
	"sync"
	"time"
)
import "math/big"

type Clerk struct {
	mu       sync.Locker
	servers  []net.ClientEnd
	id       ClientID
	nextOpId OpSeriesID
	leader   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []net.ClientEnd) *Clerk {
	ck := &Clerk{
		mu:       &sync.Mutex{},
		servers:  servers,
		id:       ClientID(nrand()),
		nextOpId: 1,
	}
	return ck
}

//
// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	return ck.sendRequest(GET, key, "")
}

// Put set key=value
func (ck *Clerk) Put(key string, value string) string {
	return ck.sendRequest(PUT, key, value)
}

// Append set key+=value
func (ck *Clerk) Append(key string, value string) string {
	return ck.sendRequest(APPEND, key, value)
}

func (ck *Clerk) sendRequest(op OpType, key string, value string) string {
	ck.mu.Lock()
	opID := ck.nextOpId
	clientId := ck.id
	ck.nextOpId++
	ck.mu.Unlock()

	DPrintf("[%s][%d-%d] k->%s v->%s request", op, clientId, opID, key, value)
	for i := ck.leader; ; i++ {
		if i >= len(ck.servers) {
			i = 0
		}

		var ok bool
		args := &OpArgs{
			Key:        key,
			Value:      value,
			OpType:     op,
			ClientID:   clientId,
			OpSeriesID: opID,
		}
		reply := &OpReply{}
		if op == GET {
			ok = ck.callGet(i, args, reply)
		} else {
			ok = ck.callPutAppend(i, args, reply)
		}

		if !ok { // 超时
			DPrintf("[%s][%d-%d] leader:%d k->%s v->%s no reply", op, clientId, opID, i, key, reply.Value)
		} else if reply.Err == OK || reply.Err == ErrNoKey {
			ck.leader = i
			DPrintf("[%s][%d-%d] leader:%d k->%s v->%s %v", op, clientId, opID, i, key, reply.Value, reply.Err)
			return reply.Value
		} else {
			DPrintf("[%s][%d-%d] k->%s v->%s %v", op, clientId, opID, key, reply.Value, reply.Err)
		}
		time.Sleep(raft.ElectionTimeOut)
	}
}

func (ck *Clerk) callGet(server int, args *OpArgs, reply *OpReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) callPutAppend(server int, args *OpArgs, reply *OpReply) bool {
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}
