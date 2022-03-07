package labpersist

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"
import "birdkv/persist"

type LabPersister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakeLabPersister() *LabPersister {
	return &LabPersister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *LabPersister) Copy() persist.Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakeLabPersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *LabPersister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

func (ps *LabPersister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *LabPersister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *LabPersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *LabPersister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *LabPersister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
