package persist

type Persister interface {
	// Copy 复制一个持久化器
	Copy() Persister
	// SaveRaftState 保存raft状态
	SaveRaftState(state []byte)
	// ReadRaftState 读取raft状态
	ReadRaftState() []byte
	// RaftStateSize 返回raft状态大小
	RaftStateSize() int
	// SaveStateAndSnapshot 保存raft状态和快照 要求必须是原子的
	SaveStateAndSnapshot(state []byte, snapshot []byte)
	// ReadSnapshot 读取快照
	ReadSnapshot() []byte
	// SnapshotSize 返回快照大小
	SnapshotSize() int
}
