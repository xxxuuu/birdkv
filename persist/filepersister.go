package persist

import (
	"fmt"
	"io/ioutil"
	"os"
)

const PERSIST_STATE_FILENAME = "birdkv-persist-raft.bin"
const PERSIST_SNAPSHOT_FILENAME = "birdkv-persist-snapshot.bin"

type FilePersister struct {
	raftStateFile *os.File
	snapshotFile  *os.File
}

func MakeFilePersister() *FilePersister {
	f1, e1 := os.OpenFile(PERSIST_STATE_FILENAME, os.O_CREATE|os.O_RDWR, 0666)
	f2, e2 := os.OpenFile(PERSIST_SNAPSHOT_FILENAME, os.O_CREATE|os.O_RDWR, 0666)
	if e1 != nil || e2 != nil {
		panic(fmt.Sprintf("无法打开持久化文件 %v %v", e1, e2))
	}
	return &FilePersister{
		f1, f2,
	}
}

func (f *FilePersister) Copy() Persister {
	// 可以不用实现
	return f
}

// 原子写入文件，通过写入到临时文件，再原子重命名实现
func (f *FilePersister) atomicWriteFile(data []byte, targetFile string) {
	tmpfile, _ := os.CreateTemp("", "*.tmp")
	defer tmpfile.Close()
	tmpfile.Write(data)
	os.Rename(tmpfile.Name(), targetFile)
}

func (f *FilePersister) SaveRaftState(state []byte) {
	f.atomicWriteFile(state, PERSIST_STATE_FILENAME)
}

func (f *FilePersister) ReadRaftState() []byte {
	data, err := ioutil.ReadFile(PERSIST_STATE_FILENAME)
	if err != nil {
		panic(fmt.Sprintf("读取持久化文件错误 %v", err))
	}
	return data
}

func (f *FilePersister) RaftStateSize() int {
	info, err := f.raftStateFile.Stat()
	if err != nil {
		panic(fmt.Sprintf("读取持久化文件错误 %v", err))
	}
	return int(info.Size())
}

func (f *FilePersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	// TODO 多个文件如何原子化？
	f.atomicWriteFile(state, PERSIST_STATE_FILENAME)
	f.atomicWriteFile(snapshot, PERSIST_SNAPSHOT_FILENAME)
}

func (f *FilePersister) ReadSnapshot() []byte {
	data, err := ioutil.ReadFile(PERSIST_SNAPSHOT_FILENAME)
	if err != nil {
		panic(fmt.Sprintf("读取持久化文件错误 %v", err))
	}
	return data
}

func (f FilePersister) SnapshotSize() int {
	info, err := f.snapshotFile.Stat()
	if err != nil {
		panic(fmt.Sprintf("读取持久化文件错误 %v", err))
	}
	return int(info.Size())
}
