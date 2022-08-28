package persist

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	PersistStorage = "birdkv-persist"
	RaftState      = "raft-state"
	Snapshot       = "snapshot"
)

type FilePersister struct {
	db *leveldb.DB
}

func MakeFilePersister(id int) *FilePersister {
	db, err := leveldb.OpenFile(fmt.Sprintf("%s-%d", PersistStorage, id), nil)
	if err != nil {
		panic(err)
	}
	return &FilePersister{db}
}

func (f *FilePersister) Copy() Persister {
	// 可以不用实现
	return f
}

func (f *FilePersister) write(key string, data []byte) error {
	return f.db.Put([]byte(key), data, nil)
}

func (f *FilePersister) read(key string) ([]byte, error) {
	data, err := f.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		data = []byte{}
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *FilePersister) size(key string) (int64, error) {
	limit := []byte(key)
	if limit[len(limit)-1]+1 == 0 {
		limit = append(limit, 0)
	} else {
		limit[len(limit)-1]++
	}
	s, err := f.db.SizeOf([]util.Range{{
		Start: []byte(key),
		Limit: limit,
	}})
	if err != nil {
		return 0, err
	}
	return s.Sum(), nil
}

func (f *FilePersister) SaveRaftState(state []byte) {
	err := f.write(RaftState, state)
	if err != nil {
		panic(err)
	}
}

func (f *FilePersister) ReadRaftState() []byte {
	data, err := f.read(RaftState)
	if err != nil {
		panic(err)
	}
	return data
}

func (f *FilePersister) RaftStateSize() int {
	size, err := f.size(RaftState)
	if err != nil {
		panic(err)
	}
	return int(size)
}

func (f *FilePersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	tx, err := f.db.OpenTransaction()
	if err != nil {
		panic(err)
	}
	err = tx.Put([]byte(RaftState), state, nil)
	if err != nil {
		panic(err)
	}
	err = tx.Put([]byte(Snapshot), snapshot, nil)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

func (f *FilePersister) ReadSnapshot() []byte {
	data, err := f.read(Snapshot)
	if err != nil {
		panic(err)
	}
	return data
}

func (f *FilePersister) SnapshotSize() int {
	size, err := f.size(Snapshot)
	if err != nil {
		panic(err)
	}
	return int(size)
}

func (f *FilePersister) Close() {
	_ = f.db.Close()
}
