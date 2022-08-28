package persist

import (
	"testing"
)

func TestRaftState(t *testing.T) {
	testdata := []byte("123")

	f := MakeFilePersister(1)
	defer f.Close()
	f.SaveRaftState(testdata)

	got := f.ReadRaftState()
	if string(got[:]) != "123" {
		t.Fatalf("read raft state, excepted: 123, got:%v\n", string(got[:]))
	}
}

func TestStateAndSnapshot(t *testing.T) {
	testdata1 := []byte("123")
	testdata2 := []byte("456")

	f := MakeFilePersister(1)
	defer f.Close()
	f.SaveStateAndSnapshot(testdata1, testdata2)

	got := f.ReadRaftState()
	if string(got[:]) != "123" {
		t.Fatalf("read raft state, excepted: 123, got:%v\n", string(got[:]))
	}
	got = f.ReadSnapshot()
	if string(got[:]) != "456" {
		t.Fatalf("read snapshot, excepted: 456, got:%v\n", string(got[:]))
	}
}
