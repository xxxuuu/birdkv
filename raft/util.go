package raft

import (
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

// Debugging
const Debug = false

type DebugMutex struct {
	mu sync.Mutex
}

func (m *DebugMutex) Unlock() {
	m.mu.Unlock()
}

func (m *DebugMutex) Lock() {
	//debug.PrintStack()
	//DPrintf("尝试获取锁\n")
	t := time.Now().Nanosecond()
	m.mu.Lock()
	cost := time.Now().Nanosecond() - t
	if time.Duration(cost) >= 10*time.Millisecond {
		DPrintf("获取锁花费时间:%v\n", cost)
		debug.PrintStack()
	}
}

func init() {
	if Debug {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
		f, _ := os.OpenFile("raft.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		//f, _ := os.OpenFile(
		//	fmt.Sprintf("./log/raft-[%v].log", time.Now().Format("01-02 15:04:05")),
		//	os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		log.SetOutput(f)
		os.Stderr = f
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("[Raft] "+format, a...)
	}
	return
}
