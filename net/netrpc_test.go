package net

import (
	"birdkv/kvraft"
	"birdkv/persist"
	"fmt"
	"net/http"
	"net/rpc"
	"testing"
	"time"
)

type TestServer struct {
}

type PingArgs struct {
	Args string
}
type PingReply struct {
	Reply string
}

func (t *TestServer) Ping(args *PingArgs, reply *PingReply) {
	reply.Reply = fmt.Sprintf("Pong %s", args.Args)
}

func TestCall(t *testing.T) {
	server1 := &TestServer{}
	c1 := MakeNetClientEnd("127.0.0.1:9998")
	rpc.Register(server1)
	rpc.HandleHTTP()
	go func() {
		err := http.ListenAndServe("127.0.0.1:9998", nil)
		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(2 * time.Second)
	args := &PingArgs{"hello"}
	reply := &PingReply{}
	c1.Call("TestServer.Ping", args, reply)
	if reply.Reply != "Pong hello" {
		t.Fatalf("expect Pong hello, got %v\n", reply.Reply)
	}
}

func TestCall2(t *testing.T) {
	listen := "127.0.0.1:9998"
	servers := []ClientEnd{MakeNetClientEnd(listen)}
	server := kvraft.StartKVServer(servers, 0, persist.MakeFilePersister(), -1)
	rpc.Register(server)
	rpc.HandleHTTP()
	go http.ListenAndServe(listen, nil)

	time.Sleep(time.Second)
	client := kvraft.MakeClerk(servers)
	client.Put("1", "2")
	if res := client.Get("1"); res != "2" {
		t.Fatalf("expect Pong 2, got %v\n", res)
	}
}
