package main

import (
	"birdkv/kvraft"
	"birdkv/net"
	"birdkv/persist"
	"flag"
	"net/http"
	"net/rpc"
	"strconv"
)

func main() {
	flag.Parse()
	args := flag.Args()
	var servers []net.ClientEnd
	for i := 0; i < len(args)-1; i++ {
		servers = append(servers, net.MakeNetClientEnd(args[i]))
	}
	me, _ := strconv.Atoi(args[len(args)-1])
	me--

	// 默认触发快照大小为 512M 后期改成可配置
	maxsaftstate := 1024 * 1024 * 512
	server := kvraft.StartKVServer(servers, me, persist.MakeFilePersister(), maxsaftstate)
	rpc.Register(server)
	rpc.Register(server.Rf)
	rpc.HandleHTTP()
	http.ListenAndServe(args[me], nil)
}
