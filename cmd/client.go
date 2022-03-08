package main

import (
	"birdkv/kvraft"
	"birdkv/net"
	"flag"
	"fmt"
)

func main() {
	flag.Parse()
	args := flag.Args()
	var servers []net.ClientEnd
	for i := range args {
		servers = append(servers, net.MakeNetClientEnd(args[i]))
	}

	client := kvraft.MakeClerk(servers)

	for true {
		fmt.Print("> ")
		var op string
		var key string
		var value string
		fmt.Scanf("%s %s", &op, &key)
		if kvraft.OpType(op) != kvraft.GET {
			fmt.Scanf("%s", &value)
		}

		var res string
		switch kvraft.OpType(op) {
		case kvraft.GET:
			res = client.Get(key)
		case kvraft.PUT:
			res = client.Put(key, value)
		case kvraft.APPEND:
			res = client.Append(key, value)
		}
		fmt.Println(res)
	}
}
