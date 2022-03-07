package net

import (
	"net/rpc"
)

type NetClientEnd struct {
	ipaddr string
}

func MakeNetClientEnd(ipaddr string) *NetClientEnd {
	return &NetClientEnd{
		ipaddr,
	}
}

func (n *NetClientEnd) Call(rpcname string, args interface{}, reply interface{}) bool {
	rpc, _ := rpc.DialHTTP("tcp", n.ipaddr)
	defer func() {
		_ = recover()
	}()
	defer rpc.Close()
	rpc.Call(rpcname, args, reply)
	return true
}
