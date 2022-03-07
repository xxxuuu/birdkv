package net

type ClientEnd interface {
	// Call 调用rpcname，参数args，结果reply，返回值bool表示请求是否成功
	Call(rpcname string, args interface{}, reply interface{}) bool
}
