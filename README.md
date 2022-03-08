# BirdKV

一个基于 Raft 共识算法的分布式 key-value 存储服务，从 MIT 6.824 分布式系统课程实验中扩展而来


## Get Started
### 编译
进入 `cmd` 目录

编译客户端
```shell
go build -o birdkv-cli client.go 
```
编译服务端
```shell
go build -o birdkv-server server.go
```

### 运行
在每个节点上启动服务端，参数中传递服务端节点地址，顺序最好一致，最后一个参数表示当前服务器是第几个节点（从 1 开始）。需要至少 3 个节点才能实现可靠的容错
```shell
birdkv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 1
```
启动客户端，传递服务器节点地址
```shell
birdkv-cli 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002
```
客户端连接成功后，进入交互模式，支持 3 种操作：
- `Get k`：获取 key `k` 的值
- `Put k v`：设置 key `k` 的值为 `v`
- `Append k v`：在 key `k` 上追加值 `v`

```shell
> Put x 100
100
> Get x
100
> Append x 200
100200
```

## TODO
- [x] 实现 Raft（Leader选举，日志复制，持久化，快照）
- [x] 基于 Raft 构建可容错、线性化的分布式 key-value 服务
- [ ] Raft 成员变更
- [ ] Multi-Raft, 数据分区
- [ ] 更多数据结构支持
- [ ] 分布式事务

## 参考资料
1. [In Search of an Understandable Consensus Algorithm
   (Extended Version)(Raft 论文)](http://nil.csail.mit.edu/6.824/2021/papers/raft-extended.pdf)
2. [MIT 6.824 分布式系统课程](http://nil.csail.mit.edu/6.824/2021/index.html)
3. [条分缕析 Raft 算法 | 多颗糖](https://mp.weixin.qq.com/s/lUbVBVzvNVxhgbcHQBbkkQ)
4. [条分缕析 Raft 算法(续)：日志压缩和性能优化 | 多颗糖](https://mp.weixin.qq.com/s?__biz=MzIwODA2NjIxOA==&mid=2247484172&idx=1&sn=f4500241002878eb23fdcadbcd8083af&chksm=970980c9a07e09dfd7d6ba064f4ef63bbae438d3e571475ee48073ecb1eb4316b30062a9b758&cur_album_id=1538679041988345857)
5. [线性一致性和 Raft | PingCAP](https://pingcap.com/zh/blog/linearizability-and-raft)
6. [TiKV 功能介绍 - Raft 的优化 | PingCAP](https://pingcap.com/zh/blog/optimizing-raft-in-tikv/)