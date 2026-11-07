# myraft 实施计划

> 规则：**代码与本计划不一致时，必须先更新本计划，再改代码。**
> 状态标记：`[ ]` 未开始 / `[x]` 已完成并通过测试。

目标：用 Go 从零实现 Raft 共识库（无第三方依赖），包含选举、日志复制、WAL 持久化、
apply 通道，以及带故障注入的模拟网络与测试。模块名 `github.com/xiaobaowan1988/myraft`，包路径 `raft/`。

## 目录结构

| 文件 | 内容 |
|---|---|
| `raft/types.go` | 状态枚举、`LogEntry`、`ApplyMsg`、RPC 参数/应答、`Transport` 接口 |
| `raft/raft.go` | `Raft` 结构体、选举、日志复制、commit 推进、applier |
| `raft/wal.go` | `Storage` 接口、`MemStorage`、`FileWAL`（追加写 + fsync + CRC） |
| `raft/simnet.go` | 进程内模拟网络：丢包、延迟、分区、节点宕机 |
| `raft/*_test.go` | 选举/复制/分区/持久化/混沌测试、WAL 单测 |

## 阶段

- [x] **1. 数据结构与并发防御**：单把 `mu` 保护全部状态；所有 RPC 调用前释放锁；
  RPC 返回后重新加锁并校验 `term`/`state` 是否仍然有效（防止处理过期应答）。
- [x] **2. 选举引擎**：后台 ticker 驱动；随机选举超时 150–300ms；心跳 50ms；
  当选 Leader 后追加一条 no-op 日志，以便提交前任期遗留的日志。
- [x] **3. RPC 层**：`RequestVote` / `AppendEntries`，严格 term 校验、prevLog 一致性检查，
  冲突时返回 `ConflictIndex/ConflictTerm` 让 Leader 快速回退 `nextIndex`；
  只在**真正冲突**时截断日志（避免乱序/重复 RPC 误删已匹配日志）。
- [x] **4. WAL 持久化**：追加记录 `meta(term,votedFor)` 与 `append(index, entries)`，
  每条记录 `长度 + CRC32 + gob 载荷`，`Sync()` 成功后才回复 RPC；
  加载时丢弃末尾撕裂/损坏的记录并截断文件。
- [x] **5. Apply 通道**：独立 applier 协程，用 `sync.Cond` 唤醒；发送 `applyCh` 时不持锁。
- [x] **6. 混沌测试**：5 节点进程内集群；丢包 15%、随机延迟、随机 2|3 分区、随机宕机/重启；
  断言：任意 index 上各节点 apply 的命令一致、已确认提交的命令不丢、恢复网络后系统可继续提交。

## 与原指南的有意偏差（原因）

1. **Ticker 用 10ms 轮询 + deadline，而不是 `time.Timer` 通道**：`Timer.Reset` 需要先 drain 通道，
   否则有竞态；deadline 方案更简单且不丢事件。
2. **选举超时在每次 `electionTimer.C` 触发时无条件重置的写法不采用**：只在
   收到合法 Leader 的 AppendEntries、投出选票、发起新选举、Leader 因应答中的更高 term 而退位时重置。
3. **Applier 用 `sync.Cond` 而不是 `Sleep(10ms)` 轮询**：无空转、延迟更低。
4. **传输层抽象为 `Transport` 接口（带类型的两个方法），不直接依赖 `net/rpc`**：
   便于模拟网络注入故障；`net/rpc` 适配器列为后续工作，不在本次范围。
5. **`Command` 仍为 `interface{}`**：WAL 用 gob，自定义类型需 `gob.Register`；
   `Command == nil` 保留给 Leader no-op，不能用作业务命令。
6. **模拟网络延迟按比例缩小**：指南的 0–2s 延迟远大于 150ms 选举超时，会使集群长期无主；
   采用"常规 0–20ms + 5% 概率 500ms 长延迟"。
7. **暂不实现**：快照/日志压缩、成员变更、线性一致读、客户端去重（均不在原指南范围内）。
8. **WAL 每条记录单独 gob 编码**（含类型描述，体积偏大）：功能正确，优化（自定义二进制/protobuf）留待后续。

## 验证方式

`go vet ./... && go test -race ./...`
