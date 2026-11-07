# myraft 实施计划

> 规则：**代码与本计划不一致时，必须先更新本计划，再改代码。**
> 状态标记：`[ ]` 未开始 / `[x]` 已完成并通过测试。

目标：用 Go 从零实现 Raft 共识库（无第三方依赖），包含选举、日志复制、WAL 持久化、
apply 通道，以及带故障注入的模拟网络与测试。模块名 `github.com/xiaobaowan1988/myraft`，包路径 `raft/`。

## 目录结构

| 文件 | 内容 |
|---|---|
| `raft/types.go` | 状态枚举、`LogEntry`、`ApplyMsg`、RPC 参数/应答、`Transport` 接口 |
| `raft/raft.go` | `Raft` 结构体、事件循环、选举、日志复制、commit 推进、applyPump |
| `raft/wal.go` | `Storage` 接口、`MemStorage`、`FileWAL`（追加写 + fsync + CRC） |
| `raft/simnet.go` | 进程内模拟网络：丢包、延迟、分区、节点宕机 |
| `raft/*_test.go` | 选举/复制/分区/持久化/混沌测试、WAL 单测 |

## 阶段

- [x] **1. 数据结构与并发模型（单协程事件循环，无互斥锁）**：一个 `run()` 协程独占全部 Raft 状态
  （term、log、角色、commitIndex、nextIndex 等），其他一切都以事件形式投递给它顺序执行：
  RPC 到达（处理器投递闭包并等待结果）、`Start()`/`State()`、定时 tick、RPC 应答。
  铁律：① 状态只在循环协程里读写；② 循环内**不做**网络调用，也不向自己投递并等待（否则死锁），
  发 RPC 由临时协程完成，应答再以事件投递回来；③ 应答事件处理时仍需校验 `term`/`state` 防止过期应答；
  ④ WAL 写入在循环内同步完成（fsync 期间循环阻塞，换取"先持久化再回复"的简单性）。
- [x] **2. 选举引擎**：后台 ticker 驱动；随机选举超时 150–300ms；心跳 50ms；
  当选 Leader 后追加一条 no-op 日志，以便提交前任期遗留的日志。
- [x] **3. RPC 层**：`RequestVote` / `AppendEntries`，严格 term 校验、prevLog 一致性检查，
  冲突时返回 `ConflictIndex/ConflictTerm` 让 Leader 快速回退 `nextIndex`；
  只在**真正冲突**时截断日志（避免乱序/重复 RPC 误删已匹配日志）。
- [x] **4. WAL 持久化**：追加记录 `meta(term,votedFor)` 与 `append(index, entries)`，
  每条记录 `长度 + CRC32 + gob 载荷`，`Sync()` 成功后才回复 RPC；
  加载时丢弃末尾撕裂/损坏的记录并截断文件。
- [x] **5. Apply 通道**：循环协程把新提交的日志交给 `applyPump` 协程（无锁的无界队列：
  `select` 同时收批次、发 `applyCh`），保证上层处理慢时不会阻塞事件循环。
- [x] **6. 混沌测试**：5 节点进程内集群；丢包 15%、随机延迟、随机 2|3 分区、随机宕机/重启；
  断言：任意 index 上各节点 apply 的命令一致、已确认提交的命令不丢、恢复网络后系统可继续提交。

- [x] **7. 确定性回放测试（`raft/detsim_test.go`）**：
  - 重构：Raft 状态机不再直接调用 `time.Now` 和起 goroutine 发 RPC，改为经 `r.now()` 和 `outbox` 接口
    （`sendVote` / `sendAppend` / `deliver`）与外界交互；真实外壳 `shellOut` 用 goroutine 实现，
    测试里的 `simOut` 把消息变成虚拟时钟上的事件。新增 `newRaft`（不起 goroutine）与 `propose`。
  - 模拟器：单线程、虚拟时钟、`(时间, 序号)` 排序的事件堆；网络丢包/重复/延迟/乱序、分区、宕机重启、
    客户端提案全部由**一个种子**的 `math/rand` 决定，模拟器代码里不遍历 map，保证可复现。
  - 每个事件后检查不变量：同 term 至多一个 Leader、term 不回退、commitIndex 不回退且不越界、
    已提交条目全局唯一（状态机安全）、Leader 完整性（任一 Leader 含全部已提交条目）、apply 序号连续；
    定期检查日志匹配性（Log Matching）。
  - 场景：12s 混沌（15% 丢包、5% 重复、随机延迟、分区、宕机）→ 恢复网络 → 要求收敛、唯一 Leader、
    各节点 commitIndex 与 apply 序列一致。
  - 阶段 7 发现并修复的真实 bug：RequestVote 的应答被网络**重复投递**时，选票计数器会把同一节点的
    同意票数两次，导致没拿到多数票就当选（原混沌测试没有"重复"故障，一直没暴露）。
    现改为 `granted[]` 按 peer 去重。
  - 变异验证（故意注入错误，确认模拟器能抓到；这些改动均未保留）：不持久化 term/投票、投票不比较日志新旧、
    AppendEntries 无条件截断、重复投票重复计数——均被检出。
    "旧任期条目直接按多数提交"与"follower 提交时不取 min(LeaderCommit, lastNew)"在当前实现下是**等价变异**
    （新 Leader 先追加 no-op、且总是把日志发到末尾），单独注入检不出；同时去掉 no-op 后，
    前者在 5000 个种子中仅 1 个触发（图 8 场景很稀有）——说明随机模拟不能保证抓到所有稀有交错。
  - 回放验证：同一种子跑两遍，事件轨迹哈希必须相同；失败时打印种子与最近事件，
    用 `go test ./raft -run TestDeterministicReplay -seed=N -trace` 精确复现。

## 与原指南的有意偏差（原因）

1. **Ticker 用 10ms 轮询 + deadline，而不是 `time.Timer` 通道**：`Timer.Reset` 需要先 drain 通道，
   否则有竞态；deadline 方案更简单且不丢事件。
2. **选举超时在每次 `electionTimer.C` 触发时无条件重置的写法不采用**：只在
   收到合法 Leader 的 AppendEntries、投出选票、发起新选举、Leader 因应答中的更高 term 而退位时重置。
3. **并发模型改为事件循环（原指南为大锁）**：按用户要求；不再有 `sync.Mutex`，`Kill()` 通过关闭 `done`
   并等待循环退出来保证之后不再写盘。Applier 用 pump 协程而不是 `Sleep(10ms)` 轮询。
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

确定性模拟的种子扫描（注意自定义参数要放在 `-args` 之后）：
`go test ./raft -run TestDeterministicReplay -args -seeds=2000`；复现单个种子：
`go test ./raft -run TestDeterministicReplay -args -seed=N -trace`
