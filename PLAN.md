# myraft 实施计划

> 规则：**代码与本计划不一致时，必须先更新本计划，再改代码。**
> 状态标记：`[ ]` 未开始 / `[x]` 已完成并通过测试。

目标：用 Go 从零实现 Raft 共识库（无第三方依赖），包含选举、日志复制、WAL 持久化、
apply 通道，以及带故障注入的模拟网络与测试。模块名 `github.com/xiaobaowan1988/myraft`，包路径 `raft/`。

## 目录结构

| 文件 | 内容 |
|---|---|
| `raft/types.go` | 状态枚举、`LogEntry`、`ApplyMsg`、`Message`（四种消息）、`Transport`（异步 `Send`）与 `Endpoint`（`Step`/`ReportUnreachable`）接口 |
| `raft/core.go` | **纯状态机 `core`**：选举、日志复制、commit 推进；不做任何 IO，输出累积在 `Ready` 里 |
| `raft/raft.go` | **IO 外壳 `Raft`**：事件循环、`flush`（先落盘再发送/回复/apply）、applyPump、对外 API |
| `raft/wal.go` | `Storage` 接口、`MemStorage`、`FileWAL`（追加写 + fsync + CRC） |
| `raft/simnet.go` | 进程内模拟网络（异步消息）：丢包、重复、延迟乱序、分区、宕机、发送队列上限；单个调度协程 |
| `raft/*_test.go` | 选举/复制/分区/持久化/混沌测试、WAL 单测 |

## 阶段

- [x] **1. 数据结构与并发模型（单协程事件循环，无互斥锁）**：一个 `run()` 协程独占全部 Raft 状态
  （term、log、角色、commitIndex、nextIndex 等），其他一切都以事件形式投递给它顺序执行：
  RPC 到达（处理器投递闭包并等待结果）、`Start()`/`State()`、定时 tick、RPC 应答。
  铁律：① 状态只在循环协程里读写；② 循环内**不做**网络调用，也不向自己投递并等待（否则死锁），
  发 RPC 由临时协程完成，应答再以事件投递回来；③ 应答事件处理时仍需校验 `term`/`state` 防止过期应答；
  ④ 状态机本身不写盘（见阶段 8）：写盘由外壳统一完成（阶段 15 起由独立的 ready 协程在批次交接后完成，事件循环不再被 fsync 阻塞）。
- [x] **2. 选举引擎**：后台 ticker 驱动；随机选举超时 150–300ms；心跳 50ms；
  当选 Leader 后追加一条 no-op 日志，以便提交前任期遗留的日志。
- [x] **3. 消息层**（最初是 RPC，阶段 14 起改为异步消息 `MsgVote`/`MsgVoteResp`/`MsgApp`/`MsgAppResp`，校验逻辑不变）：严格 term 校验、prevLog 一致性检查，
  冲突时返回 `ConflictIndex/ConflictTerm` 让 Leader 快速回退 `nextIndex`；
  只在**真正冲突**时截断日志（避免乱序/重复 RPC 误删已匹配日志）。
- [x] **4. WAL 持久化**：追加记录 `meta(term,votedFor)` 与 `append(index, entries)`，
  每条记录 `长度 + CRC32 + gob 载荷`，`Sync()` 成功后才发送消息/回复 RPC/交付 apply；
  加载时丢弃末尾撕裂/损坏的记录并截断文件。
- [x] **5. Apply 通道**：循环协程把新提交的日志交给 `applyPump` 协程（无锁的无界队列：
  `select` 同时收批次、发 `applyCh`），保证上层处理慢时不会阻塞事件循环。
- [x] **6. 混沌测试**：5 节点进程内集群；丢包 15%、随机延迟、随机 2|3 分区、随机宕机/重启；
  断言：任意 index 上各节点 apply 的命令一致、已确认提交的命令不丢、恢复网络后系统可继续提交。

- [x] **7. 确定性回放测试（`raft/detsim_test.go`）**：
  - 重构：Raft 状态机不再直接调用 `time.Now` 和起 goroutine 发 RPC，只经 `now()` 与外界交互，
    输出放进 `Ready`（阶段 8）；真实外壳用 goroutine 发送，模拟器把消息变成虚拟时钟上的事件。
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
    用 `go test ./raft -run TestDeterministicReplay -args -seed=N -trace` 精确复现。

- [x] **8. 写盘移出核心（参考 etcd/raft 的 Ready 模式）**：
  - 拆分为两个类型：`core`（纯状态机，不持有 `Storage`、channel、`time.Now`、goroutine）与 `Raft`（外壳）。
    `core` 每处理一个事件，把待办累积到 `Ready{Persist, Votes, Appends, Apply}`；
    `core` 由已恢复的状态（term/votedFor/log）构造，**不接触 `Storage`**。
  - 外壳 `flush()` 的固定顺序：① `Storage.Append(Persist)`（fsync 成功）→ ② 发 RPC 请求 → ③ 交付 apply →
    ④ 放行等待中的 RPC 应答。因此"先持久化再对外可见"由外壳**统一保证**，而不是散落在每个处理函数里；
    一个事件内的多条记录合并为一次 `Append`（一次 fsync）。
  - 模拟器同样按此顺序驱动，并新增两类故障：**落盘前崩溃**（丢弃未落盘的 Ready，不发消息/不回复）与
    **落盘后、发送前崩溃**（记录已落盘但消息/应答丢失），以验证持久化边界上的安全性。
  - 约束测试：`TestCoreHasNoIO` 扫描 `core.go`，禁止出现 `Storage`、`chan`、`go `、`sync.`、`time.Now`、`os.` 等。
  - 存储约束：同一份存储同一时刻只能被一个 `Raft` 实例使用（`Kill()` 等待循环退出保证旧实例不再写入）。
  - 验证结果：崩溃概率 0.6%（每次非空 flush、每个崩溃点），2000 个种子全部通过。变异验证：
    "先回复、后落盘"（回复不依赖发送方是否已崩溃）被稳定检出（1000 种子中 34 个失败，含"已提交条目丢失"）。
    **未检出**：只漏掉"投票后 votedFor 落盘"这一处（15000 个种子无失败）——需要某节点投票、回复送达后崩溃、
    重启后又投给同任期的另一候选人且两人都凑够多数，组合太稀有，属于已知检测盲区。
    教训：写变异时要确认它真的改变了行为（本阶段有两个变异因模拟器的可达性/返回值逻辑而是空操作）。

- [x] **9. 限制在途协程数量（RPC 超时 + 每个 follower 至多一个在途 AppendEntries）**〔其中的 RPC 超时、`context`、协程上界已被阶段 14 取代：超时改由核心在 tick 里按 `AppendTimeout` 判定，节点不再为请求起协程；在途窗口的思路保留〕：
  - 问题：此前每次广播都起新协程，对端卡住时数量无上限。
  - `Transport` 方法增加 `context.Context` 参数；外壳用 `RPCTimeout`（默认 80ms）为每个 RPC 设超时，
    超时即视为丢失。Transport 实现必须遵守 ctx（SimNet 已遵守）。
  - 状态机内新增 per-peer `inflight` + 序号 `seq`：`replicate` 在已有在途请求时直接跳过（心跳也一样）；
    应答（任意任期内的有效应答）或新增的 `onAppendLost(peer, seq)`（超时/丢失事件）才会释放名额；
    释放按 `seq` 匹配，过期的应答/超时不会误释放新请求的名额；成为 Leader 时重置。
    成功应答后若还有未发送的日志，立即继续发送（避免吞吐被心跳间隔限制）。
  - 约束：`RPCTimeout + HeartbeatInterval < ElectionTimeoutMin`，否则一次丢包可能让 follower 超过选举超时（测试断言）。
  - 投票请求不设名额：它们受超时限制，且选举间隔（≥150ms）大于超时，在途数量天然有界。
  - 模拟器同步：每个 AppendEntries 另排一个 +RPCTimeout 的超时事件；超过期限才到达的应答被丢弃（与外壳一致）；
    请求本身仍可迟到并被对端处理。
  - 新测试：`TestInFlightBounded`——对端永不应答（一种遵守 ctx，一种完全无视 ctx）时，协程数量必须有界。
  - 上界：每节点常驻 2 个 + 至多 (n-1) 个 AppendEntries + 至多 (n-1) 个投票请求（阶段 11 起 AppendEntries 的界改为 (n-1)×窗口）。
  - 验证：`TestInFlightBounded` 去掉名额限制后失败（协程多出 74 个、每个 follower 同时 37 个请求在途），
    恢复后通过。模拟器新增不变量 `STUCK SLOT`：Leader 的在途名额存在超过 `RPCTimeout` 即失败；
    "超时不释放名额"的变异 300 个种子全部被检出，正常代码 2000 个种子通过。
    教训：该变异不会让正常的收敛/进展检查失败（被冷落的 follower 会选举超时，系统靠换届自愈，只是性能变差），
    最初的"恢复后任期稳定"检查也抓不到，改为直接检查机制本身才有效。

- [x] **10. 提案侧限流（对应 etcd 的 `MaxUncommittedEntriesSize`）**：
  - 问题：客户端写入快于复制/提交时（例如被隔离的少数派 Leader），Leader 日志会无限增长。
  - Leader 的"未提交条目数" = `len(log)-1-commitIndex`；`>= MaxUncommittedEntries`（默认 1024，`<= 0` 表示不限）时，
    `propose` 拒绝并返回 `ErrProposalDropped`，不阻塞。Leader 当选时追加的 no-op 不受限。
    用条目个数而非字节数：命令是 `interface{}`，大小不可知（etcd 用字节，因为它的条目是序列化后的字节串）。
  - 新 API `Propose(cmd) (index, term, err)`，错误为 `ErrNotLeader` / `ErrProposalDropped` / `ErrStopped` /
    `ErrNilCommand`；`Start` 保留为薄封装（第三个返回值现在表示"已接受"，不再只表示"是 Leader"）。
  - 已知限制：从上一任期继承的未提交日志也计入，若超过上限，新 Leader 在它们提交前会拒绝所有提案。
  - 验证：外壳测试（对端永不应答 → 恰好接受 `Max-1` 个后全部被拒；放行后恢复接受）；
    模拟器把上限调小到 20，新增不变量"Leader 本任期的未提交条目 ≤ 上限+1（no-op）"，并要求扫描中确有提案被拒。
    变异验证（去掉限流检查）：外壳测试立即失败（接受 50 个而非 9 个）；模拟器 300 个种子中仅 11 个检出——
    模拟器对该功能的覆盖偏弱（约每 10 个种子才有 1 个触发拒绝），外壳测试才是主要保障。

- [x] **11. 复制流水线（取代"每个 follower 只有一个在途请求"，对应 etcd 的 Progress 探测/复制状态与 MaxInflightMsgs）**：
  - 问题：每个 follower 一次只能有 1 个在途 AppendEntries，吞吐 ≈ 单批大小 / 往返时间。
  - 每个 follower 两种状态：**探测**（刚当选或出错后；`nextIndex` 未确认，一次只发 1 个请求，与现在一致）、
    **复制**（已确认匹配；发送时乐观推进 `nextIndex`，允许最多 `MaxInflightAppends` 个请求在途，默认 8）。
    单个请求最多带 `MaxAppendEntries` 条日志（默认 128），以便分批流水。
  - 状态转移：探测中收到包含日志的成功应答 → 复制；复制中收到拒绝应答，或任一在途请求超时/丢失 →
    清空在途集合并回到探测（`nextIndex` 按冲突提示或 `matchIndex+1` 重置）。
  - 应答归属：每个请求有序号；在途集合里的序号才算"当前"应答。过期的拒绝应答忽略；过期的成功应答仍可单调推进
    `matchIndex`（同一任期内它依然成立），但不改变状态。成功应答后继续填满窗口。
  - 纯心跳（没有新日志时）不带日志、`prevLogIndex = matchIndex`（已确认匹配，不受乱序影响），
    follower 的 commit 因此不会越过已确认的前缀；数据请求同样携带 `LeaderCommit`。
  - **协程上界变化**：AppendEntries 协程上界由 `n-1` 变为 `(n-1) × MaxInflightAppends`；该代价已在阶段 12 通过固定 worker 协程消除
    （协程数不再随负载变化）。探测状态下仍恒为每个 follower 1 个在途请求。
  - 验证：模拟器按种子变化窗口（1–6）与批大小（2/8/128）；新增不变量：在途数 ≤ 窗口、探测状态 ≤ 1、
    每个在途请求存活 ≤ `RPCTimeout`、`matchIndex < nextIndex ≤ len(log)`；
    新测试 `TestPipelineThroughput`：在带延迟的传输上，窗口 8 的提交速度必须明显快于窗口 1。
  - 验证结果：正常代码 2000 个种子通过；`TestPipelineThroughput`（600 条、批大小 10、往返 30ms）窗口 1 用 1.9s、
    窗口 8 用 0.28s，约 6.9 倍。变异验证：发送时乐观推进 `matchIndex`（未确认就算已复制）300 个种子中 205 个失败
    （含"已提交条目丢失"）；窗口不设上限 240 个失败。

- [x] **12. 固定 worker 协程**〔已被阶段 14 取代：worker 池、队列整体删除，发送由传输层负责〕（取代"每个请求一个协程"，对应 etcd rafthttp 的 pipeline：每 peer 固定 4 个 worker + 缓冲队列）**：
  - 问题：此前每个 RPC 起一个临时协程，协程数随负载浮动；窗口调大后上界 `(n-1)×窗口` 也随之变大。
  - `Make` 时为每个 follower 一次性启动：`窗口` 个 AppendEntries worker + 1 个投票 worker，之后协程数与负载无关。
    发送只是把任务放进该 peer 的有缓冲 channel；worker 取任务、调用 `Transport`、再把结果作为事件投回事件循环。
  - （阶段 12 最初让窗口 = worker 数、不排队；该设计已被阶段 13 取代，以与 etcd 对齐。）
  - 投票 worker 独立于 AppendEntries worker，避免投票被卡住的复制请求拖慢。
  - 仍然要求 Transport 遵守 ctx：无视 ctx 的调用会永久占住一个 worker（探测状态下一个 follower 只会占 1 个）。
  - 测试：`TestInFlightBounded` 断言协程数 ≤ 常驻上界 + 少量计时器噪声，且**不随时间增长**；
    `TestPipelineThroughput` 额外断言处理 600 条提案期间协程数几乎不变（证明没有逐请求协程）。
  - 验证结果：3 节点、对端永不应答时协程数稳定在常驻上界附近（实测 15–16，上界 12 + 少量计时器协程），
    前后两个 1 秒窗口持平；600 条提案、窗口 8 期间协程数几乎不变，吞吐仍约 6.8 倍。
    变异验证：把 `sendAppend` 改回"每请求一个协程"，`TestPipelineThroughput` 失败（窗口 8 时协程多出 26 个）。
    模拟器 2000 个种子、`go test -race` 全部通过。
  - 代价：常驻协程增加（n=5、窗口 4 时每节点 22 个，空闲时也在），换取与负载无关的确定上界。

- [x] **13. 与 etcd 对齐：worker 数 < 队列容量 < 窗口上限**〔已被阶段 14 取代：worker、队列、代数跳过随外壳一起删除；`MaxInflightAppends` 窗口保留，`AppendWorkers`/`AppendQueueSize` 删除〕（取代阶段 12 的"窗口 = worker 数"）：
  - etcd rafthttp pipeline：固定 4 个 worker 从带缓冲（64）的队列取消息；**超时从 worker 开始调用时算起**（入队时不计时）；
    调用失败 → `ReportUnreachable`（raft 退回探测）并继续取下一条；队列满 → 丢弃并 `ReportUnreachable`。
  - 三个独立参数：`AppendWorkers`（默认 4，决定常驻协程）、`AppendQueueSize`（默认 64，对应 `pipelineBufSize`）、
    `MaxInflightAppends`（默认 32，核心的窗口上限，可大于 worker 数，多出的请求排队）。
    常驻协程 = `2 + (n-1)×(AppendWorkers+1)`，与窗口大小无关（n=5 为 22）。
  - `RPCTimeout` 现在从 worker 开始调用时起算；请求总寿命 = 排队时间 + 调用时间，
    因此 `STUCK SLOT` 不变量的上界改为 `(ceil(窗口/worker数)+1) × RPCTimeout`。
  - 队列满：AppendEntries 按"丢失"处理（核心 `onAppendLost` → 退回探测，对应 etcd 的丢弃并上报不可达）；
    投票请求直接丢弃（Raft 容忍丢失）。投票 worker 仍独立（1 个，队列 2）。
    注：队列满时是否上报丢失不影响活性——不上报只会在窗口里留下"幽灵名额"（最多 `窗口-(worker+队列)` 个，
    真实可用名额始终 ≥ worker+队列），被丢弃的日志会在下一次因断档被拒绝后重发；上报则让核心立即重发。
  - **过期任务跳过（etcd 没有，为避免排队带来的新问题而增加）**：核心为每个 follower 维护单调递增的"代数"，
    每次退回探测或当选时更新；每个请求带当时的代数。外壳在入队时记下该 follower 的最新代数，worker 出队时
    发现任务代数更旧就直接丢弃（核心早已忘记它，无需事件）。否则退回探测后，新的探测请求会排在最多
    `AppendQueueSize` 个已失效的旧请求后面。
  - 模拟器现在**建模 worker 池与队列**（每节点每 follower：K 个 worker、容量 Q 的 FIFO、队列满即丢、代数跳过、
    超时从开始调用起算，worker 在应答或超时时释放）；K、Q 与窗口按种子变化。
  - 验证：外壳测试 `TestQueueFullDropsAndRecovers`（队列满丢弃后恢复，并发不超过 worker 数，协程不增长）、
    `TestStaleQueuedRequestsSkipped`（重置后队列里的旧请求被跳过；去掉跳过、或去掉"等事件循环处理完丢失事件"都会失败，
    后者说明那个竞态是真实的）、`TestQueueOverflowWithSlowFollowerKeepsReplicating`、`TestPipelineThroughput`
    （窗口 8/worker 8 约 6.9 倍；窗口 32/worker 4 约 2.6 倍——提案逐条到达时先塞满 32 个只带 1 条日志的小请求，
    worker 每个往返只处理 4 个，所以窗口大于 worker 数并不带来成比例的收益）。
  - 模拟器覆盖统计（300 种子）：排队约 3.4 万次、队列满丢弃约 2400 次、过期跳过约 1.1 万次，三条路径都被要求触发过。
    模拟器里过期跳过比外壳更确定：它在处理完应答并 flush 之后才释放 worker；外壳只在"丢失"路径上等待事件循环，
    "拒绝应答"路径上 worker 不等待，偶尔会多执行一个过期请求（浪费一次调用，不影响正确性）。
  - 仍与 etcd 不同：etcd 的主路径是**流**（msgAppV2 流 + 消息流，写协程顺序发送，应答作为反向流里的独立消息），
    pipeline 只是降级通道和快照通道；本项目的 `Transport` 是请求/应答式 RPC，worker 池对应 etcd 的 pipeline，
    不改造为流式异步消息。

- [x] **14. 请求/应答式 Transport 改为流式异步消息（对应 etcd 的 stream：消息单向发送，应答是反向的独立消息）**：
  - **消息**：`Message{Type, From, To, Term, ...}`，四种类型：`MsgVote`、`MsgVoteResp`、`MsgApp`（带 `Seq`、`PrevLogIndex/Term`、
    `Entries`、`LeaderCommit`）、`MsgAppResp`（回显 `Seq`，成功时带 `Match`，失败时带 `ConflictIndex/Term`）。
    应答不再依赖"原请求参数"：`Match` 由 follower 算好随应答带回，leader 只用 `Seq` 在 `pending` 里找到发送时间和 `prev`。
  - **接口**：`Transport.Send(Message)` 非阻塞、尽力而为（可静默丢弃，必须有界）；传输层把入站消息交给 `Endpoint.Step(Message)`，
    并在连接断开时调用 `Endpoint.ReportUnreachable(peer)`（对应 etcd 的 `ReportUnreachable`，核心据此立即退回探测）。
    `*Raft` 实现 `Endpoint`。删除 `RequestVote`/`AppendEntries` RPC 入口与 `Handler` 接口。
  - **核心**：`step(Message)` 统一入口；输出改为 `Ready.Messages`（含应答）。因此"先持久化再对外可见"对应答也自动成立——
    应答就是 `Ready` 里的一条消息，在落盘之后才与其他消息一起发出；外壳里"放行等待中的 RPC 调用方"的 `release` 只保留给
    `Propose`/`State`。
  - **超时挪进核心**：不再有 `context`/`RPCTimeout`。核心在每次 tick 检查每个 follower 最旧的在途 `MsgApp`，
    超过 `AppendTimeout`（默认 80ms，即原 `RPCTimeout`）视为丢失 → 退回探测（与之前"丢失事件"同一处理）。
    约束改为 `AppendTimeout + tickInterval + HeartbeatInterval < ElectionTimeoutMin`。
    投票请求不需要超时（候选人本来就靠选举超时重试）。
  - **窗口保留**：`MaxInflightAppends`（默认 32）仍是核心的流控（对应 etcd 的 `MaxInflightMsgs`），探测/复制状态机、乐观
    `nextIndex`、`matchIndex` 只由成功应答推进、心跳锚定 `matchIndex` 全部不变。
  - **节点协程**：（阶段 15 起为 `run`、`readyLoop`、`applyPump` 三个）**不为任何消息起协程**；发送、排队、收发协程全部属于传输层。
    删除 worker 池、队列、代数跳过（阶段 12、13 的外壳机制）及 `AppendWorkers`/`AppendQueueSize`。
    后果：重置后传输层里可能还有旧的 `MsgApp` 在途，到达后按 `Seq` 查不到、被当作过期应答（成功的仍可单调推进 `Match`），
    无害；"新探测排在旧消息后面"的队头阻塞属于传输层问题（etcd 同样如此）。
  - **参考传输 `SimNet`**：单个调度协程 + 按送达时间排序的堆；`Send` 在发送端立即判定：目标不可达 → 异步通知发送方
    `ReportUnreachable`（对应流写失败）；随机丢包 → 静默丢弃；按概率重复；随机延迟（造成乱序）；每个 (from,to)
    在途消息数有上限，超过则丢弃并报不可达（对应 etcd 的 `streamBufSize` 满）。`Send` 不阻塞、不起协程。
  - **模拟器**：同一套故障模型作用于消息（丢失/重复/乱序/分区/宕机/落盘边界崩溃），分区或宕机时对发送方触发
    `onUnreachable`；删除 worker 池/队列建模。`STUCK SLOT` 不变量改为：leader 最旧的在途 `MsgApp` 年龄 ≤ `AppendTimeout` + 一个 tick。
  - **测试**：外壳测试改用"假对端"（按消息脚本化应答，可带延迟）；新增 `TestNodeStartsFixedGoroutines`
    （节点只多出 2 个协程，且不随提案数、选举次数增长）与 `TestUnansweredAppendsBounded`（对端永不应答时，
    同一 follower 的两次 `MsgApp` 至少间隔 `AppendTimeout`，且持续重试）；吞吐、限流、基础集群与混沌测试保留。
  - **验证结果**：模拟器 2000 个种子、`go test -race` 全部通过；覆盖统计（300 种子）：不可达通知约 16.6 万次、
    超时重置约 2.2 万次、流水线最深 6 个请求，三条路径都被要求触发过。外壳测试：节点恰好只多出 2 个协程，
    且 2000 条提案、对端永不应答时仍是 2 个；窗口 1/8/32 在 RTT 30ms 下提交 600 条用时 1.87s/0.28s/0.09s
    （6.7×/19.9×；不再受 worker 数限制）。
    变异验证：核心不再判定超时 → 模拟器 300/300 种子失败（`STUCK REQUEST`），外壳测试也失败；follower 在应答里谎称整条日志
    都匹配 → 31 个种子失败（含"已提交条目丢失"）；先发送后落盘 → 1000 个种子中 16 个失败（含"已提交条目丢失"）。
  - **模型修正（本阶段发现）**：模拟器与 `SimNet` 原先在送达时要求**发送方仍在运行**，等于"发送方一崩溃，它已发出的消息就消失"，
    这使"先发后存"这类错误无处暴露（上一轮的同类变异也因此没被抓到）。改为送达只要求接收方在运行且未被分区，
    之后"先发后存"的变异才被检出。
  - **仍然没做**：etcd 的 msgApp 流还会把多条消息合并发送、按流做压缩编码，真实网络（TCP 流、断线重连）适配器也未实现。

- [x] **15. 采用 etcd 的 Ready/Advance 模型（节点协程与"应用"协程分离，落盘期间继续处理事件并合并输出）**：
  - 问题：此前事件循环在 `flush` 里同步做 fsync 与发送，期间不处理任何事件——每个事件一次 fsync（200 次提案 = 200 次
    fsync），慢盘会拖延 tick 与消息处理。
  - **节点协程 `run`**：只处理事件（`events`、tick）并让核心累积输出；当"没有未完成的 Ready 且核心有输出"时取走输出
    （`takeReady`）并通过 `readyc` 交出，等待 `advancec` 确认；**确认之前不交出下一个 Ready**，期间到达的事件照常处理，
    其输出累积到下一个 Ready（自然合并）。**应用协程 `readyLoop`**：从 `readyc` 取 Ready，按"先落盘 → 再发送 →
    再交付 apply → 放行等待者"的固定顺序处理，最后向 `advancec` 确认（`Advance`）。
  - **安全论证（落盘期间核心状态领先于已落盘状态，仍然安全）**：批次严格串行，且每批先落盘再发送。leader 统计自己的副本时，
    这份日志可能还在待落盘的批次里，但 follower 只有收到该批次发出的 `MsgApp` 才可能确认它，而 `MsgApp` 要等该批落盘之后才发出，
    所以不会出现"多数派里有一份未落盘的 leader 副本"；当选之前，所有此前暂存的记录都已随 `MsgVote` 所在批次落盘。
    与 etcd 的区别：etcd 允许 leader 先发送再落盘（论文 10.2.1），并为此区分"落盘后才能发"的消息；这里保持更保守的先落盘再发送。
  - **`Propose`**：只有产生了持久化效果才等待其所在批次落盘后返回；被拒绝的立即返回。等待期间若节点被停止则返回 `ErrStopped`
    （结果不确定：可能已落盘也可能没有）。`State()` 立即应答，不等待落盘（它只是观测）。
  - **`Kill`**：关闭 `done` 并等待 `run` 与 `readyLoop` 都退出（正在落盘的批次会处理完），因此返回之后不会再写存储。
  - **协程**：每节点 3 个（`run`、`readyLoop`、`applyPump`），仍不为任何消息起协程。
  - **已知代价**：核心给请求打的时间戳是创建时刻，而不是实际发出时刻；落盘卡顿超过 `AppendTimeout` 时，尚未发出的请求会被当作丢失而退回探测（无害，
    只是多一次重发）。
  - **模拟器**：新增"磁盘延迟"（多数 ≤3ms，约 9% 为 10–40ms，约 1% 为 150ms 卡顿）与"Ready 在途"状态：取走输出后在随机延迟后才落盘/发送，
    期间的事件继续处理并累积；在途时崩溃则整批丢失；崩溃点保留在落盘前后。
  - **测试**：`TestPersistBeforeSend`（应答发出时，对应的状态已落盘——补上此前一直缺的外壳顺序测试）、
    `TestRespondsWhileDiskIsSlow`（磁盘慢时 `State()` 仍然很快）、`TestBatchesWritesWhileDiskIsSlow`（200 个并发提案远少于 200 次存储写入）；
    `TestNodeStartsFixedGoroutines` 的期望改为 3 个。
  - **验证结果**：`go test -race`、模拟器 2000 个种子全部通过。实测：200 个并发提案在 20ms/次的慢盘上只触发 10 次存储写入
    （此前每个事件一次 fsync，是 200 次）；慢盘期间 `State()` 仍立即应答、任期已前进而对应记录尚未落盘，且应答在落盘之后才发出。
    变异验证：外壳先发送后落盘 → `TestPersistBeforeSend` 失败；模拟器先发送后落盘 → 1000 个种子中 11 个失败（含"已提交条目丢失"）；
    **核心里投票不暂存 `votedFor` → 300 个种子全部失败**（`VOTE NOT DURABLE`），这是此前 15000 个种子也抓不到的盲区。
  - **模拟器不变量的调整（本阶段）**：原先的"节点任期不倒退"按内存任期检查，在新模型下会误报（内存任期可领先于已落盘任期，
    崩溃后恢复到落盘值）。改为更强的两条：发出的每条消息其任期 ≤ 已落盘任期；发出的每个同意投票，对应的
    (任期, 投给谁) 必须已在盘上。
  - **模型修正（本阶段）**：磁盘卡顿（150ms，长于选举超时下限）只出现在混沌阶段，恢复阶段与网络一起恢复；否则卡顿期间 Leader 的心跳被卡住，
    follower 反复超时选举，收尾时刻恰好处在换届中而误判"未收敛"。这也说明缺少 PreVote 会放大磁盘抖动造成的不必要换主（见问题清单）。

## 与原指南的有意偏差（原因）

1. **Ticker 用 10ms 轮询 + deadline，而不是 `time.Timer` 通道**：`Timer.Reset` 需要先 drain 通道，
   否则有竞态；deadline 方案更简单且不丢事件。
2. **选举超时在每次 `electionTimer.C` 触发时无条件重置的写法不采用**：只在
   收到合法 Leader 的 AppendEntries、投出选票、发起新选举、Leader 因应答中的更高 term 而退位时重置。
3. **并发模型改为事件循环（原指南为大锁）**：按用户要求；Raft 核心不再有 `sync.Mutex`，`Kill()` 通过关闭
   `done` 并等待循环退出来保证之后不再写盘。Applier 用 pump 协程而不是 `Sleep(10ms)` 轮询。
4. **传输层抽象为 `Transport` 接口，不直接依赖 `net/rpc`**：便于模拟网络注入故障。阶段 14 起它是异步消息接口
   （`Send(Message)` 非阻塞、`Endpoint.Step` 入站），而不是请求/应答式 RPC；真实网络适配器仍列为后续工作。
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
