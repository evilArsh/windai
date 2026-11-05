# feed：事件总线的宿主实现

本文件描述 `wind_core::feed` 的内部结构、它与 `wind_event` 的职责分工，以及一条事件从
`submit` 到落库、到前端、到 mod 的完整链路。

**feed** 是进程内唯一的事件总线宿主：它为事件分配身份、维持全序、把事件分发给三类接收方
（前端实时流、mod 派发层、事件日志），并负责关闭时的收尾。**事件**指一次已经定稿、
携带信封与载荷的不可变记录；**信封**是事件的元数据部分，含名称、作用域、来源、时刻与形态。

本文件只描述实现事实，不构成契约。 

---

## 定位

feed 处在这条链的中间：上游是派发层 `wind-event`，下游是持久化 `wind-storage`。

```text
调用方 ──► wind-event（形态校验、限流、mod 链）
             │ EventOutlet::submit(Pending)
             ▼
           feed（身份、全序、保留窗口、广播、落库）
             │                    │
             ▼                    ▼
       wind-storage        前端实时流 / mod observe
```

feed 只做与"事件身份和投递"有关的事，不做下列事情：判断事件该不该发（`Guards` 在派发层）、
决定 mod 链的顺序（`SubscriptionIndex` 在派发层）、解释事件语义（领域侧）。

---

## 构造入口

```rust
// windai/core/src/feed/hub.rs
pub async fn open(storage: Storage, options: FeedOptions)
    -> Result<(FeedHandle, FeedRuntime), FeedError>
```

`open` 只构造，不运行。它依次完成：校验两个必须非零的参数（`subscriber_capacity`、
`batch_size`）、向数据库申请本次运行的 epoch、建立两条通道、拼出 `FeedHandle` 与
`FeedRuntime`。`Storage` 在此被移交给 `Writer`；由于 `Storage` 是 16 字节的克隆句柄，
调用方如需自留一份，应先克隆。

返回值中 `FeedHandle` 是写入与控制入口，可以任意克隆后分发；`FeedRuntime` 是一次性对象，
必须由宿主（其描述见**运行时任务与通道**）驱动 `run()`，否则事件只会在队列里堆积。

---

## 对象拓扑

### 所有权树

```text
open(Storage, FeedOptions)
├─ Storage::allocate_feed_epoch() ──► epoch: i64
│
├─ FeedHandle { tx: mpsc::UnboundedSender<Command> }
│
└─ FeedRuntime { rx, hub, writer, observers }
   ├─ rx: mpsc::UnboundedReceiver<Command>
   ├─ observers: Vec<Weak<dyn Observer>>
   │
   ├─ hub: Hub
   │   ├─ epoch: i64
   │   ├─ last_seq: i64
   │   ├─ buffer: Buffer
   │   │   ├─ entries: BTreeMap<EventKey, BufferedEvent>
   │   │   │              BufferedEvent { event: Arc<FeedEvent>, retention,
   │   │   │                               bytes: usize, buffered_at: Instant }
   │   │   └─ groups: HashMap<GroupKey, BTreeSet<EventKey>>
   │   ├─ subscribers: Vec<Subscriber>
   │   │   └─ Subscriber { tx: mpsc::Sender<Arc<FeedEvent>>,
   │   │                    cancel: CancellationToken, capacity: usize }
   │   ├─ observers: Vec<Weak<dyn Observer>>
   │   ├─ writer: WriterHandle
   │   │   └─ { tx: UnboundedSender<WriterCommand> }
   │   ├─ writer_state: WriterState
   │   ├─ last_error: Option<String>
   │   └─ options: FeedOptions
   │
   └─ writer: Writer
       ├─ rx: UnboundedReceiver<WriterCommand>
       ├─ storage: Storage
       ├─ commands: UnboundedSender<Command>
       └─ unpersisted: usize
```

`FeedHandle`、`Writer` 与 `FeedRuntime` 共享同一个 `Command` 队列，因此 writer 的回执
（`Committed`、`WriterState`）与外部提交（`Emit`）在同一条队列上串行，由 Hub 单线程处理。
"某条回执一定会看到它之前提交的事件"这一性质由此成立，无需额外同步。

### 旁路对象

下列对象不由 `open` 创建，但属于同一张图。

| 对象 | 创建方式 | 与 Hub 的关系 |
| --- | --- | --- |
| `Opening` | `FeedHandle::subscribe()` 的应答 | 携带 `cut = EventKey { epoch, last_seq }`，供调用方定位回补终点 |
| `Subscription` | `Hub::subscribe()` 内部构造 | 前端实时流的消费端，`recv()` 先回补后实时 |
| `Dispatcher` | 宿主构造，以 `Weak` 注入 `set_observers` | feed 内部唯一的 `Observer` 实现 |

---

## 运行时任务与通道

```text
写入方（LlmRunner / Agent / ModEmitter / 前端桥）
   │ EventOutlet::submit(Pending)     同步入队，不退避
   ▼
┌────────────────────────┐
│ Command 队列（无界）    │◄───────────────────────────┐
└───────────┬────────────┘                            │
            │                                         │ Command::Committed
            ▼                                         │ Command::WriterState
┌──────────────────────────────┐                      │
│ FeedRuntime::run() 单任务循环 │                      │
│   loop { rx.recv → Hub::handle }                     │
└──┬────────────────────┬──────┘                      │
   │ WriterHandle       │ deliver / notify             │
   ▼                    ▼                              │
┌────────────────┐   ┌───────────────────────────┐    │
│ Writer 任务     │   │ Subscriber tx  × N        │    │
│ (tokio::spawn)  │   │ mpsc::Sender(capacity)    │    │
└──┬─────────────┘   └───────────────────────────┘    │
   │ Storage::append_feed_batch                       │
   ▼                                                   │
┌────────────────┐                                     │
│ event_log      │──── 唯一键 (epoch, seq) ────────────┘
└────────────────┘
```

三条队列的语义差别是这张图的核心。

- `Command` 无界：`submit` 只保证入队，不保证落库，也不做背压。
- `Subscriber.tx` 有界（`subscriber_capacity`，默认 512）：满即取消该连接，不阻塞 Hub。
- `WriterCommand` 无界：真正的限流发生在 Writer 内部按批取事件的循环。

---

## `wind_core::feed` 的类型

| 类型 | 作用 |
| --- | --- |
| `FeedOptions` | 三个参数：`subscriber_capacity`（512）、`batch_size`（256）、`shutdown_timeout`（5s） |
| `FeedHandle` | 对外写入与控制入口。实现 `EventOutlet`；另有 `subscribe`、`stats`、`shutdown` 三个请求-应答方法，经 `oneshot` 回包 |
| `Command` | Hub 的完整命令集：`Emit`、`Subscribe`、`Stats`、`Committed`、`WriterState`、`Shutdown` |
| `FeedRuntime` | 驱动器。`run()` 单循环消费 `Command`，负责派生与中止 writer 任务、关闭时排空队列、超时判定与报告 |
| `Hub` | 有状态核心。分配事件身份、维护保留窗口、推送订阅者、通知观察者、投递 Writer，并记录 writer 状态与最后错误 |
| `Buffer` | 保留窗口。`BTreeMap` 按事件身份有序，故快照天然有序；`groups` 支持整组释放（用于流式消息的增量） |
| `BufferedEvent` | 一条在窗事件：事件本体、保留策略、载荷字节数、入窗时刻（供"最老待写时长"使用） |
| `Subscriber` | 单连接的投递槽。投递用 `try_send`，失败即取消并令 Hub 剔除该连接 |
| `Opening` | 订阅应答：回补条数、回补终点、订阅句柄 |
| `Subscription` | 前端消费端。`recv()` 返回 `Result<Option<Arc<FeedEvent>>>`，`None` 表示已取消或已关闭 |
| `Writer` / `WriterHandle` | 落库任务及其句柄。`Writer` 聚批、写入并维护状态；写入失败只丢这一批，不重试 |
| `WriterCommand` / `WriterReport` | `Emit(Arc<FeedEvent>)` 与 `Shutdown { reply }`；报告只含 `unpersisted` |
| `WriterState` | `Idle`、`Writing`、`Faulted`、`Stopped`，供统计接口观测 |
| `FeedStats` | 窗口条数与字节、组数、待写数、最老待写毫秒、连接数、最大积压、writer 状态、最后错误 |
| `ShutdownReport` | `journal_remaining`、`buffer_remaining`、`timed_out` |
| `FeedError` | `Closed`、`Lagged`、`Storage`、`InvalidQuery`、`IdentityConflict`、`SequenceExhausted` |

`Retention::Journal` 是**保留策略**取值（见**保留策略速查**），与已移除的 `Journal` trait 无关：
后者曾抽象"落库后端"，现在由 `Writer` 直接持有 `Storage` 并调用 `Storage::append_feed_batch`
承担，不再有替换后端的需求支撑该层间接。

---

## `wind_event` 的类型

feed 的上游模块，负责"事件该不该发、发给哪个 mod"。此处只列与 feed 相接的部分。

| 类型 | 作用 |
| --- | --- |
| `EventOutlet` | 事件离开总线的唯一出口。`FeedHandle` 是其当前唯一实现 |
| `Pending` | 一条已定稿、待落库待广播的事件：信封、保留策略、展示去向 |
| `Retention` | 四态保留策略：`Journal`（留到落库确认）、`Group`（留到组关闭）、`Close`（推送后关组）、`Live`（只推当前连接） |
| `GroupKey` | 组标识，取值为 `Message(i64)` 或 `Volatile(String)` |
| `Observer` | 已提交事件的接收者，在 Hub 内同步调用，实现方不得阻塞 |
| `Dispatcher` | 派发总线。持有事件目录、订阅索引、mod 清单、mod 调用入口、observe 投递池、事件出口与限流器；同时实现 `Observer` |
| `Directory` / `EventMeta` / `EventDef` | 事件目录。`EventDef` 定义名称、形态、是否入库、schema；`log` 决定保留策略取 `Journal` 还是 `Group(Volatile)` |
| `Shape` | `Intercept`（可被 mod 改写，不进前端）与 `Observe`（前端可见） |
| `Envelope` / `Origin` / `Scope` / `Source` | 信封与三重身份：作用域（全局或话题+实例）、来源路径（agent / ui / internal）、发出者（core 子系统或 mod 及其版本） |
| `DispatchCx` / `DispatchGuard` | 一次派发上下文：共享的截止时刻、取消令牌与已发事件计数，外加嵌套层数与作用域 |
| `Guards` / `Limits` / `OverLimit` | 三级限流：嵌套深度 8、单次触发 1000 条、每 mod 3 秒内 100 条 |
| `ObservePool` / `Job` | observe 投递池。队列上界 1024，按 5ms 窗口、以（mod、事件、作用域）为键合批 |
| `Subscription` / `SubscriptionIndex` / `EnabledSet` | mod 的订阅记录、事件名到订阅者的倒排索引（按 rank 升序）、某作用域内启用的 mod 名集合 |
| `ModCaller` / `ModRoster` | 调用 mod 处理函数的入口 / 查询某作用域启用清单的入口 |
| `ModEmitter` / `EmitChannel` / `ModIdentity` | mod 的可信出口。名称与保留策略由宿主固定（`m_<mod>/<channel>`，一律 `Journal`，去向为 `Target::Mod`），脚本只能选择已声明的频道与载荷 |
| `CallCx` / `CallMode` / `CallError` | 单次调用约束：`Intercept`、`Observe`、`Request` 三种模式，其中 `Intercept` 不注入 I/O 能力 |
| `CORE_DISPATCH_FAILED` / `CORE_DISPATCH_TIMEOUT` | 派发层自产的两条元事件，连同 `DispatchFailed`、`DispatchTimeout`、`DispatchFailure`、`DeliveryTarget` |

---

## `wind_wire::event` 的类型

跨层数据契约，feed、派发层、存储层共用。

| 类型 | 作用 |
| --- | --- |
| `EventKey` | 事件身份 `{ epoch, seq }`，两分量均非负；线上序列化为十进制字符串 |
| `FeedEvent` | 实时流与事件日志共用的唯一结构：身份、信封、展示去向 |
| `Target` | 展示去向：`Core` 或 `Mod { name, channel }` |

---

## `wind_storage` 的接口

| 接口 | 作用 |
| --- | --- |
| `allocate_feed_epoch` | `UPDATE feed_meta SET epoch = epoch + 1 ... RETURNING epoch`，保证重启后身份不复用 |
| `append_feed_batch` | 单事务批量插入。逐条 `INSERT ... ON CONFLICT (epoch, seq) DO NOTHING RETURNING id`；冲突时回读十三个字段逐项比对，全同视为重复提交成功，不同则报身份冲突 |

写入幂等由此成立：同一批事件重复写入只会命中"内容全同"分支，不会产生重复行。
Writer 本身不重试，这条性质属于存储函数自身的约束。

---

## 事件的完整链路

以下按发生顺序分阶段。**阶段之间不存在事务性**：任何一步失败都不回滚前面的步骤。

### 产生

派发层有四条产生路径，最终都汇聚到 `Dispatcher::submit`。

```text
① notify(def, cx, payload)                     普通 observe 通知
   校验形态为 Observe → 限流 → 按 def.log 定保留策略 → 提交

② notify_host(def, cx, payload, retention)     宿主路径，保留策略由调用方指定
   校验形态为 Observe → 只校验嵌套深度（不占 mod 事件额度）
   → 沿调用方上下文组信封 → 提交，去向 Core

③ decide(def, cx, payload) -> P                拦截路径，可改写
   校验形态为 Intercept → 限流 → 查订阅链 → 逐个同步调用 mod
   每次调用预算 = min(订阅声明的时间、剩余总预算)
   返回可解析的值即累积改写；超过预算或报错则产出元事件
   → 提交累积值 → 把累积值返回调用方

④ ModEmitter::emit(cx, channel, payload)       mod 主动发事件
   频道已声明？→ 载荷过校验 → 限流 → 每 mod 速率限制
   → 信封名为 "m_<mod>/<channel>"，来源为 Mod，形态 Observe
   → 提交，保留策略 Journal，去向 Mod
```

三条 observe 路径的差别在于**谁决定保留策略**，以及**是否占用 mod 的事件额度**；
拦截路径不进入 observe 分发，因为它的载荷改写必须在调用方拿到返回值之前完成。

### 入队

`FeedHandle::submit` 把 `Pending` 包成 `Command::Emit` 送入无界队列，随即返回。
队列入队失败只意味着 runtime 已经退出，返回 `SubmitError::Closed`。
此阶段成功**不表示**事件已落库，也不表示任何接收方已收到。

### 出队与身份分配

`FeedRuntime::run` 从队列取出 `Command` 交给 `Hub::handle`。`Emit` 按键位递增 `last_seq`，
构造 `EventKey { epoch, seq }`，包成 `Arc<FeedEvent>`。

全序由两点共同保证：Hub 是单任务循环（同一时刻只有一个事件在分配身份），seq 单调递增。
这套身份同时是数据库唯一键与前端排序键。

`seq` 溢出返回 `SequenceExhausted`，它是 `Hub::handle` 唯一可能的错误，会让整个 runtime 退出。

### 保留策略分叉

`Hub::emit` 在同步代码里完成四件判断，顺序不可交换。

```text
可见 = 信封形态为 Observe

可见：
    策略 ∈ {Journal, Group}  ──► 入保留窗口（记录载荷字节数）
    一律                      ──► 推给所有订阅者（try_send）
    策略 ≠ Journal            ──► 立即交给观察者链

策略 = Journal                ──► 投给 Writer；失败则状态记为 Stopped 并记错
策略 = Close(组)              ──► 推送后释放整个组

不可见（Intercept）：
    只投给 Writer（若该事件标记入库），不缓冲、不推前端、从不通知观察者
```

由此得到两条容易记错的性质：`Journal` 事件的观察者通知被**推迟到落库确认之后**；
`Intercept` 事件永远不会出现在窗口和前端，它的改写只在**产生**阶段的同步调用链里完成。

### observe 分发

`Hub::notify` 遍历观察者弱引用，升级失败即剔除。当前唯一实现是 `Dispatcher`：

```text
查目录，事件不在目录则丢弃
形态不是 Observe 则丢弃
按作用域查订阅链（索引按 rank 升序，再按启用清单过滤）
为每个订阅构造一个投递任务，交给 observe 投递池
```

投递池的入队同样是 `try_send`：队列满即丢弃并计数，只记日志，不重试、不阻塞 Hub。
这是设计上的取舍——事件总线不为 mod 的正确性负责。

### 合批与调用

投递池任务收到第一条后开启 5ms 窗口，窗口内尽量多收，然后按（mod、事件、作用域）分组。
组内按到达序保持；单条时载荷是值本身，多条时载荷是数组。随后逐个调用：

```text
以订阅声明的时间为超时，调用 mod 处理函数，模式为 Observe
├─ 返回成功        → 丢弃返回值（observe 不可改写）
├─ 处理函数报错    → 提交 core/dispatch.failed
├─ mod 运行时出错  → 提交 core/dispatch.failed
└─ 调用超时        → 提交 core/dispatch.timeout
```

两条元事件都以 `Origin::Internal`、来源为事件子系统、保留策略 `Journal`、去向 `Core`
重新进入**入队**阶段，形成一个回环。这是限流存在的直接原因。

### 聚批与落库

Writer 任务是独立 tokio 任务。它阻塞等待第一条命令，随后用 `try_recv` 继续取，
最多取 `min(batch_size, 256)` 条或遇到关闭命令为止，然后写这一批。

写入只做一次尝试，不重试：

| 结果 | 处理 |
| --- | --- |
| 成功 | 递减待写计数，回 `Command::Committed`，状态转 `Idle`，处理下一批 |
| 失败 | 递减待写计数，同样回 `Command::Committed`，状态转 `Faulted` 并把错误交给 Hub 记录，然后处理下一批 |

两条分支都回 `Committed`，因为 Hub 收摊的动作与写入成败无关（见**批次收摊与出窗**）。
丢批而非重试的理由：`event_log` 当前没有读取方，它不是会话数据的记录源；
真正的记录源是 `message` 与 `block`，那两处的失败会以领域错误向外传播。
在这里堵住 writer 会让一批坏数据演变成整个 journal 停摆，并使保留窗口不再收缩。

批内事件在写入前被逐条克隆成 `Vec<FeedEvent>`；这是 Writer 唯一的分配热点。

### 批次收摊与出窗

Hub 收到 `Committed` 后按身份从保留窗口取出对应事件，并对每一条执行一次 `notify`。
这个动作与写入成败无关：写成功是"已持久化"，写失败是"已放弃"，两者都必须把事件从窗口拿出，
否则失败的批次会永久占住窗口。成败的区分只体现在 `WriterState` 与 `last_error` 上。

对成功那一支，这一步的实际含义是：

> `Journal` 事件的 observe 分发发生在数据库提交之后，因此 mod 看到的事件顺序与事件日志一致。

对失败那一支，采用同样动作的理由是：事件本身已经发生并已推给前端，只有持久化失败；
把观察者一并丢掉会把"历史有洞"升级成"mod 漏事件"。

被取出的事件只用于通知，不再回推前端——前端在**保留策略分叉**阶段已经收到。
`release_keys` 同时维护组集合，组内清空即删除该组。

### 订阅与回补

前端接入分两步：先取回补，再接实时。

```text
subscribe()
  → Command::Subscribe
  → Hub::subscribe：
        建取消令牌与有界投递槽
        对保留窗口取有序快照作为回补内容
        登记订阅者
        应答 { 回补条数, cut = (epoch, last_seq), 订阅句柄 }

Subscription::recv()
  → 已取消则返回 None
  → 有回补内容则先给回补
  → 否则推诿选择：取消优先于实时事件
```

- 回补只覆盖"仍在窗口内"的可见事件：`Journal` 中未落库的、以及尚未关闭的组。
- `cut` 的作用：调用方若另有历史来源，以 `cut` 为界取历史、以实时流接其后，故不重不漏。
- 连接落后时投递失败，令牌被取消，该连接在下一次广播时被剔除，`recv()` 返回 `None`。
  这是唯一会丢弃数据的正规路径，语义是"该连接太慢，应由调用方重连并重放"。

### 关闭

```text
shutdown()
  → Command::Shutdown{reply}
  → runtime 关闭队列入口，排空已入队命令（多条 Shutdown 只保留最后一条应答）
  → 在 shutdown_timeout 内等待 writer 关闭：
        成功 → 剩余待写数取自 writer 报告
        超时 → 中止 writer 任务，timed_out = true，剩余待写数取自保留窗口计数
  → 组装 ShutdownReport，应答调用方，同时作为 run() 的返回值
```

write 的关闭命令**不会先刷完积压**，它立即回报当前待写数。被打断的事件仅以
`journal_remaining` 的形式报出，是否补写由调用方决定。因为 writer 不再在错误上挂起，
关闭只可能因单批写入本身超时而走到中止分支。

---

## 保留策略速查

| 策略来源 | 入窗口 | 推前端 | 通知 mod 的时机 | 落库 |
| --- | --- | --- | --- | --- |
| `Journal`（标记入库的 observe、`ModEmitter`、两条元事件） | 是 | 立即 | 数据库提交后 | 是 |
| `Group(Message)`（流式增量） | 是 | 立即 | 立即 | 否 |
| `Close(Message)`（消息终结） | 否，并释放同组 | 立即 | 立即 | 否 |
| `Group(Volatile)`（未标入库且经 `notify` 的 observe） | 是 | 立即 | 立即 | 否 |
| `Live`（已存在的持久化块推给当前连接） | 否 | 立即 | 立即 | 否 |
| 标记入库的 Intercept | 否 | 否 | 从不（另走同步拦截链） | 是 |

---

## 时序总览

```text
写入方      Command队列     Hub                 Writer         Storage     Subscriber   Observer→投递池→mod
  │             │            │                    │              │             │              │
  ├ submit ────►│            │                    │              │             │              │
  │             ├── Emit ───►│                    │              │             │              │
  │             │      分配身份，包成 Arc         │              │             │              │
  │             │            ├ Journal/Group ⇒ 入窗              │             │              │
  │             │            ├ 推送 ─────────────────────────────────────────►│ try_send     │
  │             │            ├ Journal ⇒ 投递 ───►│              │             │              │
  │             │            ├ 非 Journal ⇒ 通知 ──────────────────────────────────────────►│ 查订阅链
  │             │            │                    ├ 取一批 ─────►│             │              │
  │             │            │                    │ append_feed_batch（事务、幂等）            │
  │             │            │◄ Committed{keys} ──┤              │             │              │
  │             │            ├ 出窗                │              │             │              │
  │             │            └ 逐条通知 ────────────────────────────────────────────────────►│ 5ms 合批
  │             │                                 │              │             │      调用处理函数
  │             │◄──── 回环：mod 主动发事件、超时与失败元事件 重新入队 ────────────────────────┤
  │             │      受限于深度 8、单次 1000 条、每 mod 3 秒 100 条
```

---

## 实现现状中的缺口

下列事实来自当前代码，记录在此以便后续决策，不代表期望行为。

1. **丢批只留一处痕迹。** 写入失败后 `last_error` 会被下一次写成功后的 `Idle` 状态保留下来，
   但没有累计计数，无法回答"历史里到底缺了多少条"。需要这个数字时再补一个统计字段。
2. **`FeedError::Lagged` 与 `FeedError::IdentityConflict` 从未被构造。** 连接落后走的是
   取消令牌加 `Ok(None)`，身份冲突以 `StorageError::IdentityConflict` 表达。
3. **`Group(Volatile)` 的组永不关闭。** 关闭组只发生在策略为 `Close` 时；派发层对未标入库的
   observe 事件使用 `Group(Volatile(事件名))`，宿主侧没有对应的关闭动作，这些事件会一直留在
   保留窗口。同族问题：消息以中断收尾时用的是 `Group(Message)` 而非 `Close`，该组此前累积的
   增量不会释放。
4. **`set_observers` 在仓库内无调用点。** 宿主若不作注入，`Hub::notify` 恒为空，
   **observe 分发**整段链路不会启动。
5. **窗口上界不受配置约束。** `FeedOptions` 没有窗口容量参数，`Buffer` 只在策略为 `Close`
   或批次收摊时收缩，因此窗口的实际上界由事件发生率与订阅者组合决定。
