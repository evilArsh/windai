# 事件

事件是本仓库各子系统之间唯一的跨层通知形式。一次事件由信封、载荷与展示去向组成；
它在 `wind-core::feed`（下称 feed，事件总线的宿主实现）中获得身份，然后被投递给前端
订阅者、mod 派发层与事件日志。

本文先定义事件的身份与分类，再说明派发层与 feed 的实现，最后给出事件目录与前端消费规则。
领域表的写入见「存储」；mod 如何声明订阅见「Mod」；脚本如何产生 mod 事件与
`core/mod.notify` 见「脚本」。

## 概念

| 词 | 含义 |
| --- | --- |
| 事件 | 一次已定稿、携带信封与载荷的不可变记录 |
| 信封 | 事件的元数据：名称、作用域、来源路径、发出者、时刻、形态与载荷 |
| 形态 | `observe` 或 `intercept`。形态决定事件是否对前端可见、是否走改写链 |
| 改写链 | 拦截事件按订阅顺序逐个调用 mod 的串行调用链 |
| 观察者 | 已提交或即时事件的接收者，对应 `Observer` trait |
| 订阅索引 | 事件名到 mod 订阅记录的倒排表 |
| 保留策略 | 事件在 feed 中留到什么时候、是否写入 `event_log` |
| 展示去向 | `Target::Core` 或 `Target::Mod { name, channel }`，写入 `event_log` 的路由列 |
| 位置 | 事件的身份 `EventKey`，不是数据库行号 |
| mod | 宿主安装的扩展包，可订阅事件或提供资源；见「Mod」 |
| 脚本调用 | 派发层通过 `ModCaller` 调用 mod 处理函数；当前实现是 `ScriptRuntime` |

### 信封与身份

```rust
pub struct Envelope<P> {
    /// 产生时刻，epoch 毫秒
    pub time: i64,
    pub name: String,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    pub payload: Option<P>,
    pub shape: Shape,
}

pub struct EventKey {
    pub epoch: i64,
    pub seq: i64,
}

pub struct FeedEvent {
    pub key: EventKey,
    pub envelope: Envelope<serde_json::Value>,
    pub target: Target,
}
```

`Envelope` 是事件本身，不含位置。`EventKey` 是事件在 feed 中的身份，同时是 `event_log` 的
唯一键。`FeedEvent` 是实时流与历史共用的结构。

`Scope` 有三种信息组合：

```rust
pub enum Scope {
    Global,
    Topic { topic_id: i64, instance_id: i64 },
}
```

`Origin` 表示操作从哪条路径进入：`agent`、`ui`、`internal`。`Source` 表示事件由谁发出：

```rust
pub enum Source {
    Core { subsystem: String },
    Mod { name: String, version: u32 },
}
```

`Target` 是展示去向，不参与投递过滤：

```rust
pub enum Target {
    Core,
    Mod { name: String, channel: String },
}
```

### 位置

`EventKey` 由 `(epoch, seq)` 组成，按字典序比较。`epoch` 在每次 `feed::open` 时由
`feed_meta.epoch + 1` 领取；`seq` 由本次 feed 的 Hub 从 0 开始递增。两个分量都必须非负。

序列化为十进制字符串，两个分量分别编码；不接受负号、前导零或空串。`Display` 与
`event_id()` 输出 `"<epoch>:<seq>"`。

`event_log` 对 `(epoch, seq)` 建唯一索引。事件身份不复用，也不从数据库的 `event_log.id`
派生。`event_log.id` 只是行号；重启后 `epoch` 改变，`seq` 重新从 0 开始，而
`(epoch, seq)` 仍然唯一。

`seq` 溢出时 Hub 返回 `FeedError::SequenceExhausted`，feed 任务退出。

### 形态

| 形态 | 对前端可见 | 派发方式 | 典型用途 |
| --- | --- | --- | --- |
| `observe` | 是 | 提交后由 `Observer` 查订阅索引，进入 observe 投递池 | 宣告已发生的事实 |
| `intercept` | 否 | 调用方同步等待改写链，逐个调用 mod | 在宿主执行操作前改写载荷 |

`Shape::visible_to_frontend` 只对 `observe` 返回真。`intercept` 事件即使标记 `log = true`
也不会进入前端队列，只写入 `event_log`。

### 保留策略

`Retention` 决定 feed 的保留窗口与持久化。保留窗口是内存中按 `EventKey` 有序的事件集合；
出窗是从保留窗口移除一条事件。

```rust
pub enum Retention {
    /// 保留到日志提交确认
    Journal,
    /// 保留到指定组关闭
    Group(GroupKey),
    /// 向当前连接排入后关闭指定组
    Close(GroupKey),
    /// 只向当前连接推送
    Live,
}

pub enum GroupKey {
    Message(i64),
}
```

组的唯一取值是消息。流式增量与实时块提交使用 `Group(Message(id))`；消息定稿或中断使用
`Close(Message(id))`，把该组此前累积的增量一并移出保留窗口。

保留策略由产生方决定：

- `Dispatcher::notify`、`decide`、`decide_without_payload`：`EventDef.log` 为真时用
  `Journal`，否则用 `Live`。
- `Dispatcher::notify_host`：调用方直接给出策略。当前调用方使用 `Group`、`Close` 与 `Live`。
- `Dispatcher::notice`：固定 `Journal`。
- `ModEmitter::emit`：使用 `EmitChannel` 携带的保留策略，去向为 `Target::Mod`。

## 派发层

派发层是 `wind-event`。它决定一条事件该不该发、能不能被改写、该投给哪些 mod，但不决定
事件如何写库、如何送到前端。

### 事件定义与目录

```rust
pub struct EventDef<P> {
    pub name: &'static str,
    pub shape: Shape,
    pub log: bool,
    pub schema: fn() -> serde_json::Value,
    _payload: PhantomData<fn() -> P>,
}

pub struct EventMeta {
    pub name: &'static str,
    pub shape: Shape,
    pub log: bool,
    pub schema: fn() -> serde_json::Value,
}
```

`EventDef` 是编译期常量，`EventMeta` 是运行期副本。`Directory` 是事件名到 `EventMeta` 的
映射，由宿主把各子系统的 `directory()` 合并而成。`Directory::merge` 中先插入的目录优先：
`base.merge(other)` 先放入 `other` 的条目，再用 `base` 覆盖同名项。

`require_shape` 先查目录。事件不在目录里时返回 `NotInDirectory`，不会发出该事件，但会发出
一条 `core/dispatch.failed`。

### Dispatcher

```rust
pub struct Dispatcher {
    directory: Directory,
    index: RwLock<SubscriptionIndex>,
    mod_roster: Arc<dyn ModRoster>,
    mod_caller: Arc<dyn ModCaller>,
    mod_ob: Arc<ObservePool>,
    ev_out: Arc<dyn EventOutlet>,
    guards: Arc<Guards>,
}
```

`Dispatcher::new` 会启动 observe 投递池。因此 `Dispatcher` 必须在 tokio 运行时内构造。

`rebuild_index` 用新的订阅集合整体替换索引。`Installer` 在每次安装、更新、删除、启用、
停用与恢复之后调用它。

### 四条产生路径

#### 普通通知

```rust
pub fn notify<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P)
```

执行顺序：

```text
require_shape(Observe)      形态不对 → dispatch.failed，返回
guards.admit(cx)            超限 → dispatch.failed，返回
emit(def, cx, Some(payload)) 序列化失败 → dispatch.failed
                             保留策略 = def.log ? Journal : Live
                             EventOutlet::submit
```

`notify` 不等待写库，也不调用 mod。提交失败只写日志。

#### 宿主通知

```rust
pub fn notify_host<P>(
    &self,
    def: &EventDef<P>,
    cx: &DispatchCx,
    payload: P,
    retention: Retention,
) -> Result<(), BusError>
```

只校验形态与嵌套深度，不占用每触发事件预算。保留策略由调用方提供，`def.log` 不参与。
当前用于流式块、消息定帧、消息定稿与已存在块的实时补发。

#### 拦截

```rust
pub async fn decide<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P) -> P
pub async fn decide_without_payload<P>(... ) -> P
```

两者走同一条链，区别只在提交信封时是否携带载荷。`decide_without_payload` 用于
`core/llm.request` 与 `core/rule.apply`：mod 仍然收到完整载荷，但 `event_log` 中
`payload` 为 `NULL`。

```text
require_shape(Intercept)     失败 → dispatch.failed，返回原载荷
guards.admit(cx)             超限 → dispatch.failed，返回原载荷
chain = index.ordered_in(name, roster.enabled(scope))
逐个订阅：
    budget = min(sub.timeout, cx.remaining_budget())
    budget 为 0 → 跳过该订阅
    select! {
        取消 → 跳过该订阅
        timeout(budget, ModCaller::call(sub, 当前载荷, CallCx{Intercept})) → 结果
    }
    Ok(None)        → 载荷不变
    Ok(Some(value)) → 解析为 P；解析失败 → dispatch.failed，载荷不变
    Handler 错误    → dispatch.failed，载荷不变
    Internal 错误   → dispatch.failed，载荷不变
    超时            → dispatch.timeout，载荷不变
提交最终载荷（或 NULL）并返回最终载荷
```

链是串行的：后一个 mod 看到前一个改写后的载荷。没有订阅时仍然提交一次事件。

`chain_of` 在索引中查事件名，再用 `EnabledSet` 过滤。索引的顺序就是调用顺序。当前索引
没有 `rank` 字段；顺序来自安装器的装配次序：先按 mod 名升序（`list_mods` 的
`ORDER BY name`），同一 mod 内按 `events.on` 的 `BTreeMap` 键升序。

#### 通知元事件

`dispatch.failed` 与 `dispatch.timeout` 由派发层自己产生：

```rust
Envelope::new(
    name,
    cx.scope.clone(),
    Origin::Internal,
    Source::core("event"),
    Some(payload),
    Shape::Observe,
)
```

保留策略固定为 `Journal`，去向为 `Target::Core`。这两条事件不经过 `Guards`，否则
“预算用尽 → 发通知 → 再检查预算”会递归。

### 限流

```rust
pub struct Limits {
    pub max_depth: u32,              // 默认 8
    pub max_events_per_trigger: u32, // 默认 1000
    pub window: Duration,            // 默认 3s
    pub per_window: u32,             // 默认 100
}
```

`Guards::admit` 检查嵌套深度与当前 `DispatchCx` 已产生的事件数。`admit_host` 只检查深度。
`admit_mod_emit` 按 mod 名维护时间窗，限制每个 mod 在窗口内产生的事件数。

计数挂在 `DispatchCx` 的 `DispatchGuard` 上。`DispatchCx::root` 建一份新的守卫；
`nested()` 共享守卫并增加深度。当前仓库内的产生方都直接使用根上下文，没有调用
`nested()`，因此 `depth` 恒为 0，深度限制只在将来接入嵌套派发时生效。每触发事件预算
因此等于每轮 LLM 或每次 agent 通知内的 `notify` / `decide` 次数。

### 订阅索引

```rust
pub struct Subscription {
    pub mod_name: String,
    pub mod_version: u32,
    pub event: &'static str,
    pub handler: String,
    pub timeout_ms: u64,
}

pub struct SubscriptionIndex {
    by_event: HashMap<&'static str, Vec<Subscription>>,
}
```

`ordered_in(event, enabled)` 返回该事件的订阅，按索引内的插入顺序过滤出启用集合中的 mod。
`EnabledSet` 是 mod 名的集合；`ModRoster::enabled` 是它的来源。

`Roster` 只按话题建立索引：

```rust
Scope::Global => EnabledSet::none()
Scope::Topic { topic_id, .. } => 该话题启用的 mod 名
```

因此全局作用域的事件（当前是 `core/mcp.*`）不会命中任何 mod 订阅。mod 清单允许订阅
`core/` 下的任意事件，但只有话题作用域的事件能真正进入订阅链。

### observe 投递池

`ObservePool` 接收 `Job { event, scope, payload, sub }`：

```text
队列上界 1024，try_send 失败即丢弃并计数
收到第一条后开 5ms 窗口，窗口内继续收
按 (mod_name, event, scope) 分组，组内保持到达顺序
单条 → payload 是值本身
多条 → payload 是数组
逐组调用 ModCaller::call，模式 Observe，超时取 sub.timeout_ms
```

调用结果：

| 结果 | 处理 |
| --- | --- |
| 成功 | 丢弃返回值；observe 不能改写 |
| `CallError::Handler` | 提交 `core/dispatch.failed`，`failure = handler_error` |
| `CallError::Internal` | 提交 `core/dispatch.failed`，`failure = call_error` |
| 超时 | 提交 `core/dispatch.timeout` |

投递池的 `CallCx.cancel` 当前是新建的空令牌，不随产生方的上下文取消。observe 投递发生在
事件提交之后，也不受 `DispatchCx` 的剩余总预算约束。

### 观察者实现

`Dispatcher` 实现 `Observer`：

```text
on_event(event):
    查目录；不存在 → 丢弃
    形态不是 observe → 丢弃
    chain = chain_for(event.name, event.scope)
    chain 为空 → 丢弃
    为每个订阅入 ObservePool
```

`Observer` 是同步方法，实现方只能入队，不得阻塞。`FeedRuntime::run` 接收
`Vec<Weak<dyn Observer>>`，在每次通知时尝试升级；升级失败即从观察者列表移除。当前只有一个
实现：`Dispatcher` 按事件目录与订阅索引把事件转给 mod。

## feed

feed 是 `wind_core::feed`。它是进程内唯一的事件总线宿主：分配身份、维持序、保留窗口、
向订阅者投递、向 Writer 批交，以及关停收尾。

### 打开与运行

```rust
pub async fn open(storage: Storage, options: FeedOptions)
    -> Result<(FeedHandle, FeedRuntime), FeedError>
```

`open` 只构造，不运行：

- 校验 `subscriber_capacity > 0` 与 `batch_size > 0`。
- 调用 `Storage::allocate_feed_epoch`，即
  `UPDATE feed_meta SET epoch = epoch + 1 WHERE singleton = 1 RETURNING epoch`。
- 建立命令通道与 Writer 通道，构造 `FeedHandle`、`FeedRuntime` 与 `Hub`。

`FeedOptions` 的默认值：

| 字段 | 默认 | 作用 |
| --- | --- | --- |
| `subscriber_capacity` | 512 | 每条订阅的帧队列容量 |
| `batch_size` | 256 | Writer 每批取的事件数；实际再与 256 取小 |
| `shutdown_timeout` | 5s | 等待 Writer 关闭的上限 |

`FeedRuntime::run(observers)` 挂上观察者，派生 Writer 任务，然后单任务消费命令。没有调用
`run` 时事件只在命令队列里堆积。

### 句柄与命令

`EventOutlet` 是事件离开派发层的唯一出口；它的入参是 `Pending { envelope, retention,
target }`，即一条已定稿、等待落库与广播的事件。feed 为 `Pending` 分配身份后得到
`FeedEvent`。

```rust
impl FeedHandle {
    pub async fn subscribe(&self) -> Result<Opening, FeedError>;
    pub async fn stats(&self) -> Result<FeedStats, FeedError>;
    pub async fn shutdown(&self) -> Result<ShutdownReport, FeedError>;
}

impl EventOutlet for FeedHandle {
    fn submit(&self, pending: Pending) -> Result<(), SubmitError>;
}
```

`submit` 把 `Pending` 包成 `Command::Emit` 送入无界通道，立即返回。返回成功只表示入队，
不表示写入数据库或任何接收方已收到。通道关闭时返回 `SubmitError::Closed`。

内部命令集：

| 命令 | 来源 | 作用 |
| --- | --- | --- |
| `Emit(Pending)` | `FeedHandle::submit` | 分配身份并投递 |
| `Subscribe` | `FeedHandle::subscribe` | 取回补快照与订阅句柄 |
| `Stats` | `FeedHandle::stats` | 读统计 |
| `Committed { keys }` | Writer | 出窗并通知观察者 |
| `WriterState` | Writer | 记录 Writer 状态与错误 |
| `Shutdown` | `FeedHandle::shutdown` | 关停收尾 |

Writer 的回执与外部提交进入同一条 Hub 命令队列，由 Hub 单任务处理。“某条回执一定看到它
之前提交的事件”由此成立。

### 分配与分支

```text
emit(pending):
    seq = last_seq + 1
    key = EventKey { epoch, seq }
    event = FeedEvent { key, envelope, target }
    visible = envelope.shape == Observe
    if visible:
        if retention ∈ {Journal, Group}: buffer.insert(event, retention, payload_bytes)
        deliver(event)                         // 推给所有订阅者
        if retention != Journal: notify(event) // 立即通知观察者
    if retention == Journal:
        writer.submit(event)；失败则 writer_state = Stopped 并记错
    if retention == Close(group):
        buffer.release_group(group)
```

四件事的顺序不可交换：

- `Journal` 先入窗再推订阅者，观察者通知推迟到 Writer 回报 `Committed`。
- `Group` 入窗、推送、立即通知；它永不进入 Writer。
- `Close` 不缓冲，推送、通知并释放同组。
- `Live` 不缓冲、不写库，推送并通知。

`intercept` 不可见，所以上述 `visible` 分支全部跳过。它若标记 `log = true`，仍会进入
Writer；若 `log = false`，事件在入队后即被丢弃，不产生任何可观察结果。

### 保留窗口

```rust
struct Buffer {
    entries: BTreeMap<EventKey, BufferedEvent>,
    groups: HashMap<GroupKey, BTreeSet<EventKey>>,
}
```

- `entries` 按身份有序，所以 `snapshot()` 天然按 `(epoch, seq)` 升序。
- `groups` 只记录 `Retention::Group` 的成员，用于整组释放。
- `release_keys` 在 Writer 回报后移除指定身份，并维护组集合；返回被移除的事件供通知。
- `release_group` 在 `Close` 时移除整组。
- `pending_write` 只统计 `Journal` 条目；`oldest_pending_ms` 也只取 `Journal` 条目的
  入窗时长。
- `bytes` 是载荷序列化后的近似字节数，不含信封。

保留窗口没有容量参数。它只在 `Close` 或 Writer 回报时收缩。`buffer_events` 的上界由事件
产生速率与 Writer 写入速率共同决定，不由 `FeedOptions` 约束。

### 订阅与回补

```rust
pub struct Opening {
    pub replay_len: usize,
    pub cut: EventKey,
    pub subscription: Subscription,
}
```

`Hub::subscribe` 在同一个同步步骤内：

1. 建取消令牌与有界投递槽。
2. 对保留窗口取有序快照。
3. 登记订阅者。
4. 返回 `cut = (epoch, last_seq)`。

`Subscription::recv` 的返回类型是 `Result<Option<Arc<FeedEvent>>, FeedError>`，但当前实现
只产生 `Ok`：

- 取消令牌已取消 → `Ok(None)`。
- 有回放内容 → 先返回回放。
- 否则在取消与 `rx.recv()` 之间选择；取消优先。
- 通道关闭 → `Ok(None)`。

回放只覆盖仍在保留窗口内的可见事件：尚未收到 Writer 回执的 `Journal` 事件，以及尚未关闭的
`Group` 事件。已经 `Close` 的消息增量、`Live` 事件与全部 `intercept` 事件不在回放内。

`cut` 是订阅时刻已分配的最大身份，不是已写库的最大身份。宿主若另有历史来源，应以 `cut`
为界拼历史，并对回放中可能重复的事件按身份去重。

连接落后时 `Subscriber::deliver` 的 `try_send` 失败（队列满或接收端关闭），该订阅的取消
令牌立即触发，并在下一次广播时从订阅者表移除。`recv` 随即返回 `None`。这是唯一会丢弃
数据的正规路径，语义是“该连接太慢，应由调用方重连并从领域表或历史重建”。

### Writer

Writer 是独立 tokio 任务，独占 `event_log` 的写入路径：

```text
recv 第一条命令
  Shutdown → 立即回报当前 unpersisted 并退出
  Emit → 放入 batch
try_recv 继续取，直到 batch 达到 min(batch_size, 256) 或队列为空
写这一批：
  unpersisted -= batch.len()
  state = Writing
  storage.append_feed_batch(events)   // 单事务，逐条 INSERT
  不论成败都发送 Committed { keys }
  成功 → state = Idle
  失败 → state = Faulted，错误交给 Hub 记录
```

写入只尝试一次，不重试。写入失败时整批回滚，该批事件从保留窗口取出后仍然通知观察者；
区别只体现在 `WriterState` 与 `last_error` 上。这样一批坏数据不会让 journal 永久停摆，
但历史会出现缺口。

`WriterState` 取值：`Idle`、`Writing`、`Faulted`、`Stopped`。`Stopped` 由 Hub 在
`writer.submit` 失败时设置。

### 统计与关停

`FeedStats` 字段：

| 字段 | 含义 |
| --- | --- |
| `buffer_events` | 保留窗口中的事件数 |
| `buffer_bytes` | 保留窗口中载荷序列化字节数的合计 |
| `groups` | 未关闭的组数 |
| `pending_write` | 尚未写入数据库的 `Journal` 事件数 |
| `oldest_pending_ms` | 最老待写事件的入窗毫秒数 |
| `connections` | 当前订阅者数 |
| `max_connection_backlog` | 各订阅队列积压的最大值 |
| `writer` | Writer 当前状态 |
| `last_error` | 最近一次 Writer 错误文本 |

关停顺序：

```text
shutdown()
  → Command::Shutdown
  → Hub 关闭命令入口，排空已入队命令（多条 Shutdown 只保留最后一条应答）
  → 在 shutdown_timeout 内等待 writer.shutdown()
        收到 WriterReport → journal_remaining = report.unpersisted
        超时 → abort Writer，journal_remaining = buffer.pending_write()
  → ShutdownReport { journal_remaining, buffer_remaining, timed_out }
```

Writer 的 `Shutdown` 在队列为空时立即回报，不会先刷完积压；若它在取批过程中遇到
`Shutdown`，会先写完当前批再回报。`FeedRuntime::run` 在 Hub 出错时直接 abort Writer 并
返回 `Err`，不产生 `ShutdownReport`。

### 错误

```rust
pub enum FeedError {
    Closed,
    Lagged,
    Storage(StorageError),
    InvalidQuery(String),
    IdentityConflict,
    SequenceExhausted,
}
```

当前只有以下分支会被构造：

| 变体 | 产生位置 |
| --- | --- |
| `Closed` | 命令通道关闭、应答通道关闭 |
| `Storage` | `allocate_feed_epoch` |
| `InvalidQuery` | `subscriber_capacity` 或 `batch_size` 为 0 |
| `SequenceExhausted` | `seq + 1` 溢出或 `EventKey::new` 失败 |

`Lagged` 与 `IdentityConflict` 在 `wind_core::feed` 内没有构造点。连接落后走取消令牌与
`Ok(None)`；身份冲突以 `StorageError::IdentityConflict` 表达。

## 写入 `event_log`

`Storage::append_feed_batch(&[FeedEvent])` 在一个写事务里逐条调用
`append_feed_event`：

```sql
INSERT INTO event_log
    (epoch, seq, create_time, name, shape, route_kind, route_name,
     route_channel, topic_id, instance_id, origin, source_kind, source_name,
     source_version, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, seq) DO NOTHING
RETURNING id
```

列与来源：

| 列 | 来源 |
| --- | --- |
| `epoch`, `seq` | `event.key` |
| `create_time` | `envelope.time` |
| `name`, `shape`, `origin` | 信封 |
| `route_kind` | `Target::Core` → `"core"`；`Target::Mod` → `"mod"` |
| `route_name`, `route_channel` | `Target::Mod` 的名称与频道；`Target::Core` 为 `NULL` |
| `topic_id`, `instance_id` | `Scope::Topic` 的两个值；`Scope::Global` 均为 `NULL` |
| `source_kind`, `source_name`, `source_version` | `Source::Core` 为 `"core"`、子系统名、0；`Source::Mod` 为 `"mod"`、mod 名、版本 |
| `payload` | 信封载荷序列化为 JSON；无载荷为 `NULL` |

`ON CONFLICT` 命中时函数直接返回 `StorageError::IdentityConflict`，不比较已有行的内容。
整批写入在一个事务里，任何一条冲突都会回滚整批。

`wind_storage::models::LogRecord` 是 `event_log` 的读取模型，但当前没有查询函数使用它。
事件历史读取、分页与按作用域过滤都留给宿主。

## 事件目录

事件目录由宿主合并以下子系统的 `directory()`：

```rust
wind_event::directory()
    .merge(wind_core::agent::events::directory())
    .merge(wind_core::llm::events::directory())
    .merge(wind_mod::events::directory())
    .merge(wind_mcp::events::directory())
    .merge(wind_core::script::directory())
```

`wind_event::directory()` 只含派发层自己的两条元事件。事件名不在目录中时，`notify`
不提交事件，`decide` 返回原载荷，两者都发出 `core/dispatch.failed`；`notify_host` 返回
`BusError::NotInDirectory`。

### 派发层

| 事件 | 形态 | `log` | 载荷 | 生产者 |
| --- | --- | --- | --- | --- |
| `core/dispatch.timeout` | observe | 是 | `DispatchTimeout { event, target, duration_ms }` | `Dispatcher::timeout`、`ObservePool` |
| `core/dispatch.failed` | observe | 是 | `DispatchFailed { event, failure, target?, reason }` | `Dispatcher::failed`、`ObservePool` |

`DispatchFailure` 取值：`bad_rewrite`、`handler_error`、`call_error`、`too_deep`、
`too_many_events`、`rate_limited`、`bad_payload`、`wrong_shape`、`not_in_directory`。

### Agent

| 事件 | 形态 | `log` | 载荷 | 生产者 |
| --- | --- | --- | --- | --- |
| `core/agent.create` | intercept | 是 | `AgentCreate { topic_id, parent_id?, mode, task? }` | 无 |
| `core/agent.terminate` | intercept | 是 | `AgentTerminate { instance_id, reason }` | 无 |
| `core/agent.cancel` | intercept | 是 | `AgentCancel { instance_id, origin }` | 无 |
| `core/agent.spawn` | observe | 是 | `AgentSpawn { instance_id, parent_id?, detached }` | `Agent::notify` 的 `NodeCreated` |
| `core/agent.status` | observe | 是 | `AgentStatus { instance_id, phase, phase_payload? }` | `Agent::notify` 的 `PhaseChanged` |
| `core/agent.sealed` | observe | 是 | `AgentSealed { instance_id }` | `Agent::notify` 的 `Sealed` |
| `core/agent.persist_failed` | observe | 否 | `AgentPersistFailed { topic_id, instance_id, phase, error }` | `Agent::persist` 失败 |
| `core/agent.tree` | observe | 是 | `AgentTree { nodes }` | 无 |

`core/agent.create`、`terminate`、`cancel` 是给宿主 API 预留的拦截点，当前没有产生者；
`core/agent.tree` 也没有产生者。`core/agent.persist_failed` 不写数据库，只推给当前订阅者。
三条观察事件都由 `Agent::notify` 以 `Origin::Internal`、`Source::core("agent")` 发出，
作用域是 `Scope::Topic { topic_id, instance_id }`，预算 50ms。

### LLM

| 事件 | 形态 | `log` | 载荷 | 生产者 |
| --- | --- | --- | --- | --- |
| `core/prompt.collect` | intercept | 是 | `PromptCollect { topic_id, instance_id, fragments }` | `turn::prepare` |
| `core/rule.apply` | intercept | 是 | `RuleApply { adapter, body }`（信封不携带载荷） | `LlmRunner::rounds` |
| `core/llm.request` | intercept | 是 | `LlmRequest { contexts, tools }`（信封不携带载荷） | `LlmRunner::rounds` |
| `core/llm.toolcall` | intercept | 是 | `ToolCalls { instance_id, message_id, calls, suspend? }` | `LlmRunner::rounds` |
| `core/message.seal` | observe | 否 | `MessageSeal { message_id, finish_reason, usage }` | `MessageStream::seal` |
| `core/message.interrupted` | observe | 否 | `MessageInterrupted { message_id, reason }` | `MessageStream::interrupted`、`Drop` |
| `core/block.delta` | observe | 否 | `BlockDelta { message_id, ...Block }` | `MessageStream::delta` |
| `core/block.committed` | observe | 否 | `BlockCommitted { message_id, seq, block_id, block? }` | `MessageStream::committed`、`notify_persisted_block` |
| `core/input.accepted` | observe | 是 | `InputAccepted { topic_id, instance_id, pending_id }` | `Runtime::submit` |

`core/message.seal`、`core/message.interrupted` 使用 `Close(Message)`；块增量与实时块使用
`Group(Message)`；`notify_persisted_block` 使用 `Live`。这些事件都不写 `event_log`。

`core/input.accepted` 由 `Runtime::submit` 在 Agent 应用命令后发出，作用域是
`Scope::Topic { topic_id, instance_id }`，来源是 `Source::core("runtime")`，预算 50ms，
`log = true`。用户内容本身经 `agent_pending` 表与 `LlmSpec` 进入；事件只宣告受理。

### Mod

| 事件 | 形态 | `log` | 载荷 | 生产者 |
| --- | --- | --- | --- | --- |
| `core/mod.installed` | observe | 是 | `ModInstalled { name, version, manifest, ui_entry? }` | `Installer` 的安装完成 |
| `core/mod.updated` | observe | 是 | `ModUpdated { name, from_version, to_version, ui_entry? }` | `Installer` 的更新完成 |
| `core/mod.deleted` | observe | 是 | `ModDeleted { name, version }` | `Installer` 的删除完成 |
| `core/mod.notify` | observe | 是 | `CoreModNotify { level, text }` | 脚本的 `ctx.notify`，经 `wind-core::script` 的 `EventSink` |

mod 自己发出的事件不在目录中。`ModEmitter` 由 `wind-core::script` 在调用脚本时按清单构造，
事件名与保留策略来自 `EmitChannel`：`journal` 频道的事件名是 `m_<mod>/<eventKey>`，
`live` 频道的事件名是 `m_<mod>/<channel>`；去向都是 `Target::Mod { name, channel }`。
这些事件会写入 `event_log`（`journal`）或只推给订阅者（`live`），但 `Dispatcher::on_event`
查不到目录项，因此不会派发给任何 mod。载荷校验只检查清单 schema 的顶层 `type`；完整
JSON Schema 尚未接入。

`core/mod.notify` 由 `EventSink::notify` 产生：作用域沿用本次脚本调用的作用域，来源是
`Source::Mod { name, version }`，载荷是 `{ level, text }`，保留策略固定为 `Journal`，
去向为 `Target::Core`。它同样受 `Guards::admit` 约束。

### 脚本调用

`Dispatcher` 不直接认识 QuickJS。它通过 `ModCaller` 调用 mod：

```text
decide / notify_host / ObservePool
  → ModCaller::call(subscription, payload, CallCx)
  → ScriptRuntime
      读当前 Mod 行与清单
      版本与订阅不一致 → CallError::Internal
      构造 ModEmitter、DispatchCx、Host
  → ScriptService::call(ScriptCall)
  → 解释器线程执行 handler(input, ctx)
```

`CallCx` 携带截止时间、取消令牌、调用模式与作用域。`ScriptRuntime` 用其中的剩余预算构造
`DispatchCx`，因此脚本产生的事件预算从本次调用剩余时间起算。`intercept` 的预算取
`min(订阅 timeout, 调用方剩余预算)`；`observe` 的预算取订阅 timeout，生成时的新
`DispatchCx` 不随产生方的上下文取消。

脚本的返回值映射：`Ok(None)` 表示不改写；`Ok(Some(value))` 交给 `decide` 解析；
`Err(CallError::Handler)` 记 `handler_error`，`Err(CallError::Internal)` 记 `call_error`；
超时记 `dispatch.timeout`。`observe` 忽略返回值。

### MCP

| 事件 | 形态 | `log` | 载荷 | 生产者 |
| --- | --- | --- | --- | --- |
| `core/mcp.launch` | intercept | 是 | `McpLaunch { name, refs, definition }` | `Registry` 启动前的改写 |
| `core/mcp.replaced` | intercept | 是 | `McpReplaced { name, refs, old, new }` | 同名定义替换前的改写 |
| `core/mcp.starting` | observe | 是 | `McpStarting { name, refs }` | `Registry` |
| `core/mcp.started` | observe | 是 | `McpStarted { name, refs, tools }` | `Registry` |
| `core/mcp.failed` | observe | 是 | `McpFailed { name, refs, error }` | `Registry` |
| `core/mcp.stopped` | observe | 是 | `McpStopped { name, refs, reason }` | `Registry` |
| `core/mcp.log` | observe | 否 | `McpLog { name, stream, line }` | 外部服务的 stderr |

MCP 事件的作用域固定为 `Scope::Global`，来源为 `Source::core("mcp")`。`launch` 与
`replaced` 的改写链因此查不到任何订阅（见「订阅索引」）。

## 前端投递

前端通过 `FeedHandle::subscribe` 取得 `Opening`，然后循环 `Subscription::recv`：

```text
subscribe()
  → Opening { replay_len, cut, subscription }
recv()
  → 先给保留窗口的回放（按 EventKey 升序）
  → 再给实时队列
```

投递规则：

1. `Target` 不参与过滤。所有可见事件都进入每条订阅的队列；`Target::Mod` 只影响
   `event_log` 的路由列。
2. 只投递 `observe` 形态。`intercept` 从不出现。
3. 队列满即取消该订阅，`recv` 返回 `None`。客户端应重连，并从领域表或历史重建状态。
4. 回放与历史的边界是 `cut`。`cut` 之前的事件可能已在 `event_log`，也可能已被
   `Close` 移出保留窗口；宿主必须自行处理重叠与缺口。
5. 事件身份按 `(epoch, seq)` 去重。客户端记录已应用的最大身份，丢弃不大于它的回放项。
6. 回放不含已定稿消息的内容。`Close` 已经释放该消息的增量与实时块；晚订阅者必须从
   `block` / `message` 表读取定稿内容。

块事件的前端解释：

| 事件 | 客户端动作 |
| --- | --- |
| `core/block.delta` | 按 `message_id` 与 `seq` 找块，`revision` 大于本地才追加；`kind` 决定落入哪种内容 |
| `core/block.committed`（`block` 为空） | 该块已在流式中完整接收；用 `block_id` 建立持久身份，可放弃本地临时块 |
| `core/block.committed`（`block` 有值） | 非实时消息的完整块；直接建立块，不等待增量 |
| `core/message.seal` | 标记消息完成，记录 `finish_reason` 与用量；该消息的组已关闭 |
| `core/message.interrupted` | 标记消息未正常完成；`reason` 为 `failed`、`cancelled` 或 `aborted` |

## 当前边界

| 项 | 现状 |
| --- | --- |
| 事件历史 | `event_log` 只写不读；`LogRecord` 没有查询入口 |
| 身份冲突 | Writer 失败即丢弃整批，Hub 仍通知观察者；历史可能缺口 |
| 保留窗口容量 | 无配置项；只在 `Close` 或 Writer 回报时收缩 |
| 全局作用域订阅 | `Roster` 只按话题索引，`core/mcp.*` 的改写链与 observe 订阅都为空 |
| mod 事件互订阅 | `m_<mod>/<channel>` 不在目录中，`Observer` 会丢弃 |
| observe 取消传播 | 投递池的 `CallCx.cancel` 是新建的空令牌 |
| 嵌套派发 | `DispatchCx::nested` 没有调用方，深度限制当前不生效 |
| 速率限制 | `admit_mod_emit` 由 `ModEmitter` 在每次 emit 时执行 |
| 脚本 HostApi | `ScriptHost` 只有 `emit` 与 `notify`；`state`、`storage`、`http`、`agent`、`fs` 未实现 |
| 脚本能力裁剪 | 清单 `capabilities` 不参与注入；`intercept` 没有 I/O 能力可裁剪 |
| `FeedError::Lagged` / `FeedError::IdentityConflict` | 在 feed 内无构造点 |
| 连接回放与历史 | 没有去重或拼接实现，只有 `cut` 与事件身份 |
