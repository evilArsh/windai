# 架构骨架

目标：单实例、一个话题一个会话、除数据库写之外全部并发、多个浏览器看同一话题时一致。
本文给骨架：crate、任务、职责、规则、重构顺序。事件与日志的字段合同见 `docs/event-identity.md`，调用关系见 `docs/event-flow.md`。

## 约束

- 单进程，不做多实例，不做跨进程广播。
- 同一话题同时只有一个会话在改状态；多个浏览器只读。
- 数据库写串行（SQLite 单写者），其余并发。
- 崩溃后不丢已广播的事件号，号不复用。

## crate 与依赖

| crate | 依赖 | 职责 |
| --- | --- | --- |
| `wind-wire` | — | 线格式：`Message`、`Content`、`Role`、工具声明 |
| `wind-stream` | wire | `Block`、`BlockKind`、事件载荷、块累加器 |
| `wind-event` | — | 信封、事件目录、派发、订阅索引、预算、`EventOutlet` 与 `Observer` 契约 |
| `wind-storage` | event, wire, ai | schema、SQL、行模型 |
| `wind-feed` | event, stream, storage, wire | 写入者、订阅者表、帧、历史、快照、补值 |
| `wind-ai` | wire | provider 配置、适配器、SSE 解析 |
| `wind-rule` | wire | 规则引擎 |
| `wind-core` | feed, ai, rule, stream, wire | agent 状态机、轮次、投影、工具执行 |
| `wind-mod` | core, event | 清单、安装、编译、能力裁剪、启用集 |
| `wind-script` | mod, core, event | QuickJS 宿主：运行时、解释器线程池、门面 |
| `wind-mcp` | core, event, wire | MCP 连接、注册表、工具目录 |
| `wind-runtime` | 全部 | 组合根、启动、恢复、关停 |

`wind-event` 不依赖任何其他内部 crate；`storage` 依赖 `event`，不允许反向。
HTTP/SSE 端点不在本骨架内，宿主应用只调用 `Hub::subscribe` 取帧流。

## 进程内任务

```text
┌ runtime ───────────────────────────────────────────────────────┐
│ assembly                                                       │
│                                                                │
│  writer 任务 ×1      DB 写：批量事务、拿 pos、提交后广播       │
│                                                                │
│  hub                每话题一张表：订阅连接队列 + 未完成块       │
│                                                                │
│  session(话题 A)     agent 循环：输入、模型、工具、定稿         │
│  session(话题 B)     ↑ 互不阻塞                                │
│                                                                │
│  SSE 连接 × N        只读自己那条队列                          │
│                                                                │
│  解释器线程池        mod 调用，每次调用新建运行时              │
│  MCP 连接 × M        工具调用，按请求 id 复用                  │
└────────────────────────────────────────────────────────────────┘
```

| 单元 | 实例数 | 并发方式 | 共享什么 |
| --- | --- | --- | --- |
| 话题会话 | 每话题一个 | tokio task | 本话题的累加器与订阅者表 |
| 写入者 | 1 | 单任务串行 | 写连接 |
| 订阅连接 | 每浏览器一条 | tokio task | 本连接的帧队列 |
| 读查询 | 连接池 | 并发 | WAL 读快照 |
| mod 调用 | 线程池 | 每次调用新建运行时 | 无（状态在库） |
| MCP 调用 | 每服务一条 | 请求 id 复用 | 连接 |

串行点只有三处：数据库写锁、写入者队列、单个话题的投递锁。三者都不跨话题。

调用方向、每条边的性质、拦截与流式的时序，见 `docs/event-flow.md` 的「模块交互模型」。

## 两条通路

写库的东西和只广播的东西不走同一条路，否则一个话题的提交会拖住另一个话题的打字流。

```text
话题 A 的会话                        话题 B 的会话
   │ plan/apply（A 的累加器）           │ plan/apply（B 的累加器）
   ├─ 分片 ──► A 的订阅者表 ──► 连接1   ├─ 分片 ──► B 的订阅者表 ──► 连接2
   │                                    │
   └─ 开块/提交/定稿 ──┐                └─ 开块/提交/定稿 ──┐
                       ▼                                    ▼
                  写入者（单任务）
                  批量事务 ──► pos ──► 广播 ──► 回执
```

规则：

- 写库的事件进写入者，批量提交，拿到 `pos` 后按顺序广播。
- 分片不写库，直接进本话题的订阅者表。
- **广播先于回执**：写入者先广播开块帧，再把 `block.id` 交回会话；会话拿到回执后才允许产生分片。因此分片永远不会早于它所属的开块帧。
- `boundary` 按话题取：`MAX(id) WHERE scope`。

## 一致性规则

1. 客户端可见 ⇒ 已提交。广播发生在提交之后。
2. `pos` 只增不复用。表自增 + 单写入者 + `synchronous = FULL`。
3. 广播先于回执。分片只能在其开块帧广播之后产生。
4. 每个订阅只读一个作用域。跨话题顺序不做保证。
5. 日志只存键与事实。值在读取时从对象表取。
6. 未完成块的内容只有一份，在内存；`block` 行里内容是空串。
7. 块标识由库给，会话拿到后才写入累加器。
8. 快照的集合查库，内容用内存覆盖；两者都在本话题的锁内取。

## 模块骨架

### `wind-event`（已实现）

```text
envelope.rs     Envelope<P>（无位置）、Scope、Origin、Source
def.rs          Shape、EventDef、EventMeta、Directory
context.rs      DispatchCx、DispatchGuard
guard.rs        Guards、Limits、OverLimit
dispatch.rs     形态检查、预算、改写链、submit、Observer 实现
subscription.rs Subscription、EnabledSet、SubscriptionIndex（按 rank 排序）
pool.rs         ObservePool：合批、超时、失败回报
mod_call.rs     ModCaller、CallCx、CallMode、CallError
outlet.rs       Pending、Committed、EventOutlet
events.rs       dispatch.timeout / dispatch.failed
lib.rs          Observer、ModCaller、ModRoster 三个 trait
```

### `wind-stream`

```text
id.rs           BlockId(i64)、MessageId、TurnId
block.rs        Block、BlockKind、content/data 约定
payload.rs      block.open / delta / committed 的载荷
accumulator.rs  开块、续写、定稿；标识由外部绑定
```

### `wind-storage`

```text
db.rs           连接池、WAL、synchronous=FULL、BEGIN IMMEDIATE
schema.rs       全部建表与索引
executor.rs     事务封装
models/         turn、message、block、agent_instances、pending、wait、spawn、mod、config
vendors/        按领域的语句集
```

### `wind-feed`

```text
writer.rs       队列、批量事务、pos、提交后广播、回执
hub.rs          订阅者表、历史、快照、补值
session.rs      每话题的累加器与投递锁
frame.rs        Frame::Event{pos, envelope} / Frame::Snapshot{baseline}
queue.rs        有界帧队列：Fragment 丢旧、Journal 断开
subscribe.rs    握手参数、Subscriber
log.rs          LogRecord（组合 Envelope）、行互转、可见性
```

### `wind-core`

```text
agent/hub.rs        会话登记、启停、恢复
agent/tree.rs       FSM、域事件、归约
agent/behavior.rs   一次输入的完整路径
agent/tool.rs       工具计划与执行
agent/jobs.rs       后台任务
llm/turn.rs         轮次编排：建轮、建消息、请求、定稿
llm/execute.rs      模型调用
llm/prompt.rs       提示词
llm/tools.rs        工具清单
conversation/writer.rs   plan、apply、commit、seal
conversation/assembler.rs 投影、normalize、repair_tools
conversation/events.rs    块事件定义
```

### `wind-mod` 与 `wind-script`

```text
mod/manifest.rs   清单
mod/compiler.rs   安装期编译为字节码
mod/catalogue.rs  事件目录合并
mod/enablement.rs 话题启用集
mod/capability.rs 能力裁剪矩阵
script/runtime.rs 一次调用的装配
script/threads.rs 解释器线程池
script/loader.rs  模块加载器
script/facade/    ctx.agent / ctx.state / ctx.http / ctx.fs
script/deployment.rs  部署与版本
```

### `wind-mcp` 与 `wind-runtime`

```text
mcp/registry.rs   服务注册、工具目录
mcp/connector.rs  连接与请求 id
mcp/runtime.rs    调用执行
runtime/assembly.rs  装配：storage → feed → event → core → mod → script → mcp
runtime/recovery.rs  启动恢复：清理残块、恢复未完成轮次
runtime/tree.rs      多话题会话的持有与关停
runtime/shutdown.rs  排空写入者、关闭连接
```

## 关键接口

```rust
// wind-event（已实现）
pub struct Envelope<P> { time, name, scope, origin, source, payload }
pub struct Pending { shape: Shape, log: bool, envelope: Envelope<Value> }
pub struct Committed { pos: i64, shape: Shape, envelope: Envelope<Value> }
pub trait EventOutlet: Send + Sync { fn submit(&self, pending: Pending); }
pub trait Observer: Send + Sync { fn on_committed(&self, meta: &EventMeta, committed: &Committed); }
pub trait ModCaller: Send + Sync { async fn call(..) -> Result<Option<Value>, CallError>; }
pub trait ModRoster: Send + Sync { fn enabled(&self, scope: &Scope) -> EnabledSet; }

// wind-feed（未实现）
impl EventOutlet for WriteHandle;
impl WriteHandle {
    fn block_open(&self, req: BlockOpen) -> oneshot::Receiver<Result<i64, WriteError>>;
    fn block_commit(&self, req: BlockCommit);
    fn message_seal(&self, req: MessageSeal);
}
impl Hub {
    fn subscribe(&self, req: Subscribe) -> Opening;      // 锁内取 boundary 与快照
    fn publish_committed(&self, event: &Committed);      // 写入者调用
    fn publish_delta(&self, delta: Delta);               // 会话直接调用，不写库
}
impl Observer for Hub;

// wind-core（未实现）
impl ModCaller for Session;                               // 拦截链
impl ModRoster for Session;                               // 该作用域启用的 mod
```

## 数据库

`turn`、`message`、`block`、`event_log` 的列与索引见 `docs/event-identity.md`。
本骨架补两条运行时约束：

- `PRAGMA journal_mode = WAL`、`PRAGMA synchronous = FULL`。
- 写事务一律 `BEGIN IMMEDIATE`；读不加锁，直接走 WAL 快照。

## 重构顺序

每一步都有可验收的出口。

| 步 | 内容 | 出口 |
| --- | --- | --- |
| 1 | 建 `wind-feed`；`storage` 换新 schema 与语句 | 存储测试通过：开块、提交、日志区间、快照、补值 |
| 2 | `wind-stream`：去掉发号，改为外部绑定 | 累加器测试通过 |
| 3 | `wind-event`：`sink`/`observer`，派发改“先写后发” | 派发测试用 `RecordingSink` 通过 |
| 4 | `wind-feed`：写入者 + hub + 帧 | 并发测试：两话题同时出字，互不阻塞 |
| 5 | `wind-core`：会话写入与投影 | 一轮对话端到端，落库与帧一致 |
| 6 | `wind-core`：agent 状态机与恢复 | 崩溃后重启，未完成轮次可解释 |
| 7 | `wind-mod` + `wind-script`：旁路收进 `Dispatcher` | mod 发事件不再直推 hub |
| 8 | `wind-mcp` + `wind-runtime` 装配 | 多浏览器同话题一致、断线重连一致 |

## 从 `windai-bak` 删掉

| 删除 | 原因 |
| --- | --- |
| `feed/hub.rs` 的 `pending` / `flush` / `high_water` / `inflight` | 日志与水位改由写入者与库承担 |
| `stream/accumulator.rs` 的 `next_id` / `after` / `allocate_id` | 标识由库给 |
| `feed/log.rs` | 并入 `feed/log.rs` 的组合信封版本 |
| `mod/src/feed.rs` 的 `emit` / `retract` 自建信封 | 改走 `Dispatcher::emit_mod` |
| `event` 的 `EventSeq` 与播种 | 位置来自日志行 |
| `schema` 的 `is_boundary` | 从未写过真值 |

## 我替你定的

以下几条是我的判断，改动成本低，但会影响后续代码，写在这里便于你否掉：

1. **新 crate `wind-feed`**：写入者与 hub 不放进 `core`，让“只有这里能写 `event_log`”变成 crate 边界。
2. **`synchronous = FULL`**：`NORMAL` 掉电会丢号并复用，客户端会静默丢事件。批量提交抵消 fsync 开销。
3. **分片不进写入者**：否则一个话题的提交会拖慢别的话题的打字流；靠“广播先于回执”保证顺序。
4. **`hub` 按话题分表**：订阅者表与累加器都按话题隔离，锁粒度是话题。
5. **`core/llm.request` 的请求体不入日志**：一条几 MB 的记录会穿过唯一写入者，拖住所有话题。日志只存 `model_id`、`message_id`、消息条数与字节数。
6. **mod 调用不共享运行时**：沿用 `windai-bak/script` 的做法，每次调用新建 QuickJS 运行时并复用解释器线程池，池大小默认 `min(4, 核数)`；因此 mod 之间、话题之间都不串行。
7. **会话按话题懒启动、空闲回收**：状态全部落库，回收后可从 `agent_instances` 恢复。
