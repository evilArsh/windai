# 事件从产生到前端：流程图

所有图都在这一篇。规则见 `docs/event-identity.md`。

`wind-event` 部分与代码一致。写入者、Hub、存储还没有实现，涉及它们的图会标「未实现」。

## 模块分层

```text
子系统（llm / agent / mcp / fs …）
  │ decide(cx, payload)  等结果
  │ notify(cx, payload)  不等
  ▼
wind-event::Dispatcher
  ├ Directory          事件名 → 形态、是否落日志、schema
  ├ SubscriptionIndex  事件名 → mod，按 rank 排序
  ├ Guards             深度、每触发事件数、mod 发事件速率
  └ ModRoster          某个作用域启用了哪些 mod
  │ EventOutlet::submit(Pending)
  ▼
事件出口（`feed::WriteHandle`，未实现）
  │ 批量事务：领域行 + event_log → pos
  │ 按 pos 顺序回调 Observer
  ├──► Dispatcher::on_committed ──► ObservePool ──► ModCaller::call
  ├──► Hub ──► 帧队列 ──► SSE ──► 浏览器
  └──► 其它 Observer
```

`wind-event` 决定“这条事件该不该发、该给哪些 mod、谁可以改”，Hub 决定“这些帧怎么送到每个浏览器”。

## 两条订阅链

```text
① 事件订阅：按事件名，主体是 mod
   core/block.committed ──► Dispatcher::on_committed
                              └─► SubscriptionIndex ──► mod-a / mod-b
                              └─► ObservePool ──► ModCaller::call

② 连接订阅：按话题，主体是浏览器
   topic 7 ──────────────► Hub 的连接表 ──► 浏览器 A（帧队列）
                                        ──► 浏览器 B（帧队列）
```

① 在事件提交之后才发生，决定“要不要调 mod”。② 决定“这一帧塞进哪些队列”。两者不共享状态：mod 启停只重建 ①，浏览器连断只增减 ②。

## 一条 observe 事件的经过

```text
调用方
  │ notify(def, cx, payload)                     同步
  ▼
Dispatcher
  ├ require_shape(Observe)     形态不对 → dispatch.failed
  ├ guards.admit(cx)           超限 → dispatch.failed（TooDeep / TooManyEvents）
  ├ to_json(payload)           失败 → dispatch.failed
  └ EventOutlet::submit(Pending { shape, log, envelope })
```

`notify` 到此结束，不等落库，也不等 mod。

## 一条 intercept 事件的经过

```text
调用方
  │ decide(def, cx, payload) -> P                异步
  ▼
Dispatcher
  ├ require_shape(Intercept)
  ├ guards.admit(cx)
  ├ 订阅链 = index.ordered_in(event, roster.enabled(scope))    按 rank
  ├ 逐个 sub：
  │     预算 = min(sub.timeout, cx.remaining_budget())；为 0 则跳过
  │     ModCaller::call(sub, 当前载荷, CallCx{mode: Intercept})
  │       Ok(None)            → 不改
  │       Ok(Some(v))         → 解析成 P，成为新载荷
  │       超时                → dispatch.timeout
  │       Handler / Internal  → dispatch.failed
  │       取消                → 跳过这个 sub，不发通知
  └ emit(最终载荷) → EventOutlet::submit
  返回最终载荷
```

改写链是串行的：后一个 mod 看到的是前一个改写后的载荷。

## 提交之后

```text
事件出口（未实现）
  ├ 批量事务 → pos
  └ 按 pos 顺序调 Observer::on_committed(meta, committed)      同步，不得阻塞
       ├─ Dispatcher：只看 observe 形态；查索引；入 ObservePool
       └─ Hub：造帧，推给各连接（未实现）
```

`Observer` 是同步 `fn`，实现方只能入队或 push。慢活留给各自的任务。

## wind-feed 的结构（未实现）

这一节说清 `wind-feed` 由什么组成，以及它与 `wind-ai`、`wind-event`、`wind-storage`、`wind-stream` 的接缝。

### LLM 流怎么进来

```text
wind-ai
  chat::stream()
    └ Chunk::Delta(wire::Message)
          │
          ▼
core::llm::turn::exchange
  ├ 每个 delta：BlockWriter::plan / apply
  │              ├ 已有块 ──► Hub::publish（分片，不写库）
  │              └ 新块   ──► WriteHandle::block_open，等回执拿 id
  └ 流结束：    WriteHandle::block_commit / message_seal
          │
          ▼
feed::WriteHandle → 队列
```

feed 不认识 `Chunk`，也不持有 stream。对接点是 core 的循环，输入类型是 `wire::Message`。

### 谁实现哪个接口

```text
                    wind-event（契约 + 派发语义）
        ┌────────────────────────────────────────────────┐
        │ Envelope / Pending / Committed                 │
        │ EventOutlet::submit(Pending)     ← 写入侧契约   │
        │ Observer::on_committed(...)      ← 广播侧契约   │
        │ Dispatcher  ← 形态、预算、改写链、mod 投递      │
        └────────────────────────────────────────────────┘
                  ▲                            ▲
    实现 submit   │                            │  实现 on_committed
                  │                            │
          feed::WriteHandle             feed::Hub
                                        feed::ModFanout
                                        遥测观察者
```

`Dispatcher` 只持有 `Arc<dyn EventOutlet>`，它不知道实现者是句柄、任务还是测试替身。

`ModFanout` 目前没有单独类型：`Dispatcher::on_committed` 做的就是这件事。要把它抽成 `feed` 里的独立类型，得改 `wind-event`。

### 句柄与任务

```text
core ── Arc<WriteHandle> ─┐
http ── Arc<WriteHandle> ─┤   多个克隆，都只往同一个队列塞
                          ▼
                 mpsc::Sender<Work>
                          │
                          ▼
        ┌──────── Writer 任务（进程内唯一）────────┐
        │ rx: Receiver<Work>                      │
        │ storage: DbPool                         │
        │ observers: Vec<Arc<dyn Observer>>       │
        └─────────────────────────────────────────┘
```

- 句柄：生产者侧。`submit`、`block_open`、`block_commit`、`message_seal` 都同步入队；`submit` 就是 `EventOutlet` 的实现。
- 任务：消费者侧。唯一实例，串行处理：一批一个事务，提交后广播。
- 通道由 `WriteQueue::open()` 建，返回 `(WriteHandle, Writer)`；不写库、不 spawn，监听者必须先挂上。
- `block_open` 带回执：`oneshot::Receiver<Result<i64, WriteError>>`。会话拿到标识之后才允许产生分片。`block_commit`、`message_seal` 不带回执。
- 累加器归会话（`feed::session`），写入者只碰库与广播。

### 队列里放什么

| 变体 | 事务里写什么 | 载荷里的 id 从哪来 | 线上载荷 |
| --- | --- | --- | --- |
| `Event(Pending)` | 只有 `event_log` | 提交前已有 | 就是提交的那份 |
| `BlockOpen` | `block` INSERT + `event_log` | INSERT 返回 | 写入者用刚写的列拼 |
| `BlockCommit` | `block` UPDATE + `event_log` | 已有 | 同上 |
| `MessageSeal` | `message` UPDATE + `event_log` | 已有 | 同上 |

没有 `BlockGrow`：分片不进队列。

```text
loop {
    batch = try_recv 直到空（≤256）
    事务 { 逐条写：块行 / 消息行 / 日志行 }
    提交                      // 失败整批回滚
    广播（按 pos 升序，同步调用 Observer）
    回执（只给 BlockOpen）
}
```

写入者自己失败时只记日志，不再 `submit` 通知，否则递归。

### Writer 与 Hub

```text
Writer 任务
  │ 提交后：for obs in observers { obs.on_committed(meta, &committed) }
  ├──────────────► Hub           → 拼帧、按 scope 塞进各连接队列
  ├──────────────► ModFanout     → 观察型 mod 入投递池
  └──────────────► 遥测

Hub 的另外两条入口（都不经 Writer）：
├─ SSE 握手 ──► hub.subscribe(scope, since_pos) ──► 历史 + 快照 + 订阅
└─ 分片     ──► hub.publish(scope, frame)       ──► 本话题各连接队列
```

单向：Writer 调 Hub 的 `on_committed`，Hub 从不回调 Writer。

回执在广播之后：会话先等到开块帧广播完、拿到 `block.id`，才允许产生分片。所以分片永远排在自己的开块帧之后。

### Hub 是什么

```text
Hub
├ 按话题分表：{ 订阅者连接 + 帧队列 }，锁粒度是话题
├ subscribe(scope, since_pos) -> Opening
│    临界区 { boundary = MAX(id) WHERE scope；快照；注册 }
│    锁外   { 按 pos 区间查历史并补值 }
├ publish_committed(committed)   → 写入者调，推给本话题的连接
├ publish_delta(scope, frame)    → 会话直接调，不写库
└ 快照 = SQL 查未完成块 + 累加器覆盖 content
```

它是连接级订阅服务，不是事件总线：事件有多少 mod 订阅与它无关，它只决定“这条帧给哪几个浏览器”。

### 累加器在哪

```text
wind-stream::BlockAccumulator
  纯结构：plan / apply / settle，不认识 topic、不碰库

feed::session（每话题一个）
  ├ 累加器（Arc<Mutex<..>>）
  └ 投递锁（本话题的订阅者表）
  使用者：
  ├ 话题会话    plan / apply / settle；开块后等回执拿标识
  └ Hub         快照时按 id 覆盖 content
```

累加器不碰数据库、不认识 topic、不发帧。

### 全景

```text
                  wind-ai
                    │ Chunk::Delta(wire::Message)
                    ▼
    core::llm::turn::exchange ──► core::conversation（建轮、建消息、定稿）
                    │
                    ├─ 分片 ──────────► feed::Hub::publish ──► 连接队列
                    │                     ▲
                    │                 feed::session（每话题的累加器与投递锁）
                    │
                    └─ 开块 / 提交 / 定稿 ──► feed::WriteHandle
                                                    │ 队列
                                                    ▼
                                            feed::Writer ──► wind-storage（事务）
                                                    │ 广播
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                                 feed::Hub      Dispatcher         遥测
```

### 文件

| 文件 | 管什么 |
| --- | --- |
| `lib.rs` | 对外只导出 `Hub`、`WriteHandle` |
| `writer.rs` | `WriteHandle`、`enum Work`、`Writer` 任务、回执、广播 |
| `hub.rs` | 按话题的订阅者表、帧、历史、快照、补值 |
| `session.rs` | 每话题的累加器与投递锁 |
| `frame.rs` | `Frame::{Event, Snapshot}`、`Baseline` |
| `queue.rs` | 有界帧队列：Fragment 丢旧、Journal 断开 |
| `subscribe.rs` | 握手参数、`Subscriber` |
| `log.rs` | `LogRecord` 与行互转、补值、可见性 |

## 通知事件

`core/dispatch.timeout` 与 `core/dispatch.failed` 由派发层自己产生，形状固定为：

| 字段 | 值 |
| --- | --- |
| `shape` | `Observe` |
| `log` | `true` |
| `origin` | `Internal` |
| `source` | `core("event")` |

它们和普通事件走同一个出口，只是不占每触发的事件预算。否则“预算用尽 → 发通知 → 又检查预算”会自己套自己。

## 一次对话的完整流程（未实现）

```text
用户输入
  │
  ▼  agent_pending 入队
建 turn                                  （不产生日志）
建 message(role = user)                  （不产生日志）
开块  text      ── 事务：block 行 + core/block.open
提交块          ── 事务：UPDATE block + core/block.committed
  │
  ▼  第 1 轮
建 message(role = assistant, model_id)   （不产生日志）
  │
  ├─ core/llm.request（intercept：先让 mod 改写，再提交）
  │
  ├─ 流式分片 ──── 只进内存，发 core/block.delta（不落库）
  ├─ 开块    ───── 事务：block 行 + core/block.open
  ├─ 提交块  ───── 事务：UPDATE block + core/block.committed
  └─ 定稿    ───── 事务：UPDATE message + core/message.seal
  │
  ▼  有工具调用
执行工具
建 message(role = tool)
每个结果一块 tool_result
  开块 × N ─────── 每个结果一次开块事务
  提交块 × N ───── 每个结果一次提交事务
  │
  ▼  第 2 轮 ……重复，直到某一轮没有工具调用
结束
```

只有三处产生日志：块开、块提交、消息定稿。分片永远不落库。

## 握手与回补（未实现）

```text
浏览器 ──握手(scope, since_pos)──► 临界区
                                    boundary = MAX(id) WHERE scope
                                    blocks   = 快照()
                                    注册订阅
                                  │
             ┌────────────────────┴─────────────────────┐
             ▼                                          ▼
   历史：event_log WHERE id > since            快照：block ⋈ message ⋈ turn
   按事件名 JOIN block / message                    WHERE finished = FALSE
   回填载荷                                          + 内存累加器覆盖 content
             │                                          │
             └────────────► 依次发送 ◄──────────────────┘
                        history → snapshot → 实时帧
```

左边回答“注册之前发生过什么”，右边回答“此刻哪些块还没写完”。两者都在注册那一刻取定。

```text
注册在历史查询之前发生，实时帧因此有地方落。
补历史期间到达的帧进这个连接自己的队列，不丢。
读端先发历史与快照，再排空队列。
```

`boundary` 取本话题的 `MAX(id)`，与注册在同一临界区：

| 事件的位置 | 在历史里 | 在队列里 |
| --- | --- | --- |
| 广播早于注册 | 是 | 否 |
| 已提交，广播晚于注册 | 是 | 是 |
| 提交与广播都晚于注册 | 否 | 是 |

第二行会重复一次，客户端按“`pos` 不大于已应用最大值就丢”处理，不会重复渲染。宁可重一次，也不盯广播位置。队列有上限；撑不住就断开这条连接，由客户端重连重来。

## `pos` 从哪来

```text
INSERT INTO event_log ... RETURNING id   →  41
                                             │
                                             ▼
                                  Committed { pos: 41, envelope }
                                             │
                          ┌──────────────────┴──────────────────┐
                          ▼                                     ▼
                 LogRecord { pos, shape, event }        Frame::Event { pos, event }
                       （读取侧）                            （下发侧）
                                                                  │
                                                           浏览器游标 = 41
```

同一个数，只分配一次。客户端拿它排序、去重、断线续传。

## 三种类型怎么套

```text
Envelope<Value>                     事件本身
├ time · name · scope · origin · source
└ payload                           日志里是引用；下发时是值

Pending               离开总线              Committed            回到总线
├ shape               （wind-event 出）    ├ pos                （出口给出）
├ log                                     ├ shape
└ envelope                                └ envelope
```

`LogRecord` 与 `Frame` 属于 `wind-feed`，尚未实现；形状见 `docs/event-identity.md`。
`Snapshot` 帧不属于这条链：它没有信封，也没有位置。

## 两个 subscribe

图见「两条订阅链」。两者的差别在两件事上：

| | `SubscriptionIndex` | Hub 的连接表 |
| --- | --- | --- |
| 换什么 | 要不要调 mod、按 `rank` 排先后 | 这一帧塞进哪些队列 |
| 何时变 | mod 启用或停用 | 连接建立或断开 |

## 两个“发布”

| | `Dispatcher::notify` / `decide` | 事件出口的广播 |
| --- | --- | --- |
| 输入 | 带类型的 payload | `Committed` |
| 输出 | 一个 `Pending` | 回调所有 `Observer` |
| 含义 | 产生一条事件 | 分发已提交事件 |

## `notify` 与 `decide` 对照

| | `notify` | `decide` |
| --- | --- | --- |
| 形态 | `Observe` | `Intercept` |
| 同步性 | 同步，立刻返回 | 异步，等改写链 |
| 是否调 mod | 不调 | 调，串行 |
| 返回 | 无 | 改写后的载荷 |
| 副作用 | `submit` 一次 | `submit` 一次 |

两者都先过形态检查和 `Guards`，区别只在于中间那段改写链。

## 四个常见误解

1. **“Dispatcher 里有一张观察者名单”**：没有。观察者由事件出口调用；`Dispatcher` 自己实现 `Observer`，在 `on_committed` 里给 mod 排任务。
2. **“`notify` 会把事件推给浏览器”**：不会。它只 `submit` 给出口。
3. **“mod 可以直接推帧”**：出口只有一个，所有事件都 `submit`。
4. **“observe 事件在 `notify` 里就发给 mod”**：不是。mod 投递发生在提交之后，由 `on_committed` 触发。

## wind-event 代码位置

| 文件 | 管什么 |
| --- | --- |
| `envelope.rs` | `Envelope`、`Scope`、`Origin`、`Source` |
| `def.rs` | `Shape`、`EventDef`、`EventMeta`、`Directory` |
| `context.rs` | `DispatchCx`、`DispatchGuard` |
| `guard.rs` | `Guards`、`Limits`、`OverLimit` |
| `dispatch.rs` | 形态检查、预算、改写链、`submit`、`Observer` 实现 |
| `subscription.rs` | `Subscription`、`EnabledSet`、`SubscriptionIndex` |
| `pool.rs` | `ObservePool`：合批、超时、失败回报 |
| `mod_call.rs` | `ModCaller`、`CallCx`、`CallMode`、`CallError` |
| `outlet.rs` | `Pending`、`Committed`、`EventOutlet` |
| `events.rs` | `DispatchTimeout`、`DispatchFailed`、`DeliveryTarget` |
| `lib.rs` | `Observer`、`ModCaller`、`ModRoster` 三个 trait 与再导出 |
