# 事件从产生到前端：流程图

所有图都在这一篇。规则（标识、位置、日志内容）在 `docs/event-identity.md`。

## 模块分层

```text
子系统（llm / agent / mcp / fs …）
  │ decide(cx, payload) / notify(cx, payload)
  ▼
wind-event::Dispatcher
  ├ 目录：事件名 → 形态、是否落日志
  ├ 订阅索引：事件名 → mod（intercept / observe）
  └ 预算、限流、投递追踪
  │
  │ 事件
  ▼
写入者（单写者）
  │ 事务：领域行 + event_log → pos
  │ 按 pos 顺序广播
  ├──► FeedHub ── 连接表（话题 → 帧队列）──► SSE ──► 浏览器
  ├──► 其他观察者（遥测）
  └──► ObservePool ──► mod（host.call）
```

`wind-event` 决定“这条事件该给谁、谁能改”，`FeedHub` 决定“这些帧怎么送到每个浏览器”。

## 两条订阅链

名字都叫订阅，对象完全不同。

```text
① 事件订阅：按事件名，主体是 mod
   core/block.committed ──► SubscriptionIndex ──► mod-a
                                                ──► mod-b

② 连接订阅：按话题，主体是浏览器
   topic 7 ──────────────► FeedHub 连接表 ─────► 浏览器 A（帧队列）
                                               ─────► 浏览器 B（帧队列）
```

① 决定“要不要调 mod、按什么顺序改写”。② 决定“这一帧塞进哪些队列”。两者不共享状态：mod 启停只重建 ①，浏览器连断只增减 ②。

## 旧实现的关系

```text
子系统
  │ decide / notify
  ▼
Dispatcher
  │ 事件（seq 在派发前就分配好了）
  ├──► 观察者
  │       ├─ FeedHub：攒日志 + 推帧
  │       └─ 遥测
  └──► ObservePool ──► mod
```

和现在只差两处：日志与推帧都在观察者里做，位置在派发前就分配。这两点合起来就是“前端到 1000、库里还在 10”。

## 一次对话的完整流程

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
  ├─ core/llm.request（intercept：先让 mod 改写，再落库）
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

## 实时路径：一条事件

```text
子系统
  │  payload + DispatchCx{origin, scope, source, cause}
  ▼
Dispatcher::decide / notify
  │  reserve(cx)                 预算检查，不发号
  │  intercept：mod 改写链         只改 payload
  ▼
写入者
  │
  ├─ 事务：领域行 + INSERT event_log → id
  │
  ├─ LogRecord { pos: id, shape, event }     读取侧形态
  └─ Frame::Event { pos: id, event }         下发形态，载荷换成值
  ▼
订阅者帧队列（每个 SSE 连接一条）
  ▼
浏览器
```

## 握手与回补

```text
浏览器 ──握手(scope, since_pos)──► 临界区
                                    boundary = MAX(event_log.id)
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

## `pos` 从哪来，用在哪

```text
INSERT INTO event_log ... RETURNING id   →  41
                                             │
             ┌───────────────────────────────┴────────────────┐
             ▼                                                ▼
LogRecord { pos: 41, shape, event }              Frame::Event { pos: 41, event }
        （读取侧）                                      （下发侧）
                                                             │
                                                      浏览器游标 = 41
```

同一个数，只分配一次。客户端拿它排序、去重、断线续传。

## 三种类型怎么套

```text
EventEnvelope<Value>
├ time · name · scope · origin · source · cause
└ payload                     日志里是键；下发时是值

LogRecord                         Frame::Event
├ pos   : i64                     ├ pos   : Option<i64>
├ shape : Shape                   └ event : EventEnvelope<Value>
└ event : EventEnvelope<Value>        （payload 已回填）
```

`Snapshot` 帧不属于这条链：它没有信封，也没有位置。

## 两个 subscribe

图见「两条订阅链」。两者的差别在两件事上：

| | `SubscriptionIndex` | `FeedHub` 的订阅者 |
| --- | --- | --- |
| 换什么 | 要不要调 mod、按什么顺序改写 | 这一帧塞进哪些队列 |
| 何时变 | mod 启用或停用 | 连接建立或断开 |

## 两个“发布”

| | `Dispatcher::notify` | `FeedHub::publish` |
| --- | --- | --- |
| 输入 | 带类型的 payload | 已经造好的帧 |
| 输出 | 交给观察者，并排进 mod 投递队列 | 塞进各连接的帧队列 |
| 含义 | 产生一条事件 | 分发一帧 |

## `notify` 做五件事

```text
notify(def, cx, payload)
  1. 检查形态：必须是 observe，否则报 dispatch.failed
  2. 序列化 payload；失败也报 dispatch.failed
  3. 检查预算：单次触发的事件数上限
  4. 组信封，逐个交给观察者
       FeedHub 在这里：写事件日志、推帧
  5. 查 mod 订阅，给每个 observe 订阅者投一个任务
       进 ObservePool，异步、按窗口合批、超时单独上报
  不等待 mod
```

## `decide` 和它差在哪

```text
decide(def, cx, payload) -> payload
  1. 形态必须是 intercept
  2. 检查预算
  3. 取订阅链，按相位、注册次序排序
  4. 逐个调 mod：能改写 payload，超时或报错就跳过
  5. 最终 payload 交给观察者
  返回改写后的 payload
```

`notify` 是“告诉你已经发生了”，不等回话。
`decide` 是“先问你同不同意”，阻塞在 mod 上，返回值就是被改过的 payload。

## 三个常见误解

1. **“FeedHub 订阅了事件”**：不是。它订阅的是“某个话题的帧”，按连接算；事件订阅在 `SubscriptionIndex` 里，按 mod 算。
2. **“`notify` 会把事件推给浏览器”**：不会。它只把事件交给观察者；把帧塞进连接队列是 `FeedHub` 的事。
3. **“mod 可以自己发事件”**：旧代码有这条路——`windai-bak/mod/src/feed.rs` 自己组信封、自己调 `hub.publish`，还手工取号。新设计要收掉：所有事件都走写入者，否则会出现没有 `pos` 的帧。

## 代码位置

| 事 | 旧代码 | 新代码 |
| --- | --- | --- |
| 事件目录、形态、是否落日志 | `windai/event/src/def.rs` | 保留 |
| 派发、改写链、预算 | `windai/event/src/dispatch.rs` | 改：去掉取号，加“先写后发” |
| mod 订阅索引 | `windai/event/src/subscription.rs` | 保留 |
| observe 投递池 | `windai/event/src/pool.rs` | 保留，`seq` 改 `pos` |
| 连接、帧队列、历史与快照 | `windai-bak/core/src/feed/hub.rs`、`queue.rs`、`frame.rs` | 重写 `hub.rs`，其余保留 |
| 日志行结构与读写 | `windai-bak/core/src/feed/log.rs`、`storage/log.rs` | 合并到存储层的日志模块 |
| mod 自造事件 | `windai-bak/mod/src/feed.rs` | 删除旁路，改走写入者 |
