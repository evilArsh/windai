# 事件设计问答记录

本文件是本轮设计的问答与决策记录。规则本身见 `docs/event-identity.md`，调用关系见 `docs/event-flow.md`；这里保留“为什么这么定”和讨论中出现的图与表。

## 已定方案速览

- 对外编号全部由库分配。`turn` / `message` / `block` 用表自增主键；块在开块时插入行，随即拿到标识。
- 事件的位置 `pos` 就是 `event_log.id`。先落库，后广播。
- 同一父行内按 `id` 排序，删除 `message.seq` 与 `block.seq`。
- 日志只存 subject 引用与事件事实，不存对象的值；值在读取时从对象表取回。
- 快照：有哪些块由 SQL 决定，内容由内存累加器决定。
- 单运行时、单写入者。块行与其日志行同一事务，提交后由写入者按 `pos` 顺序广播。
- 分片（`core/block.delta`）不落库、无 `pos`，直接从会话层推给 `Hub`。

## 图集

图 G1 模块分层：

```text
子系统（llm / agent / mcp / fs …）
  │ decide(cx, payload) / notify(cx, payload)
  ▼
wind-event::Dispatcher
  ├ 目录：事件名 → 形态、是否落日志
  ├ 订阅索引：事件名 → mod（intercept / observe）
  └ 预算、限流
  │ 事件
  ▼
写入者（单写者）
  │ 事务：领域行 + event_log → pos
  │ 按 pos 顺序广播
  ├──► Hub ── 连接表（话题 → 帧队列）──► SSE ──► 浏览器
  ├──► 其他观察者（遥测）
  └──► ObservePool ──► mod（ModCaller::call）
```

图 G2 两条订阅链：

```text
① 事件订阅：按事件名，主体是 mod
   core/block.committed ──► Dispatcher::on_committed
                              └─► SubscriptionIndex ──► mod-a / mod-b

② 连接订阅：按话题，主体是浏览器
   topic 7 ──────────────► Hub 连接表 ─────► 浏览器 A（帧队列）
                                               ─────► 浏览器 B（帧队列）
```

图 G3 实时路径（一条事件）：

```text
子系统
  │  payload + DispatchCx{origin, scope, source}
  ▼
Dispatcher::decide / notify
  │  guards.admit(cx)             预算检查，不发号
  │  intercept：mod 改写链         只改 payload
  ▼
写入者
  ├─ 事务：领域行 + INSERT event_log → id
  ├─ LogRecord { pos: id, shape, event }     读取侧形态
  └─ Frame::Event { pos: id, event }         下发形态，载荷换成值
  ▼
订阅者帧队列（每个 SSE 连接一条）──► 浏览器
```

图 G4 握手与回补：

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

图 G5 `pos` 从哪来，用在哪：

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

图 G6 三种类型怎么套：

```text
Envelope<Value>                      事件本身（wind-event）
├ time · name · scope · origin · source
└ payload                            日志里是 subject/事实；下发时是值

Pending / Committed                  wind-event 的两个出口形态
├ shape                              Committed 另带 pos

LogRecord                            Frame::Event（wind-feed，未实现）
├ pos   : i64                        ├ pos   : Option<i64>
├ shape : Shape                      └ event : Envelope<Value>
└ event : Envelope<Value>                （payload 已回填）
```

图 G7 `core/block.open`：

```text
streaming chunk
  │  accumulator.plan(message_id, chunk)：发现要开新块
  ▼
WriteHandle::block_open(message_id, role, kind, call_id)
  │
  ▼
Work::BlockOpen  ──入队──►  写入者
                              │
        事务 {                 │
          INSERT block(...) RETURNING id = 512
          INSERT event_log('core/block.open', {subject:{block,512}}) RETURNING id = 41
        }                      │
        commit                 │
                               │
   ┌───────────────────────────┤
   ▼                           ▼
回执 512 给会话          广播 Committed { pos: 41, 线上载荷 }
（拿到才允许发 delta）      ├─► Hub：{block_id, of_message, role, kind, call_id}
                            └─► ObservePool：订阅了 core/block.open 的 mod
```

图 G8 `core/block.committed`：

```text
accumulator.settle(512) → content, data, revision
  ▼
WriteHandle::block_commit(512, content, data, revision)
  ▼
写入者：
  事务 {
    UPDATE block SET content, data, revision, finished = TRUE WHERE id = 512
    INSERT event_log('core/block.committed', {subject:{block,512}}) RETURNING id = 44
  }
  commit
  ▼
广播 Committed { pos: 44, 线上载荷 {block_id, kind, call_id, content, data, revision} }
  ▼
客户端：整体覆盖，置 finished
```

图 G9 分片：

```text
SSE 分片 → exchange → BlockWriter::plan / apply
  ├─ 累加器：落到哪个块、append、revision += 1
  └─ 造帧 Frame::Event { pos: None, envelope: core/block.delta{block_id, revision, fragment} }
        │
        ▼
   hub.publish(scope, frame) ──► 各连接队列 ──► SSE ──► onDelta
```

图 G10 写入者循环：

```text
recv ──► 取批（try_recv 直到空，≤256）
          ├─ 开事务
          ├─ 逐条写：块行 / 消息行 / 日志行
          ├─ 提交                        ← 失败整批回滚
          ├─ 合并内存（本地键 → id）
          ├─ 广播（按 pos 升序，同步调用监听者）
          └─ 回执（只给 block_open）
```

## 旧设计的三层映射

表 T1 旧表结构：

| 层 | 语义 | 表 | 独有属性 |
| --- | --- | --- | --- |
| `turn` | 一次用户输入引发的全部产出 | `turn(id, instance_id, topic_id, from_id, is_boundary, created_at)` | `from_id`、`is_boundary` |
| `message` | 一次模型 API 往返 | `message(id, turn_id, seq, role, model_id, finish_reason, tokens, created_at)` | 用量、结束原因、模型 |
| `block` | 内容单元 | `block(id, message_id, seq, kind, call_id, revision, content, data, finished)` | `kind`、`call_id`、`revision` |

表 T2 三个 id 的来源（旧设计）：

| id | 分配者 | 何时分配 | 线上形态 |
| --- | --- | --- | --- |
| `turn.id` | 表自增 | 插入时 | 数字 |
| `message.id` | 表自增 | 插入时 | 数字 |
| `block.id` | **内存计数器**，启动时用 `MAX(id)` 播种 | 解析出块时，早于落库 | 字符串 |

旧设计的七条隐含约定：

1. `block.role` 不落库，读回时取自所属 `message.role`。
2. 落库的块 `finished` 恒为 `TRUE`，该列对行没有区分力。
3. `is_boundary` 全仓只写 `false`，是死字段。
4. `turn.from_id` 只在有用户输入的路径上设置，且与建 turn 不在同一事务。
5. `block.id` 与旧 `EventSeq` 同类：发布早于落库。
6. 块身份规则不统一（text 按 `(message, kind)`、tool_call 按 call_id、媒体每次新开、tool_result 每结果一块），只存在于累加器代码里。
7. `block.seq` 是提交顺序，`block.id` 是开块顺序，两者可能分叉。

## 关键表格

表 T3 旧 `seq` 的用途（决定了能否去掉）：

| 用途 | 位置 | 是否真需要跨作用域全序 |
| --- | --- | --- |
| 身份 | 改写留痕、投递记录、超时通知 | 否，只需唯一 |
| 定序 | 合批排序、前端排序 | 否，同一 scope 内有序即可 |
| 起因链 | `cause` | 否（后决定整字段删除） |
| 游标 | 日志 `since_seq` / `until_seq` | 否——每个读者只读一个 scope |

表 T4 两个 subscribe：

| | `SubscriptionIndex` | Hub 的订阅者 |
| --- | --- | --- |
| 谁在订阅 | mod | 浏览器连接 |
| 按什么订阅 | 事件名 | `scope` |
| 用来做什么 | 决定要不要调 mod、按 rank 排序 | 决定这一帧塞进哪些队列 |
| 何时变 | mod 启停 | 连接建立或断开 |

表 T5 两个“发布”：

| | `Dispatcher::notify` | `Hub::publish_committed` |
| --- | --- | --- |
| 输入 | 带类型的 payload | 已造好的帧 |
| 输出 | 交给观察者，并排进 mod 投递队列 | 塞进各连接帧队列 |
| 含义 | 产生一条事件 | 分发一帧 |

表 T6 `decide` 与 `notify`：

| | `decide` | `notify` |
| --- | --- | --- |
| 形态 | intercept | observe |
| 是否等 mod | 等，返回值是改写后的载荷 | 不等，只排任务 |
| 调用方用途 | 拿改写后的参数继续 | 宣告已发生的事 |
| 是否进事务 | 否 | 否 |

表 T7 两个端口：

| 端口 | 谁用 | 能做什么 | 为什么分开 |
| --- | --- | --- | --- |
| `Dispatcher` | llm、mcp、usecase、agent | 拦截、通知 | 只需要目录、订阅索引、mod 宿主 |
| `WriteHandle` | 会话写入（turn/message/block） | 事件，以及“领域行 + 日志行”的原子写 | 块行与日志行必须同事务 |

表 T8 `enum Work`：

| 变体 | 事务里写什么 | 载荷里的 id 从哪来 | 线上载荷 |
| --- | --- | --- | --- |
| `Event(Pending)` | 只有 event_log | 提交前已有 | 就是提交的那份 |
| `BlockOpen` | block INSERT + event_log | INSERT 返回 | 写入者用刚写的列拼 |
| `BlockCommit` | block UPDATE + event_log | 已有 | 同上 |
| `MessageSeal` | message UPDATE + event_log | 已有 | 同上 |

表 T9 三层并发性：

| 层 | 并发吗 | 说明 |
| --- | --- | --- |
| `submit` 入队 | 并发 | 多任务随时 `try_send` |
| 写入者处理 | 串行 | 一张日志表、一个 id 空间、一个广播入口 |
| 广播进队列 | 串行但只是 push | 真正的 SSE 写在各自任务里 |
| mod 投递 | 并行 | 独立任务、合批、超时 |
| 分片 | 不经写入者 | 内存直推 |

表 T10 分片与提交帧：

| | `core/block.delta` | `core/block.committed` |
| --- | --- | --- |
| 频率 | 每片一条 | 每块一条 |
| 落库 | 否 | 是 |
| `pos` | 无 | 有 |
| 载荷 | `fragment` | 完整内容、`data`、`revision` |
| 客户端动作 | 追加 | 整体覆盖 |
| 丢失后果 | 暂时少字 | 数据缺失 |

## 问题与结论

**问题：`EventSeq` 设计为全局递增，不接受，能否重新设计？**
结论：`seq` 承担四件事（身份、定序、起因链、游标），几乎没有一处真的需要跨作用域全序。可去。

**问题：前端序号到 1000，内存还没 flush 仍在 10。**
结论：根因不是全局递增，而是“发布早于落库 + 重启从库里续号”。三种解法：提交后发布、双序号、前端只读已提交日志。选“提交后发布 + 日志即流”。

**问题：旧设计里 Block / Turn / Message 如何映射？**
结论：见表 T1、T2 与七条隐含约定。关键点是块 id 由内存发号、`block.role` 不落库、`finished` 恒真。

**问题：不允许内存发号，靠数据库自增是否够？**
结论：够，且必须：两个进程各自 `MAX(id)+1` 会撞号；单进程也会在重启后重号。

**问题：只有一个运行时，多个浏览器。**
结论：跨实例理由消失，但“崩溃重放”和“单一身份权威”仍成立，方案不变。

**问题：为什么否决“开块即插行”？**
结论：那次否决站不住。否决理由是“快照会拿到空内容”，但快照本来可以“SQL 定集合 + 内存定内容”。开块即插行换来行与事件同事务、标识由库给、`finished` 列复活。

**问题：旧实现的快照获取方式能否参考？**
结论：能。同锁取“快照 + 注册 + 边界”这一条必须照搬；`inflight` 簿记在新设计里删掉，改由 `finished = FALSE` 加表连接得出。

**问题：`Envelope` 和 `LogRecord` 有重复。**
结论：字段清单原有四份副本（信封、`LogRecord`、`LogRecord::envelope()`、`from_envelope`）。改成 `LogRecord` 组合信封，观察者直接收结构体。

**问题：块本身进入 `event_log`，是否存了两份？**
结论：只有 `block.committed` 重复。日志改存 subject 引用，读取时按 id 取回值。

**问题：`payload` 里是谁规定有 `block_id`？**
结论：原来没有任何人规定，靠约定，是设计缺口。改为事件定义声明 `subject: fn(&P) -> Subject`，日志存 `{"subject":{"kind":"block","id":512}}`，还原函数按事件名分派。

**问题：`cause` 是什么，要不要留？**
结论：旧 `cause` 是父事件序号，只服务审计；深度由上下文计数，不依赖它。已从信封删除。

**问题：去掉 `EventSeq` 后 `wind-event` 怎么改？**
结论：删 `EventSeq`、`next_seq`、`emit_seq`、`current_seq`、`seed_seq`、`seq_only`；预算检查由 `Guards::admit` 承担；`Dispatcher` 注入 `EventOutlet`；观察者搬出 `Dispatcher`，改由写入者调用。

**问题：`LogRecord` 与 `Frame::Event` 的 `pos` 是什么？**
结论：都是 `event_log.id`，同一个数。`LogRecord.pos` 用于区间查询与边界，`Frame::Event.pos` 用于客户端排序、去重、续传。分片没有它。

**问题：`Envelope` 是什么，为什么不加 id？**
结论：信封是事件自身（时间、名字、作用域、来源、载荷）。`pos` 是记录的位置，不是事件的属性；加进去会让“分配”早于“落库”，并让不落日志的事件多出一个可空的 id 字段。

**问题：`wind-event` 和 `Hub` 是什么关系，`notify` 做什么？**
结论：两个轴。`wind-event` 决定给谁、谁能改；Hub 决定怎么送到浏览器。`notify` 是“宣告已发生”，`decide` 是“征求决定”。

**问题：`Dispatcher::observe` 和 `observers` 是什么？**
结论：最终实现里两者都不存在。没有 `observers` 字段，也没有 `observe()` 方法。观察者改由写入者调用，`Dispatcher` 自己实现 `Observer`，在 `on_committed` 里查索引、把任务排进 `ObservePool`（字段名 `mod_ob`）。mod 投递池本身还在，只是不再由 `notify` 直接入队。

**问题：写入者是什么？走写入者是什么意思？**
结论：一个任务 + 一个队列 + 对 `event_log` 的独占写权限。走写入者 = 想进日志或进前端，只能 `submit` 给它，不能自己 `INSERT event_log`、也不能自己推帧。

**问题：`EventOutlet` 具体做什么？谁接上？**
结论：事件离开 `wind-event` 的唯一出口，实现只有一句入队。组合根建一次，克隆两份：一份给 `Dispatcher`（当 `dyn EventOutlet`），一份给会话写入方。

**问题：`chain_of` 放哪？**
结论：留在 `wind-event`。它只读订阅索引与启用集合。

**问题：`decide` 之后调用方写库，`decide` 要进事务吗？**
结论：不进。`decide` 只调 mod 与入队；业务行由调用方在自己的事务里写；日志行由写入者在另一事务里写。

**问题：`WriteHandle` 是什么，在哪个环节？**
结论：队列的发送端 + 四个方法（`submit`、`block_open`、`block_commit`、`message_seal`）。位于产生方与落库之间，自己不开事务。

**问题：`tx` 的接收端全貌？**
结论：`Writer` 持接收端、storage、累加器、监听者；一个循环，队列排空即一批，一批一个事务，提交后“合并内存 → 广播 → 回执”。监听者是同步 `fn`，不得阻塞。

**问题：`WriteQueue::open` 是什么？**
结论：不是类型，是建通道的构造函数，返回 `(WriteHandle, Writer)`，不写库、不 spawn（监听者必须先挂上）。

**问题：分片怎么处理？**
结论：不落库、无 `pos`；累加器决定落到哪个块并递增 `revision`，`BlockWriter` 直接造帧推给 `Hub`。

**问题：不改并行的话，所有订阅是否收同一份？以后改并行改动大吗？**
结论：按话题算，所有连接收同一份帧；mod 只收 payload。改并行要动 `event_log`（加话题内序号）、游标、写入者路由，接口不变，属于中等改动。

**问题：`compose_pending` 为什么把事务切成两段，新设计还会吗？**
结论：旧代码切两段是因为 `absorb` 需要 `message_id` 且又要发事件。新设计里 `absorb` 拆成 plan 与 apply，事件与领域行同事务，可以合成一段。旧代码在 `pending_delete` 不匹配时还会在内存与前端留下永远不提交的块。

## 被推翻的方案

| 方案 | 为什么放弃 |
| --- | --- |
| 进程内单调序号 + 启动播种 | 重启后重号；客户端无法区分新旧号 |
| 每个作用域一个内存计数器 | 仍然是进程内发号，重启重号；崩溃窗口仍在 |
| `block_seq` 侧表取号、提交时才插行 | 多一套标识来源；烧号无行；不如行本身就是标识 |
| 实时与回补各写一套载荷组装 | 两份逻辑会漂移；改成共用一个函数 |
| 流式期间 checkpoint 到 block 行 | 一致性不需要它（快照用内存覆盖）；只对崩溃抢救有价值，暂不做 |
| `inflight` 簿记做快照话题过滤 | 需要与累加器保持同步；改 SQL 定集合，`finished` 列剪枝 |
| 让 `Pending` 携带领域写操作 | `wind-event` 不能认识 storage；改为 `feed` 里的 `enum Work` |
| 一个 `Pending` 覆盖所有事件 | 块事件的载荷在 INSERT 之后才有，无法在提交前构造 |

## 未决项

- `EventDef` 是否现在就加 `subject` 声明，还是先由 `feed` 的 `match` 承担。
- SQLite 的 `synchronous` 取值（`WAL` + `NORMAL` 断电可能丢最后几批）。
- 事件日志是否仍只随话题删除。
- `core/llm.request` 的请求体是否入日志。
- 是否改成每话题一个写入者（并行化）。
- observe 事件的 mod 投递位置：已定为广播之后，`Dispatcher` 在 `on_committed` 里转发。

## 附：本次会话中与本设计无关的两件事

**pi 的会话树**：`/tree` 是会话内的分支树（`alt+left` / `ctrl+left` 折叠或跳到上一段，回车把 leaf 移到该节点）；fork 出来的独立会话树在 `/resume` 里，默认 Threaded 排序按 `parentSession` 缩进显示，选中父会话回车即回到上级。

**技能目录迁移**：`.claude/skills` 的内容移到 `.agents/skills`，原路径改为软链接 `../.agents/skills`。校验为同一 inode。`.claude` 在 `.gitignore` 里，`.agents` 不在。
