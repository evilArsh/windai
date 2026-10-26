# 事件标识与流式一致性

本文定三件事：编号从哪来、日志存什么、前端怎么读。
它替换旧实现的做法：进程内发号、把块内容复制进日志。

## 规则

- 对外可见的编号全部由数据库分配。进程内不发号，也不播种。
- `turn`、`message`、`block` 用表自增主键。块在开块时插入行，随即拿到标识。
- 事件的位置 `pos` 就是 `event_log.id`。先落库，后广播。
- 同一父行内按 `id` 排序。`message.seq` 与 `block.seq` 删除。
- 日志只存键和事件自己的事实，不存对象的值。值在读取时从对象表取回。
- 快照：有哪些块由 SQL 决定，块内容由内存累加器决定。
- 单运行时。多个浏览器连的是同一个进程，帧只从一个地方广播。
- 进程内的排序键（订阅注册次序）不受第一条约束，它不出进程。

| 词 | 意思 |
| --- | --- |
| 标识 | 对象的身份，由表分配，不复用 |
| 位置 | 事件在日志里的次序，取日志行主键 |
| 修订号 | 块内容的版本，每追加一段加一 |
| 未完成块 | `finished = false` 的块，内容只在内存里 |

`event_log.id` 唯一、随提交递增、可以有洞（回滚会烧号）。客户端不能假定它连续。

## 类型

字段只定义一次。

| 层 | 类型 | 字段 |
| --- | --- | --- |
| 事件 | `EventEnvelope<P>` | `time`、`name`、`scope`、`origin`、`source`、`cause`、`payload` |
| 日志 | `LogRecord` | `pos`、`shape`、`event` |
| 帧 | `Frame::Event` | `pos`（可空）、`event`（`payload` 已换成值） |

- 位置和形态不属于事件。位置由日志行给，形态由目录给，所以放在信封外面。
- `LogRecord` 里装一个信封，不再重复声明字段。
- `shape` 只进日志，不上线。
- 日志行与 `LogRecord` 的互转只写一份，放在存储层。
- 观察者直接收 `EventEnvelope<Value>`，不再经过“转 JSON 再解析”。
- 图见 `docs/event-flow.md`。

## 数据库

```sql
CREATE TABLE turn (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id BIGINT  NOT NULL,
    topic_id    BIGINT  NOT NULL,
    from_id     BIGINT,
    created_at  BIGINT
);

CREATE TABLE message (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    turn_id       BIGINT  NOT NULL,
    role          TEXT    NOT NULL,
    model_id      BIGINT,
    finish_reason TEXT,
    input_tokens  BIGINT  NOT NULL DEFAULT 0,
    output_tokens BIGINT  NOT NULL DEFAULT 0,
    created_at    BIGINT
);

CREATE TABLE block (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id BIGINT  NOT NULL,
    kind       TEXT    NOT NULL,
    call_id    TEXT,
    revision   BIGINT  NOT NULL DEFAULT 0,
    content    TEXT    NOT NULL DEFAULT '',
    data       TEXT,
    finished   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE event_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    time       BIGINT  NOT NULL,
    name       TEXT    NOT NULL,
    shape      TEXT    NOT NULL,
    scope_kind TEXT    NOT NULL,
    scope_id   BIGINT  NOT NULL DEFAULT 0,
    origin     TEXT    NOT NULL,
    source     TEXT    NOT NULL,
    cause      BIGINT,
    payload    TEXT    NOT NULL
);

CREATE INDEX idx_turn_instance  ON turn(instance_id, id);
CREATE INDEX idx_turn_topic     ON turn(topic_id, id);
CREATE INDEX idx_message_turn   ON message(turn_id, id);
CREATE INDEX idx_block_message  ON block(message_id, id);
CREATE INDEX idx_block_call     ON block(call_id) WHERE call_id IS NOT NULL;
CREATE INDEX idx_block_open     ON block(message_id) WHERE finished = FALSE;
CREATE INDEX idx_event_scope    ON event_log(scope_kind, scope_id, id);
```

几列的含义：

| 列 | 放什么 | 限制 |
| --- | --- | --- |
| `payload` | 键与事件事实 | 不写块正文、块载荷、消息用量、模型请求体 |
| `shape` | `intercept` 或 `observe` | 日志自带一份，读时不依赖目录 |
| `cause` | 父事件位置 | 可空，只给审计看 |
| `time` | 产生时刻 | 只展示，不当游标 |

其余取舍：

- 会出现在客户端或日志里的表用 `AUTOINCREMENT`。裸 rowid 删行后会复用，复用号发出去就指错对象。
- `is_boundary` 删除：旧实现从没写过真值。
- 没有 `(x, seq)` 唯一索引，也没有 `MAX(seq) + 1` 子查询。

## 写入

### 建轮、建消息

```sql
INSERT INTO turn(...) RETURNING id
INSERT INTO message(turn_id, role, model_id, ...) RETURNING id
```

不产生日志行。消息在请求之前建好（`model_id` 已知，用量为 0），块一到就有归属。
用户消息与工具结果消息也是先建行、再挂块。

### 三个事务

| 事件 | 事务内容 | 日志载荷 |
| --- | --- | --- |
| `core/block.open` | INSERT block + INSERT event_log | `{ block_id }` |
| `core/block.committed` | UPDATE block + INSERT event_log | `{ block_id }` |
| `core/message.seal` | UPDATE message + INSERT event_log | `{ message_id }` |

开块事务长这样：

```text
事务 {
    INSERT INTO block(message_id, kind, call_id, revision, content, finished)
        VALUES (?, ?, ?, 0, '', FALSE) RETURNING id
    INSERT INTO event_log(..., 'core/block.open', { block_id }) RETURNING id
}
提交后广播
```

载荷只放键。`of_message`、`role`、`kind`、`call_id` 都能从行里取回，不复制。
状态转移写在事件名上：`open` 与 `committed` 是两件事，载荷里不用再标。

### 分片

```text
只改内存，只广播：core/block.delta
```

不落库、不进日志、没有 `pos`。它带的是值：`block_id`、`revision`、`fragment`。

### 其它事件

- `observe`：一个事务插一批，拿到位置后广播。
- `intercept`：改写链跑完再落库、广播。链里的嵌套事件没有 `cause`；深度由派发上下文数，不看 `cause`。
- 不落日志的事件没有位置，也不进历史。

批量提交由一个写入者做：队列排空就提交一批，然后按位置顺序广播。广播只有这一处，订阅者看到的位置因此单调。每条事件等一次提交，不等一次往返。

日志不再是观察者。它在派发路径里，观察者收的是广播出来的已提交事件。

## 读取

### 历史

```sql
SELECT ... FROM event_log
WHERE scope_kind = ? AND scope_id = ? AND id > ? AND id <= ?
ORDER BY id LIMIT ?
```

`since_pos` 不含，`until_pos` 含。一页的尾巴就是下一个游标。作用域在 SQL 里过滤。

### 补值

日志里只有键，帧里要有值。两条路，一个组装函数：

| 路径 | 值从哪来 | 查库 |
| --- | --- | --- |
| 实时 | 写入者刚写进事务的那几列 | 不查 |
| 回补 | `id IN (...)` 批量取 | 每页一次 |

一页日志取回后：

1. 收 `core/block.open`、`core/block.committed` 的 `block_id`，一条 JOIN 取回块行与 `message.role`。
2. 收 `core/message.seal` 的 `message_id`，一条查询取回 `finish_reason` 与用量。
3. 其余事件直接用日志载荷，它们没有对象行。

三条约束：

- 行和日志在同一个事务写。否则日志会指向不存在的行，或行先于日志可见。
- 历史给的是最终值。块和消息定稿后不再改，所以与当时的实时帧一致。
- 缺行分两种：`block.open` 缺行是启动清理删掉的残块，跳过；`block.committed`、`message.seal` 缺行是数据损坏，报错。

### 快照

```sql
SELECT b.id, b.message_id, m.role, b.kind, b.call_id, b.revision
FROM block b
JOIN message m ON m.id = b.message_id
JOIN turn t    ON t.id = m.turn_id
WHERE b.finished = FALSE AND t.topic_id = ?
```

每一行再用内存累加器里的同 id 块覆盖 `content` 与 `revision`。库里不存流式中间内容，所以没有 checkpoint。
`role` 在 `message` 上，不在 `block` 上。

### 投影

已提交块按 `message_id, id` 升序读出，按 `kind` 拼成模型消息。`finished = FALSE` 的不读。投影与前端流用同一份块行。

## 发给浏览器

| 帧 | 内容 | 带 `pos` |
| --- | --- | --- |
| `Event` | 信封与线上载荷 | 落日志的带，不落的不带 |
| `Snapshot` | 未完成块、状态类基线 | 不带 |

握手与回补的流程见 `docs/event-flow.md`。规则三条：

1. 历史的上界是注册那一刻的 `boundary`，注册之后的事件不会混进历史。
2. 快照与注册在同一个临界区，不漏块也不重复。
3. 实时帧与历史帧形状一样，客户端不用写两套解析。

客户端：

- 带 `pos` 的帧：记住最大的 `pos`，小于等于它的丢掉。不带 `pos` 的按到达顺序用，不进游标。
- 开块帧：建 `blocks[block_id]`，记 `of_message`、`role`、`kind`、`call_id`。
- 分片帧：`revision` 比本地大才追加。
- 提交帧：整体覆盖 `content`、`data`、`revision`，置 `finished`。
- 快照帧：未完成块的权威。本地 `finished` 为假、又不在快照里的，全丢。
- 重连：拿最后的 `pos` 当 `since_pos`，未完成块靠快照重建。
- 编号不复用，所以不用为“号被重发”写规则。

## 崩溃

- 流式中进程死掉：未完成块的内容随内存没了，库里只剩空行。
- 启动时 `DELETE FROM block WHERE finished = FALSE`。单运行时，这些必然是上一个进程留下的。
- 删掉的块在日志里留下悬空的 `core/block.open`。客户端先收到它，再在快照里看不到这个 id，按“快照是权威”丢掉。日志不用清理。
- 已提交块、消息、日志都在。`pos` 跨重启有效，客户端游标不用改。
- 消息可能停在 `finish_reason` 为空。投影会因为消息没有内容而丢掉它。

## 代码改动

`windai/stream` 是新建的：从 `windai-bak/stream` 移植，按下面 stream 那几条改。

### `windai/event`

- 删 `EventSeq`；`EventEnvelope` 去掉 `seq`，`cause` 改 `Option<i64>`；`DispatchCx::nested` 收 `Option<i64>`。
- 删 `Dispatcher` 的 `seq`、`next_seq`、`emit_seq`、`current_seq`、`seed_seq`、`seq_only`。
- 注入写入者与广播器；`decide`、`notify` 改“先写后发”。
- 预算检查独立成 `reserve`，只查预算不发号。
- `Job.seq`、`DeliveryRecord.event_seq`、`DispatchTimeout.event_seq`、`DispatchFailed.event_seq` 改成位置字段（`i64` / `Option<i64>`）。
- 合批按到达顺序，不排序。
- `registration_seq` 改名 `registration_rank`。
- `BuiltinObserver::on_event` 收 `&EventEnvelope<Value>`，不再收 JSON。
- 日志不再挂成观察者。
- `mod/src/feed.rs` 自建信封再 `hub.publish` 的旁路收进写入者。

### `windai/storage`

- schema 按本文重写。
- 加 `block_open`、`block_commit`、`block_snapshot`、`block_values`、`message_values`、`event_log_append`、`event_log_range`、`event_log_boundary`。
- 加日志模块：`LogRecord` 与日志行互转、可见性判断、补值。
- 存储层只提供语句，事务由调用方组。
- 删 `block_id_seed`、`event_log_max_seq`、`block_insert`、以及 `message`、`block` 插入里的 `MAX(seq) + 1`。

### `windai/stream`

- `BlockId` 改 `i64`，线上仍是字符串。
- 删 `next_id`、`after`、`allocate_id`。
- 开块变化只带本地键与描述；写入者插行后绑定标识，之后一律用标识。
- 累加器绑定后按标识索引，快照覆盖因此总能命中。

### `windai/feed`

- `Frame::Event` 加 `pos`。
- `subscribe` 在一个临界区里取 `boundary`、取快照、注册。
- 历史按「补值」组装。
- 删 `pending`、`flush`、`take_pending`、`restore_pending`、`high_water`、`seed_high_water`、`inflight`。

### 会话写入

- `absorb`：开块变化先走开块事务，绑定标识，再广播；增长变化只广播。
- `commit`：提交块事务，然后移出累加器。
- `seal`：定稿事务。

## 旧代码取舍

| 位置 | 处理 |
| --- | --- |
| `event/def.rs`、`subscription.rs`、`guard.rs`、`host.rs` | 保留。目录、订阅索引、相位排序、限流不变。 |
| `event/dispatch.rs` | 改。删取号与播种，加“先写后发”。 |
| `event/pool.rs` | 保留合批与超时；`seq` 改 `pos`。 |
| `stream/accumulator.rs` | 保留开块、续写、定稿与 `current`/`calls` 映射；删内存发号。 |
| `stream/block.rs`、`payload.rs` | 保留字段与载荷形状；`Block.id` 改用数据库标识。 |
| `conversation/assembler.rs` | 保留投影、`normalize`、`repair_tools`、相邻同类合并。 |
| `conversation/writer.rs` | 保留 `absorb`/`commit`/`seal` 的分界；写入改成事务内同时写日志。 |
| `feed/queue.rs` | 保留帧队列与溢出即断开。 |
| `feed/frame.rs` | 改。`Event` 加 `pos`。 |
| `feed/log.rs` | 删除。`LogRecord` 移进存储层，改成组合信封；`from_envelope`、`envelope()` 一并删除。 |
| `feed/hub.rs` | 重写。删攒批、水位、inflight 剪枝。 |
| `storage/*` | 保留 CRUD 与 `FromRow`；删序号列与子查询。 |
| `llm/turn.rs` | 保留轮次编排；`write_task`、`fold` 改用开块/提交接口。 |
| `runtime/assembly.rs`、`shutdown.rs` | 删播种调用与收尾 flush。 |
| `schema.rs` 的 `is_boundary` | 删除。 |

## 实施顺序

1. `windai/storage`：新 schema，开块、提交、日志、快照、历史、补值的语句与测试。
2. `windai/stream`：从 `windai-bak` 移植，去掉发号，改外部绑定标识。
3. `windai/event`：信封、上下文、派发改位置模型。
4. 写入者：单写者、批量提交、返回位置、按位置广播。
5. 帧与订阅：帧形状、同锁握手、历史与快照。
6. 会话写入：开块、提交、定稿三个事务。
7. 文档：重写事件目录与协议文档里涉及序号、块标识、游标、载荷的章节。
8. 测试：重启后游标有效、两个浏览器一致、握手中途不断流、崩溃清理、补值与实时帧同形。

## 待定

- `cause` 留不留。它只给审计看，不要因果链就整列删掉。
- SQLite 的 `synchronous`。`WAL` + `NORMAL` 时提交不落盘：进程崩溃不丢，断电可能丢最后几批。
- 事件日志是否仍只随话题删除。
- `core/llm.request` 的请求体要不要入日志。它体量大，又和“日志不存值”冲突，得单独定。
