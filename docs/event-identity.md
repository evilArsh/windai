# 事件标识与流式一致性

本文定三件事：编号从哪来、日志存什么、前端怎么读。
当前只有 `wind-event` 实现完成，`wind-storage`、`wind-feed`、`wind-stream` 仍是设计。

## 规则

- 对外可见的编号全部由数据库分配。进程内不发号，也不播种。
- `turn`、`message`、`block` 用表自增主键。块在开块时插入行，随即拿到标识。
- 事件的位置 `pos` 就是 `event_log.id`。先落库，后广播。
- 同一父行内按 `id` 排序。`message.seq` 与 `block.seq` 删除。
- 日志只存对象引用与事件事实，不存对象的值。值在读取时从对象表取回。
- 信封里没有位置，也没有因果字段。事件之间的关联靠载荷里的 id。
- 快照：有哪些块由 SQL 决定，块内容由内存累加器决定。
- 单运行时。多个浏览器连的是同一个进程，帧只从一个地方广播。
- 进程内的排序键（订阅的 `rank`）不受第一条约束，它不出进程。

| 词 | 意思 |
| --- | --- |
| 标识 | 对象的身份，由表分配，不复用 |
| 位置 | 事件在日志里的次序，取日志行主键 |
| 修订号 | 块内容的版本，每追加一段加一 |
| 未完成块 | `finished = false` 的块，内容只在内存里 |

`event_log.id` 唯一、随提交递增、可以有洞（回滚会烧号）。客户端不能假定它连续。

## 类型

### 已实现：`wind-event`

```rust
pub struct Envelope<P> {
    pub time: i64,
    pub name: String,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    pub payload: P,
}

pub struct Pending {
    pub shape: Shape,
    pub log: bool,
    pub envelope: Envelope<serde_json::Value>,
}

pub struct Committed {
    pub pos: i64,
    pub shape: Shape,
    pub envelope: Envelope<serde_json::Value>,
}

pub trait EventOutlet: Send + Sync {
    fn submit(&self, pending: Pending);
}

pub trait Observer: Send + Sync {
    fn on_committed(&self, meta: &EventMeta, committed: &Committed);
}
```

- 信封是事件本身：时间、名字、作用域、来源、载荷。位置不在里面。
- `Pending` 是离开总线的形态，`Committed` 是回到总线的形态，只多一个 `pos`。
- `submit` 同步、不返回；`on_committed` 同步、不得阻塞。
- `DispatchCx` 只有 `origin`、`scope`、`source`、`guard`、`depth`。

### 待建：`wind-feed`

| 类型 | 字段 |
| --- | --- |
| `LogRecord` | `pos`、`shape`、`event: Envelope<Value>` |
| `Frame::Event` | `pos: Option<i64>`、`event: Envelope<Value>`，载荷已换成值 |
| `Frame::Snapshot` | 基线：未完成块、状态类值 |

- `LogRecord` 组合信封，不重复声明字段。
- `shape` 只进日志，不上线。
- 日志行与 `LogRecord` 的互转只写一份，放在 `wind-feed` 的 `log.rs`（存储层不认识 `LogRecord`）。
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
| `payload` | 对象引用与事件事实 | 不写块正文、块载荷、消息用量、模型请求体 |
| `shape` | `intercept` 或 `observe` | 日志自带一份，读时不依赖目录 |
| `time` | 产生时刻 | 只展示，不当游标 |

其余取舍：

- 会出现在客户端或日志里的表用 `AUTOINCREMENT`。裸 rowid 删行后会复用，复用号发出去就指错对象。
- `is_boundary` 不设：旧实现从没写过真值。
- 没有 `(x, seq)` 唯一索引，也没有 `MAX(seq) + 1` 子查询。

## 写入

这一节描述 `wind-storage` 与 `wind-feed` 的写入路径，两者尚未实现。

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
| `core/block.open` | INSERT block + INSERT event_log | `{ subject: { block, id } }` |
| `core/block.committed` | UPDATE block + INSERT event_log | 同上 |
| `core/message.seal` | UPDATE message + INSERT event_log | `{ subject: { message, id } }` |

开块事务长这样：

```text
事务 {
    INSERT INTO block(message_id, kind, call_id, revision, content, finished)
        VALUES (?, ?, ?, 0, '', FALSE) RETURNING id
    INSERT INTO event_log(..., 'core/block.open', { subject: { block, id } }) RETURNING id
}
提交后广播
```

载荷只放引用。`of_message`、`role`、`kind`、`call_id` 都能从行里取回，不复制。
状态转移写在事件名上：`open` 与 `committed` 是两件事，载荷里不用再标。
提交后先广播开块帧，再把 `block.id` 回执给会话。

### 日志载荷：subject

事件定义声明它指向哪个对象，日志里存这个引用，不手拼键名：

```rust
pub enum Subject {
    Block { id: i64 },
    Message { id: i64 },
    Pending { id: i64 },
}

pub struct EventDef<P> {
    // ...
    pub subject: Option<fn(&P) -> Subject>,
}
```

| 事件 | subject | 日志里存什么 | 行怎么还原成载荷 |
| --- | --- | --- | --- |
| `core/block.open` | `Block(512)` | `{"subject":{"kind":"block","id":512}}` | 按事件名分派的函数 |
| `core/block.committed` | `Block(512)` | 同上 | 同上 |
| `core/message.seal` | `Message(88)` | `{"subject":{"kind":"message","id":88}}` | 同上 |
| `core/dispatch.failed` 等 | 无 | 完整载荷 | 不需要 |

没有 subject 的事件，载荷本身就是事实，原样入日志。

还原函数放在 `windai/feed`，因为它同时看得到 `event` 与 `storage`。形状是按事件名的一个 `match`：从行值造出该事件定义的载荷类型。实时与回补调同一个函数，只是一个拿刚写的行，一个拿查回来的行。

当前的 `EventDef` 还没有 `subject` 字段，只有 `name`、`shape`、`log`、`schema`。

### 分片

```text
只改内存，直接推给 Hub：core/block.delta
```

不落库、不进日志、没有 `pos`，也不经过写入者。它带的是值：`block_id`、`revision`、`fragment`。

分片只在拿到开块回执之后才产生。写入者的顺序是“提交 → 广播开块帧 → 回执标识”，因此分片永远排在自己的开块帧之后。

### 其它事件

- `observe`：一个事务插一批，提交后按 `pos` 顺序广播。
- `intercept`：改写链跑完再提交。嵌套深度由 `DispatchCx::depth` 与 `Guards` 数，信封里没有因果字段。
- 不落日志的事件没有位置，也不进历史。
- 通知事件（`core/dispatch.timeout`、`core/dispatch.failed`）同样走出口，只是不占事件预算，避免“预算用尽 → 发通知 → 又检查预算”的自套。

批量提交由一个写入者做：队列排空就提交一批，然后按位置顺序广播。广播只有这一处，订阅者看到的位置因此单调。

## 读取

### 历史

```sql
SELECT ... FROM event_log
WHERE scope_kind = ? AND scope_id = ? AND id > ? AND id <= ?
ORDER BY id LIMIT ?
```

`since_pos` 不含，`until_pos` 含。一页的尾巴就是下一个游标。作用域在 SQL 里过滤。

### 补值

日志里只有引用，帧里要有值。两条路，一个组装函数：

| 路径 | 值从哪来 | 查库 |
| --- | --- | --- |
| 实时 | 写入者刚写进事务的那几列 | 不查 |
| 回补 | `id IN (...)` 批量取 | 每页一次 |

一页日志取回后：

1. 收 `core/block.open`、`core/block.committed` 的块 id，一条 JOIN 取回块行与 `message.role`。
2. 收 `core/message.seal` 的消息 id，一条查询取回 `finish_reason` 与用量。
3. 其余事件直接用日志载荷，它们没有对象行。

三条约束：

- 行和日志在同一个事务写。否则日志会指向不存在的行，或行先于日志可见。
- 历史给的是最终值。块和消息定稿后不再改，所以与当时的实时帧一致。
- 缺行分两种：块开事件缺行是启动清理删掉的残块，跳过；块提交或消息定稿缺行是数据损坏，报错。

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

1. 历史的上界取本话题的 `MAX(id)`，与注册在同一临界区。已提交但还没广播的事件可能既在历史里也在队列里，客户端按 `pos` 去重。
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
- 删掉的块在日志里留下悬空的开块事件。客户端先收到它，再在快照里看不到这个 id，按“快照是权威”丢掉。日志不用清理。
- 已提交块、消息、日志都在。`pos` 跨重启有效，客户端游标不用改。
- 消息可能停在 `finish_reason` 为空。投影会因为消息没有内容而丢掉它。

## 尚未实现

| 东西 | 状态 |
| --- | --- |
| `wind-storage` | 未建。表结构、语句、行模型都在本文里 |
| `wind-feed` | 未建。写入者、Hub、帧、历史、快照 |
| `wind-stream` | 未建。块累加器、块类型 |
| `EventDef.subject` | 未加。日志载荷的引用怎么声明还没定 |
| 写入者与 `Hub` | 未写。`EventOutlet` 与 `Observer` 只有契约 |

`wind-event` 的现状见 `docs/event-flow.md`。

## 待定

- `EventDef` 是否加 `subject` 声明，还是先由 `feed` 的按事件名 `match` 承担。
- SQLite 的 `synchronous`。`WAL` + `NORMAL` 时提交不落盘：进程崩溃不丢，断电可能丢最后几批。
- 事件日志是否仍只随话题删除。
- `core/llm.request` 的请求体要不要入日志。它体量大，又和“日志不存值”冲突。
- 是否改成每话题一个写入者。当前是单写入者。
