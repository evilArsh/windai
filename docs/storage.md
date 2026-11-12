# 存储

`wind-storage` 是 SQLite 的访问层。它提供建表、事务封装、行模型与按领域划分的语句集；
不解释事件语义，也不决定领域状态如何变化。

本文给出连接参数、完整 schema、事务用法、各表的写入路径与行模型映射。领域表指
`turn`、`message`、`block`、`agent_*` 等业务表，不含 `event_log`。事件如何写入
`event_log` 见「事件」；对话轮次如何组合这些写入见「LLM 回合」。

## 连接与事务

`Storage::open(path)` 打开连接池并执行建表。连接参数：

| 参数 | 值 |
| --- | --- |
| `journal_mode` | `WAL`（write-ahead log） |
| `busy_timeout` | 8 秒 |
| `foreign_keys` | 开启 |
| 连接池上限 | 5 |

`synchronous` 没有显式设置，使用 SQLite 对该 journal 模式的默认值。持久性若需要改变，
应在 `wind-storage::db::open_pool` 统一设置。

事务 API：

```rust
impl Storage {
    /// 写事务，BEGIN IMMEDIATE
    pub async fn with_tx<F, Fut, T>(&self, f: F) -> Result<T>;

    /// 只读事务，BEGIN
    pub async fn with_read_tx<F, Fut, T>(&self, f: F) -> Result<T>;

    /// 开启写事务并返回事务句柄
    pub async fn begin(&self) -> Result<StorageTx>;
}

pub struct StorageTx { /* ... */ }

impl StorageTx {
    pub fn storage(&self) -> Storage;
    pub async fn commit(self) -> Result<()>;
    pub async fn rollback(self) -> Result<()>;
}
```

`with_tx` 与 `with_read_tx` 在已经处于事务中时直接复用当前事务，不再嵌套开启。回调拿到的是
一个共享同一事务的 `Storage` 句柄。`begin` 返回显式句柄，适合跨多个语句集、需要在中间
持有事务的场景；`StorageTx::storage()` 给出同一事务上的 `Storage`。

写事务一律 `BEGIN IMMEDIATE`。读事务使用 `BEGIN`，走 WAL 快照。

语句执行的统一语义：

- `execute`：返回受影响行数；调用方用 `ensure_affected` 把 0 行转成 `NoRowsAffected`。
- `fetch_optional` / `fetch_all`：按 `FromRow` 解码。
- `fetch_one_scalar`：取单值，常用于 `INSERT ... RETURNING id`。
- 事务上下文存在时，所有语句走该事务；否则从连接池取一个连接。

## Schema

```sql
CREATE TABLE IF NOT EXISTS providers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    alias       TEXT,
    description TEXT,
    base_url    TEXT    NOT NULL,
    doc         TEXT,
    active      BOOLEAN NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS models (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id BIGINT  NOT NULL,
    name        TEXT    NOT NULL,
    alias       TEXT,
    adapter     TEXT    NOT NULL,
    modalities  TEXT,
    active      BOOLEAN NOT NULL,
    icon        TEXT,
    endpoint    TEXT,
    config      TEXT,
    frequency   BIGINT,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS credentials (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id BIGINT  NOT NULL,
    key         TEXT    NOT NULL,
    active      BOOLEAN NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS json_rule (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id BIGINT  NOT NULL,
    adapter     TEXT    NOT NULL,
    json_rule   TEXT    NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id   BIGINT,
    label       TEXT    NOT NULL,
    model_id    BIGINT,
    workspace   TEXT,
    icon        TEXT,
    created_at  BIGINT,
    updated_at  BIGINT
);

CREATE TABLE IF NOT EXISTS turn (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id BIGINT  NOT NULL,
    topic_id    BIGINT  NOT NULL,
    from_id     BIGINT,
    created_at  BIGINT
);

CREATE TABLE IF NOT EXISTS message (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    turn_id         BIGINT  NOT NULL,
    role            TEXT    NOT NULL,
    model_id        BIGINT,
    finish_reason   TEXT,
    input_tokens    BIGINT  NOT NULL DEFAULT 0,
    output_tokens   BIGINT  NOT NULL DEFAULT 0,
    created_at      BIGINT
);

CREATE TABLE IF NOT EXISTS block (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      BIGINT  NOT NULL,
    seq             BIGINT  NOT NULL,
    kind            TEXT    NOT NULL,
    role            TEXT    NOT NULL,
    revision        BIGINT  NOT NULL DEFAULT 0,
    content         TEXT,
    format          TEXT,
    item_id         TEXT,
    call_id         TEXT,
    call_name       TEXT,
    call_arguments  TEXT,
    call_res        TEXT
);

CREATE TABLE IF NOT EXISTS agent_instances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id       BIGINT,
    topic_id        BIGINT  NOT NULL,
    role            TEXT    NOT NULL,
    mode            TEXT,
    phase           TEXT    NOT NULL,
    phase_payload   TEXT,
    created_at      BIGINT
);

CREATE TABLE IF NOT EXISTS agent_pending (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  BIGINT  NOT NULL,
    kind         TEXT    NOT NULL,
    payload_json TEXT    NOT NULL,
    source_id    BIGINT,
    created_at   BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS spawn_calls (
    parent_id           BIGINT NOT NULL,
    call_id             TEXT   NOT NULL,
    child_id            BIGINT NOT NULL,
    created_at          BIGINT NOT NULL,
    result_delivered_at BIGINT,
    PRIMARY KEY (parent_id, call_id),
    UNIQUE (child_id)
);

CREATE TABLE IF NOT EXISTS external_waits (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id   BIGINT  NOT NULL,
    message_id    BIGINT  NOT NULL,
    payload_json  TEXT    NOT NULL,
    created_at    BIGINT  NOT NULL,
    answered_at   BIGINT,
    decision_json TEXT
);

CREATE TABLE IF NOT EXISTS event_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch           BIGINT  NOT NULL,
    seq             BIGINT  NOT NULL,
    create_time     BIGINT  NOT NULL,
    name            TEXT    NOT NULL,
    shape           TEXT    NOT NULL,
    route_kind      TEXT    NOT NULL,
    route_name      TEXT,
    route_channel   TEXT,
    topic_id        BIGINT,
    instance_id     BIGINT,
    origin          TEXT    NOT NULL,
    source_kind     TEXT    NOT NULL,
    source_name     TEXT    NOT NULL,
    source_version  BIGINT  NOT NULL DEFAULT 0,
    payload         TEXT,
    UNIQUE (epoch, seq)
);

CREATE TABLE IF NOT EXISTS feed_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    epoch     BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS mods (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT    NOT NULL UNIQUE,
    version             INTEGER NOT NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    manifest            TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS topic_mods (
    topic_id INTEGER NOT NULL,
    mod_id   INTEGER NOT NULL,
    PRIMARY KEY (topic_id, mod_id),
    FOREIGN KEY (topic_id) REFERENCES topics (id) ON DELETE CASCADE,
    FOREIGN KEY (mod_id) REFERENCES mods (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_turn_instance ON turn(instance_id, id);
CREATE INDEX IF NOT EXISTS idx_turn_topic ON turn(topic_id, id);

CREATE INDEX IF NOT EXISTS idx_message_turn ON message(turn_id, id);

CREATE INDEX IF NOT EXISTS idx_credentials_active
    ON credentials(provider_id) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_json_rule_provider_adapter
    ON json_rule(provider_id, adapter);

CREATE UNIQUE INDEX IF NOT EXISTS idx_block_message ON block(message_id, seq);
CREATE INDEX IF NOT EXISTS idx_block_call_id ON block(call_id);
CREATE INDEX IF NOT EXISTS idx_block_tool_result ON block(call_id) WHERE kind = 'tool_result';

CREATE INDEX IF NOT EXISTS idx_agent_instances_topic ON agent_instances(topic_id);
CREATE INDEX IF NOT EXISTS idx_agent_instances_parent ON agent_instances(parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_instances_main
    ON agent_instances(topic_id) WHERE role = 'main';

CREATE INDEX IF NOT EXISTS idx_agent_pending_instance ON agent_pending(instance_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_pending_source
    ON agent_pending(instance_id, kind, source_id) WHERE source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_external_waits_instance ON external_waits(instance_id);
CREATE INDEX IF NOT EXISTS idx_external_waits_open
    ON external_waits(instance_id) WHERE answered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_event_log_topic_key
    ON event_log(topic_id, instance_id, epoch, seq);
CREATE INDEX IF NOT EXISTS idx_event_log_route_key
    ON event_log(route_name, route_channel, epoch, seq);
CREATE INDEX IF NOT EXISTS idx_event_log_global_key
    ON event_log(epoch, seq) WHERE topic_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_topic_mods_mod ON topic_mods(mod_id, topic_id);

INSERT INTO feed_meta (singleton, epoch) VALUES (1, 0)
    ON CONFLICT (singleton) DO NOTHING;
```

建表在一个写事务中执行。`feed_meta` 的单行在初始化时插入，`epoch` 从 0 开始；第一次
`allocate_feed_epoch` 返回 1。

## 表语义

| 表 | 一行表示 | 关键约束 |
| --- | --- | --- |
| `providers` | 一个模型提供商 | `name` 唯一 |
| `models` | 提供商下的一个模型 | 无外键；`adapter` 是 `openai_completion` 或 `openai_response` |
| `credentials` | 一份提供商密钥 | `key` 明文存储 |
| `json_rule` | 某提供商 + 适配器的一组请求改写规则 | 按 `id` 顺序应用 |
| `topics` | 一个话题 | `workspace` 是话题工作区 |
| `turn` | 一次运行产生的消息集合 | `instance_id`、`topic_id`、可选的 `from_id` |
| `message` | 一次模型往返或一条用户/工具消息 | `role`、可选的 `model_id`、结束时写 `finish_reason` 与用量 |
| `block` | 消息内的一个内容单元 | `(message_id, seq)` 唯一 |
| `agent_instances` | agent 树的一个节点 | 每个话题最多一个 `role = 'main'` |
| `agent_pending` | 一条待消费的输入 | `(instance_id, kind, source_id)` 在 `source_id` 非空时唯一 |
| `spawn_calls` | 父实例的一次派生调用与子实例的配对 | 主键 `(parent_id, call_id)`；`child_id` 唯一 |
| `external_waits` | 一次挂起等待 | `answered_at` 与 `decision_json` 同时为空表示未答复 |
| `event_log` | 一条已写入数据库的事件 | `(epoch, seq)` 唯一；`id` 只是行号 |
| `feed_meta` | 单例，记录当前 epoch | `singleton` 必须为 1 |
| `mods` | 一个已安装 mod 的当前版本 | `name` 唯一；只存清单原文，不存编译产物 |
| `topic_mods` | 话题与 mod 的启用关系 | 两个外键均 `ON DELETE CASCADE` |

没有外键的表：`models`、`credentials`、`json_rule`、`turn`、`message`、`block`、
`agent_instances`、`agent_pending`、`spawn_calls`、`external_waits`、`event_log`。
删除话题或 mod 时不会级联清理这些表的数据；`topic_mods` 是唯一带外键的关系表。

## 写入路径

### 话题

```rust
pub async fn create_topic(
    &self,
    parent_id: Option<i64>,
    label: &str,
    model_id: Option<i64>,
    icon: Option<&str>,
    workspace: Option<&str>,
) -> Result<i64>
```

在一个写事务里：

1. 插入 `topics`，`created_at` 为当前秒。
2. 计算 `workspace`：显式值优先；否则 `WIND_ROOT_DIR`（缺省 `~/.windai`）下的
   `topics/<id>`。
3. `UPDATE topics SET workspace = ? WHERE id = ?`。

读取入口：`get_topic_by_id`、`get_topic_by_instance`（通过 `agent_instances.topic_id`）。
切换话题使用的模型：`update_topic_model(topic_id, model_id)`，更新 `topics.model_id`。

### 提供商、模型与规则

| 函数 | 写入 |
| --- | --- |
| `create_provider(name, base_url)` | `providers`，`active = TRUE` |
| `create_model(provider_id, name, adapter, endpoint, config)` | `models`，`active = TRUE`，`frequency = 0`，`config` 序列化为 JSON 文本 |
| `create_credential(provider_id, key)` | `credentials`，`active = TRUE` |
| `create_rule(provider_id, adapter, json_rule)` | `json_rule` |
| `update_provider(id, name, base_url)` | 更新名称、地址与 `updated_at`；行不存在时报错 |
| `update_model(id, name, adapter, endpoint, config)` | 更新名称、适配器、端点、配置与 `updated_at` |
| `update_model_config(id, config)` | 只更新 `config` 与 `updated_at` |
| `replace_credential(provider_id, key)` | 一笔事务：现有活动凭证置 `active = FALSE`，再插入新凭证 |
| `list_rules(provider_id, adapter)` | 按 `id` 升序读取规则 |
| `get_model_config(model_id)` | 读模型、提供商与第一条启用的凭证 |

`get_model_config` 的凭证子查询：

```sql
LEFT JOIN credentials c ON c.id = (
    SELECT id FROM credentials
    WHERE provider_id = p.id AND active = TRUE
    ORDER BY id LIMIT 1
)
```

`Credentials` 的 `Debug`、`Display`、`Serialize` 都把 `key` 按长度替换为星号；明文字段只在
写入数据库与构造请求时使用。

### 轮、消息与块

| 函数 | 写入 |
| --- | --- |
| `create_turn(topic_id, instance_id, from_id)` | 插入 `turn`，返回 id |
| `create_message(turn_id, role, model_id)` | 插入 `message`，返回 id |
| `create_blocks(message_id, blocks)` | 批量插入 `block`，返回与输入顺序一致的 id 列表 |
| `seal_message(message_id, finish_reason, input_tokens, output_tokens)` | 更新 `message` 的结束原因与用量 |

`create_blocks` 使用一条 `INSERT ... VALUES (...), (...)`，列由 `BlockColumns::of` 从
`Body` 展开，末尾 `RETURNING seq, id`。返回结果按 `seq` 升序排序，因此与输入块的顺序一致。
`(message_id, seq)` 的唯一索引保证同一消息内不重复。

块行没有 `finished` 字段，也没有流式中间值。写入的块都是定稿块：

- 用户消息与工具结果消息：建消息后立即写块。
- 助手消息：流式结束后一次写入全部块，并在同一事务里定稿消息。

`list_instance_context(instance_id)` 读取该实例的全部历史块：

```sql
SELECT m.input_tokens, m.output_tokens, m.created_at,
       b.id, b.message_id, b.seq, b.kind, b.role, b.revision,
       b.content, b.format, b.item_id, b.call_id, b.call_name,
       b.call_arguments, b.call_res
FROM turn t
JOIN message m ON m.turn_id = t.id
JOIN block b ON b.message_id = m.id
WHERE t.instance_id = ?
ORDER BY m.id, b.seq
```

### 待消费输入

| 函数 | 语义 |
| --- | --- |
| `create_pending(instance_id, kind, payload, source_id)` | 插入 `agent_pending`，`payload` 是 `Vec<Content>` 的 JSON |
| `get_pending_inputs_by_ids(ids)` | 按 `id` 升序返回 |
| `delete_pending_by_ids(ids)` | 按 id 删除 |

`PendingKind` 取 `user` 或 `background_result`。`source_id` 在后台结果时指向子实例。
当前核心只用 `user`；`background_result` 没有生产者。

### 工具调用配对

`tool_calls_without_results(instance_id)` 找出该实例中还没有对应结果的全部工具调用：

```sql
SELECT t.id AS turn_id, m.id AS message_id, b.call_id, b.call_name, b.call_arguments
FROM block b
JOIN message m ON m.id = b.message_id
JOIN turn t ON t.id = m.turn_id
WHERE t.instance_id = ?
  AND b.kind = 'tool_call'
  AND b.call_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM block r
      WHERE r.kind = 'tool_result' AND r.call_id = b.call_id
  )
ORDER BY b.id
```

配对只按 `call_id`，不限定消息或实例。`UnpairedToolCall` 把 `call_name` 与
`call_arguments`（解析为 JSON，失败时为 `null`）组合成
`data = { "name": call_name, "arguments": arguments }`。

### 子实例与派生配对

| 函数 | 语义 |
| --- | --- |
| `create_spawn_call_map(parent_id, call_id, child_id)` | 写一行 `spawn_calls` |
| `list_child_states(parent_id, call_ids)` | 读子实例的 `mode`、`phase`、`phase_payload` 与回复文本 |

`list_child_states` 通过 `spawn_calls` 连接 `agent_instances`，再用
`json_extract(i.phase_payload, '$.message_id')` 找到子实例当前消息的 `text` 块：

```sql
SELECT s.call_id, i.id AS child_id, i.mode, i.phase, i.phase_payload,
       b.content AS reply_part
FROM spawn_calls s
JOIN agent_instances i ON i.id = s.child_id
LEFT JOIN block b
    ON b.message_id = CASE
        WHEN json_valid(i.phase_payload)
        THEN json_extract(i.phase_payload, '$.message_id')
        ELSE NULL
    END
    AND b.kind = 'text'
WHERE s.parent_id = ? AND s.call_id IN (...)
ORDER BY s.call_id, b.seq
```

`spawn_calls.result_delivered_at` 当前没有读写点。

### Agent 实例

| 函数 | 语义 |
| --- | --- |
| `create_agent_instance(topic_id, parent_id, role, mode, phase, phase_payload)` | 插入节点，`created_at` 为当前秒 |
| `get_instance_by_id(instance_id)` | 读取节点 |
| `update_instance_phase(instance_id, phase, phase_payload)` | 更新相位；0 行返回 `NoRowsAffected` |

`role` 取 `main` 或 `child`，`mode` 取 `sync`、`background` 或空。相位列由
`Phase::columns` 生成：`phase` 是判别式，`phase_payload` 是 JSON 文本。

### 外部等待

| 函数 | 语义 |
| --- | --- |
| `create_external_wait(instance_id, message_id, payload)` | 插入等待，`payload` 序列化为 JSON |
| `get_external_wait(id)` | 读取等待 |
| `answer_external_wait(id, decisions)` | 只在 `answered_at` 与 `decision_json` 都为空时写入；返回是否由本次调用写入 |

`answer_external_wait` 的更新语句带 `WHERE id = ? AND answered_at IS NULL AND
decision_json IS NULL RETURNING id`，因此重复答复不会覆盖第一次的结果。

### Mod

| 函数 | 语义 |
| --- | --- |
| `install_mod(name, manifest)` | 插入 `mods`，版本固定为 1；清单中的 `name` 与 `version` 必须匹配 |
| `update_mod(mod_id, expected_version, version, manifest)` | 校验名称、版本单调与期望版本后更新 |
| `mod_by_name(name)` / `list_mods()` | 读取单个或全部 mod，按名称升序 |
| `enabled_mods(topic_id)` | 话题启用的 mod，按名称升序 |
| `all_enabled_mods()` | 全部 `(topic_id, mod_name)`，按话题与名称升序 |
| `topics_with_mod(mod_id)` | 启用该 mod 的话题，按 id 升序 |
| `enable_mod(topic_id, mod_id)` / `disable_mod(...)` | 写 `topic_mods`；返回关系是否变化；先校验话题与 mod 都存在 |
| `delete_mod(mod_id)` | 删除 mod；`topic_mods` 级联删除；返回受影响话题 |

`install_mod` 与 `update_mod` 的版本规则：

- 首次安装只接受版本 1。
- 更新必须给出大于当前版本的版本，且清单中的版本等于参数版本。
- 更新使用 `WHERE id = ? AND version = ?`，受影响行数不为 1 时返回 `Conflict`。

### 事件日志

`allocate_feed_epoch`：

```sql
UPDATE feed_meta SET epoch = epoch + 1 WHERE singleton = 1 RETURNING epoch
```

`append_feed_batch` 在写事务中逐条插入 `event_log`，详见「事件」的「写入 `event_log`」。
该函数不读取、不查询日志。

## 行模型

### 块与列

`Body` 的每个变体映射到固定的列组合。空字符串在写入时变为 `NULL`，读取时再还原为空串。

| `Body` | `content` | `format` | `item_id` | `call_id` | `call_name` | `call_arguments` | `call_res` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Reasoning` | 文本 | — | 可选 | — | — | — | — |
| `Text` | 文本 | — | — | — | — | — | — |
| `Image` | 数据 | — | — | — | — | — | — |
| `File` | 数据 | — | — | — | — | — | — |
| `Audio` | 数据 | 格式 | 可选 | — | — | — | — |
| `ToolCall` | — | — | 可选 | 调用 id | 工具名 | 参数 JSON 文本 | — |
| `ToolResult` | — | — | — | 调用 id | — | — | 结果 JSON |

`kind` 列存 `Body` 的 `AsRefStr` 值：`reasoning`、`text`、`image`、`file`、`audio`、
`tool_call`、`tool_result`。`role` 列存 `Role` 的小写名。

`Block` 行模型：

```rust
pub struct Block {
    pub id: i64,
    pub message_id: i64,
    pub role: Role,
    pub seq: i64,
    pub revision: i64,
    pub body: Body,
}
```

`From<Block> for wind_wire::Block` 丢弃 `id` 与 `message_id`，只保留 `role`、`seq`、
`revision` 与 `body`。

### 其他行模型

| 类型 | 对应表 | 说明 |
| --- | --- | --- |
| `Topic` | `topics` | `workspace` 在创建后不再由库修改 |
| `Provider` | `providers` | |
| `Credentials` | `credentials` | 明文只在字段里；打印与序列化时打星号 |
| `Model` | `models` | `adapter` 解析为 `AdapterType`；`modalities` 与 `config` 解析为 JSON |
| `Rule` | `json_rule` | `adapter` 解析为 `AdapterType` |
| `Turn` | `turn` | |
| `Block` | `block` | 见上表 |
| `ContextRow` | `turn ⋈ message ⋈ block` | 投影上下文的一行，含消息用量与块 |
| `ChildStateRow` | `spawn_calls ⋈ agent_instances ⋈ block` | 子实例状态与回复文本片段 |
| `ExternalWait` | `external_waits` | `payload` 与 `decision` 解析为 JSON |
| `AgentInstance` | `agent_instances` | `role`、`mode` 解析为枚举 |
| `AgentPending` | `agent_pending` | `payload` 解析为 `Vec<Content>` |
| `Mod` | `mods` | `version` 转为 `u32` |
| `LogRecord` | `event_log` | 组合 `Scope`、`Source`、`Shape`、`Origin`；当前无查询入口 |

`LogRecord` 的解析规则：

- `topic_id` 与 `instance_id` 同时为空 → `Scope::Global`；同时有值 → `Scope::Topic`；
  只有一边 → `MalformedColumn`。
- `source_kind = "core"` → `Source::Core`，版本为 0；`source_kind = "mod"` → `Source::Mod`，
  版本从 `source_version` 转为 `u32`；其他值 → `MalformedColumn`。
- `shape`、`origin` 用 `FromStr` 解析；失败转成 `ColumnDecode`。

### 错误

```rust
pub enum StorageError {
    Database(sqlx::Error),
    Internal(String),
    NoRowsAffected,
    TooManyRows { found: usize },
    MalformedColumn { column: String, detail: String },
    Parse(strum::ParseError),
    NotFound { entity: String, key: String },
    Conflict { entity: String, key: String, detail: String },
    Bus(String),
    Denied(String),
    IdentityConflict { epoch: i64, seq: i64 },
    Json(serde_json::Error),
}
```

`NotFound`、`Conflict`、`IdentityConflict` 由语句集显式构造；`ensure_affected` 把 0 行转成
`NoRowsAffected`；`ensure_lte_one` 把多行转成 `TooManyRows`。

## 只读查询

宿主用以下只读查询枚举领域行。查询不改写任何表。

| 查询 | 返回 | 用途 |
| --- | --- | --- |
| `list_topics()` | 全部话题，按 `topics.id` 降序 | 话题枚举 |
| `list_providers()` | 全部提供商，按 `providers.id` 升序 | 提供商枚举 |
| `list_models(provider_id)` | `provider_id` 为 `Some` 时返回该提供商的模型，按 `models.id` 升序；为 `None` 时返回全部模型，按 `models.provider_id`、`models.id` 升序 | 模型枚举与按提供商过滤 |
| `ContextRow.message_id` | 上下文行携带所属消息的 `message_id` | 把块按消息归组；同一消息的块共享该值 |

`ContextRow` 由 `list_instance_context(instance_id)` 返回，行序为 `message_id`、`block.seq` 升序。
`message_id` 与 `block` 并列，宿主不需要根据 `seq` 推断消息边界。

## 当前边界

| 项 | 现状 |
| --- | --- |
| 事件读取 | `event_log` 只有写入语句；`LogRecord` 没有查询函数 |
| 领域表外键 | 只有 `topic_mods` 带外键；删除话题或 mod 不清理 turn/message/block/agent_* |
| 后台子实例结果 | `spawn_calls.result_delivered_at`、`PendingKind::BackgroundResult` 没有读写点 |
| Mod 状态 | 没有 `mod_state` 表；`Capability::State` 与 `Capability::Storage` 没有存储实现 |
| 脚本字节码 | 只写文件 `<root>/bytecode/<mod>/<engine_id>/`，不写数据库 |
| `StorageError::Bus` / `Denied` | 在存储语句集中没有构造点 |
| 主实例行 | `create_agent_instance` 可用于任何角色，但库内没有自动创建主实例的入口 |
| `block` 的流式中间态 | 不存在列；流式内容只在内存，定稿时一次写入 |
