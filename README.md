# windai

An AI engine core library providing database-driven multi-turn chat, hierarchical agent orchestration, tool calling with approval flow, multi-provider adapters, and declarative request transformation. Ships an HTTP service (`wind-http`) exposing the core over REST + SSE.

## Quick Start

```bash
cargo build
cargo test                   # Every crate's tests in one run; .env-gated tests are #[ignore]d
cargo test -p wind-http      # HTTP route/facade/DTO tests (no .env needed)
cargo test -p wind-core --test core_chat -- --include-ignored --test-threads=1
```

Copy `.env.example` to `.env` and fill in `TEST_*` values before running the `.env`-gated tests.

```toml
[dependencies]
wind-core = { git = "https://github.com/evilArsh/windai" }
```

## Usage

### Initialization

```rust
use wind_core::WindCore;

// In-memory SQLite (tests / ephemeral)
let core = WindCore::init_memory().await?;

// File-backed SQLite — path comes from the app dirs (`WIND_ROOT_DIR`, default `~/.windai/windai.db`)
let core = WindCore::init_local().await?;

// Custom connection pool (e.g. shared-cache for tests)
let core = WindCore::init_with_pool(pool).await?;

// Custom connection pool + shared MCP registry (test harnesses / advanced embedding)
let core = WindCore::init_with_pool_and_registry(pool, registry).await?;
```

`WindCore` is a process-level runtime root: one instance per process, owns the storage (DB pool) + MCP registry, and manages one `TopicRuntime` actor per topic. Initialization also registers the two builtin MCP servers (`FsServer`, `SkillsServer`) on the registry.

The SQLite schema is created with `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` and there is **no migration mechanism** — after a schema change, delete the local DB file (`~/.windai/windai.db`, or the file under `WIND_ROOT_DIR`) so it is rebuilt.

### Register Provider & Model

```rust
use wind_core::models::{CreateProvider, CreateCredentials, CreateModel, ModelConfig, ModelType};
use wind_ai::model::AdapterType;

let storage = core.storage();

let provider = storage.provider().create(CreateProvider {
    name: "deepseek".into(),
    base_url: "https://api.deepseek.com".into(),
    description: None,
    doc: None,
    alias: None,
}).await?;
let pid = provider.id;

storage.provider().create_credentials(CreateCredentials {
    provider_id: pid,
    key: "sk-xxx".into(),
}).await?;

let model = storage.model().create(CreateModel {
    name: "deepseek-chat".into(),
    provider_id: pid,
    adapter: AdapterType::OpenAICompletion,
    alias: None,
    modalities: Some(vec![ModelType::Chat]),
    active: Some(true),
    icon: None,
    endpoint: None,
    // 请求配置按模型配置（只有 stream 与 reasoning 两个键）
    config: Some(ModelConfig { stream: Some(true), reasoning: None }),
}).await?;
let mid = model.id;
```

### Create a Topic & Start a Chat

Chats run through the **agent system**: a `Topic` is a scope owning a `TopicRuntime`, one *main* `AgentInstance` plus its sibling child instances. Agent 能力通过 `TopicAgentMap` 挂到 topic 上 —— 每条映射把一份 `AgentDefinition` 暴露给该 topic（只表达「拥有该能力」，没有主次之分）。每个 topic 有一个主 `AgentInstance`，它不绑定任何定义，只负责调度：模型通过 `agent_list_agents` / `agent_spawn_agent` 从能力映射里挑。消息记录在实例上（`Message.instance_id`）。The runtime streams progress as `TopicEvent`s over a `broadcast` channel.

```rust
use wind_core::agent::event::TopicEvent;
use wind_core::models::{
    AgentDefinitionData,
    CreateAgentDefinition, CreateTopic, CreateTopicAgentMap,
};
use wind_ai::message::Content;

let storage = core.storage();

// 1. Create a topic (a scope; parent_id is reserved for a tree nobody builds yet)
//    模型与工具审批策略都挂在 topic 上
let topic = storage.topic().create(CreateTopic {
    parent_id: None,
    label: "My Chat".into(),
    icon: None,
    model_id: Some(mid),
    tool_approval_policy: None,   // None 视同 AllowAll
}).await?;
let tid = topic.id;

// 2. Define a reusable agent (what the agent *can* do)
let agent_def = storage.agent().create_definition(CreateAgentDefinition {
    name: "assistant".into(),
    description: "Default assistant".into(),
    owner_topic_id: None,     // None = global definition
    cloned_from_id: None,
    active: Some(true),
    data: AgentDefinitionData::default(),
}).await?;

// 3. 把该能力映射到 topic（主实例由此获得可调度的能力列表）
storage.agent().create_topic_agent_map(CreateTopicAgentMap {
    topic_id: tid,
    agent_id: agent_def.id,
}).await?;

// 4. Submit user input, then consume the event stream
//    首次 create_task 会自动创建该 topic 的主实例（不绑定任何定义）
let handle = core.fetch_topic(tid);
let mut events = handle.subscribe().await?;
handle.create_task(vec![Content::new_text("Hello!".into())]).await?;

while let Ok(event) = events.recv().await {
    match event {
        TopicEvent::Message { data, .. } => {
            for c in &data.content {
                if let Content::Text { data } = c {
                    print!("{data}");
                }
            }
        }
        TopicEvent::MessageFinished { .. } => break,
        TopicEvent::Error { error, .. } => {
            eprintln!("Error: {error}");
            break;
        }
        _ => {}
    }
}
```

**Key points:**
- `create_task` submits `Vec<Content>` (the full `wind_ai::message::Content` protocol) and returns immediately — the runtime accepts it asynchronously. You do **not** hand-build `Message` records; the engine creates the user/assistant messages bound to the main instance, plus tool results, internally.
- A topic has one `TopicRuntime` and one main `AgentInstance` (plus children spawned by `agent_spawn_agent`). There is no per-agent sub-topic: isolation comes from `Message.instance_id`, so read an instance's history with `MessageStorage::list_by_instance`. HTTP 上主实例与子实例同等对待，都用 `GET /api/v1/agent-instances/{instance_id}/messages` 读该实例的对话
- 每次 spawn 都新建实例，不复用空闲实例
- The event stream channel closes once the main instance reaches a terminal state (`Finished` / `Failed` / `Cancelled`) or waits for approval — re-subscribe per conversation.
- `TopicEvent` variants: `Error`, `Snapshot`, `MessageCreated`, `Message` (streaming delta), `MessageFinished`, `TaskStatusChanged`, `ApprovalRequired`。（`Snapshot` 目前没有任何代码产出。）

### MCP Tool Calling

Register MCP servers, then attach them to an agent definition. The engine discovers tools, filters them per agent, sends them with each request, and executes approved calls.

```rust
use wind_core::models::agent::{AgentDefinitionData, AgentMcpBinding};
use wind_core::models::{CreateMcpServer, CreateAgentDefinition};
use wind_mcp::client::TransportType;

let mcp = storage.mcp().create(CreateMcpServer {
    r#type: TransportType::Stdio,
    name: "everything".into(),
    command: Some("npx".into()),
    args: Some(vec![
        "-y".into(),
        "@modelcontextprotocol/server-everything".into(),
    ]),
    url: None,
    description: None,
    env: None,
}).await?;
let sid = mcp.id;

// Give the agent access to that MCP server
storage.agent().create_definition(CreateAgentDefinition {
    name: "tool-user".into(),
    description: "Assistant with MCP tools".into(),
    owner_topic_id: None,
    cloned_from_id: None,
    active: Some(true),
    data: AgentDefinitionData {
        mcp_servers: vec![AgentMcpBinding {
            mcp_server_id: sid,
            alias: None,
            allowed_tools: vec![],
            denied_tools: vec![],
            enabled: true,
        }],
        ..Default::default()
    },
}).await?;
```

Agent definition data also carries `prompt_modules`, `builtin_mcp_servers`, `context_policy`, `permission_policy`, and `runtime_limits` — see the `AgentDefinitionData` type for the full surface。（其中只有 `context_policy.max_context` 目前被真正使用。）

`AgentDefinition.key` 是唯一短标识，**由系统在创建时生成、生成后不可修改**（12 字符，首字符为小写字母）。`create_definition` 不接受调用方指定 key；`clone_definition_for_topic` 会生成全新 key，来源关系记录在 `cloned_from_id`。

### Tool Approval Flow

Tool execution is controlled by **`Topic.tool_approval_policy`**（不再有 binding/instance 级策略）

```rust
use wind_core::models::{ToolApprovalPolicy, UpdateTopic};

// Require manual approval for every tool call in this topic
// `storage.topic().update(id, UpdateTopic)` — `None` 字段不会被写入
storage.topic().update(tid, UpdateTopic {
    tool_approval_policy: Some(ToolApprovalPolicy::Manual),
    ..Default::default()
}).await?;
```

Policies:

| Policy                   | Behavior                                                     |
| ------------------------ | ------------------------------------------------------------ |
| `AllowAll`               | Default (also what `None` means). Execute all requested MCP tools automatically. |
| `AllowList(Vec<String>)` | Execute listed tool names automatically; pause for the rest. |
| `Manual`                 | Pause for every tool call and emit `TopicEvent::ApprovalRequired`. |

When manual review is required, the runtime persists `ToolApprovalRequest` rows (status `Pending`) and emits `ApprovalRequired` (carrying the request ids). Reply through the handle — do not write approval state directly:

```rust
use wind_core::agent::event::TopicEvent;

// In your event loop:
TopicEvent::ApprovalRequired { instance_id, requests, .. } => {
    let allow: Vec<i64> = requests.iter().map(|r| r.id).collect();
    handle.approve(instance_id, allow, vec![]).await?;  // or deny via the third arg
}
```

`approve(instance_id, allow_ids, deny_ids)` sets the rows' status and resumes the agent, which re-loads its state and continues. 注意终态与等待审批都会关流，审批后需要重新 `subscribe()` 才能继续消费事件。Denied tools receive `{"error": "tool call denied", "tool": "..."}` as their result and the model continues with those markers in context.

### JSON Rule Engine

Define declarative request transformations stored in the database. Rules are applied to every API request before sending.

```rust
use wind_core::models::CreateJsonRule;
use wind_ai::model::AdapterType;

storage.provider().create_json_rule(CreateJsonRule {
    provider_id: pid,
    adapter: AdapterType::OpenAICompletion,
    json_rule: r#"{
        "rules": [{
            "type": "map_value",
            "path": "reasoning_effort",
            "mappings": {
                "medium": {"thinking": {"type": "enabled"}},
                "high":   {"thinking": {"type": "enabled"}}
            },
            "default": {"thinking": {"type": "disabled"}},
            "remove_source": true
        }]
    }"#.into(),
}).await?;
```

**Operations:** `set`, `remove`, `map_value`, `compute`, `when`.

**Conditions:** `eq`, `neq`, `exists`, `and`, `or`, `not`（条件对象恰好一个 key；未知算子返回 `Condition` 错误）

**Context variables** (`$ctx.*`) auto-injected: `$ctx.provider`, `$ctx.model`, `$ctx.adapter`, `$ctx.endpoint`.

### Entity Management

```rust
let s = core.storage();

// Lists
s.provider().list_all().await?;
s.model().list_by_provider().await?;
s.topic().list_topics().await?;
s.agent().list_instances_by_topic(tid).await?;      // 含主实例
s.agent().get_main_instance(tid).await?;            // 单个主实例
s.agent().list_agent_maps_by_topic(tid).await?;      // topic 能力映射

// Messages belong to an instance, not a topic
s.message().list_by_instance(instance.id).await?;   // full history
// list_contexts 只供 core 内部使用（pub(crate)），对外一律读全量历史

// Cascade delete — 删除实例连同 tool_approval_requests / messages；topic 行与它自己的
// definitions、能力映射、实例一起删除。Child topics are NOT cascaded, pass their ids explicitly.
s.topic().delete_topics(&[tid]).await?;
s.provider().delete(pid).await?;  // cascades credentials + json_rules

// MCP
s.mcp().get_by_name("everything").await?;
s.mcp().list().await?;

// Graceful shutdown (cancels topic runtimes + MCP clients)
core.shutdown().await;
```

## Crate Map

| Crate         | Responsibility                                                                             |
| ------------- | ------------------------------------------------------------------------------------------ |
| `wind-core`   | Orchestration — SQLite storage, agent/FSM runtime, MCP coordination, rule application       |
| `wind-ai`     | Provider abstraction — streaming/non-streaming, adapter pattern (`ChatAdapter`), SSE parsing |
| `wind-mcp`    | MCP client — actor-based registry, stdio/HTTP transports, builtin servers, tool discovery & execution |
| `wind-rule`   | JSON rule engine — declarative request transformation, expression evaluation                |
| `wind-skills` | `SKILL.md` frontmatter parsing (`SkillsMeta`, `scan`) — consumed by the builtin skills server |
| `wind-http`   | HTTP service — axum REST + SSE over the core, facade layer, OpenAPI (`/api-docs/openapi.json`) |
| `wind-tui`    | Terminal UI (skeleton) — ratatui + crossterm                                                |

## Topic Events

The public event contract is `TopicEvent`, consumed via `TopicRuntimeHandle::subscribe()`:

| Event              | When                                                    |
| ------------------ | ------------------------------------------------------- |
| `MessageCreated`   | A user or assistant message was persisted                |
| `Message`          | Streaming content delta                                  |
| `ApprovalRequired` | Tool calls require manual approval before execution      |
| `MessageFinished`  | A message is complete                                    |
| `TaskStatusChanged`| An agent task changed status (`Idle`/`Running`/`Finished`/…) |
| `Error`            | A task or the runtime failed                             |
| `Snapshot`         | 某个实例的全量消息（变体已定义，目前无代码产出）          |

A typical tool-call flow: `MessageCreated → Message (streaming) → ApprovalRequired → [approve] → Message (tool results + text) → MessageFinished`. The low-level `ChatEvent` (`Partial`/`AwaitToolCall`/`Finish`) is an internal detail of `AgentRuntime` — external code consumes `TopicEvent`.

## HTTP API 摘要

- **消息**：`POST /api/v1/topics/{topic_id}/messages`（提交对话输入，受理返回 `ApiResponse<()>`：`code: 200` + `msg: "ok"`）、`GET /api/v1/agent-instances/{instance_id}/messages`（该实例的全部消息，主实例与子实例同等对待）、`GET|PUT /api/v1/messages/{message_id}`。话题级消息 GET 与上下文路由、`/topics/by-instance/*`、`/messages/{id}/from-message` 已删除
- **实例**：只读，且不区分主实例与子实例 —— `/api/v1/agent-instances/*` 下全部是 GET —— `GET /api/v1/agent-instances/{instance_id}`、`GET /api/v1/agent-instances/{instance_id}/messages`、`.../tool-approvals/pending`，外加 `GET /api/v1/topics/{topic_id}/agent-instances`（含主实例）；实例由 core 内部创建
- **能力映射**：`GET /api/v1/topics/{topic_id}/agent-maps`、`POST /api/v1/agent-maps`（请求体是 core 的 `CreateTopicAgentMap`，自带 `topic_id`）、`DELETE /api/v1/agent-maps/{map_id}`。映射只表达能力归属，没有角色概念，故没有 PUT
- **事件流**：`GET /api/v1/topics/{topic_id}/events`（SSE）、`GET /api/v1/mcp-servers/events`（MCP 客户端状态 SSE）
- **审批**：`POST /api/v1/topics/{topic_id}/tool-approvals/{message_id}/approve`、`POST /api/v1/topics/{topic_id}/agent-instances/{instance_id}/cancel`

## Test Organization

下面的数字是当前快照，会随代码变化

| File                                 | Content                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------ |
| `windai/core/tests/storage.rs`       | 29 tests — storage CRUD, validation, cascades, batch operations (no `.env` needed) |
| `windai/core/tests/schema.rs`        | 14 tests — schema↔model column contract for all 12 tables, plus `dropped_columns_are_absent` / `dropped_tables_are_absent` |
| `windai/core/tests/agent_runtime.rs` | 2 tests — runtime lifecycle, and the terminal event must reach subscribers before the stream closes |
| `windai/core/tests/core_chat.rs`     | One test (`test_agent_chat`, `#[ignore]` behind `.env`): seeds providers/agents/maps, subscribes to topic events, drives `create_task` |
| `windai/core/tests/chat.rs`          | AI adapter tests (needs `.env`)                                          |
| `windai/core/tests/common/lib.rs`    | Shared helpers: `init_test_pool()`, `init_test_core()`, `init_test_core_with_registry()`, `seed_definition()`, `seed_chat_fixture()`, `McpTestEnv`, MCP server params |
| `windai/core/src/storage/message.rs` (cfg test) | 3 tests — `list_contexts` semantics (crate-internal API) |
| `windai/http/tests/*`                | HTTP router/facade/DTO tests via `tower::ServiceExt::oneshot`             |

## Environment Variables

| Variable          | Purpose                                      |
| ----------------- | -------------------------------------------- |
| `WIND_ROOT_DIR`   | Core data directory (default `~/.windai/`; DB file is `windai.db` inside it) |
| `RUST_LOG`        | Log level (`debug`, `info`, `warn`, `error`) |
| `WIND_HTTP_HOST`  | `wind-http` bind host (default `127.0.0.1`)  |
| `WIND_HTTP_PORT`  | `wind-http` bind port (default `7324`)       |

## License

MIT OR Apache-2.0
