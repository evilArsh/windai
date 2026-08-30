# windai

An AI engine core library providing database-driven multi-turn chat, hierarchical agent orchestration, tool calling with approval flow, multi-provider adapters, and declarative request transformation. Ships an HTTP service (`wind-http`) exposing the core over REST + SSE.

## Quick Start

```bash
cargo build
cargo test                   # unit + storage tests; .env-gated tests are #[ignore]d
cargo test -p wind-http      # HTTP route/facade/mirror tests (no .env needed)
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

// File-backed SQLite
let core = WindCore::init_local(Some("/path/to/windai.db")).await?;

// Custom connection pool (e.g. shared-cache for tests)
let core = WindCore::init_with_pool(pool).await?;

// Custom connection pool + shared MCP registry (test harnesses / advanced embedding)
let core = WindCore::init_with_pool_and_registry(pool, registry).await?;
```

`WindCore` is a process-level runtime root: one instance per process, owns the DB pool + MCP registry, and manages one `TopicRuntime` actor per topic.

### Register Provider & Model

```rust
use wind_core::models::{CreateProvider, CreateCredentials, CreateModel, ModelType};
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
}).await?;
let mid = model.id;
```

### Create a Topic & Start a Chat

Chats run through the **agent system**: a `Topic` owns a `TopicRuntime`; an `AgentBinding` (with role `Main`) says which agent the topic uses. The runtime streams progress as `TopicEvent`s over a `broadcast` channel.

```rust
use wind_core::agent::event::TopicEvent;
use wind_core::models::{
    AgentRole, AgentScope, AgentDefinitionData,
    CreateAgentBinding, CreateAgentDefinition, CreateTopic,
};
use wind_ai::message::Content;

let storage = core.storage();

// 1. Create a topic
let topic = storage.topic().create(CreateTopic {
    parent_id: None,
    binding_id: None,
    label: "My Chat".into(),
    icon: None,
}).await?;
let tid = topic.id;

// 2. Define a reusable agent (what the agent *can* do)
let agent_def = storage.agent().create_definition(CreateAgentDefinition {
    name: "assistant".into(),
    key: "assistant".into(),
    description: "Default assistant".into(),
    scope: AgentScope::Global,
    owner_topic_id: None,
    cloned_from_agent_id: None,
    active: Some(true),
    data: AgentDefinitionData::default(),
}).await?;

// 3. Bind it as the topic's main agent (an agent *instance*)
let binding = storage.agent().create_binding(CreateAgentBinding {
    parent_topic_id: tid,
    agent_id: agent_def.id,
    role: AgentRole::Main,
    model_id: Some(mid),
    chat_config_id: None,
    enabled: Some(true),
}).await?;

// 4. Submit user input, then consume the event stream
let handle = core.fetch_topic(tid);
let mut events = handle.subscribe().await?;
handle.create_chat(vec![Content::new_text("Hello!".into())]).await?;

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
- `create_chat` submits `Vec<Content>` (the full `wind_ai::message::Content` protocol) and returns immediately — the runtime accepts it asynchronously. You do **not** hand-build `Message` records; the engine creates sub-topics, user/assistant messages, and tool results internally.
- Each agent instance runs in its own sub-topic (`parent_topic_id` on the binding tracks the owning topic). The event stream channel closes when the main task goes idle — re-subscribe per conversation.
- `TopicEvent` variants: `Error`, `Snapshot`, `MessageCreated`, `Message` (streaming delta), `MessageFinished`, `TaskStatusChanged`, `ApprovalRequired`.

### MCP Tool Calling

Register MCP servers, then attach them to an agent definition. The engine discovers tools, filters them per agent, sends them with each request, and executes approved calls.

```rust
use wind_core::models::agent::{AgentDefinitionData, AgentMcpBinding};
use wind_core::models::{CreateMcpServer, CreateAgentDefinition, AgentScope};
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
    key: "tool-user".into(),
    description: "Assistant with MCP tools".into(),
    scope: AgentScope::Global,
    owner_topic_id: None,
    cloned_from_agent_id: None,
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

Agent definition data also carries `prompt_modules`, `context_policy`, `permission_policy`, and `runtime_limits` — see the `AgentDefinitionData` type for the full surface.

### Tool Approval Flow

Tool execution is controlled by `AgentBinding.tool_approval_policy`.

```rust
use wind_core::models::{ToolApprovalPolicy, UpdateAgentBinding};

// Require manual approval for every tool call of this binding
storage.agent().update_binding(binding.id, UpdateAgentBinding {
    tool_approval_policy: Some(ToolApprovalPolicy::Manual),
    ..Default::default()
}).await?;
```

Policies:

| Policy                   | Behavior                                                     |
| ------------------------ | ------------------------------------------------------------ |
| `AllowAll`               | Default. Execute all requested MCP tools automatically.      |
| `AllowList(Vec<String>)` | Execute listed tool names automatically; pause for the rest. |
| `Manual`                 | Pause for every tool call and emit `TopicEvent::ApprovalRequired`. |

When manual review is required, the runtime persists `ToolApprovalRequest` rows (status `Pending`) and emits `ApprovalRequired` (carrying the request ids). Reply through the handle — do not write approval state directly:

```rust
use wind_core::agent::event::TopicEvent;

// In your event loop:
TopicEvent::ApprovalRequired { binding_id, requests, .. } => {
    let allow: Vec<i64> = requests.iter().map(|r| r.id).collect();
    handle.approve(binding_id, allow, vec![]).await?;  // or deny via the third arg
}
```

`approve(binding_id, allow_ids, deny_ids)` sets the rows' status and resumes the agent, which re-loads its state and continues. Denied tools receive `{"error": "tool call denied", "tool": "..."}` as their result and the model continues with those markers in context.

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

**Conditions:** `eq`, `neq`, `gt`, `lt`, `contains`, `and`, `or`, `not`, `in`.

**Context variables** (`$ctx.*`) auto-injected: `$ctx.provider`, `$ctx.model`, `$ctx.adapter`, `$ctx.endpoint`.

### Entity Management

```rust
let s = core.storage();

// Lists
s.provider().list_all().await?;
s.model().list_by_provider().await?;
s.topic().list_topics().await?;
s.message().list_by_topic(tid).await?;

// Cascade delete
s.topic().delete_topics(&[tid]).await?;
s.provider().delete(pid).await?;  // cascades credentials + json_rules

// MCP
s.mcp().get_by_name("everything").await?;
s.mcp().list().await?;

// Graceful shutdown (cancels topic runtimes + MCP clients)
core.shutdown().await;
```

## Crate Map

| Crate        | Responsibility                                                                             |
| ------------ | ------------------------------------------------------------------------------------------ |
| `wind-core`  | Orchestration — SQLite storage, agent/FSM runtime, MCP coordination, rule application      |
| `wind-ai`    | Provider abstraction — streaming/non-streaming, adapter pattern (`ChatAdapter`), SSE parsing |
| `wind-mcp`   | MCP client — actor-based registry, stdio/HTTP transports, tool discovery & execution       |
| `wind-rule`  | JSON rule engine — declarative request transformation, expression evaluation               |
| `wind-http`  | HTTP service — axum REST + SSE over the core, facade layer, OpenAPI (`/swagger-ui`)        |
| `wind-tui`   | Terminal UI (skeleton) — ratatui + crossterm                                               |

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
| `Snapshot`         | Full message list for a topic                            |

A typical tool-call flow: `MessageCreated → Message (streaming) → ApprovalRequired → [approve] → Message (tool results + text) → MessageFinished`. The low-level `ChatEvent` (`Partial`/`AwaitToolCall`/`Finish`) is an internal detail of `AgentRuntime` — external code consumes `TopicEvent`.

## Test Organization

| File                                 | Content                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------ |
| `windai/core/tests/storage.rs`       | Storage CRUD, validation, cascades, batch operations (no `.env` needed)  |
| `windai/core/tests/core_chat.rs`     | Non-MCP chat flows: streaming, history, errors, JSON rules, persistence  |
| `windai/core/tests/chat.rs`          | AI adapter tests (needs `.env`)                                          |
| `windai/core/tests/core_chat_mcp.rs` | MCP approval tests — currently **commented out**                         |
| `windai/http/tests/*`                | HTTP router/facade/mirror tests via `tower::ServiceExt::oneshot`         |

## Environment Variables

| Variable          | Purpose                                      |
| ----------------- | -------------------------------------------- |
| `WINDAI_ROOT_DIR` | Core data directory (default `~/.windai/`)   |
| `RUST_LOG`        | Log level (`debug`, `info`, `warn`, `error`) |
| `WIND_HTTP_HOST`  | `wind-http` bind host (default `127.0.0.1`)  |
| `WIND_HTTP_PORT`  | `wind-http` bind port (default `7324`)       |
| `WINDAI_DB_PATH`  | `wind-http` SQLite file path                 |

## License

MIT OR Apache-2.0
