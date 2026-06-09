# windai

An AI engine core library providing database-driven multi-turn chat, tool calling with approval flow, multi-provider adaptors, and declarative request transformation.

## Quick Start

```bash
cargo build
cargo test                   # unit + storage tests; ignored integration tests need .env
cargo test -p wind-core --test core_chat -- --include-ignored --test-threads=1
cargo test -p wind-core --test core_chat_mcp -- --include-ignored --test-threads=1
```

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

### Register Provider & Model

```rust
use wind_core::models::{CreateProvider, CreateCredentials, CreateModel, ModelType};
use wind_ai::model::AdaptorType;

let storage = core.storage();

let pid = storage.provider().create(CreateProvider {
    name: "deepseek".into(),
    base_url: "https://api.deepseek.com".into(),
    description: None,
    doc: None,
    alias: None,
}).await?;

storage.provider().create_credentials(CreateCredentials {
    provider_id: pid,
    key: "sk-xxx".into(),
}).await?;

let mid = storage.model().create(CreateModel {
    name: "deepseek-chat".into(),
    provider_id: pid,
    adaptor: AdaptorType::OpenAICompletion,
    alias: None,
    modalities: Some(vec![ModelType::Chat]),
    active: Some(true),
    icon: None,
    endpoint: None,
}).await?;
```

### Create Topic & Start Chat

```rust
use wind_core::models::{CreateTopic, CreateMessage};
use wind_ai::message::{Message, Content, Role, ReqConfig};
use futures::StreamExt;
use wind_core::chat::ChatEvent;

let tid = storage.topic().create(CreateTopic {
    parent_id: None,
    chat_config_id: 0,
    label: "My Chat".into(),
    icon: None,
    max_context: Some(50),
    mcp_server_ids: None,
}).await?;

storage.topic().create_chat_config(tid, ReqConfig {
    temperature: Some(0.7),
    stream: Some(true),
    ..Default::default()
}).await?;

// Create user message
let uid = storage.message().create(CreateMessage {
    from_id: None,
    stream: false,
    is_boundary: false,
    content: vec![Message::new_simple(Role::User, vec![
        Content::new_text("Hello!".into())
    ], None)],
    topic_id: tid,
    model_id: mid,
    input_tokens: 0,
    output_tokens: 0,
    tools_allowed: None,
    tools_denied: None,
}).await?;

// Create assistant message placeholder
let aid = storage.message().create(CreateMessage {
    from_id: Some(uid),
    stream: false,
    is_boundary: false,
    content: vec![],
    topic_id: tid,
    model_id: mid,
    input_tokens: 0,
    output_tokens: 0,
    tools_allowed: None,
    tools_denied: None,
}).await?;

// Start streaming chat
let mut stream = core.chat().start(tid, uid, aid);
while let Some(event) = stream.next().await {
    match event {
        ChatEvent::Created { .. } => {}
        ChatEvent::Partial { delta, .. } => {
            for c in &delta.content {
                match c {
                    Content::Text { data } => print!("{data}"),
                    _ => {}
                }
            }
        }
        ChatEvent::AwaitToolCall { message_id, tools } => {
            println!("Waiting for approval: {} tools", tools.len());
            // See Tool Approval section below
        }
        ChatEvent::Finish { error, .. } => {
            if let Some(e) = error {
                eprintln!("Error: {e}");
            }
        }
    }
}
```

### MCP Tool Calling

Register MCP servers and attach them to topics. The engine auto-discovers tools, sends them with each request, and executes approved calls.

```rust
use wind_core::models::CreateMcpServer;
use wind_mcp::client::TransportType;

let sid = storage.mcp().create(CreateMcpServer {
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

// Attach server to topic — tools become available on next chat request
storage.topic().create(CreateTopic {
    // ...
    mcp_server_ids: Some(vec![sid]),
    // ...
}).await?;
```

### Tool Approval Flow

Tool execution is controlled by `Topic.tool_approval_policy`.

```rust
use wind_core::models::{ToolApprovalPolicy, UpdateMessage, UpdateTopic};

storage.topic().update(tid, UpdateTopic {
    tool_approval_policy: Some(ToolApprovalPolicy::Manual),
    ..Default::default()
}).await?;
```

Policies:

| Policy                   | Behavior                                                     |
| ------------------------ | ------------------------------------------------------------ |
| `AllowAll`               | Default. Execute all requested MCP tools automatically.      |
| `AllowList(Vec<String>)` | Execute listed tool names automatically; pause for the rest. |
| `Manual`                 | Pause for every tool call and emit `AwaitToolCall`.          |

When manual review is required, the engine emits `AwaitToolCall` and saves the assistant message state. The caller must explicitly approve or reject each pending tool call before resuming.

```rust
// After receiving AwaitToolCall:
// Approve specific tool call IDs
storage.message().update(
    message_id,
    UpdateMessage {
        tools_allowed: Some(vec!["call_abc123".into()]),
        ..Default::default()
    },
).await?;

// Resume — the engine re-loads state and executes approved tools
let mut stream = core.chat().start(topic_id, user_msg_id, message_id);
```

**Approve:** Set `tools_allowed` with the tool call IDs to execute. Approved tools run, results are fed back as context, and the model continues.

**Reject:** Set `tools_denied` with the tool call IDs to reject.

```rust
storage.message().update(
    message_id,
    UpdateMessage {
        tools_denied: Some(vec!["call_abc123".into()]),
        ..Default::default()
    },
).await?;
```

Rejected tools receive `{"error": "User denied this tool call"}` as their result, and the model continues with those rejection markers in context.

**Explicit review required:** Resuming a manual tool request without either `tools_allowed` or `tools_denied` returns an approval error instead of implicitly rejecting calls. After a reviewed batch is processed, approval fields are cleared on the persisted assistant message.

### JSON Rule Engine

Define declarative request transformations stored in the database. Rules are applied to every API request before sending.

```rust
use wind_core::models::CreateJsonRule;

storage.provider().create_json_rule(CreateJsonRule {
    provider_id: pid,
    adaptor: AdaptorType::OpenAICompletion,
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

**Context variables** (`$ctx.*`) auto-injected: `$ctx.provider`, `$ctx.model`, `$ctx.adaptor`, `$ctx.endpoint`.

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
s.mcp().list_all().await?;

// Graceful shutdown
core.shutdown().await;
```

## Crate Map

| Crate       | Responsibility                                                                       |
| ----------- | ------------------------------------------------------------------------------------ |
| `wind-core` | Orchestration — SQLite storage, chat engine, MCP coordination, rule application      |
| `wind-ai`   | Provider abstraction — streaming/non-streaming, adaptor pattern, SSE parsing         |
| `wind-mcp`  | MCP client — actor-based registry, stdio/HTTP transports, tool discovery & execution |
| `wind-rule` | JSON rule engine — declarative request transformation, expression evaluation         |

## Chat Events

| Event           | When                                                    |
| --------------- | ------------------------------------------------------- |
| `Created`       | Chat round started                                      |
| `Partial`       | Streaming content chunk or intermediate tool-call frame |
| `AwaitToolCall` | Tool calls require manual approval before execution     |
| `Finish`        | Chat round ended (may carry final message or error)     |

A typical tool-call flow: `Created → Partial(tool_request) → AwaitToolCall → [user approves] → Partial(tool_result) → Partial(text) → Finish`.

## Test Organization

| File                                 | Content                                                                 |
| ------------------------------------ | ----------------------------------------------------------------------- |
| `windai/core/tests/storage.rs`       | Storage CRUD, validation, cascades, batch operations                    |
| `windai/core/tests/core_chat.rs`     | Non-MCP chat flows: streaming, history, errors, JSON rules, persistence |
| `windai/core/tests/core_chat_mcp.rs` | MCP approval, rejection, and explicit-review resume behavior            |
| `windai/core/tests/common/lib.rs`    | Shared `.env`, SQLite, provider, and MCP test helpers                   |

## Environment Variables

| Variable          | Purpose                                      |
| ----------------- | -------------------------------------------- |
| `WINDAI_ROOT_DIR` | Data directory (default `~/.windai/`)        |
| `RUST_LOG`        | Log level (`debug`, `info`, `warn`, `error`) |

## License

MIT OR Apache-2.0
