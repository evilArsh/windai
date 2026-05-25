# windai

An embeddable AI core library built in Rust. Provides a database-backed chat engine with multi-turn tool calling, provider-agnostic API adaptors, MCP client coordination, and a JSON-based rule engine for request transformation. Designed as a shared engine for desktop (Electron/Tauri), web (WASM), and mobile apps.

## Architecture

```
wind-core  ──depends-on──>  wind-ai, wind-mcp, wind-rule
wind-mcp   ──depends-on──>  rmcp (MCP transport)
wind-ai    ──depends-on──>  reqwest (HTTP)
wind-rule  ──depends-on──>  evalexpr (standalone)
```

| Crate | Purpose |
|-------|---------|
| **wind-core** | Central orchestration — SQLite-backed chat engine, CRUD services, multi-turn tool calling, JSON rule application, MCP coordination |
| **wind-ai** | Provider-agnostic chat layer — streaming/non-streaming, adaptors for OpenAI Completion & Response APIs, SSE parser, unified message types |
| **wind-mcp** | MCP client registry — actor-based shared server pool, stdio & streamable HTTP transports, tool discovery and calling |
| **wind-rule** | JSON rule engine — declarative request transformation with `set`/`remove`/`map_value`/`compute`/`when` operations, pre-compiled expression trees |

## Quick Start

```bash
# Build the workspace
cargo build

# Run all tests (uses SQLite; no external DB needed)
cargo test

# Run tests for a specific crate
cargo test -p wind-core
cargo test -p wind-ai

# Integration tests that require API keys
# Copy .env.example to .env and fill in your keys, then:
cargo test -p wind-core -- test_chat
cargo test -p wind-core -- test_chat_mcp
```

Add to your `Cargo.toml`:

```toml
[dependencies]
wind-core = { git = "https://github.com/evilArsh/windai" }
wind-ai   = { git = "https://github.com/evilArsh/windai" }
wind-mcp  = { git = "https://github.com/evilArsh/windai" }
wind-rule = { git = "https://github.com/evilArsh/windai" }
```

The four crates map to the four crates in this repo:
- `wind-core` — central orchestration (depends on the other three)
- `wind-ai` — AI provider adaptors and message types
- `wind-mcp` — MCP client registry
- `wind-rule` — JSON rule engine

## Usage Tutorial

Every interaction starts with `WindCore`. Initialize it with a local SQLite database or in-memory storage, then use the service facades to manage providers, models, topics, and messages. The `chat()` method returns a `ChatEngine` that drives the full conversation loop.

### 1. Initialize WindCore

```rust
use wind_core::WindCore;

// Persistent local database (default: ~/.windai/windai.db)
let core = WindCore::init_local(Some("sqlite:windai.db")).await?;

// In-memory (useful for testing)
let core = WindCore::init_memory().await?;
```

### 2. Register a provider

A provider represents an AI API service. You configure its base URL and later attach credentials and models to it.

```rust
use wind_core::models::CreateProvider;

let provider = core.provider().create(CreateProvider {
    name: "deepseek".into(),
    base_url: "https://api.deepseek.com".into(),
    description: Some("DeepSeek API".into()),
    doc: Some("https://api-docs.deepseek.com".into()),
    alias: None,
    active: Some(true),
}).await?;
```

### 3. Add credentials

Each provider needs at least one API key.

```rust
use wind_core::models::CreateCredentials;

core.provider().create_credentials(CreateCredentials {
    provider_id: provider.id,
    key: "sk-xxx".into(),
}).await?;
```

### 4. Register a model

Models belong to providers and specify which adaptor to use for request/response processing.

```rust
use wind_core::models::CreateModel;
use wind_ai::model::AdaptorType;

let model = core.model().create(CreateModel {
    name: "deepseek-chat".into(),
    provider_id: provider.id,
    adaptor: AdaptorType::OpenAICompletion,
    alias: Some("DeepSeek V4".into()),
    modalities: None,
    active: Some(true),
    icon: None,
    endpoint: None,        // uses provider default
}).await?;
```

`AdaptorType` determines the HTTP shape:

| Variant | API style |
|---------|-----------|
| `OpenAICompletion` | `/v1/chat/completions` (OpenAI, DeepSeek, OpenRouter, etc.) |
| `OpenAIResponses` | `/v1/responses` (newer OpenAI API) |

### 5. Create a topic and configure chat parameters

A topic is a conversation thread. You can set a chat config to control temperature, streaming, token limits, etc.

```rust
use wind_core::models::CreateTopic;
use wind_ai::message::ReqConfig;

let topic = core.topic().create_topic(CreateTopic {
    parent_id: None,
    chat_config_id: 0,
    label: "My first chat".into(),
    icon: None,
    max_context: Some(50),
}).await?;

// Set per-topic request parameters
core.topic().create_chat_config(topic.id, ReqConfig {
    temperature: Some(0.7),
    max_tokens: Some(4096),
    stream: Some(true),
    ..Default::default()
}).await?;
```

### 6. Send a user message and stream the response

Save a user message, then call `chat().send()` to get a stream of `ChatEvent` values.

```rust
use wind_core::models::CreateMessage;
use wind_ai::message::{Content, Message, Role};
use futures::StreamExt;

// Persist the user's message
let user_msg = core.message().create(CreateMessage {
    from_id: None,
    stream: true,
    content_json: serde_json::to_string(&vec![
        Message::new_simple(Role::User, vec![
            Content::new_text("Hello, who are you?".into())
        ], None)
    ]).unwrap(),
    model_id: model.id,
    topic_id: topic.id,
    is_boundary: false,
    is_excluded: false,
    input_tokens: 0,
    output_tokens: 0,
}).await?;

// Create a placeholder for the assistant's response
let assistant_msg = core.message().create(CreateMessage {
    from_id: Some(user_msg.id),
    stream: true,
    content_json: "[]".into(),
    model_id: model.id,
    topic_id: topic.id,
    is_boundary: false,
    is_excluded: false,
    input_tokens: 0,
    output_tokens: 0,
}).await?;

// Stream the response
let mut stream = core.chat().send(topic.id, model.id, user_msg.id, assistant_msg.id);
while let Some(event) = stream.next().await {
    match event {
        ChatEvent::Created { message_id } => {
            println!("[chat started] message_id={message_id}");
        }
        ChatEvent::Partial { index, delta, .. } => {
            if let Some(text) = delta.content.first() {
                print!("{text}");
            }
        }
        ChatEvent::Finish { message, error, .. } => {
            if let Some(err) = error {
                eprintln!("[error] {err}");
            }
            println!("\n[done]");
        }
    }
}
```

**Event flow:** `Created` -> `Partial` x N -> `Finish`. On tool-calling turns there may be multiple `Partial` phases (tool request delta -> tool result delta -> final text delta) before `Finish`.

### 7. MCP tool calling

Link MCP servers to a topic, and the chat engine automatically discovers tools, injects them into the request, and executes tool calls in a loop.

```rust
use wind_mcp::client::{ServerParams, StdioParams};

// Start an MCP server via stdio
let mcp_snapshot = core.mcp.acquire("session-1", ServerParams::Stdio(StdioParams {
    name: "fetch".into(),
    description: Some("Web fetch server".into()),
    command: "uvx".into(),
    args: vec!["mcp-server-fetch".into()],
    env: None,
})).await?;

// Link it to the topic
core.topic().set_mcp_servers(topic.id, vec![mcp_server_db_id]).await?;

// Now chat() automatically discovers tools from "fetch" and executes tool calls.
// Tool names are namespaced as "{server_name}0m0{tool_name}".
```

**Stdio deduplication:** Concurrent `acquire()` calls for the same command (e.g., `npx` / `uvx`) are serialized automatically to avoid file-write conflicts from package managers.

### 8. JSON rules — per-provider request transformation

Store declarative JSON rules in the database to rewrite outgoing requests without code changes. Rules are keyed by `(provider_id, adaptor)` and applied to every request before it's sent.

**Example:** DeepSeek's API requires wrapping `reasoning_effort` in a `thinking` block. Instead of forking the adaptor, define a rule:

```json
{
  "rules": [
    {
      "type": "when",
      "cond": { "eq": ["$ctx.provider", "deepseek"] },
      "then": [
        {
          "type": "map_value",
          "path": "reasoning_effort",
          "mappings": {
            "medium": { "thinking": { "type": "enabled" } },
            "high":   { "thinking": { "type": "enabled" } }
          },
          "default": { "thinking": { "type": "disabled" } },
          "remove_source": true
        }
      ]
    },
    {
      "type": "compute",
      "path": "max_tokens",
      "expr": "min($value, 4096)"
    }
  ]
}
```

Save it to the database:

```rust
use wind_core::models::CreateJsonRule;

core.provider().create_json_rule(CreateJsonRule {
    provider_id: provider.id,
    adaptor: AdaptorType::OpenAICompletion,
    json_rule: rule_json_string,
    active: true,
}).await?;
```

**Available operations:**

| Op | Purpose |
|----|---------|
| `set` | Set a field value at a JSON path |
| `remove` | Remove a field at a JSON path |
| `map_value` | Map a field's value to a different structure; optionally remove the source |
| `compute` | Evaluate an expression over the current value (uses `evalexpr`; `$value` and `$ctx.*` are available) |
| `when` | Conditional branching — `cond` with `then`/`else` sub-rules |

Conditions support `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `and`, `or`, `not`, and `in`. Context variables (`$ctx.provider`, `$ctx.model`, `$ctx.endpoint`, `$ctx.adaptor`) are injected automatically by the chat engine.

### 9. Managing entities

All services support full CRUD:

```rust
// List
let providers = core.provider().list().await?;
let models = core.model().list_by_provider(provider.id).await?;
let topics = core.topic().list_topics().await?;
let messages = core.message().list_by_topic(topic.id).await?;

// Update
core.provider().update(provider.id, UpdateProvider {
    alias: Some("DS".into()),
    ..Default::default()
}).await?;

// Delete
core.provider().delete(provider.id).await?;
```

### 10. Shutdown

Always call `shutdown()` to gracefully close MCP connections and the database pool.

```rust
core.shutdown().await;
```

## API Reference

### `WindCore`

| Method | Returns | Purpose |
|--------|---------|---------|
| `init_local(path)` | `Result<Self>` | Open/create a local SQLite database |
| `init_memory()` | `Result<Self>` | In-memory database for testing |
| `provider()` | `&ProviderService` | CRUD for providers, credentials, and JSON rules |
| `model()` | `&ModelService` | CRUD for models |
| `topic()` | `&TopicService` | CRUD for topics, chat configs, and MCP server links |
| `message()` | `&MessageService` | CRUD for messages |
| `chat()` | `ChatEngine` | Create a chat engine for streaming conversations |
| `shutdown()` | `()` | Close all MCP connections and the database |

### `ChatEvent`

```rust
enum ChatEvent {
    Created { message_id: i64 },
    Partial { index: i32, message_id: i64, delta: Message },
    Finish  { message_id: i64, message: Option<Vec<Message>>, error: Option<String> },
}
```

### `ReqConfig`

| Field | Type | Purpose |
|-------|------|---------|
| `temperature` | `Option<f64>` | Sampling temperature (0–2) |
| `top_p` | `Option<f64>` | Nucleus sampling threshold |
| `max_tokens` | `Option<i32>` | Max output tokens |
| `stream` | `Option<bool>` | Enable SSE streaming |
| `presence_penalty` | `Option<f64>` | Penalize new topics (-2.0–2.0) |
| `frequency_penalty` | `Option<f64>` | Penalize repetition (-2.0–2.0) |
| `parallel_tool_calls` | `Option<bool>` | Allow concurrent tool execution |
| `reasoning` | `Option<bool>` | Enable reasoning/thinking mode |

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `WINDAI_ROOT_DIR` | Override the default data directory (`~/.windai/`) |
| `RUST_LOG` | Control log verbosity (`debug`, `info`, `warn`, `error`) |

## License

MIT OR Apache-2.0
