# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Build the entire workspace
cargo build

# Build a specific crate
cargo build -p wind-core

# Run all tests (uses SQLite; no external DB needed)
cargo test

# Run tests for a specific crate
cargo test -p wind-core
cargo test -p wind-ai
cargo test -p wind-mcp
cargo test -p wind-rule

# Run a single test
cargo test -p wind-ai -- test_build_request

# Integration tests that require API keys
# Copy .env.example to .env and fill in your keys, then run:
cargo test -p wind-core -- test_chat

# MCP integration tests (use .env values)
cargo test -p wind-core -- test_chat_mcp

# Real MCP server tests (require npx/uvx installed; #[ignore] by default)
cargo test -p wind-core -- --ignored

# The MCP registry tests also require npx/uvx
cargo test -p wind-mcp -- --ignored
```

Copy `.env.example` to `.env` and fill in the `TEST_*` values. Integration tests auto-discover this file via `dotenvy` by walking up from `CARGO_MANIFEST_DIR`. See `.env.example` for all supported variables.

## Architecture

This is a Rust workspace with four crates under `windai/`:

### Crate dependency graph

```
wind-core  ──depends-on──>  wind-ai, wind-mcp, wind-rule
wind-mcp   ──depends-on──>  (rmcp for MCP transport)
wind-ai    ──depends-on──>  (reqwest for HTTP, no other wind-* crates)
wind-rule  ──depends-on──>  (evalexpr, standalone)
```

Each crate is self-contained: `wind-ai` knows nothing about `wind-mcp` or the database. The `wind-core` crate is the only one that ties everything together.

### `wind-core` - Central orchestration

`WindCore` (`windai/core/src/lib.rs:22`) is the application entry point. It owns:
- A `SqlitePool` for persistence
- An MCP `RegistryHandle` for managing MCP server connections

It exposes service facades (`provider()`, `model()`, `topic()`, `message()`, `chat()`) that each create a service instance backed by the shared pool.

The **`ChatEngine`** (`windai/core/src/chat/engine.rs`) is the core chat loop:
1. Loads `Topic`, `Model`, `Provider`, credentials, and `ReqConfig` from the DB
2. Builds message context from history (respecting `is_boundary` markers and `max_context` limits)
3. Fetches MCP tools registered to the topic
4. Constructs the request via the adaptor's `build_request()`
5. Applies JSON rules (user-defined `RuleSet` transforms stored in `json_rule` table)
6. Sends the request; on tool calls, executes them via MCP and loops back to step 4

The chat loop emits a `ChatEvent` stream (`Created → Partial x N → Finished` for streaming; `Response` for non-streaming).

### `wind-ai` - AI provider abstraction

The **adaptor pattern** (`windai/ai/src/provider/adaptor.rs`) is the key abstraction. The `ChatAdaptor` trait has three methods:

```rust
fn build_request(model, config, contexts, tools) -> Value
fn parse_response(bytes) -> Message
fn parse_stream_chunk(bytes) -> Vec<Message>
```

Two implementations exist:
- **`OpenAICompletionAdaptor`** - for `/chat/completions` endpoints (OpenAI, DeepSeek, OpenRouter, etc.)
- **`OpenAIResponseAdaptor`** - for `/responses` endpoints (newer OpenAI API)

`adaptor::get_chat_adaptor(type)` returns a boxed trait object. The adaptor type is stored per-model in the database.

Each adaptor has companion schema types under `provider/adaptor/schema/` that define the provider-specific JSON structures.

**Message type** (`message.rs`): The unified `Message` struct carries role, content (vec of text/image/audio/file/function_call), reasoning_content, token counts, and optional `tool_calls`. `append_chunk()` merges streaming deltas.

**Request config** (`ReqConfig`): temperature, top_p, max_tokens, stream, penalties, parallel_tool_calls, reasoning.

**Streaming**: `handle_chat()` checks `req_body.stream`; if true, uses `client::request_sse()` + `client::handle_stream()` which buffers SSE chunks by `\n\n` boundaries, then `parse_stream_chunk()` converts each SSE block into `Message` fragments.

### `wind-mcp` - MCP client registry

The **Registry** (`windai/mcp/src/client/registry.rs`) uses an actor pattern: `Registry::new()` spawns a tokio task that owns all server state and communicates via `mpsc` channels through a `RegistryHandle` clone.

`RegistryHandle` supports:
- `acquire(session_id, params)` - start/reuse an MCP server (shared across sessions via reference counting)
- `release(session_id, name)` - drop a session reference; last one disconnects
- `list_all_tools()` / `call_tool(param)` - tool discovery and execution

`ServerHandle::connect()` (`connector.rs`) supports two transports:
- **Stdio** - spawns a child process, uses a dedup map to serialize concurrent starts of the same command (avoids npm/uvx file-write conflicts)
- **Streamable HTTP** - connects via URL

Tool names are namespaced: `{server_name}0m0{tool_name}` where `0m0` is the separator constant (`MCP_TOOL_IDENTIFIER`).

### `wind-rule` - JSON rule engine

`RuleSet` (`windai/rule/src/compile.rs`) is a JSON-based rule engine that transforms request bodies. Users store rule JSON in the `json_rule` table keyed by `(provider_id, adaptor)`. Before each API request, the chat engine calls `apply_json_rule()` which applies the `RuleSet` to the request body using an `EvalContext` that includes `{provider, model, endpoint, adaptor}`. This allows per-provider request rewriting without code changes.

**Operations** (defined in `RawOp` / compiled to `CompiledOp`):

| Op | Purpose |
|----|---------|
| `set` | Set a value at a JSON path; creates intermediate objects as needed |
| `remove` | Delete a field at a JSON path |
| `map_value` | Map a field's value to a target object via a lookup table; optionally removes the source field. Merges the target object into the body root |
| `compute` | Evaluate an `evalexpr` expression over `$value` (current field value) and `$ctx.*` (context variables). Result replaces the field |
| `when` | Conditional: `cond` is compiled via `CompiledCond` (supports `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `and`, `or`, `not`, `in`). Executes `then` or `else` sub-rules |

Key implementation details:
- Path segments are pre-split at compile time (dot-separated, e.g. `foo.bar.zoo` → `["foo", "bar", "zoo"]`)
- `compute` expressions are pre-compiled into `evalexpr::Node` trees at rule parse time
- `map_value` mappings are compiled from JSON into `Vec<(Value, Value)>` lookup tables with special handling for `"null"` keys and numeric-string keys
- `merge_root()` does a shallow merge at the body root level; nested objects are merged recursively one level deep
- `EvalContext` is constructed from a `Value::Object`; nested path access is not yet implemented (flat `$ctx.key` only)

### Database schema (SQLite)

`windai/core/src/schema.rs` creates these tables on init:
- `providers` - API provider configs (name, base_url, active)
- `models` - AI models linked to providers with adaptor type
- `credentials` - API keys per provider
- `topics` - conversation threads with max_context and ordering
- `messages` - chat messages with content as JSON, boundary/excluded flags, token counts
- `chat_configs` - per-topic request parameters
- `topic_mcp_servers` - MCP servers linked to topics
- `json_rule` - per-provider+adaptor JSON rule transforms

### Key flows

**Adding a new provider adaptor**: Implement `ChatAdaptor` trait in `wind-ai`, add the variant to `AdaptorType` enum in `windai/ai/src/model.rs`, and register it in `get_chat_adaptor()`.

**Adding a new provider**: Insert a row in `providers`, add credentials, insert models referencing the adaptor type. No code changes needed.

**MCP tool calling flow**: Topic → `topic_mcp_servers` links servers → Registry acquires connections → tools discovered → tool names prefixed with server name → AI model returns `tool_calls` → `execute_function_calls()` parses server+tool names, dispatches to registry → results fed back as context for next AI turn.
