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
cargo test -p wind-js

# Run a single test
cargo test -p wind-ai -- test_build_request

# Integration tests that require API keys (set env vars)
API_KEY=sk-xxx STREAM=true cargo test -p wind-core -- test_chat_mcp_env
API_KEY=sk-xxx cargo test -p wind-core -- test_chat_mcp_completion_stream

# The MCP registry tests require npx/uvx; they are #[ignore] by default
cargo test -p wind-mcp -- --ignored
```

No `.env` file exists by default. Integration tests read `API_KEY`, `API_BASE_URL`, `MODEL`, `ADAPTOR`, and `STREAM` from environment variables.

## Architecture

This is a Rust workspace with four crates under `windai/`:

### Crate dependency graph

```
wind-core  ──depends-on──>  wind-ai, wind-mcp, wind-js
wind-mcp   ──depends-on──>  (rmcp for MCP transport)
wind-ai    ──depends-on──>  (reqwest for HTTP, no other wind-* crates)
wind-js    ──depends-on──>  (rquickjs, standalone)
```

Each crate is self-contained: `wind-ai` knows nothing about `wind-mcp` or the database. The `wind-core` crate is the only one that ties everything together.

### `wind-core` - Central orchestration

`WindCore` (`windai/core/src/lib.rs:22`) is the application entry point. It owns:
- A `SqlitePool` for persistence
- An `Arc<JsEngine>` shared across chat sessions
- An MCP `RegistryHandle` for managing MCP server connections

It exposes service facades (`provider()`, `model()`, `topic()`, `message()`, `chat()`) that each create a service instance backed by the shared pool.

The **`ChatEngine`** (`windai/core/src/chat/engine.rs`) is the core chat loop:
1. Loads `Topic`, `Model`, `Provider`, credentials, and `ReqConfig` from the DB
2. Builds message context from history (respecting `is_boundary` markers and `max_context` limits)
3. Fetches MCP tools registered to the topic
4. Constructs the request via the adaptor's `build_request()`
5. Applies JS hooks (user-defined `transform(body, context)` functions stored in `js_hook_code` table)
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

### `wind-js` - JavaScript hooks

`JsEngine` (`windai/js/src/lib.rs`) wraps a `rquickjs::Runtime`. Users store JS code in the `js_hook_code` table keyed by `(provider_id, adaptor)`. Before each API request, the chat engine calls `apply_js_hook()` which runs the user's `transform(body, context)` function — the function receives the request body and a context object `{provider, model, endpoint, adaptor}` and returns a modified body. This allows per-provider request rewriting without code changes.

### Database schema (SQLite)

`windai/core/src/schema.rs` creates these tables on init:
- `providers` - API provider configs (name, base_url, active)
- `models` - AI models linked to providers with adaptor type
- `credentials` - API keys per provider
- `topics` - conversation threads with max_context and ordering
- `messages` - chat messages with content as JSON, boundary/excluded flags, token counts
- `chat_configs` - per-topic request parameters
- `topic_mcp_servers` - MCP servers linked to topics
- `js_hook_code` - per-provider+adaptor JS transform scripts

### Key flows

**Adding a new provider adaptor**: Implement `ChatAdaptor` trait in `wind-ai`, add the variant to `AdaptorType` enum in `windai/ai/src/model.rs`, and register it in `get_chat_adaptor()`.

**Adding a new provider**: Insert a row in `providers`, add credentials, insert models referencing the adaptor type. No code changes needed.

**MCP tool calling flow**: Topic → `topic_mcp_servers` links servers → Registry acquires connections → tools discovered → tool names prefixed with server name → AI model returns `tool_calls` → `execute_function_calls()` parses server+tool names, dispatches to registry → results fed back as context for next AI turn.
