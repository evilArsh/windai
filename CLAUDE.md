# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**windai** is a Rust workspace providing a unified LLM conversation layer and MCP (Model Context Protocol) client management. It abstracts multiple OpenAI-compatible API formats behind a single message/tool interface, and manages MCP server lifecycle (connect/disconnect/session-sharing) via an event-driven registry.

## Architecture

The workspace contains three crates:

### 1. `windai-conversation` — Unified Conversation Layer

Defines a common message/tool model and adapts it to different OpenAI-compatible API formats.

- **Core types** (`message.rs`, `tool.rs`): `Message`, `Content`, `Role`, `ReqConfig`, `FunctionCall`, `FunctionCallOutput`, `Tools`
- **Model/Adaptor** (`model.rs`): `AdaptorType` enum (`OpenAICompletion` | `OpenAIResponse`), `Model` struct
- **Provider** (`provider.rs`): `handle_chat()` — the main entry point. Takes a unified request, routes to the appropriate adaptor, and returns an `impl Stream<Item = ResEvent>` for streaming or non-streaming responses.
- **Adaptors** (`provider/adaptor/`): `ChatAdaptor` trait with two implementations:
  - `OpenAICompletionAdaptor` — OpenAI Chat Completion API (`/chat/completions`)
  - `OpenAIResponseAdaptor` — OpenAI Responses API (`/responses`)
- **SSE parser** (`provider/sse.rs`): Parses Server-Sent Events from streaming HTTP responses
- **HTTP client** (`provider/client.rs`): Shared `reqwest` client with SSE stream handling

### 2. `windai-mcp` — MCP Client Management

Manages MCP server connections (stdio and streamable-HTTP) with session sharing and command normalization.

- **Registry** (`client/registry.rs`): Central event-driven manager (`Registry`/`RegistryHandle`). Uses a command channel pattern — all operations go through `acquire()`/`release()` which manage server lifecycle and session references. Broadcasts `ClientEvent` (Connecting/Connected/Disconnected/Error) via a broadcast channel.
- **Connector** (`client/connector.rs`): `ServerHandle` for individual server connections. Supports stdio (via `TokioChildProcess`) and streamable-HTTP transports. Implements connection deduplication for package-based servers (same npm/uvx package = single process).
- **Command Normalizer** (`client/cmd_normalizer.rs`): Normalizes `npx`/`bun`/`bunx`/`uvx`/`uv` commands to a canonical form for deduplication.
- **Types** (`client.rs`): `ServerParams`, `ClientStatus`, `ClientEvent`, `McpError`, `ClientSnapshot`, `ToolsWithId`, `CallToolParam`

### 3. `windai-core` (legacy)

Hollowed out — `src/lib.rs` is empty. Tests in `core/tests/` still reference `windai_core` paths. These will need to be migrated to reference the new crate structure when the old crate is removed.

### Data Flow

```
Consumer (external code)
  ├── handle_chat() → [Adaptor Selection] → build_request() → HTTP → parse_response()
  └── Registry::acquire() → ServerHandle::connect() → MCP tools
```

## Important Notes

- `AdaptorType` and `Model` are defined in BOTH `message.rs` and `model.rs` in the conversation crate (duplicated — `model.rs` is the canonical location).
- `Role` enum lives in `message.rs` and is reused by both the conversation schema and OpenAI API schema types.
- The MCP registry uses a **request queue** pattern (`mpsc::Sender<RegistryRequest>`) — all state mutations go through a single `run()` loop spawned by `tokio::spawn`.
- Connection deduplication in `connector.rs` uses a global `LazyLock<Mutex<HashMap>>` keyed by normalized package name.
- Tests in `core/tests/` are integration tests that require `API_KEY` env var and external MCP servers installed (`npx`, `uvx`, `bun`). Most are marked `#[ignore]`.

## Development Commands

```bash
# Build all workspace crates
cargo build

# Build individual crates
cargo build -p windai-conversation
cargo build -p windai-mcp

# Run unit tests (no external dependencies)
cargo test -p windai-conversation
cargo test -p windai-mcp

# Run integration tests (requires API_KEY env var)
API_KEY=xxx cargo test -p windai-core --test chat
API_KEY=xxx cargo test -p windai-core --test mcp

# Run specific test
cargo test -p windai-conversation provider::sse::tests::single_data_only

# Check compilation without building
cargo check
cargo check -p windai-conversation -p windai-mcp

# Run with debug logging
RUST_LOG=debug cargo test -p windai-conversation

# Format and lint
cargo fmt
cargo clippy
```

## Module Path Conventions

After the recent refactor (moving code from `windai-core/src/conversation/` → `windai/conversation/` and `windai-core/src/mcp_client/` → `windai/mcp/`):

- Inside `windai-conversation`: use `crate::message::X`, `crate::model::X`, `crate::tool::X`, `crate::provider::X`
- Inside `windai-mcp`: use `crate::client::X`, `crate::client::connector::X`, `crate::client::registry::X`
- Do NOT use `crate::conversation::` or `crate::mcp_client::` paths
