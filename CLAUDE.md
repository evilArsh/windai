# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**windai-core** is a Rust workspace providing a cross-platform AI capability library. It acts as a unified proxy layer over multiple AI providers (currently focused on OpenAI-compatible APIs), with SQLite-based local persistence.

## Repository Structure

```
Cargo.toml              # Workspace root, resolver = "3", edition = "2024"
windai/                  # Main crate — adaptor, proxy, storage, api
windai-domain/           # Domain entities crate — zero external deps beyond serde/strum
mcp/                     # Model Context Protocol stub (not yet implemented)
```

## Key Architecture

**Layered Dependency Flow** (dependencies only go downward):

```
windai (proxy → adaptor, api, storage)     mcp (stub)
  ↓                        ↓
windai-domain  ←──────────┘
  (adaptor, chat, model, provider enums/structs — no logic)
```

**Domain Layer** (`windai-domain`): Pure data types shared across the workspace. Contains:

- `AdaptorType` enum (OpenAICompletion, OpenAIResponse)
- `Role` (System, User, Assistant, Tool, Developer), `ContentType` (Text, Image, Audio, File)
- `Message` (with `derive_builder::Builder`), `MessageContent`, `Topic`
- `Model` (with `ModelType`: Chat, Embedding, Reranker, Audio, Video), `Provider`, `Credentials`
- Depends only on `serde`, `strum`, `derive_builder`. No internal logic.

**API Layer** (`windai::api`): Unified transfer objects for external consumers (Tauri, HTTP, Node).

- `ChatConfig` — sampling parameters (temperature, top_p, max_tokens, stream, reasoning, etc.)
- `ChatInput` / `ChatMessageContext` — request-side message formats
- `ChatMessageBase` — adaptor output; flattened message without DB fields. Has `apply_to_message()` to merge streaming chunks into a domain `Message`.
- `ChatMessage` — wraps a domain `Message` with `model_name`, `provider_name`, `provider_id`, `adaptor`. Used as the unified response type throughout the proxy layer.

**Adaptor Layer** (`windai::adaptor`): Translates between unified types and provider-specific wire formats.

- `Adaptor` trait + `ChatAdaptor` trait (`build_request`, `parse_response`, `parse_stream_chunk`)
- `get_chat_adaptor(AdaptorType) -> Box<dyn ChatAdaptor>` factory
- `get_default_endpoint(AdaptorType) -> String` for default API paths
- `OpenAICompletionAdaptor` — Chat Completions API (`/chat/completions`)
- `OpenAIResponseAdaptor` — Responses API (`/responses`)
- `sse.rs` — SSE protocol parser (`SseBlock::parse_all` / `SseBlock::parse`), handles `data:`, `event:`, `id:`, `retry:`, comment fields
- Adaptors have their own `AdaptorError` and know nothing about HTTP transport.

**Proxy Layer** (`windai::proxy`): Request orchestration — the only layer that depends on everything below.

- `handle_chat(user_input, topic_id, model_id, config) -> impl Stream<Item = ChatMessage>` is the main entry point
- Flow: resolve model/provider/credentials from storage → build adaptor request → forward via HTTP → parse response (SSE streaming or single response) → yield `ChatMessage`s
- `StreamContext` holds all state needed for a chat round
- `filter_chat_contexts()` — pairs User-Assistant messages from history, capped by `max_context`
- `client.rs` — global `reqwest::Client` singleton (600s timeout)
- `forward.rs` — `request()`, `request_sse()`, `handle_response()`, `handle_stream()`
- `ProxyError` enum covers: Io, Json, Request, Internal, UrlParse, Adaptor, Storage

**Storage Layer** (`windai::storage`): SQLite via `rusqlite` (bundled). Tables: `providers`, `models`, `credentials`, `messages`, `topics`. DAO methods as `impl Storage` across `message.rs`, `model.rs`, `provider.rs`, `topic.rs`. The `lock_db!` macro wraps `Mutex<Connection>`. Global singleton via `storage::global()`.

**Environment** (`windai::env`): App directory at `~/.windai/` (overridable via `WINDAI_ROOT_DIR`). Database file: `windai.db`.

## Commands

```bash
cargo build                          # Build all workspace crates
cargo check                          # Fast compile check (no binary)
cargo test                           # Run all tests
cargo test -p windai                 # Run windai crate tests only
cargo test test_name                 # Run a specific test
cargo clippy                         # Lint
cargo fmt                            # Format
```

## SQLite Schema Details

- `providers`: id, name (unique), alias, description, base_url, doc, active, created_at, updated_at
- `models`: id (integer PK), name (unique), alias, provider_id, adaptor, modalities (JSON array), active, icon, endpoint, frequency, created_at, updated_at
- `credentials`: id, provider_id, api_key, active, created_at, updated_at
- `messages`: id (autoincrement), from_id, role, raw_content, content (JSON array of MessageContent), reasoning_content, transcript, model_id, topic_id, index (default max+10), stream, is_boundary, input_tokens, output_tokens, created_at, updated_at
- `topics`: id, parent_id, label, icon, max_context, index, created_at, updated_at
- Indices: `idx_models_provider(provider_id)`, `idx_credentials_provider(provider_id)`, `idx_messages_topic(topic_id, index)`

## Important Notes

- The global async runtime is created via `rt()` in `windai/src/lib.rs` (multi-thread, 4 workers). Use this for blocking sync-to-async bridging.
- Edition is `2024`, workspace resolver is `"3"`.
- Database path defaults to `~/.windai/windai.db`, overridable via `WINDAI_ROOT_DIR`.
- `rusqlite` uses the `bundled` feature — no system SQLite required. Also enables `backup` and `hooks`.
- Message `index` auto-increments by 10 per insert to allow up to 10 mid-sequence inserts between any two messages.
- `ChatMessageBase::apply_to_message()` accumulates streaming deltas: appends to `content[0].content`, concatenates `reasoning_content`, and accumulates token counts.
- The `Credentials` domain type has a `from_env()` constructor that reads `API_KEY`. This is used in tests.
