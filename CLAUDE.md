# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**windai-core** is a Rust workspace providing a cross-platform AI capability library. It acts as a unified proxy layer over multiple AI providers (currently focused on OpenAI-compatible APIs), with SQLite-based local persistence.

## Repository Structure

```
Cargo.toml              # Workspace root, resolver = "3", edition = "2024"
windai/
  Cargo.toml            # Main crate
  src/
    lib.rs              # Entry point, exposes modules + global tokio runtime `rt()`
    adaptor.rs          # AdaptorType enum (OpenAICompletion, OpenAIResponse)
    adaptor/openai/     # OpenAI-compatible adaptor implementations
      completion.rs     # OpenAI Chat Completions API
      response.rs       # OpenAI Responses API
    dto.rs              # Unified ChatConfig and ChatMessage structs
    env.rs              # App directory management (~/.windai/, configurable via WINDAI_ROOT_DIR)
    message.rs          # Persistent Message struct (stored in SQLite)
    model.rs            # Model struct with ModelType (Chat, Embedding, etc.)
    provider.rs         # Provider and Credentials structs
    proxy/              # Request routing, forwarding, SSE streaming
      route.rs          # Chat handler entry point
      client.rs         # HTTP client for provider requests
      forward.rs        # Request forwarding logic
      sse.rs            # Server-Sent Events streaming
      error.rs          # Proxy error types
    storage/            # SQLite persistence layer
      schema.rs         # Table creation SQL (providers, models, credentials, messages)
      dao/              # DAO implementations
        message.rs      # Message CRUD (auto-increments index by 10 for mid-insert support)
        model.rs        # (stub)
        provider.rs     # (stub)
```

## Key Architecture

**Unified Proxy Pattern**: The library provides a single entry point (`proxy::route::handle_chat`) that routes requests to different AI providers. The flow is:

1. `handle_chat` receives user input + model selection + topic_id
2. Retrieve message context from storage
3. Resolve model details (adaptor, provider, credentials)
4. Adaptor translates unified `ChatConfig` into provider-specific request format
5. Forward request and handle response (including SSE streaming)

**Adaptor System**: Different provider APIs are normalized through adaptors. Currently supports:
- `OpenAICompletion` — standard Chat Completions API
- `OpenAIResponse` — OpenAI Responses API

Adaptors are registered by `AdaptorType` enum. Adding a new provider means adding a new variant and implementation.

**Storage Layer**: Uses `rusqlite` with `bundled` feature. Tables: `providers`, `models`, `credentials`, `messages`. DAO methods are implemented as `impl Storage` methods across files in `storage/dao/`. The `lock_db!` macro wraps the `Mutex<Connection>` for thread safety. Message `index` auto-increments by 10 per insert to allow up to 10 mid-sequence inserts between any two messages.

## Commands

```bash
# Build
cargo build

# Check (no output binary)
cargo check

# Run tests
cargo test

# Run a specific test
cargo test test_name

# Run tests for windai only
cargo test -p windai

# Clippy (lint)
cargo clippy

# Format
cargo fmt
```

## SQLite Schema Details

- `providers`: id, name (unique), alias, description, base_url, doc
- `models`: id (text PK), alias, provider_id, adaptor_name, modalities (JSON array), active
- `credentials`: id, provider_id, api_key, active
- `messages`: id (autoincrement), adaptor, from_id (nullable), role, content, content_type, model_id, topic_id, index (default 10), created_at, updated_at
- Indices: `idx_messages_topic(topic_id, index)`

## Important Notes

- The global async runtime is created via `rt()` in `lib.rs` (multi-thread, 4 workers). Use this for blocking sync-to-async bridging.
- Edition is `2024`, workspace resolver is `"3"`.
- Database path defaults to `~/.windai/windai.db`, overridable via `WINDAI_ROOT_DIR`.
- `rusqlite` uses the `bundled` feature — no system SQLite required.
