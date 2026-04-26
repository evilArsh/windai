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
    domain/             # Core business entities (no external dependencies)
      adaptor.rs        # AdaptorType enum (OpenAICompletion, OpenAIResponse)
      chat.rs           # ContentType, Message, Topic
      model.rs          # Model, ModelType
      provider.rs       # Provider, Credentials
    api/                # Unified transfer objects (for Tauri/HTTP/Node consumers)
      request.rs        # RequestConfig
      response.rs       # MessageCommon, MessageResponse, filter_context()
    adaptor/            # Provider adaptor implementations (depends on domain + api only)
      mod.rs            # Adaptor trait, ChatAdaptor trait, AdaptorError
      openai.rs         # OpenAICompletionAdaptor + OpenAIResponseAdaptor
      openai_completion.rs  # OpenAI Chat Completions API DTOs
      openai_response.rs    # OpenAI Responses API DTOs
    proxy/              # Request orchestration layer (depends on all layers below)
      mod.rs            # handle_chat() entry point
      client.rs         # HTTP client singleton
      forward.rs        # Request forwarding logic
      sse.rs            # SSE event parsing
      error.rs          # ProxyError, RequestError
    storage/            # SQLite persistence layer (depends on domain only)
      schema.rs         # Table creation SQL
      message.rs        # Message CRUD
      model.rs          # Model CRUD
      provider.rs       # Provider + Credentials CRUD
      topic.rs          # Topic CRUD
    env.rs              # App directory management (~/.windai/, configurable via WINDAI_ROOT_DIR)
```

## Key Architecture

**Layered Dependency Flow** (dependencies only go downward):

```
proxy          ← depends on: adaptor, api, storage, domain
  ↓
adaptor  storage  ← independent of each other
  ↓        ↓
api   ←── domain  ← no dependencies
```

**Unified Proxy Pattern**: The library provides a single entry point (`proxy::handle_chat`) that routes requests to different AI providers. The flow is:

1. `handle_chat` receives user input + model selection + topic_id
2. Retrieve message context from storage
3. Resolve model details (adaptor, provider, credentials)
4. Adaptor translates unified `RequestConfig` into provider-specific request format
5. Forward request and handle response (including SSE streaming)

**Domain Layer**: Core business entities (`Message`, `Model`, `Provider`, `Credentials`, `Topic`, `AdaptorType`). These describe what the system knows, independent of how they're stored or transmitted.

**API Layer**: Unified transfer objects for external consumers (Tauri, HTTP, Node plugins). `MessageCommon` is the adaptor output — a flattened message without DB fields. `MessageResponse` wraps a domain `Message` with additional model/provider metadata for rich API responses.

**Adaptor System**: Different provider APIs are normalized through adaptors. Currently supports:
- `OpenAICompletion` — standard Chat Completions API
- `OpenAIResponse` — OpenAI Responses API

Adaptors are registered by `AdaptorType` enum. The adaptor layer is independent of proxy — it has its own `AdaptorError` and knows nothing about HTTP transport.

**Storage Layer**: Uses `rusqlite` with `bundled` feature. Tables: `providers`, `models`, `credentials`, `messages`, `topics`. DAO methods are implemented as `impl Storage` methods across files in `storage/`. The `lock_db!` macro wraps the `Mutex<Connection>` for thread safety. Message `index` auto-increments by 10 per insert to allow up to 10 mid-sequence inserts between any two messages.

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

- `providers`: id, name (unique), alias, description, base_url, doc, active
- `models`: id (integer PK), name, alias, provider_id, adaptor, modalities (JSON array), active, icon, endpoint, frequency
- `credentials`: id, provider_id, api_key, active
- `messages`: id (autoincrement), from_id, role, raw_content, content, reasoning_content, transcript, content_type, model_id, topic_id, index (default max+10), stream, is_boundary, input_tokens, output_tokens, created_at, updated_at
- `topics`: id, parent_id, label, icon, created_at, max_context, index
- Indices: `idx_messages_topic(topic_id, index)`

## Important Notes

- The global async runtime is created via `rt()` in `lib.rs` (multi-thread, 4 workers). Use this for blocking sync-to-async bridging.
- Edition is `2024`, workspace resolver is `"3"`.
- Database path defaults to `~/.windai/windai.db`, overridable via `WINDAI_ROOT_DIR`.
- `rusqlite` uses the `bundled` feature — no system SQLite required.
