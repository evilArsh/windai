# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                           # Build entire workspace
cargo build -p wind-core              # Build a specific crate
cargo test                            # Run all tests (SQLite, no external DB)
cargo test -p wind-core               # Tests for a specific crate
cargo test -p wind-core -- test_chat  # Integration tests that need API keys (.env)
cargo test -p wind-core -- --ignored  # MCP tests (require npx/uvx installed)
```

Copy `.env.example` to `.env` and fill in `TEST_*` values for integration tests.

## Architecture

```
wind-core  ──depends-on──>  wind-ai, wind-mcp, wind-rule
wind-mcp   ──depends-on──>  rmcp
wind-ai    ──depends-on──>  reqwest
wind-rule  ──depends-on──>  evalexpr
```

### `wind-core` — Central orchestration

`WindCore` (`lib.rs`) owns a `SqlitePool` and a `RegistryHandle`. It exposes:

```rust
core.storage()          // &Storage — all CRUD access
core.storage().provider()    // &ProviderStorage
core.storage().model()       // &ModelStorage
core.storage().topic()       // &TopicStorage
core.storage().message()     // &MessageStorage
core.chat()             // ChatEngine — conversation loop
```

The `Storage` struct (`storage.rs`) wraps `ProviderStorage`, `ModelStorage`, `TopicStorage`, `MessageStorage`. `McpStorage` is standalone. All `create()` methods return `i64` (the new ID), not the full object — use `get()` to retrieve the record.

**`init_id_generator(machine_id)`** must be called before any `create()` operation. `WindCore::init()` does this automatically; standalone tests using `XxxStorage` directly must call it in setup.

**SQL macros** (`storage.rs`): `insert!`, `update!`, `update_fields!`, `insert_fields!`, `delete_by_id!`, `select_fields!`, `get_by_id!`. Values must be `Option`-wrapped; `None` fields are skipped. `update!` appends `updated_at` and `WHERE id = ?` automatically.

**`ChatEngine`** (`chat/engine.rs`) — the core loop:
1. Load topic, model, provider, credentials, config from DB
2. Build message context from history (respects `is_boundary`, `max_context`)
3. Fetch MCP tools registered to the topic
4. Build request via adaptor, apply JSON rules
5. Send request; on tool calls, execute via MCP and loop back

Emits `ChatEvent::Created → Partial x N → Finish`.

### `wind-ai` — Provider abstraction

`ChatAdaptor` trait (`provider/adaptor.rs`): `build_request()` / `parse_response()` / `parse_stream_chunk()`. Two implementations: `OpenAICompletionAdaptor` (for `/chat/completions`) and `OpenAIResponseAdaptor` (for `/responses`).

`Message` (`message.rs`) carries role, content (vec of Content variants), reasoning_content, token counts, tool_calls. `append_chunk()` merges streaming deltas. `ReqConfig` holds temperature, top_p, max_tokens, stream, penalties, parallel_tool_calls, reasoning.

Streaming: `handle_chat()` → `request_sse()` → buffer by `\n\n` → `parse_stream_chunk()`.

### `wind-mcp` — MCP client registry

Actor pattern: `Registry::new()` spawns a tokio task; all interaction via `RegistryHandle` (cloneable, `mpsc` channels).

- `acquire(session_id, params)` — start/reuse server (ref-counted across sessions)
- `release(session_id, name)` — drop a session reference
- `list_all_tools()` / `call_tool(param)` — discovery & execution

Transports: **Stdio** (child process, with dedup map for concurrent starts) and **Streamable HTTP**. Tool names: `{server_name}0m0{tool_name}`.

### `wind-rule` — JSON rule engine

Rules stored in `json_rule` table keyed by `(provider_id, adaptor)`. Applied to request bodies before each API call.

| Op | Purpose |
|----|---------|
| `set` | Set value at JSON path (creates intermediate objects) |
| `remove` | Delete field at JSON path |
| `map_value` | Map field value via lookup table → merge result into body root |
| `compute` | Evaluate `evalexpr` over `$value` + `$ctx.*` → replace field |
| `when` | Conditional: `cond` (eq/neq/gt/lt/contains/and/or/not/in) with `then`/`else` sub-rules |

### Database tables (SQLite, `schema.rs`)

`providers`, `models`, `credentials`, `topics`, `messages`, `chat_configs`, `mcp_servers`, `topic_mcp_servers`, `json_rule`.

### Test organization

| File | Content |
|------|---------|
| `tests/storage.rs` | Integration tests for all `*Storage` structs — CRUD, validation, cascade, batch |
| `tests/chat_engine.rs` | ChatEngine integration (needs `.env`) |
| `tests/core_chat.rs` | WindCore + chat + json_rule + persistence (needs `.env`) |
| `tests/chat.rs` | AI adaptor tests (needs `.env`) |
| `tests/mcp.rs` | MCP integration tests (needs `.env`) |
| `src/storage.rs` (cfg test) | SQL macro unit tests |

### Key patterns

**Adding a new provider adaptor:** Implement `ChatAdaptor`, add variant to `AdaptorType`, register in `get_chat_adaptor()`.

**Adding a new provider:** Insert into `providers`, add credentials, insert models — no code changes.

**MCP tool flow:** Topic → `topic_mcp_servers` → Registry acquire → tools discovered → prefixed names → `tool_calls` in response → `execute_function_calls()` → results as context → loop.

**`create()` returns `i64`:** Call `get(id)` to retrieve the full record. This allows batch operations to allocate IDs without re-fetching.
