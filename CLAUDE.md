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
core.storage()              // &Storage — all CRUD access
core.storage().provider()   // &ProviderStorage
core.storage().model()      // &ModelStorage
core.storage().topic()      // &TopicStorage
core.storage().message()    // &MessageStorage
core.storage().mcp()        // &McpStorage
core.chat()                 // ChatEngine — conversation loop
core.registry()             // RegistryHandle — MCP client registry
```

The `Storage` struct (`storage.rs`) wraps `ProviderStorage`, `ModelStorage`, `TopicStorage`, `MessageStorage`, `McpStorage`. All `create()` methods return `i64` (the new ID), not the full object — use `get()` to retrieve the record.

**`init_id_generator(machine_id)`** must be called before any `create()` operation. `WindCore::init()` does this automatically; standalone tests using `XxxStorage` directly must call it in setup.

**SQL macros** (`storage.rs`): `insert!`, `update!`, `update_fields!`, `insert_fields!`, `delete_by_id!`, `select_fields!`, `get_by_id!`. Values must be `Option`-wrapped; `None` fields are skipped. `update!` appends `updated_at` and `WHERE id = ?` automatically.

**IMPORTANT — `update!` with `vec_to_str`**: When building an `UpdateMessage`, `Option` fields like `content`, `tools_allowed`, `tools_denied` are serialized via `utils::vec_to_str()`. This function returns `"[]"` for `None` input. The `update()` method in `MessageStorage` wraps the result in `Some(...)`, which means even `None` fields produce `Some("[]")` and are always included in the UPDATE SQL. To skip a field, do not wrap in `Some` — use `.map(...).transpose()` instead:

```rust
// WRONG — always updates the column, even when None:
("content", Some(utils::vec_to_str(data.content.as_deref())?))

// CORRECT — only updates when Some:
("content", data.content.as_ref().map(|c| utils::vec_to_str(Some(c)).unwrap()))
```

**`ChatEngine`** (`chat/engine.rs`) — the core loop:

1. `load_info()`: Fetch topic, model, provider, credentials, chat config, MCP tools from DB
2. `build_chat_context()`: Build message history context (respects `is_boundary`, `max_context`). Messages after `user_message_id` are excluded — assistant content is added separately via `resume_tool_approval()`
3. `start_prepare()`: Load the assistant message; check `.content.last()` to determine state:
   - `None` → new chat → `start_chat()`
   - `is_simple()` → already completed → error
   - `is_tool_request()` or `is_tool_result()` → resume → `resume_tool_approval()` then `start_chat()`
4. `start_chat()`: Build request via adaptor, apply JSON rules, send. Loop on tool calls.

**Chat events**: `Created → Partial × N → (AwaitToolCall → [resume])? → Finish`

### Tool Approval Flow

When the model requests tool calls that are not auto-approved:

1. Engine appends `tool_request` to `assistant.content`, yields `AwaitToolCall`
2. Engine **saves** assistant state (`content`, etc.) to DB via `assistant.into()` → `UpdateMessage`
3. Caller sets `tools_allowed` (approve) or leaves it empty (reject) via `message().update()`
4. Caller re-invokes `engine.start()` — `start_prepare()` sees `is_tool_request()`, calls `resume_tool_approval()`
5. `resume_tool_approval()` (`chat/function_call.rs`):
   - `find_pending_calls()`: Reverse-scan `assistant.content` for tool requests not yet executed
   - `contexts.extend(assistant.content.iter().cloned())` — restores previous context
   - Execute approved tools, mark others as denied with `{"error": "User denied this tool call"}`
   - Append `tool_result` to both `assistant.content` and `contexts`
6. `start_chat()` sends request with full context — model continues

**`From<Message> for UpdateMessage`** (`models.rs`) preserves `tools_allowed` and `tools_denied` from the source message. Do NOT set them to `None` — that would clear the approval state on save, causing `resume_tool_approval()` to treat all tools as rejected.

**Rejection markers**: Unapproved tools get `FunctionCallOutput` with `content: {"error": "User denied this tool call"}`. Tests assert these markers are absent in the approve path.

### `wind-ai` — Provider abstraction

`ChatAdaptor` trait (`provider/adaptor.rs`): `build_request()` / `parse_response()` / `parse_stream_chunk()`. Two implementations: `OpenAICompletionAdaptor` (for `/chat/completions`) and `OpenAIResponseAdaptor` (for `/responses`).

`Message` (`message.rs`) carries role, content (vec of Content variants), reasoning_content, token counts, tool_calls. `append_chunk()` merges streaming deltas. `ReqConfig` holds temperature, top_p, max_tokens, stream, penalties, parallel_tool_calls, reasoning.

**Key methods on `Message`**:
- `is_simple()` — no tool_calls, role is User or Assistant
- `is_tool_request()` — role is Assistant and tool_calls is non-empty
- `is_tool_result()` — role is Tool

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
| `tests/core_chat.rs` | Non-MCP chat tests: basic chat, streaming, error handling, JSON rules, message history, persistence |
| `tests/core_chat_mcp.rs` | MCP tool approval/rejection tests with global WindCore singleton |
| `tests/chat.rs` | AI adaptor tests (needs `.env`) |
| `tests/mcp.rs` | MCP integration tests (needs `.env`) |
| `tests/common/lib.rs` | Shared test helpers: `init_test_core()`, `McpTestEnv`, MCP server params |
| `src/storage.rs` (cfg test) | SQL macro unit tests |

**Test architecture for MCP tests:**
- Global `WindCore` singleton (`OnceLock` + `tokio::sync::Mutex` double-check locking)
- `shared_cache(true)` SQLite `:memory:` pool shared across all tests
- MCP `everything` server acquired once via `global_mcp_server_id()`
- Each test creates its own provider/model/topic via `seed_mcp_data()`

### VS Code debugging

`.vscode/launch.json` — uses `cargo build --tests` (NOT `cargo test`) to bypass `codelldb-launch` which crashes on Windows MSVC. LLDB debugs the compiled test binary directly. Two template configs: `Debug MCP Test` and `Debug Chat Test` — modify `filter.name`, `args[0]`, and `--ignored` flag per test function.

### Key patterns

**Adding a new provider adaptor:** Implement `ChatAdaptor`, add variant to `AdaptorType`, register in `get_chat_adaptor()`.

**Adding a new provider:** Insert into `providers`, add credentials, insert models — no code changes.

**MCP tool flow:** Topic → `topic_mcp_servers` → Registry acquire → tools discovered → prefixed names → `tool_calls` in response → `execute_function_calls()` → results as context → loop.

**`create()` returns `i64`:** Call `get(id)` to retrieve the full record. This allows batch operations to allocate IDs without re-fetching.

**`build_chat_context()`** (`chat/context.rs`): Builds context from `raw_messages` up to `user_message_id` (messages after it are split off). For each message, pops the last content item — must be `is_simple()`. Assistant content (tool call frames) is added separately by `resume_tool_approval()` via `contexts.extend(assistant.content.iter().cloned())`.
