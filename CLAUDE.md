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
WindCore::init()                                            // Standard init (creates its own Registry)
WindCore::init_with_pool(pool)                              // Init with external pool (creates its own Registry)
WindCore::init_with_pool_and_registry(pool, registry)       // Init with external pool + shared Registry (used in tests)

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
   - `is_tool_request()` or `is_tool_result()` → call `resume_tool_approval()`, which may return `Some(ChatEvent::AwaitToolCall)` (more tools need manual approval → yield event, save state, return) or `None` (all resolved → proceed to `start_chat()`)
4. `start_chat()`: Build request via adaptor, apply JSON rules, send. Loop on tool calls.

**Chat events**: `Created → Partial × N → (AwaitToolCall → [resume])? → Finish`

**Return type**: `start()` returns `Pin<Box<dyn Stream<Item = ChatEvent> + '_>>`. All stream return paths in `start_prepare()` must be wrapped in `Box::pin()`.

### Tool Approval Flow

Tool approval is governed by `ToolApprovalPolicy` on the `Topic`:

```rust
pub enum ToolApprovalPolicy {
    Manual,                    // All tools require manual approval
    AllowList(Vec<String>),   // Listed tools auto-execute; others require approval
    AllowAll,                 // All tools auto-execute (default)
}
```

When the model requests tool calls:

1. Engine appends `tool_request` to `assistant.content`
2. `partition_tool_calls_by_policy()` splits tools into auto-approved and manual-review groups
3. Auto-approved tools execute immediately and append results
4. If manual-review tools remain → `assistant` saved to DB via `assistant.into()` → `UpdateMessage`, yield `AwaitToolCall`
5. Caller sets `tools_allowed` (approve by call ID) or `tools_denied` (reject) via `message().update()`
6. Caller re-invokes `engine.start()` — `start_prepare()` sees `is_tool_request()`, calls `resume_tool_approval()`
7. `resume_tool_approval()` (`chat/function_call.rs`):
   - `find_pending_calls()`: Reverse-scan `assistant.content` for unexecuted tool calls
   - `contexts.extend(assistant.content.iter().cloned())` — restores previous context
   - `partition_tool_calls_by_policy()` again: any policy-allowed tools execute immediately
   - `allowed_set` vs `denied_set`: approved tools execute, denied tools get `{"error": "User denied this tool call"}`
   - Unreviewed tools (not in `tools_allowed` or `tools_denied`) → return `Some(ChatEvent::AwaitToolCall)` for another approval round
   - Clears `tools_allowed` and `tools_denied` to `Some(vec![])` after processing
   - Returns error if no tools were approved/denied (prevents infinite loop)
   - Append `tool_result` to both `assistant.content` and `contexts`
8. Returning `Some(event)` → engine saves assistant state and yields the event; returning `None` → engine proceeds to `start_chat()`

**`From<Message> for UpdateMessage`** (`models.rs`) preserves `tools_allowed` and `tools_denied` from the source message. Do NOT set them to `None` — that would clear the approval state on save.

**Rejection markers**: Denied tools get `FunctionCallOutput` with `content: {"error": "User denied this tool call"}`. Tests assert these markers are absent in the approve path.

**`resume_tool_approval()` returns `Option<ChatEvent>`**: `None` means all pending tools are resolved; `Some(AwaitToolCall)` means unreviewed tools remain and need another approval round. This supports batch-partitioned approval where the user may approve only a subset of tools per round.

### `wind-ai` — Provider abstraction

`ChatAdaptor` trait (`provider/adaptor.rs`): `build_request()` / `parse_response()` / `parse_stream_chunk()`. Two implementations: `OpenAICompletionAdaptor` (for `/chat/completions`) and `OpenAIResponseAdaptor` (for `/responses`).

**OpenAIResponseAdaptor**: Function call output must use `Value::String(data.content.to_string())` — not `data.content.clone()`. The Responses API expects a JSON string for the `output` field of `function_call_output` items.

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

`providers`, `models`, `credentials`, `topics` (with `tool_approval_policy` JSON column), `messages`, `chat_configs`, `mcp_servers`, `topic_mcp_servers`, `json_rule`.

### Test organization

| File | Content |
|------|---------|
| `tests/storage.rs` | Integration tests for all `*Storage` structs — CRUD, validation, cascade, batch |
| `tests/core_chat.rs` | Non-MCP chat tests: basic chat, streaming, error handling, JSON rules, message history, persistence |
| `tests/core_chat_mcp.rs` | MCP tool approval/rejection tests — per-test WindCore with shared RegistryHandle |
| `tests/chat.rs` | AI adaptor tests (needs `.env`) |
| `tests/mcp.rs` | MCP integration tests (needs `.env`) |
| `tests/common/lib.rs` | Shared test helpers: `init_test_core()`, `McpTestEnv`, MCP server params |
| `src/storage.rs` (cfg test) | SQL macro unit tests |

**Test architecture for MCP tests:**
- Shared `RegistryHandle` in a dedicated long-lived tokio runtime thread (`OnceLock` + `mpsc::sync_channel`)
- MCP `everything` server acquired once in the shared registry via `shared_mcp_registry()`
- Each test creates its **own** `WindCore` via `init_test_core_with_registry(shared_mcp_registry())` — `sqlite::memory:` pools use `max_connections(1)` to ensure all queries hit the same in-memory DB
- MCP server record created per-test via `create_mcp_server_record()` (not globally shared)
- Each test creates its own provider/model/topic via `seed_mcp_data()`
- Pure chat tests (no MCP) use the same pattern with an empty shared registry via `shared_chat_registry()`

**Test helpers in `tests/common/lib.rs`:**
- `init_test_core()` — standalone core with fresh pool + fresh registry
- `init_test_core_with_registry(registry)` — fresh pool + shared registry (for MCP tests)
- `McpTestEnv` — holds env-loaded MCP provider config for completion and responses adaptors
- `everything_params()` / `create_everything_server_params()` — MCP server connection params

### VS Code debugging

`.vscode/launch.json` — uses `cargo build --tests` (NOT `cargo test`) to bypass `codelldb-launch` which crashes on Windows MSVC. LLDB debugs the compiled test binary directly. Two template configs: `Debug MCP Test` and `Debug Chat Test` — modify `filter.name`, `args[0]`, and `--ignored` flag per test function.

### Key patterns

**Adding a new provider adaptor:** Implement `ChatAdaptor`, add variant to `AdaptorType`, register in `get_chat_adaptor()`.

**Adding a new provider:** Insert into `providers`, add credentials, insert models — no code changes.

**MCP tool flow:** Topic → `topic_mcp_servers` → Registry acquire → tools discovered → prefixed names → `tool_calls` in response → `partition_tool_calls_by_policy()` splits by `Topic.tool_approval_policy` → auto-approved execute immediately, manual-review yield `AwaitToolCall` → `resume_tool_approval()` on resume → results as context → loop.

**`ToolApprovalPolicy`**: Per-topic enum controlling tool execution: `AllowAll` (default — all auto-execute), `AllowList(Vec<String>)` (listed tool names auto-execute), `Manual` (all require approval). Stored as JSON in the `topics.tool_approval_policy` column. Replaces the old `auto_approves: Option<Vec<String>>` field. MCP-server-level `auto_approves` has been removed entirely — approval policy is now topic-scoped only.

**`create()` returns `i64`:** Call `get(id)` to retrieve the full record. This allows batch operations to allocate IDs without re-fetching.

**`build_chat_context()`** (`chat/context.rs`): Builds context from `raw_messages` up to `user_message_id` (messages after it are split off). For each message, pops the last content item — must be `is_simple()`. Assistant content (tool call frames) is added separately by `resume_tool_approval()` via `contexts.extend(assistant.content.iter().cloned())`.
