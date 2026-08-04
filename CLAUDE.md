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
wind-tui   ──depends-on──>  wind-core, wind-ai, wind-mcp, wind-rule, ratatui, crossterm
```

### `wind-core` — Central orchestration

`WindCore` (`lib.rs`) owns a `SqlitePool` and a `RegistryHandle`. It exposes:

```rust
WindCore::init_memory()                                          // In-memory SQLite (tests/ephemeral)
WindCore::init_local(Some("/path/to/windai.db"))                 // File-backed SQLite (None = default path)
WindCore::init_with_pool(pool)                                   // Init with external pool (creates its own Registry)
WindCore::init_with_pool_and_registry(pool, registry)            // Init with external pool + shared Registry (used in tests)
```

`WindCore::init()` (private) is the internal constructor used by all public init methods.

core.storage()              // &Storage — all CRUD access
core.storage().provider()   // &ProviderStorage
core.storage().model()      // &ModelStorage
core.storage().topic()      // &TopicStorage
core.storage().message()    // &MessageStorage
core.storage().mcp()        // &McpStorage
core.storage().agent()      // &AgentStorage
core.storage().prompt()     // &PromptStorage
core.storage().approval()   // &ToolApprovalStorage
core.registry()             // RegistryHandle — MCP client registry
```

The `Storage` struct (`storage.rs`) wraps `ProviderStorage`, `ModelStorage`, `TopicStorage`, `MessageStorage`, `McpStorage`, `AgentStorage`, `PromptStorage`, `ToolApprovalStorage`. All `create()` methods return `i64` (the new ID), not the full object — use `get()` to retrieve the record.

**Transactions**: `storage.tx(|inner| async { ... }).await` provides transactional execution. For multi-step transactions, use `storage.begin().await` → `StorageTx` with `.commit()` / `.rollback()`.

**`init_id_generator(machine_id)`** must be called before any `create()` operation. `WindCore::init_*()` methods do this automatically; standalone tests using `XxxStorage` directly must call it in setup. IDs are Snowflake-based via `ferroid`.

**SQL macros** (`storage.rs`): `insert!`, `update!`, `update_fields!`, `insert_fields!`, `delete_by_id!`, `select_fields!`, `get_by_id!`. Values must be `Option`-wrapped; `None` fields are skipped. `update!` appends `updated_at` and `WHERE id = ?` automatically.

**IMPORTANT — `update!` with `vec_to_str`**: When building an `UpdateMessage`, `Option` fields like `content`, `tools_allowed`, `tools_denied` are serialized via `utils::vec_to_str()`. This function returns `"[]"` for `None` input. The `update()` method in `MessageStorage` wraps the result in `Some(...)`, which means even `None` fields produce `Some("[]")` and are always included in the UPDATE SQL. To skip a field, do not wrap in `Some` — use `.map(...).transpose()` instead:

```rust
// WRONG — always updates the column, even when None:
("content", Some(utils::vec_to_str(data.content.as_deref())?))

// CORRECT — only updates when Some:
("content", data.content.as_ref().map(|c| utils::vec_to_str(Some(c)).unwrap()))
```

**Entity IDs**: Use Snowflake IDs via `storage::next_id()`. Do NOT rely on SQLite auto-increment.

### Agent System

The agent system replaces the old direct `ChatEngine` approach with a hierarchical, multi-agent architecture. Each `Topic` gets a `TopicRuntime` that manages a tree of agent tasks.

#### Core concepts

| Component | Role |
|-----------|------|
| `TopicRuntime` | Actor spawned per topic — owns the `TaskRegistry`, processes commands/notifications, coordinates parent-child agent relationships |
| `TopicRuntimeHandle` | Cloneable handle to a running `TopicRuntime` — `create_chat()`, `cancel_task()`, `subscribe()` |
| `TaskRegistry` | In-memory map of `binding_id → TaskEntry` tracking all running agents in a topic |
| `SyncTask` | A task actor (one per agent instance) that wraps `AgentRuntime` — handles start/cancel commands |
| `SyncTaskHandler` | Cloneable handle to `SyncTask` — `start()`, `cancel()` |
| `AgentRuntime` | The actual LLM interaction loop — runs `ChatLoops`, handles tool calls including agent tools |
| `AgentHost` | Trait (`async_trait`) — abstract interface for `AgentRuntime` to interact with the outside world (emit events, execute MCP tools, spawn sub-agents, list agents, list approvals) |
| `SyncHost` | The `AgentHost` impl used by `SyncTask` — bridges `AgentRuntime` back to `TopicRuntime` via `TopicMailbox` |

#### Agent lifecycle flow

```
User calls TopicRuntimeHandle::create_chat(user_input)
  → TopicRuntime receives CreateChat command
    → start_main_agent():
      1. Checks registry — only one main agent at a time
      2. Creates sub-topic for agent isolation
      3. Loads AgentBinding, AgentDefinition, ChatContext (model/provider/credentials/rules/tools)
      4. Creates user + assistant messages, emits MessageCreated events
      5. Calls start_task() → creates SyncTask, registers in TaskRegistry
      6. SyncTaskHandler::start(spec, config) → spawns AgentRuntime
    → AgentRuntime::run():
      Loop:
        ChatLoops::run() → stream of ChatEvent
        For each event:
          Partial → emit MessageDelta
          AwaitToolCall → make_tool_plan() → partition into:
            - exec_mcp: execute via AgentHost::execute_tool_calls()
            - exec_agent: parse agent calls, execute via AgentHost::spawn_agent()/list_agents()
            - denied: inject "tool call denied" errors
            - waiting: emit ApprovalRequired → pause until user approves
          Finish → emit Completed or Failed
```

#### Agent modes

| Mode | Behavior |
|------|----------|
| `Sync` | Runs synchronously; parent waits for completion before continuing |
| `Fork` | Runs with a copy of the parent agent's message context (forked from main task) |
| `Background` | Not yet implemented — designed for fire-and-forget sub-tasks |

#### Agent tools (virtual tools injected for LLM)

Agent tools use the `agent.` prefix (constant `AGENT_TOOL_PREFIX`). These are NOT MCP tools — they are intercepted by `AgentRuntime` and handled in-process.

| Tool | Purpose |
|------|---------|
| `agent.list_agents` | List available agent bindings in the current topic (key, alias, description) |
| `agent.spawn_agent` | Create a sub-agent with `agent_key`, `mode` (sync/background/fork), and `task` description |

Multiple `list_agents` calls in a single tool-request batch are coalesced into one query via `parse_agent_action()`.

#### Task mailbox protocol

`TopicMailbox` carries three message types:

- **`TopicCommand`** — external commands: `CreateChat`, `CancelTask`, `Approval`, `Shutdown`
- **`TaskNotification`** — upward events from `SyncTask`: `Started`, `Message`, `WaitingApproval`, `Completed`, `Failed`, `Cancelled`
- **`SupervisorRequest`** — internal requests between tasks: `SpawnAgent` (parent spawns child)

#### TaskRegistry pending children

When `AgentRuntime` processes a `spawn_agent` call, `TopicRuntime` inserts a `PendingChild` into the registry linking the parent and child `binding_id`s. When the child completes/fails, `walk_task()` resolves the pending entry and sends the `SpawnAgentResponse` back to the parent's waiting `oneshot` channel. This keeps the parent's AgentRuntime in a `WaitingChild` state until children finish.

#### Topic events (broadcast)

`TopicEvent` enum — external consumers subscribe via `TopicRuntimeHandle::subscribe()`:

- `Error` — binding/topic/parent ids + error string
- `Snapshot` — full message list for a topic
- `MessageCreated` — new message persisted
- `Message` — streaming delta chunk
- `MessageFinished` — message complete
- `TaskStatusChanged` — agent status transition
- `ApprovalRequired` — tool calls need user review

### Chat loops (low-level LLM interaction)

`ChatLoops` (`chat/loops.rs`) is the internal engine that replaced the old public `ChatEngine`. It is used by `AgentRuntime`, not called directly by external code.

**`ChatContext`**: Holds `Model`, `Provider`, `Credentials`, `ReqConfig`, `Option<JsonRule>`, `Option<Vec<Tools>>` — the resolved runtime config for an agent instance.

**`ChatLoops::run()`**: Takes `ChatContext`, an `assistant` message, and `contexts` (message history). Returns a `Stream<Item = ChatEvent>`. Handles the tool-call loop internally: when `AwaitToolCall` events occur, `AgentRuntime` processes them (partition, execute MCP tools, execute agent tools, handle approvals) and resumes `ChatLoops::run()` with updated assistant + contexts.

### Tool Approval Flow

Tool approval is governed by `ToolApprovalPolicy` on the `Topic` (or `AgentBinding`):

```rust
pub enum ToolApprovalPolicy {
    Manual,                    // All tools require manual approval
    AllowList(Vec<String>),   // Listed tools auto-execute; others require approval
    AllowAll,                 // All tools auto-execute (default)
}
```

**Agent-level override**: `AgentBinding.tool_approval_policy` overrides the topic-level policy for that agent instance.

When the model requests tool calls:

1. `AgentRuntime` calls `make_tool_plan()` which first checks `ToolApprovalStorage` for existing approval records on the current message
2. Calls that have `Approved`/`Denied` status are resolved accordingly
3. Unhandled calls are partitioned via `partition_tool_calls_by_policy()`
4. Approved calls split into MCP tools (executed via `AgentHost::execute_tool_calls()`) and agent tools (parsed by `parse_agent_action()` and executed via `AgentHost::spawn_agent()`/`list_agents()`)
5. Waiting calls trigger `ApprovalRequired` — `SyncHost` bubbles this up via `TaskNotification::WaitingApproval`
6. `TopicRuntime::handle_task_notification` calls `helper::save_approval_state()` which persists `ToolApprovalRequest` rows in the DB AND persists the assistant message state
7. External caller sends `Approval` command with `allow_ids`/`deny_ids`
8. `TopicRuntime::apply_approvals()` calls `ToolApprovalStorage::batch_set_status()` then `resume_task()` which re-loads the agent state and calls `AgentRuntime::run()` again

**Rejection markers**: Denied tools get `{"error": "tool call denied", "tool": "..."}` in their `FunctionCallOutput`.

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

### `wind-tui` — Terminal UI

Skeleton TUI application using `ratatui` + `crossterm`. Depends on all other crates. Currently renders a placeholder — the app loop + event handling structure is in place for further development.

### Database tables (SQLite, `schema.rs`)

`providers`, `models`, `credentials`, `topics`, `messages`, `chat_configs`, `mcp_servers`, `json_rule`, `prompt_modules`, `agent_definitions`, `topic_agent_bindings`, `tool_approval_requests`.

**Key constraints**: `topic_agent_bindings` has a partial unique index ensuring at most one enabled `main` role binding per `parent_topic_id`.

### Agent data model

**`AgentDefinition`** — what an agent *can do* (key, name, description, scope, prompt_modules, mcp_servers, context_policy, permission_policy, runtime_limits). Scopes: `Global` (reusable) or `TopicLocal` (topic-specific clone).

**`AgentBinding`** (`topic_agent_bindings`) — an agent *instance* in a topic (agent_id, role: `Main`/`Child`, mode: `Sync`/`Fork`/`Background`, status, model_id, chat_config_id, tool_approval_policy).

**`AgentStatus`**: `Created → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`

**`PromptModule`** — reusable prompt fragments assembled into an agent's system prompt. Referenced by `AgentDefinitionData.prompt_modules`.

**`ToolApprovalRequest`** — persisted per-tool-call approval record (binding_id, topic_id, message_id, tool_call_id, tool_name, arguments, status: `Pending`/`Approved`/`Denied`).

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

**Agent tool flow:** LLM calls `agent.list_agents` or `agent.spawn_agent` → `AgentRuntime.handle_await_tool_call()` identifies agent-prefixed calls → `parse_agent_action()` coalesces duplicate `list_agents` → `AgentHost::spawn_agent()` sends `SupervisorRequest::SpawnAgent` to `TopicRuntime` → `TopicRuntime` spawns child `SyncTask` → when child completes, `walk_task()` resolves the pending entry and sends response back.

**`ToolApprovalPolicy`**: Per-topic OR per-agent-binding enum controlling tool execution: `AllowAll` (default — all auto-execute), `AllowList(Vec<String>)` (listed tool names auto-execute), `Manual` (all require approval). Stored as JSON in the `topics.tool_approval_policy` / `topic_agent_bindings.tool_approval_policy` column. Agent-binding-level policy overrides topic-level.

**`create()` returns `i64`:** Call `get(id)` to retrieve the full record. This allows batch operations to allocate IDs without re-fetching.

**Agent topic isolation**: Each agent instance gets its own sub-topic (created via `helper::create_sub_topic()`). Messages from agent interactions are isolated to that sub-topic. The `parent_topic_id` on `AgentBinding` tracks the root topic that owns the agent hierarchy.

**`From<Message> for UpdateMessage`** (`models.rs`) preserves fields from the source message. Do NOT set optional fields to `None` unless you intend to clear them.

**Fork mode context**: When spawning an agent in `Fork` mode, `helper::create_fork_contexts()` copies the main agent's message history as the child's starting context — enabling the child to have full visibility of the parent's conversation.

## Agent skills

### Issue tracker

GitHub Issues on `evilArsh/windai` — use the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout — one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
