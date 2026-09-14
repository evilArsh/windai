# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                           # Build entire workspace
cargo build -p wind-core              # Build a specific crate
cargo test                            # Run all tests (SQLite, no external DB)
cargo test -p wind-core               # Tests for a specific crate
cargo test -p wind-http               # HTTP route/facade/mirror tests (no .env needed)
cargo test -p wind-core -- test_chat  # Integration tests that need API keys (.env)
cargo test -p wind-core -- --ignored  # .env-gated integration tests
cargo test -p wind-core --test core_chat -- --include-ignored --test-threads=1  # one file's ignored tests
```

Copy `.env.example` to `.env` and fill in `TEST_*` values for the `.env`-gated tests.

**Test file status** (so you don't expect dead tests to run):
- `windai/core/tests/storage.rs` — active, no `.env` needed
- `windai/core/tests/core_chat.rs` — active; its one test is `#[ignore]`d behind `.env`
- `windai/core/tests/chat.rs` — AI adapter tests, `#[ignore]` behind `.env`
- `windai/http/tests/*` — active, no `.env`; drive `app(state)` via `tower::ServiceExt::oneshot`

## Architecture

```
wind-http  ──depends-on──>  wind-core
wind-tui   ──depends-on──>  wind-core, wind-ai, wind-mcp, wind-rule, ratatui, crossterm
wind-core  ──depends-on──>  wind-ai, wind-mcp, wind-rule
wind-mcp   ──depends-on──>  rmcp
wind-ai    ──depends-on──>  reqwest
wind-rule  ──depends-on──>  evalexpr
```

`wind-core` is the shared business core; `wind-ai`/`wind-mcp`/`wind-rule` are capability crates. **Nothing depends on `wind-http`** — it is just one adapter over the core (future targets: napi-rs, Android). `arch.md` at repo root is the authoritative `wind-http` architecture design (layering, facade, DTO contract, SSE, OpenAPI) — read it before changing `wind-http`.

The vocabularies of this file and `README.md` are kept in sync with the code — if you find a mismatch, trust the code and fix the doc.

### `wind-core` — Central orchestration

`WindCore` (`lib.rs`) is a process-level runtime root owning a `DbPool` and a `RegistryHandle`. One instance per process/tests — independent runtimes share nothing. Public surface:

```rust
WindCore::init_memory()                                          // In-memory SQLite (tests/ephemeral)
WindCore::init_local()                                           // File-backed SQLite at app_dirs().db_path()
WindCore::init_with_pool(pool)                                   // Init with external pool (own Registry)
WindCore::init_with_pool_and_registry(pool, registry)            // Init with external pool + shared Registry (tests)

core.storage()              // &Storage — all CRUD access
core.registry()             // &RegistryHandle — MCP client registry
core.fetch_topic(id)        // TopicRuntimeHandle — get-or-create (panics after shutdown())
core.shutdown().await
```

All four init methods converge on `init_with_pool_and_registry`, which runs `schema::init_schema` + `storage::init_id_generator`. The private `init(db_url)` is only used by `init_local`/`init_memory`. There is **no `core.chat()`** anymore — the old public `ChatEngine` is gone; use `fetch_topic()` + `TopicRuntimeHandle::create_chat()`.

### Storage

`Storage` (`storage.rs`) is `Clone` and holds 8 sub-storages + an executor. `create_credentials`/`get_provider_credentials`/`delete_credentials` live on `ProviderStorage` (there is no separate credentials storage):

```rust
core.storage().provider()   // &ProviderStorage (incl. credentials + json_rules)
core.storage().topic()      // &TopicStorage
core.storage().model()      // &ModelStorage
core.storage().message()    // &MessageStorage
core.storage().mcp()        // &McpStorage
core.storage().agent()      // &AgentStorage
core.storage().prompt()     // &PromptStorage
core.storage().approval()   // &ToolApprovalStorage
```

All `create()` methods return the **full record** (`Result<Topic>`, `Result<Model>`, `Result<AgentBinding>`, …) — read `.id` off the result (older docs that say they return `i64` are stale). IDs are Snowflake via `ferroid`; never rely on SQLite auto-increment.

**Messages hang off a binding, not a topic**: `MessageStorage` queries by binding — `list_by_binding(binding_id)` (all messages of a binding, ordered) and `list_contexts(binding_id)` (the subset usable as chat context after the `is_boundary`/`is_excluded` rules).

**Transactions**: `storage.with_tx(|inner| async { ... }).await` (NOT `.tx()`). For multi-step transactions: `storage.begin().await` → `StorageTx` (a `Storage` bound to a transaction) with `.commit()` / `.rollback()`.

**`init_id_generator(machine_id)`** must run before any `create()` — `next_id()` panics otherwise. `WindCore::init_*()` does this automatically; standalone `XxxStorage` tests must call it in setup.

**SQL macros** (`storage.rs`): `insert!`, `update!`, `update_fields!`, `insert_fields!`, `delete_by_id!`, `select_fields!`, `get_by_id!`. Values are `Option`-wrapped; `None` fields are skipped. `update!` appends `updated_at` and `WHERE id = ?`. (`executor.rs`'s `with_transaction!`/`with_connection!` are the pool plumbing.)

**Serializing `Option` vec/map fields** (`storage/utils.rs`):
- `vec_to_str_default(Some(v))` → JSON string; `vec_to_str_default(None)` → `"[]"` — always writes the column
- `vec_to_str_optional(v)` → `Some(json)` / `None` — skip the column when `None`; use this for `update!` so absent fields are not written

`UpdateMessage` has only `content`, `model_id`, `input_tokens`, `output_tokens` — tool approvals are no longer stored on messages (see Tool Approval Flow).

### Agent system

The agent system replaces the old `ChatEngine`. Each `Topic` gets a `TopicRuntime` that runs a **finite-state machine**: every incoming command, task notification, and supervisor request is reduced into effects that the runtime then executes.

#### FSM core (`agent/fsm.rs`)

- `TopicFsm` — a **pure reducer with no side effects**: `reduce(&mut self, event: FsmEvent) -> Vec<Effect>`. Tracks `TopicState` (`Idle`/`Running`/`Stopped`), `main_binding_id`, and one `TaskFsm` per binding.
- `TaskFsm` (`fsm/task_fsm.rs`) — per-task state machine whose state is `AgentStatus`.
- `FsmEvent` (`fsm/event.rs`) — `Topic(TopicMsg)`, `Start`, `StartChild`, `ChildResolved`, `Emit(TopicEvent)`, `Signal { binding_id, event: TaskEvent }`.
- `Effect` (`fsm/effect.rs`) — side effects the runtime performs: `PersistStatus`, `Emit`, `Start`, `Resume`, `Cancel`, `SendChildResponse`, `SpawnChild`, `Approval`, `CloseEventStream`, `StopRuntime`, `PrepareMain`, `ApprovalRequest`, `Finish`, `Failed`.
- `TopicRuntime::apply()` executes effects **depth-first**: an effect's follow-up events are reduced and executed immediately before the next sibling effect, so each effect's full side-effect chain (including broadcasts) completes before later effects such as `CloseEventStream` run.

#### Components

| Component | Role |
|-----------|------|
| `TopicRuntime` | Actor per topic — owns `TopicFsm`, `TaskRegistry`, mailbox; reduces events and executes effects (`agent/topic.rs`) |
| `TopicRuntimeHandle` | Cloneable handle — `create_chat(Vec<Content>)`, `cancel_task(binding_id)`, `approve(binding_id, allow_ids, deny_ids)`, `subscribe() -> broadcast::Receiver<TopicEvent>`, `shutdown()`, `is_stopped()` |
| `TopicMailbox` | mpsc sender carrying `TopicMsg` = `Command(TopicCommand)` / `Task(TaskNotification)` / `Supervisor(SupervisorRequest)` (`agent/event.rs`) |
| `TaskRegistry` | In-memory map `binding_id → TaskEntry` + pending-children list (`agent/task.rs`) |
| `PendingChild` | Links parent/child bindings while a spawned agent is pending; resolved by `resolve_pending_child()` when the child finishes |
| `SyncTask` / `SyncTaskHandler` | Task actor per agent instance (`agent/task/sync.rs`) — `SyncTask::spawn(ctx, binding_id, topic_id, topic_tx, storage, mcp_registry)` returns a `SyncTaskHandler`, whose `start(&self, task: TaskSpec)` / `cancel(&self)` send over an `mpsc`; the task loop builds `AgentRuntime` |
| `AgentRuntime` | The LLM loop — runs `ChatLoops`, partitions tool calls, handles approval/agent tools (`agent/runtime.rs`) |
| `AgentHost` | `async_trait` — how `AgentRuntime` reaches the outside world (execute MCP tools, spawn/list agents, emit notifications) (`agent/host.rs`) |
| `SyncHost` | The `AgentHost` impl used by `SyncTask`; private in `agent/task/sync.rs`, bridges back to `TopicRuntime` via `TopicMailbox` |

#### Lifecycle

```
TopicRuntimeHandle::create_chat(user_input)
  → TopicCommand::Start { user_input }
  → TopicFsm::reduce → Effect::PrepareMain
  → prepare_main_agent(): errors if main is busy; else loads binding/agent/chat-context,
    opens a tx to create the user + assistant messages bound to the main binding,
    returns Emit(MessageCreated ×2) + Effect::Start
  → start_agent_task(): SyncTask::spawn → register TaskEntry → handler.start(spec) spawns AgentRuntime
  → AgentRuntime::run(): ChatLoops::run() → ChatEvent stream
      Partial       → TaskNotification::Message → Effect::Emit(TopicEvent::Message)
      AwaitToolCall → make_tool_plan() → partition_tool_calls_by_policy() →
                      unhandled calls → TaskNotification::ApprovalRequired → Effect::ApprovalRequest
      Finish        → TaskNotification::Finish → TaskFsm → Effect::Finish → save message → Emit(MessageFinished)
      error         → Effect::Failed → Emit(TopicEvent::Error)
  When main finishes → TopicFsm::sync_topic_state → TopicState back to Idle → Effect::CloseEventStream
```

There is no `walk_task()` and no `TopicCommand::CreateChat` — the mechanism is FSM reduction. `AgentStatus` transitions: `Idle → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`.

#### Agent modes

`AgentMode`: `Sync` (parent waits), `Fork` (child copies the main agent's message history via `helper::create_fork_contexts()`), `Background` (**not implemented** — `agent/task/background.rs` is empty). Spawning is governed by the definition's `PermissionPolicy` (`can_spawn_*`, `max_spawn_depth`).

#### Agent tools (virtual tools injected for the LLM)

Agent tools use the `agent_` prefix (`AGENT_TOOL_PREFIX = "agent_"`, `agent/tool.rs`) — note the **underscore**, not a dot. They are NOT MCP tools; `AgentRuntime` intercepts them in-process.

| Tool | Purpose |
|------|---------|
| `agent_list_agents` | List available agent bindings in the current topic |
| `agent_spawn_agent` | Spawn a sub-agent with `agent_key`, `mode` (sync/background/fork), and `task` |

Only the main role gets these built-ins (`helper::build_agent_tools`; FIXME notes this is to avoid recursive agent creation). Multiple `list_agents` calls in one batch are coalesced via `parse_agent_action()`. Spawning sends `SupervisorRequest::SpawnAgent` → `Effect::SpawnChild` → `spawn_child()` → child `SyncTask`; `resolve_pending_child()` replies to the parent's waiting `oneshot` when the child completes.

#### Topic events (broadcast)

`TopicEvent` (`agent/event.rs`) — consumed via `TopicRuntimeHandle::subscribe()`:

- `Error` — binding/topic/message ids + error string
- `Snapshot` — full message list for a binding
- `MessageCreated` — new message persisted
- `Message` — streaming delta chunk (binding + topic + message id, index, `AiMessage` data)
- `MessageFinished` — message complete
- `TaskStatusChanged` — agent status transition
- `ApprovalRequired` — tool calls need user review

The broadcast channel is closed when the main task goes Idle or the runtime stops — re-subscribe per conversation.

### Tool Approval Flow

`ToolApprovalPolicy` (`Manual` / `AllowList(Vec<String>)` / `AllowAll` default) lives on **`AgentBinding.tool_approval_policy`** — there is no topic-level policy anymore.

When the model requests tool calls:

1. `AgentRuntime::make_tool_plan()` first checks `ToolApprovalStorage` for existing approval rows on the current message.
2. `partition_tool_calls_by_policy()` splits by the binding's policy: auto-approved tools execute via MCP; agent tools go through `parse_agent_action()`; unhandled calls become `ApprovalRequired`.
3. `helper::save_approval_state()` persists `ToolApprovalRequest` rows (status `Pending`) and the assistant message state, then emits `TopicEvent::ApprovalRequired`.
4. The caller replies with `TopicRuntimeHandle::approve(binding_id, allow_ids, deny_ids)` → `TopicCommand::Approval` → FSM (rejected unless the task is `WaitingApproval`) → `Effect::Approval` → `batch_set_status()` → `TaskEvent::ApprovalResolved` → `Effect::Resume` → `resume_task()` re-loads agent state and re-runs `AgentRuntime`.

**Rejection markers**: Denied tools get `{"error": "tool call denied", "tool": "..."}` in their `FunctionCallOutput`.

### Chat loops (low-level LLM interaction)

`ChatLoops` (`chat/loops.rs`) is the internal engine used by `AgentRuntime`, not called directly by external code.

**`ChatContext`**: `Model`, `Provider`, `Credentials`, `ReqConfig`, `Option<JsonRule>`, `Option<Vec<Tools>>`.

**`ChatLoops::run(ctx, assistant, contexts)`** returns `Stream<Item = ChatEvent>`. `ChatEvent` (`chat/events.rs`): `Partial`, `AwaitToolCall`, `Finish { error: Option<String> }` — there is **no** `Created`/`Completed`/`Failed` variant; errors ride on `Finish`. Streaming: `handle_chat()` → `request_sse()` → buffer by `\n\n` → `parse_stream_chunk()`.

### `wind-ai` — Provider abstraction

`ChatAdapter` trait (`provider/adapter.rs`): `build_request()` / `parse_response()` / `parse_stream_chunk()`. Two impls: `OpenAICompletionAdapter` (`/chat/completions`) and `OpenAIResponseAdapter` (`/responses`), registered via `get_chat_adapter(AdapterType)`. `AdapterType` variants: `OpenAICompletion` / `OpenAIResponse`. The spelling is always `Adapter` — the older spelling with an `-or` suffix is gone from the codebase.

**OpenAIResponseAdapter gotcha**: function call output must use `Value::String(data.content.to_string())` — not `data.content.clone()`. The Responses API expects a JSON string for the `output` field of `function_call_output`.

`Message` (`message.rs`) carries role, content (vec of Content variants), reasoning_content, token counts, tool_calls. `append_chunk()` merges streaming deltas. Key methods: `is_simple()`, `is_tool_request()` (assistant + tool_calls), `is_tool_result()`. `ReqConfig` holds temperature, top_p, max_tokens, stream, penalties, parallel_tool_calls, reasoning.

### `wind-mcp` — MCP client registry

Actor pattern: `Registry::new()` spawns a tokio task; all interaction via `RegistryHandle` (cloneable, `mpsc`). `Registry`/`RegistryHandle` live in `client/registry.rs`.

- `acquire(session_id, params)` — start/reuse server (ref-counted across sessions)
- `release(session_id, name)` — drop a session reference
- `list_all_tools()` / `list_tools_by_names(&[names])` / `call_tool(param)` — discovery & execution

Transports: `Stdio` (child process, with dedup map for concurrent starts) and `Streamable` (HTTP). Tool names: `{server_name}0m0{tool_name}` (`MCP_TOOL_IDENTIFIER = "0m0"`).

### `wind-rule` — JSON rule engine

Rules stored in `json_rule` table keyed by `(provider_id, adapter)`. Applied to request bodies before each API call.

| Op | Purpose |
|----|---------|
| `set` | Set value at JSON path (creates intermediate objects) |
| `remove` | Delete field at JSON path |
| `map_value` | Map field value via lookup table → merge result into body root |
| `compute` | Evaluate `evalexpr` over `$value` + `$ctx.*` → replace field |
| `when` | Conditional: `cond` (eq/neq/gt/lt/contains/and/or/not/in) with `then`/`else` sub-rules |

### `wind-tui` — Terminal UI

Skeleton TUI application using `ratatui` + `crossterm`. Currently renders a placeholder — the app loop + event handling structure is in place for further development.

### `wind-http` — HTTP service

Axum service exposing the core via REST + SSE. **Read `arch.md` for the full design**; this is the operational summary.

- **Layering**: `wind-http` is pure protocol adaptation (router, middleware, DTO, facade). Handlers call facades; facades call `WindCore`/storage; core never depends on axum/http/tower.
- **Module convention**: no `mod.rs` — same-name files + directories (`routes.rs` + `routes/…`).
- **`app(state) -> Router<()>`** (`app.rs`): `build_router()` composes sub-routers + layers, `with_state` applied last. Exposed for `tower::ServiceExt::oneshot` tests.
- **`AppState`** (`state.rs`): `config: AppConfig`, `core: Arc<WindCore>`, `started_at: i64`, `cancel: CancellationToken` (process shutdown signal — SSE streams `select!` on it so they cannot block graceful shutdown) — shared per request; `FromRef` sub-extraction for `AppConfig` / `Arc<WindCore>`.
- **Middlewares** (`middleware/`): `trace`, `request_id`, `timeout` (CRUD only — SSE routes are not wrapped). Layer order matters: timeout innermost, request-id outermost, trace outermost.
- **Facade layer** (`facade/`): `SystemFacade::health` (`system.rs`), `TopicFacade` (`topic.rs`: topics, binding messages/context, chat, cancel, approve, chat-config), `McpRuntimeFacade` (`mcp_runtime.rs`: server lifecycle + tool/prompt/resource discovery), and per-resource `StorageFacade` sub-facades (`facade/storage/`): provider, model, mcp, prompt, agent, approval. Facades do HTTP pre-validation, call storage/runtime, map to DTOs, and collapse `CoreError` into `ApiResponse`.
- **DTO / envelope** (`dto/`): `ApiResponse<T> { code: u16, data: Option<T>, msg: String }`. Business success/failure returns HTTP 200 with `code` (200/404/500) distinguishing the result; real HTTP status is reserved for protocol errors (extractor rejection, middleware, 404 fallback). Mirror schemas are named `XSchema` and map `From`/`TryFrom` to core models — core models carry no `ToSchema` derives.
- **OpenAPI** (`openapi.rs`): `utoipa` aggregate of all public routes + schemas; `GET /api-docs/openapi.json` serves the generated JSON directly (`serve_openapi_json`) — no `utoipa-swagger-ui` dependency (its build script downloads UI assets; none needed). The SSE route is annotated separately (`text/event-stream`, not `ApiResponse`).
- **Extractors** (`extractor.rs`): `ApiQuery`/`ApiPath`/`ApiJson` wrap rejections into `ApiResponse`; `json_body()` lets handlers keep native `Result<Json<T>, JsonRejection>` so utoipa still sees the requestBody.
- **Env vars**: `WIND_HTTP_HOST` (default 127.0.0.1), `WIND_HTTP_PORT` (7324). `main.rs` calls `WindCore::init_local()`, so the DB file comes from the core data dir: `WIND_ROOT_DIR` (default `~/.windai/`), file `windai.db`.
- **SSE**: `GET /api/v1/topics/{topic_id}/events` subscribes the broadcast receiver (`event:` = `TopicEvent` variant snake_case, `data:` = JSON). It checks topic existence first — it does not get-or-create.
- **Message routes are binding-scoped**: `GET /api/v1/agent-bindings/{binding_id}/messages` and `.../messages/context`. Starting a chat stays topic-scoped (`POST /api/v1/topics/{topic_id}/messages`); `GET /api/v1/topics/by-binding/{binding_id}` takes no query params.

### Database tables (SQLite, `schema.rs`)

`providers`, `models`, `credentials`, `topics`, `messages`, `chat_configs`, `mcp_servers`, `json_rule`, `prompt_modules`, `agent_definitions`, `topic_agent_bindings`, `tool_approval_requests`.

Column notes: `topics` has `parent_id` (tree structure is kept, but **no code currently creates child topics**); `messages` is scoped by its own `binding_id` (there is no `topic_id` column on it); `topic_agent_bindings` is scoped by `topic_id`; `tool_approval_requests` carries only `topic_id` + `binding_id` (no topic-parent column).

**No migrations**: `schema.rs` runs only `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`. An existing DB file is never altered, so after any column/index change you must delete the local DB file (`~/.windai/windai.db`, or the file under `WIND_ROOT_DIR`) and let it be re-created — otherwise queries fail against the stale schema.

**Key constraints**: `topic_agent_bindings` has plain indexes (topic, agent, role) plus two UNIQUE indexes — `(topic_id, agent_id)` and a partial `(topic_id) WHERE role = 'main'`. The matching invariants ("a definition is bound at most once per topic", "at most one `main` binding per topic") are enforced **both** by these DB indexes **and** by application pre-checks in `create_binding` / `update_binding` (`AgentStorage`), which return `CoreError::Validation` before hitting the constraint.

**Delete cascades**: deleting bindings removes `tool_approval_requests` → `chat_configs` → `messages` → `topic_agent_bindings` (`AgentStorage::delete_bindings`). Deleting topics removes only that topic's own definitions + its bindings (with the four tables above) + the topic's approval rows + the topic row itself; it does **not** cascade into child topics (`TopicStorage::delete_topics` — callers must pass child ids explicitly).

### Agent data model

**`AgentDefinition`** (`models/agent/definition.rs`) — what an agent *can do*: `key`, `name`, `description`, `owner_topic_id` (`None` = global), `cloned_from_id`, `active`, `data: AgentDefinitionData` (prompt_modules, mcp_servers via `AgentMcpBinding`, builtin_mcp_servers via `BuiltinMcpBinding`, context_policy, permission_policy, runtime_limits). There is no `scope` field — global vs. topic-local is expressed by `owner_topic_id`, and "cloned from" is `cloned_from_id`.

**`AgentBinding`** (`models/agent/binding.rs`) — an agent *instance* in a topic: `topic_id`, `agent_id`, `mode` (`Sync`/`Fork`/`Background`), `role` (`Main`/`Child`), `status`, `model_id`, `tool_approval_policy`, `chat_config_id`, `enabled`.

**`AgentStatus`**: `Idle → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`

**`PromptModule`** (`models/agent/prompt.rs`) — reusable prompt fragments referenced by `AgentDefinitionData.prompt_modules`.

**`ToolApprovalRequest`** (`models/agent/approval.rs`) — persisted per-tool-call approval record (binding_id, topic_id, message_id, tool_call_id, tool_name, arguments, status: `Pending`/`Approved`/`Denied`).

### Test organization

| File | Content |
|------|---------|
| `windai/core/tests/storage.rs` | Integration tests for all `*Storage` structs — CRUD, validation, cascade, batch |
| `windai/core/tests/core_chat.rs` | Non-MCP chat test (`test_agent_chat`, `#[ignore]` behind `.env`): seeds providers/agents/bindings, subscribes to topic events, drives `create_chat` |
| `windai/core/tests/chat.rs` | AI adapter tests (needs `.env`) |
| `windai/core/tests/common/lib.rs` | Shared helpers: `init_test_core()`, `init_test_core_with_registry()`, `McpTestEnv`, MCP server params |
| `windai/http/tests/*` | Router/facade/mirror tests via `app(state)` + `tower::ServiceExt::oneshot`; `common::test_core()` |
| `windai/core/src/storage/utils.rs` (cfg test) | SQL macro unit tests |

**Shared-registry test architecture**: the pattern outlived the MCP tests — it now serves the chat tests. `core_chat.rs::shared_chat_registry()` parks one empty `RegistryHandle` in a dedicated long-lived tokio runtime thread (`OnceLock` + `mpsc::sync_channel`), and each test builds its **own** `WindCore` via `init_test_core_with_registry(shared)`. The per-test pool is `sqlite::memory:` with `max_connections(1)` (`common/lib.rs::init_test_pool`) so schema init and later queries always hit the same in-memory DB. A test that did need MCP servers would seed its own server record + provider/model/topic against that shared registry.

### VS Code debugging

`.vscode/launch.json` — uses `cargo build --tests` (NOT `cargo test`) to bypass `codelldb-launch` which crashes on Windows MSVC. LLDB debugs the compiled test binary directly. Two template configs: `Debug MCP Test` and `Debug Chat Test` — modify `filter.name`, `args[0]`, and `--ignored` flag per test function.

### Key patterns

**Adding a new provider adapter:** Implement `ChatAdapter`, add a variant to `AdapterType`, register in `get_chat_adapter()`.

**Adding a new provider:** Insert into `providers`, add credentials, insert models — no code changes.

**MCP tool flow:** `AgentDefinitionData.mcp_servers` (enabled `AgentMcpBinding`s) → `batch_get_by_ids` → server names → `list_tools_by_names()` → filter by `is_tool_allowed()` (allowed/denied tool lists) → merged with built-in agent tools (main role only) via `helper::build_agent_tools()` → `tool_calls` in response → `partition_tool_calls_by_policy()` splits by the binding's policy → auto-approved execute immediately, manual-review yield `ApprovalRequired` → `resume_task()` on resume → results as context → loop.

**Agent tool flow:** LLM calls `agent_list_agents` or `agent_spawn_agent` → `AgentRuntime` identifies `agent_`-prefixed calls → `parse_agent_action()` coalesces duplicate `list_agents` → `AgentHost::spawn_agent()` sends `SupervisorRequest::SpawnAgent` → `TopicFsm` → `Effect::SpawnChild` → `spawn_child()` starts a child `SyncTask` → when the child completes, `resolve_pending_child()` resolves the pending entry and replies over the `oneshot` channel.

**`ToolApprovalPolicy`**: Per-agent-binding enum controlling tool execution: `AllowAll` (default — all auto-execute), `AllowList(Vec<String>)` (listed tool names auto-execute), `Manual` (all require approval). Stored as JSON in `topic_agent_bindings.tool_approval_policy`.

**`create()` returns the record:** `storage.xxx().create(...)` returns the full object (e.g. `Topic`, `Model`) with its `.id` populated — there is no separate "return `i64`, then `get(id)`" round-trip.

**Topic / binding / message scoping**: there is exactly one `TopicRuntime` and one main Agent per topic; a topic holds multiple `AgentBinding`s (`list_bindings_by_topic`, each with its own `role`, `model_id`, `tool_approval_policy`, and `chat_config_id`). Agent-to-agent isolation is **not** achieved by wrapping each agent in its own sub-topic — instead every `Message` carries a `binding_id`, so a binding's conversation lives in the messages filtered by that binding (`MessageStorage::list_by_binding` / `list_contexts`). `topics.parent_id` remains in the schema but no code creates child topics today.

**`From<Message> for UpdateMessage`** (`models/message.rs`) preserves fields from the source message. Do NOT set optional fields to `None` unless you intend to clear them.

**Fork mode context**: When spawning an agent in `Fork` mode, `helper::create_fork_contexts()` copies the main agent's message history as the child's starting context — enabling the child to have full visibility of the parent's conversation.

## Agent skills

### Issue tracker

GitHub Issues on `evilArsh/windai` — use the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout — one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
