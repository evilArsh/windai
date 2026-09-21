# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                           # Build entire workspace
cargo build -p wind-core              # Build a specific crate
cargo test                            # Run all tests (SQLite, no external DB)
cargo test --workspace                # Every crate's tests in one run (counts grow as tests are added)
cargo test -p wind-core               # Tests for a specific crate
cargo test -p wind-http               # HTTP route/facade/DTO tests (no .env needed)
cargo test -p wind-core -- test_chat  # Integration tests that need API keys (.env)
cargo test -p wind-core -- --ignored  # .env-gated integration tests
cargo test -p wind-core --test core_chat -- --include-ignored --test-threads=1  # one file's ignored tests
```

Copy `.env.example` to `.env` and fill in `TEST_*` values for the `.env`-gated tests.

**Test file status** (so you don't expect dead tests to run)。下面的数字是当前快照，会随代码变化：
- `windai/core/tests/storage.rs` — 30 tests, active, no `.env` needed
- `windai/core/tests/schema.rs` — 14 tests, the schema↔model contract（12 张表各一条列断言 + `dropped_columns_are_absent` + `dropped_tables_are_absent`），no `.env`
- `windai/core/tests/autoincrement.rs` — 7 tests, 自增主键的回归测试（12 张表自增生效 / 不复用 / 严格递增 / `create()` 返回值落库一致 / 批量插入的自然键关联与分块 / 布尔列往返），no `.env`
- `windai/core/src/storage/message.rs` (cfg test) — 3 tests：`list_contexts` 的排除/删除/boundary 语义（`list_contexts` 是 crate 内部 API，集成测试访问不到）
- `windai/core/tests/agent_runtime.rs` — 2 tests: runtime 生命周期与「终态事件先于关流到达订阅者」
- `windai/core/tests/core_chat.rs` — active; its one test is `#[ignore]`d behind `.env`
- `windai/core/tests/chat.rs` — AI adapter tests, `#[ignore]` behind `.env`
- `windai/core/tests/common/lib.rs` — shared helpers (not a test target)
- `windai/http/tests/*` — active, no `.env`; drive `app(state)` via `tower::ServiceExt::oneshot` (`agent_instance.rs`, `facade.rs`, `routes.rs`, `topic_children.rs`, `openapi.rs`, `mcp_registry.rs`, `mcp_runtime.rs`, `dto.rs`, `envelope.rs`, `extractor.rs`, `middleware.rs`)
- `windai/core/src/storage/utils.rs` (cfg test) — SQL macro unit tests

## Architecture

```
wind-http   ──depends-on──>  wind-core, wind-ai, wind-mcp, wind-rule
wind-tui    ──depends-on──>  wind-core, wind-ai, wind-mcp, wind-rule, ratatui, crossterm
wind-core   ──depends-on──>  wind-ai, wind-mcp, wind-rule
wind-mcp    ──depends-on──>  wind-skills, rmcp
wind-ai     ──depends-on──>  reqwest
wind-rule   ──depends-on──>  evalexpr
wind-skills ──depends-on──>  serde, yaml-rust2, schemars
```

`wind-core` is the shared business core; `wind-ai`/`wind-mcp`/`wind-rule`/`wind-skills` are capability crates. **Nothing depends on `wind-http`** — it is just one adapter over the core (future targets: napi-rs, Android). `arch.md` at repo root is the authoritative `wind-http` architecture design (layering, facade, DTO contract, SSE, OpenAPI) — read it before changing `wind-http`（该文件被根目录 `/*.md` 规则忽略，不在版本控制内；本地缺失时以本文件为准）

The vocabularies of this file and `README.md` are kept in sync with the code — if you find a mismatch, trust the code and fix the doc.

### `wind-core` — Central orchestration

`WindCore` (`lib.rs`) is a process-level runtime root: it owns a `Storage` (which holds the `DbPool`), a `RegistryHandle`, and a `Mutex<HashMap<i64, TopicRuntimeHandle>>` of per-topic runtimes. One instance per process/tests — independent runtimes share nothing. Public surface:

```rust
WindCore::init_memory()                                          // In-memory SQLite (tests/ephemeral)
WindCore::init_local()                                           // File-backed SQLite at app_dirs().db_path()
WindCore::init_with_pool(pool)                                   // Init with external pool (own Registry)
WindCore::init_with_pool_and_registry(pool, registry)            // Init with external pool + shared Registry (tests)

core.storage()              // &Storage — all CRUD access
core.registry()             // &RegistryHandle — MCP client registry
core.fetch_topic(id)        // TopicRuntimeHandle — get-or-create；缓存句柄已停止（is_stopped()）时换成新的运行时（panics after shutdown()）
core.shutdown().await
```

All four init methods converge on `init_with_pool_and_registry`, which runs `schema::init_schema` and registers the two builtin MCP servers (`FsServer`, `SkillsServer`) onto the registry. The private `init(db_url)` is only used by `init_local`/`init_memory`. There is **no `core.chat()`** anymore — the old public `ChatEngine` is gone; use `fetch_topic()` + `TopicRuntimeHandle::create_task()`.

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

`TableName` (`storage.rs`, `pub(crate)`) centralises the 12 table names as associated constants, and the `*Storage` files build queries from them；列名仍以字面量传入 `select_fields!` / `insert!` 等宏

All `create()` methods return the **full record** (`Result<Topic>`, `Result<Model>`, `Result<AgentInstance>`, …) — read `.id` off the result (older docs that say they return `i64` are stale). **id 由数据库自增生成**：SQLite 侧是 `INTEGER PRIMARY KEY AUTOINCREMENT`，`create()` 用 `INSERT ... RETURNING id`（`executor.fetch_one_scalar`）取回；不再有应用层的 id 生成器（`ferroid` / `next_id()` / `init_id_generator` 已全部移除）。自增 id 保证「id 顺序 = 插入顺序」，这正是 `ORDER BY id`（17 处）与 `list_contexts` 的 `id > MAX(id)` 所依赖的

**Messages hang off an instance, not a topic**: `MessageStorage` queries by instance — `list_by_instance(instance_id)` (all messages of an instance, ordered). `list_contexts(instance_id)` (the subset usable as chat context: not `is_excluded`, and only messages after the last `is_boundary = true` row) is **`pub(crate)`** —— 只供 core 内部（`helper::get_message_contexts`）使用，不对外暴露

**Transactions**: `storage.with_tx(|inner| async { ... }).await` (NOT `.tx()`). For multi-step transactions: `storage.begin().await` → `StorageTx` (a `Storage` bound to a transaction) with `.storage()` / `.commit()` / `.rollback()`.

**取回自增 id 一律用 `INSERT ... RETURNING id`**，不要用 `last_insert_rowid()`（SQLite 驱动专有，PostgreSQL 无对应物）。批量插入时 **不要依赖 `RETURNING` 的返回顺序**（两个驱动都不承诺），而要用自然键关联 —— 见 `ToolApprovalStorage::create_requests` 用 `tool_call_id` 回填 id，并按 `input.calls` 原始顺序组装返回值

**SQL macros** (`storage/utils.rs`): `insert!`, `update!`, `update_fields!`, `insert_fields!`, `delete_by_id!`, `delete_from!`, `select_fields!`, `get_by_id!`. Values are `Option`-wrapped; `None` fields are skipped. `update!` appends `updated_at` and `WHERE id = ?`. (`executor.rs`'s `with_transaction!`/`with_connection!` are the pool plumbing.)

**Serializing `Option` vec/map fields** (`storage/utils.rs`):
- `vec_to_str_default(Some(v))` → JSON string; `vec_to_str_default(None)` → `"[]"` — always writes the column
- `vec_to_str_optional(v)` → `Some(json)` / `None` — skip the column when `None`; use this for `update!` so absent fields are not written

`UpdateMessage` has only `content`, `model_id`, `input_tokens`, `output_tokens` — tool approvals are no longer stored on messages (see Tool Approval Flow).

**`AgentStorage` 的可见性**：`get_instance` / `get_main_instance` / `list_instances_by_topic` / `list_child_instances_by_topic` / `list_sub_definitions_by_topic`，以及 4 个面向调用方的能力映射方法（`create_topic_agent_map` / `list_agent_maps_by_topic` / `get_agent_map` / `delete_topic_agent_map`）都是 `pub`；`create_instance` / `update_instance` / `delete_instances` 同样是 `pub`，级联用的 `batch_delete_agent_maps_by_topics` 是 `pub(crate)`，`select_instances` / `select_agent_maps` 两个查询构造器是私有的（`select_definitions` 是 `pub`，供 facade 直接构造查询）

### Agent system

The agent system replaces the old `ChatEngine`. Each `Topic` gets a `TopicRuntime` that runs a **finite-state machine**: every incoming command, task notification, and supervisor request is reduced into effects that the runtime then executes.

#### FSM core (`agent/fsm.rs`)

- `TopicFsm` — a **pure reducer with no side effects**: `reduce(&mut self, event: FsmEvent) -> Vec<Effect>`. Its only state is `topic_id`, `main_instance_id`, and a `HashMap<i64, TaskFsm>` keyed by instance — **there is no queue**.
- `TaskFsm` (`fsm/task_fsm.rs`) — per-task state machine whose state is `AgentStatus` (plus `mode`). There is **no separate `TopicState` type**.
- `FsmEvent` (`fsm/event.rs`) — `Topic(TopicMsg)`, `Start { spec: TaskSpec }`, `ChildResolved { instance_id }`, `Emit(TopicEvent)`, `Signal { instance_id, event: TaskEvent }`. There is no `StartChild` — a child is started by the parent's `Effect::SpawnChild` follow-up `FsmEvent::Start`.
- `Effect` (`fsm/effect.rs`) — the real variants: `PersistStatus`, `Emit(TopicEvent)`, `Start { spec }`, `Resume { instance_id }`, `Cancel { instance_id }`, `SpawnChild { instance_id, call_id, request, reply }`, `Approval { instance_id, allow_ids, deny_ids }`, `StopRuntime`, `Init { user_input }`, `ApprovalRequest { instance_id, data, calls }`, `Completed { instance_id, data, status }`, `Failed { instance_id, data, status, error }`, `Canceled { instance_id, status, error }`, `CloseEventStream`。There is no `PrepareMain` / `StartChild` / `SendChildResponse` variant — 子任务回执由 `TopicRuntime::handle_pending` 发送
- `TopicRuntime::apply()` executes effects **depth-first**: an effect's follow-up events are reduced and executed immediately before the next sibling effect, so each effect's full side-effect chain (including broadcasts) completes before the round ends.

**任务队列本轮未实现**：`TopicFsm::main_status` / `main_instance_id` / `is_task_busy` / `is_main_busy` 是留给后续「任务队列」的查询入口，**当前没有任何调用方**（`is_task_busy` 只被 `is_main_busy` 调用）；`has_queued` / `dequeue` / `Effect::DrainQueue` / `TopicEvent::TaskQueued` 并不存在。一轮 Sync/Fork 结束前不接受新任务

**并发 `TopicCommand::Start` 不设防（本轮接受的决定）**：`TopicFacade::create_chat` 只确认话题存在且已配置模型，不做「主实例是否正忙」的判断，因此两个并发的 `POST /topics/{id}/messages` 会在同一个主实例上交错跑两轮。这是「任务队列本轮不做」的直接后果，不是遗漏；补上互斥即等于引入队列语义，留待队列落地时一并处理

#### Components

| Component | Role |
|-----------|------|
| `TopicRuntime` | Actor per topic — owns `TopicFsm`, `TaskManager`, mailbox and the `app_rx` broadcast sender; reduces events and executes effects (`agent/topic.rs`) |
| `TopicRuntimeHandle` | Cloneable handle — `create_task(Vec<Content>)`, `cancel_task(instance_id)`, `approve(instance_id, allow_ids, deny_ids)`, `subscribe() -> broadcast::Receiver<TopicEvent>`, `shutdown()`, `is_stopped()` |
| `TopicMailbox` | mpsc sender carrying `TopicMsg` = `Command(TopicCommand)` / `Task(TaskNotification)` / `Supervisor(SupervisorRequest)` (`agent/event.rs`) |
| `TaskManager` | Per-topic task registry (`agent/task.rs`) — `instance_map: HashMap<instance_id, TaskEntry>` + `pending: Vec<PendingChild>`; owns `init` / `resume` / `start` / `cancel` / `spawn_child` / `persist_*` |
| `PendingChild` | Links parent/child instances while a spawned agent is pending; taken by `TaskManager::take_pending()` when the child finishes |
| `SyncTask` / `SyncTaskHandler` | Task actor per agent instance (`agent/task/sync.rs`) — `SyncTask::spawn(ctx, instance_id, topic_id, topic_tx, storage, mcp_registry)` returns a `SyncTaskHandler`, whose `start(&self, spec: TaskSpec)` / `cancel(&self)` send over an `mpsc`; the task loop builds `AgentRuntime` |
| `AgentRuntime` | The LLM loop — runs `ChatRunner`, partitions tool calls, handles approval/agent tools (`agent/runtime.rs`) |
| `ChatRunner` | The low-level LLM interaction engine (`chat/runner.rs`) |
| `AgentHost` | `async_trait` — how `AgentRuntime` reaches the outside world (execute MCP tools, spawn/list agents, emit notifications) (`agent/host.rs`) |
| `SyncHost` | The `AgentHost` impl used by `SyncTask`; private in `agent/task/sync.rs`, bridges back to `TopicRuntime` via `TopicMailbox` |

#### Lifecycle

```
TopicRuntimeHandle::create_task(user_input)
  → TopicCommand::Start { user_input }
  → TopicFsm::reduce → Effect::Init { user_input }
  → TaskManager::init(): get_or_create_main_instance()（不存在时按 topic 的主偏好映射创建），
    在事务内创建绑定到主实例的 user + assistant 消息，
    返回 Emit(MessageCreated ×2) + Effect::Start
  → TaskManager::start(): SyncTask::spawn → 登记 TaskEntry → handler.start(spec) 启动 AgentRuntime
  → AgentRuntime::run(): ChatRunner::run() → ChatEvent stream
      Partial       → TaskNotification::Message → Effect::Emit(TopicEvent::Message)
      AwaitToolCall → make_tool_plan() → partition_tool_calls_by_policy() →
                      unhandled calls → TaskNotification::ApprovalRequired → Effect::ApprovalRequest
      Finish        → TaskNotification::Finish → TaskFsm → Effect::Completed → 落库消息 → Emit(MessageFinished)
      error         → Effect::Failed → Emit(TopicEvent::Error)
  主实例进入终态（Finished / Failed / Cancelled）或等待审批 → 本轮 apply() 结束后关流
```

There is no `walk_task()`, no `prepare_main_agent()` and no `TopicCommand::CreateChat` — the mechanism is FSM reduction. `AgentStatus` transitions: `Idle → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`.

#### 事件流关闭时机

关流是一个普通 effect。`TopicFsm::close_main_stream_guard`（`agent/fsm.rs`）在主实例收到 `Cancelled` / `ApprovalRequired` / `Failed` / `Finish` 时追加 `Effect::CloseEventStream`，`TopicRuntime::close_event_stream()` 把 `app_rx: Option<broadcast::Sender<TopicEvent>>` 置 `None`，此后 `emit()` 不再广播

该 effect 由 `TaskFsm::reduce` 的返回值追加在同一次 `Signal` 归约里，而 `apply()` 是深度优先的，因此终端 `Effect::Emit` 一定先于关流执行 —— 主实例的 `MessageFinished` / `Error` / `ApprovalRequired` 不会被丢弃（`windai/core/tests/agent_runtime.rs` 覆盖了该时序）。`TopicRuntime::shutdown()` 也会关流。代价是：主实例一旦进入终态或等待审批，事件流立即关闭，再次对话必须重新 `subscribe()`

#### Agent modes

`AgentMode`: `Sync` (parent waits), `Fork` (child copies the main agent's message history via `helper::create_fork_contexts()`), `Background` (**not implemented** — `agent/task/background.rs` is empty, and `TaskManager::spawn_child` **rejects it with `CoreError::Validation` before opening the transaction**).

`AgentDefinitionData.permission_policy`（`can_spawn_agents` / `can_spawn_sync` / `can_spawn_background` / `can_spawn_fork` / `can_spawn_recursive` / `max_spawn_depth`）目前**没有任何调用方**，`runtime_limits` 同样未被读取 —— spawn 不受它们约束。只有 `context_policy.max_context` 生效（由 `helper.rs` 内的私有函数 `build_context` 读取）

#### Agent tools (virtual tools injected for the LLM)

Agent tools use the `agent_` prefix (`AGENT_TOOL_PREFIX = "agent_"`, `agent/tool.rs`) — note the **underscore**, not a dot. They are NOT MCP tools; `AgentRuntime` intercepts them in-process.

| Tool | Purpose |
|------|---------|
| `agent_list_agents` | 列出当前 topic 能力映射（`topic_agent_maps`）中的全部 AgentDefinition，**不做任何过滤**（`active = false` 的定义同样返回） |
| `agent_spawn_agent` | Spawn a sub-agent with `agent_key`, `mode` (sync/background/fork), and `task` |

Only the main role gets these built-ins (`helper::build_agent_tools`; FIXME notes this is to avoid recursive agent creation). Multiple `list_agents` calls in one batch are coalesced via `parse_agent_action()`. Spawning sends `SupervisorRequest::SpawnAgent` → `Effect::SpawnChild` → `TaskManager::spawn_child()` → child `SyncTask`; when the child completes, `TopicRuntime::handle_pending` 取出 `PendingChild` 并把结果沿它携带的 `oneshot` 回给等待中的父实例

#### Topic events (broadcast)

`TopicEvent` (`agent/event.rs`) — consumed via `TopicRuntimeHandle::subscribe()`:

- `Error` — `instance_id` (Option) + topic/message ids + error string
- `Snapshot` — 某实例的全量消息；该变体已定义但目前**没有任何代码产出它**（保留待后续实现）
- `MessageCreated` — new message persisted (topic + instance + `data`)
- `Message` — streaming delta chunk (topic + instance + message id, index, `AiMessage` data)
- `MessageFinished` — message complete (topic + instance + message id)
- `TaskStatusChanged` — instance status transition (topic + instance + `status` + `mode`)
- `ApprovalRequired` — tool calls need user review (topic + instance + message id + requests)

The broadcast channel is closed after the main instance reaches a terminal state or waits for approval, and when the runtime stops — re-subscribe per conversation.

### Tool Approval Flow

`ToolApprovalPolicy` (`Manual` / `AllowList(Vec<String>)` / `AllowAll` default) lives on **`Topic.tool_approval_policy`** (`Option`, `None` 视同 `AllowAll`) — there is no per-instance policy anymore.

When the model requests tool calls:

1. `AgentRuntime::make_tool_plan()` first checks `ToolApprovalStorage` for existing approval rows on the current message.
2. `partition_tool_calls_by_policy()` splits by the topic's policy: auto-approved tools execute via MCP; agent tools go through `parse_agent_action()`; unhandled calls become `ApprovalRequired`.
3. `helper::save_approval_state()` persists `ToolApprovalRequest` rows (status `Pending`) and the assistant message state, then emits `TopicEvent::ApprovalRequired`.
4. The caller replies with `TopicRuntimeHandle::approve(instance_id, allow_ids, deny_ids)` → `TopicCommand::Approval` → FSM (rejected unless the task is `WaitingApproval`) → `Effect::Approval` → `batch_set_status()` → `TaskEvent::ApprovalResolved` → `Effect::Resume` → `TaskManager::resume()` re-loads agent state and re-runs `AgentRuntime`.

**Rejection markers**: Denied tools get `{"error": "tool call denied", "tool": "..."}` in their `FunctionCallOutput`.

### Chat runner (low-level LLM interaction)

`ChatRunner` (`chat/runner.rs`) is the internal engine used by `AgentRuntime`, not called directly by external code.

**`ChatContext`**: `topic`, `model`, `provider`, `credential`, `rule_set: Option<JsonRule>`, `tools: Option<Vec<Tools>>`.

**`ChatRunner::run(&self, ctx, assistant, contexts)`** returns `Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>>`. `ChatEvent` (`chat/events.rs`): `Partial`, `AwaitToolCall`, `Finish { error: Option<String> }` — there is **no** `Created`/`Completed`/`Failed` variant; errors ride on `Finish`. Streaming: `handle_chat()` → `client::request_sse()` → buffer by `\n\n` → `parse_stream_chunk()`.

### `wind-ai` — Provider abstraction

`ChatAdapter` trait (`provider/adapter.rs`): `build_request()` / `parse_response()` / `parse_stream_chunk()`. Two impls: `OpenAICompletionAdapter` (`/chat/completions`) and `OpenAIResponseAdapter` (`/responses`), registered via `get_chat_adapter(AdapterType)`. `AdapterType` variants: `OpenAICompletion` / `OpenAIResponse`. The spelling is always `Adapter` — the older spelling with an `-or` suffix is gone from the codebase.

**OpenAIResponseAdapter gotcha**: function call output must use `Value::String(data.content.to_string())` — not `data.content.clone()`. The Responses API expects a JSON string for the `output` field of `function_call_output`.

`Message` (`message.rs`) carries role, content (vec of Content variants), reasoning_content, token counts, tool_calls. `append_chunk()` merges streaming deltas. Key methods: `is_simple()`, `is_tool_request()` (assistant + tool_calls), `is_tool_result()`.

`wind_ai::model::Model` 只带 `name` / `adapter` / `endpoint` / `config: Option<JsonObject>` —— **不再有 `ReqConfig`**。`build_request(chat_adapter, model, contexts, tools)` 生成请求体，`model.config` 中当前只读取 `stream` 与 `reasoning` 两个键（`TODO: merge model.config` 见 `openai_completion.rs`）

### `wind-mcp` — MCP client registry

Actor pattern: `Registry::new()` spawns a tokio task; all interaction via `RegistryHandle` (cloneable, `mpsc`). `Registry`/`RegistryHandle` live in `client/registry.rs`.

- `acquire(session_id, params)` — start/reuse server (ref-counted across sessions)
- `release(session_id, name)` — drop a session reference
- `acquire_builtin(server)` — register an in-process builtin server (`FsServer`, `SkillsServer`)
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
| `when` | Conditional: `cond` (`eq`/`neq`/`exists`/`and`/`or`/`not`) with `then`/`else` sub-rules |

### `wind-skills` — SKILL.md parser

`SkillsMeta` / `from_path()` / `scan()` — 解析技能目录下的 `SKILL.md` YAML frontmatter（`SKILL_NAME = "SKILL.md"`），暴露 `Error` / `Result`。被 `wind-mcp` 的内建 skills server（`builtin/skills.rs`）消费

### `wind-tui` — Terminal UI

Skeleton TUI application using `ratatui` + `crossterm`. Currently renders a placeholder — the app loop + event handling structure is in place for further development.

### `wind-http` — HTTP service

Axum service exposing the core via REST + SSE. **Read `arch.md` for the full design**; this is the operational summary.

- **Layering**: `wind-http` is pure protocol adaptation (router, middleware, DTO, facade). Handlers call facades; facades call `WindCore`/storage; core never depends on axum/http/tower.
- **Module convention**: no `mod.rs` — same-name files + directories (`routes.rs` + `routes/…`).
- **`app(state) -> Router<()>`** (`app.rs`): `build_router()` composes sub-routers + layers, `with_state` applied last. Exposed for `tower::ServiceExt::oneshot` tests.
- **`AppState`** (`state.rs`): `config: AppConfig`, `core: Arc<WindCore>`, `cancel: CancellationToken` (process shutdown signal — SSE streams `select!` on it so they cannot block graceful shutdown) — shared per request; `FromRef` sub-extraction for `AppConfig` / `Arc<WindCore>`.
- **Middlewares** (`middleware/`): `trace`, `request_id`, `timeout` (CRUD only — SSE routes are not wrapped). Layer order matters: timeout innermost, request-id outermost, trace outermost.
- **Facade layer** (`facade/`): `TopicFacade` (`topic.rs`: topics、子话题、topic/实例消息、chat、cancel、approve), `TopicMapFacade` (`topic_map.rs`: topic 能力映射的增删查), `McpRuntimeFacade` (`mcp_runtime.rs`: server lifecycle + tool/prompt/resource discovery), and per-resource storage sub-facades (`facade/storage/`): `ProviderStorageFacade`, `ModelStorageFacade`, `McpStorageFacade`, `PromptStorageFacade`, `AgentStorageFacade`, `ToolApprovalFacade`. Facades do HTTP pre-validation, call storage/runtime, map to DTOs, and collapse `CoreError` into `ApiResponse`.
- **DTO / envelope** (`dto.rs`): `ApiResponse<T> { code: u16, data: Option<T>, msg: String }` with helpers `ok` / `not_found` / `bad_request` / `internal` / `without_data::<U>()` / `map_core_error`. Business success/failure returns HTTP 200 with `code` (200/404/400/500) distinguishing the result; real HTTP status is reserved for protocol errors (extractor rejection, middleware, 404 fallback). **核心模型自己在 `wind-core/src/models/**` 上派生 `utoipa::ToSchema`**，`wind-http` 不再有 `XSchema` 镜像类型 —— 自定义 DTO 只剩 3 个，全部写在单个 `dto.rs` 里：`ApiResponse<T>`（含 `ok` / `not_found` / `bad_request` / `internal` / `without_data` / `map_core_error`）、`CreateChatRequest`、`ApproveToolCallsRequest`。其余请求/响应体直接复用 wind-core / wind-mcp 的类型（能力映射用 `CreateTopicAgentMap`，MCP 运行期状态用 `ClientSnapshot`）
- **OpenAPI** (`openapi.rs`): `utoipa` aggregate of all public routes + schemas; `GET /api-docs/openapi.json` serves the generated JSON directly (`serve_openapi_json`) — no `utoipa-swagger-ui` dependency (its build script downloads UI assets; none needed). The SSE routes are annotated separately (`text/event-stream`, not `ApiResponse`).
- **Extractors** (`extractor.rs`): `ApiQuery`/`ApiPath`/`ApiJson` wrap rejections into `ApiResponse`; `json_body()` lets handlers keep native `Result<Json<T>, JsonRejection>` so utoipa still sees the requestBody.
- **Env vars**: `WIND_HTTP_HOST` (default 127.0.0.1), `WIND_HTTP_PORT` (7324). `main.rs` calls `WindCore::init_local()`, so the DB file comes from the core data dir: `WIND_ROOT_DIR` (default `~/.windai/`), file `windai.db`.
- **SSE**: 两条路由都不套 `TimeoutLayer` —— `GET /api/v1/topics/{topic_id}/events`（订阅 `TopicEvent`，`event:` = 变体名 snake_case，`data:` = JSON；先检查 topic 存在，不做 get-or-create）与 `GET /api/v1/mcp-servers/events`（MCP 客户端状态）
- **Message routes**:
  - `GET|POST /api/v1/topics/{topic_id}/messages` — GET 列该 topic 主实例的消息，POST 提交对话输入（`CreateChatRequest`，路由层只把 `Vec<Content>` 交给 runtime，`Message` 记录由 `TaskManager::init` 在事务内创建，受理后返回 `ApiResponse<()>`：`code: 200` + `msg: "ok"`）
  - `GET /api/v1/agent-instances/{instance_id}/messages` — 仅子实例，主实例按 404 处理
  - `GET|PUT /api/v1/messages/{message_id}` — 取/改单条消息
  - 上下文路由（`.../messages/context`）与 `GET /topics/by-instance/{instance_id}`、`GET /messages/{id}/from-message` 已删除：`list_contexts` 只供 core 内部使用，对外一律返回完整消息
- **Agent 实例只读**：`/api/v1/agent-instances/*` 下全部是 GET —— `GET /agent-instances/{instance_id}`、`GET /agent-instances/{instance_id}/messages`、`.../tool-approvals/pending`，外加 `GET /topics/{topic_id}/agent-instances`（底层 `list_child_instances_by_topic`，**不返回主实例**）。实例由 core 内部创建，HTTP 不提供任何 create/update/delete 路由；实例维度唯一的写操作是 `POST /topics/{topic_id}/agent-instances/{instance_id}/cancel`（挂在 topic 路径下）。旧的 `/agent-bindings/*` 家族已全部移除
- **Topic 能力映射路由**：`GET /api/v1/topics/{topic_id}/agent-maps`（列表）、`POST /api/v1/agent-maps`（请求体为 core 的 `CreateTopicAgentMap`，因 DTO 自带 `topic_id`，新增走集合资源）、`DELETE /api/v1/agent-maps/{map_id}`。映射只表达「topic 拥有该能力」，没有角色概念，故没有 PUT

### Database tables (SQLite, `schema.rs`)

**驱动在编译期由 cargo feature 选定**（`windai/core/Cargo.toml`）：`sqlite`（默认）或 `postgres`，二者互斥 —— 同时开启会 `compile_error!`。`db.rs` 用 `mod driver_impl` 的两个 `#[cfg(feature = ...)]` 分支导出 `DbPool` / `DbDriver` / `DbRow` / `DbTransaction`，storage 层只认这几个别名。**只支持这两种驱动**，新增驱动需要在 `db.rs` 补分支并同步 `Cargo.toml` 的 feature

`providers`, `models`, `credentials`, `topics`, `messages`, `mcp_servers`, `json_rule`, `prompt_modules`, `agent_definitions`, `agent_instances`, `topic_agent_maps`, `tool_approval_requests` —— 共 12 张表。`chat_configs` 与 `topic_agent_bindings` 已被删除

Column notes: `topics` 有 `parent_id`（tree structure is kept, but **no code currently creates child topics**）、`model_id` 与 `tool_approval_policy`；`messages` is scoped by its own `instance_id` (there is no `topic_id` column on it)，排除标志的列名与模型字段同名 `is_excluded`；`agent_instances` is scoped by `topic_id`；`topic_agent_maps` is scoped by `topic_id` 且**没有 `role` 列**；`prompt_modules` 的展示名列名是 `alias`（与 `PromptModule.alias` 同名）；`tool_approval_requests` carries `topic_id` + `message_id` + `instance_id` (no topic-parent column).

**主键 DDL**：12 张表的 `id` 一律是 `INTEGER PRIMARY KEY AUTOINCREMENT`（SQLite 侧）。必须是 `INTEGER` 而非 `BIGINT` —— 只有前者才是 rowid 别名，用 `BIGINT` 时省略 id 插入**不报错、而是静默写入 NULL**；必须带 `AUTOINCREMENT` —— 否则删除最大 id 行后新行会复用该 id，破坏「id 顺序 = 插入顺序」

**驱动分支**：`init_schema` 按 feature 分支。`sqlite` 执行 `SCHEMA_SQLITE`；`postgres` 目前返回 `CoreError::Internal("postgres schema is not implemented yet")` —— 接入时新增一份**独立的** `SCHEMA_POSTGRES`（主键 `BIGINT GENERATED BY DEFAULT AS IDENTITY`），不要复用 SQLite 的 DDL。存储层无需改动：`INSERT ... RETURNING`、`push_bind(bool)`、`BOOLEAN` 列在两个驱动上均已实测成立

**布尔列**：一律 `BOOLEAN`，比较用 `push_bind(bool)` 或 `= TRUE` 字面量，**不要写 `= 0` / `= 1`** —— SQLite 的 BOOLEAN 只有 NUMERIC 亲和性所以能跑，PostgreSQL 会报 `operator does not exist: boolean = integer`

**No migrations**: `schema.rs` runs only `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`. An existing DB file is never altered, so after any column/index change you must delete the local DB file (`~/.windai/windai.db`, or the file under `WIND_ROOT_DIR`) and let it be re-created — otherwise queries fail against the stale schema.

**Key constraints**: `agent_instances` 有 topic / agent / parent / (topic, role) 普通索引，以及 `UNIQUE (topic_id) WHERE role = 'main'` ——「一个 topic 至多一个主实例」由 partial unique index 与应用层 `get_main_instance` 的 `ensure_lte_one` 共同保证；`topic_agent_maps` 只有 `UNIQUE (topic_id, agent_id)` ——「一个定义在一个 topic 至多映射一次」。**`topic_agent_maps` 没有 `role` 列**，能力映射不表达主 Agent 偏好

**Delete cascades**: `AgentStorage::delete_instances` 删实例时连带 `tool_approval_requests` → `messages` → `agent_instances`。`TopicStorage::delete_topics` 只删该 topic 自己的 `agent_definitions` + `topic_agent_maps` + 其下实例（含上面三张表）+ 残留审批行 + topic 行；**不**级联子 topic（callers must pass child ids explicitly）

### Agent data model

**`AgentDefinition`** (`models/agent/definition.rs`) — what an agent *can do*: `key`, `name`, `description`, `owner_topic_id` (`None` = global), `cloned_from_id`, `active`, `data: AgentDefinitionData` (prompt_modules, mcp_servers via `AgentMcpBinding`, builtin_mcp_servers via `BuiltinMcpBinding`, context_policy, permission_policy, runtime_limits). There is no `scope` field — global vs. topic-local is expressed by `owner_topic_id`, and "cloned from" is `cloned_from_id`.

**`key` 由系统生成，调用方无法指定**：`CreateAgentDefinition` 不含 `key` 字段，`AgentStorage::generate_key`（`KEY_LEN = 12`）取首字符小写字母 + 其余 nanoid 的 URL 安全字母表。`UpdateAgentDefinition` 与 `update_definition` 都不含 key，生成后不可修改；`clone_definition_for_topic` 同样走 `create_definition` 生成全新 key，来源关系由 `cloned_from_id` 记录

**`AgentInstance`** (`models/agent/instance.rs`) — an agent *instance* in a topic: `id`, `parent_id` (`None` 即主实例), `topic_id`, `agent_id` (`None` = 回退为普通对话), `mode: Option<AgentMode>`, `role: AgentRole` (`Main`/`Child`), `status: AgentStatus`, `created_at`. **`model_id` 与 `tool_approval_policy` 不在实例上 —— 它们在 `Topic` 上**；实例也不再有 `enabled` / `chat_config_id` 字段

**`TopicAgentMap`** (`models/agent/topic_map.rs`) — topic 能力映射（`topic_agent_maps`）: `id`, `topic_id`, `agent_id`, `created_at`（无 `role`，`CreateTopicAgentMap` 同形，没有 `UpdateTopicAgentMap`）。映射只表达「这个 topic 拥有该 AgentDefinition 能力」，用户只能添加或删除。`helper::get_or_create_main_instance` 创建的主实例**不绑定任何定义**（`agent_id = None`）—— 主实例只负责调度，`agent_list_agents` / `agent_spawn_agent` 让它从 `topic_agent_maps` 里选能力。`AgentStorage::list_sub_definitions_by_topic(topic_id)` 返回该 topic 映射到的**全部**定义，**不做任何过滤** —— `active = false` 的定义同样返回

**`AgentStatus`**: `Idle → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`

**`PromptModule`** (`models/agent/prompt.rs`) — reusable prompt fragments referenced by `AgentDefinitionData.prompt_modules`；展示名同时体现在字段与列名上（`alias`）

**`ToolApprovalRequest`** (`models/agent/approval.rs`) — persisted per-tool-call approval record (`instance_id`, `topic_id`, `message_id`, `tool_call_id`, `tool_name`, `arguments`, status: `Pending`/`Approved`/`Denied`).

**`ModelConfig`** (`models/model.rs`) — `Model.config: Option<ModelConfig>` 只含 `stream: Option<bool>` 与 `reasoning: Option<ReasonEffort>` (`None`/`Low`/`Medium`/`High`/`Xhigh`)。请求配置按**模型**配置 —— 旧的 `chat_configs` 表、`ChatConfig` 与 `ReqConfig` 都已删除

### Test organization

| File | Content |
|------|---------|
| `windai/core/tests/storage.rs` | 30 tests — integration tests for all `*Storage` structs: CRUD, validation, cascade, batch |
| `windai/core/tests/schema.rs` | 14 tests — the schema↔model contract: 12 张表各一条列断言（`assert_table_columns`），加 `dropped_columns_are_absent` 与 `dropped_tables_are_absent` |
| `windai/core/tests/autoincrement.rs` | 7 tests — 自增主键契约：12 张表 id 非空且落在 JS 安全整数内、不复用（守护 `AUTOINCREMENT`）、严格递增、`RETURNING id` 与库中一致、`create_requests` 的自然键关联与超限分块、布尔列往返 |
| `windai/core/tests/agent_runtime.rs` | 2 tests — runtime 生命周期与 `terminal_event_is_delivered_before_stream_closes`（`Effect::CloseEventStream` 的时序） |
| `windai/core/tests/core_chat.rs` | Non-MCP chat test (`test_agent_chat`, `#[ignore]` behind `.env`): seeds providers/agents/topic 能力映射, subscribes to topic events, drives `create_task` |
| `windai/core/tests/chat.rs` | AI adapter tests (needs `.env`) |
| `windai/core/tests/common/lib.rs` | Shared helpers: `init_test_pool()`, `init_test_core()`, `init_test_core_with_registry()`, `seed_definition()`, `seed_chat_fixture()`, `McpTestEnv`, MCP server params |
| `windai/http/tests/common/mod.rs` | `test_core()` / `test_core_with_pool()` — `Arc<WindCore>` over a single-connection in-memory pool |
| `windai/http/tests/*` | Router/facade/DTO tests via `app(state)` + `tower::ServiceExt::oneshot`; `agent_instance.rs`（13 tests）覆盖主实例解析、子实例列表与 agent-map 增删查 |
| `windai/core/src/storage/{utils,message}.rs` (cfg test) | `utils.rs`：SQL macro 单元测试；`message.rs`：`list_contexts` 语义（crate 内部 API） |

**Shared-registry test architecture**: `core_chat.rs::shared_chat_registry()` parks one empty `RegistryHandle` in a dedicated long-lived tokio runtime thread (`OnceLock` + `mpsc::sync_channel`), and each test builds its **own** `WindCore` via `init_test_core_with_registry(shared)`. The per-test pool is `sqlite::memory:` with `max_connections(1)` (`common/lib.rs::init_test_pool`) so schema init and later queries always hit the same in-memory DB. A test that did need MCP servers would seed its own server record + provider/model/topic against that shared registry. `agent_runtime.rs` 用 `core` 侧的 `init_test_core()`；`windai/http/tests/*` 有自己的一份 `common/mod.rs`（`test_core()`），走 `WindCore::init_with_pool` 自建 registry

### VS Code debugging

`.vscode/launch.json`（`.vscode` 被 `.gitignore` 忽略、不在版本控制内，本地缺失时需自行创建）— uses `cargo build --tests` (NOT `cargo test`) to bypass `codelldb-launch` which crashes on Windows MSVC. LLDB debugs the compiled test binary directly. Two template configs: `Debug MCP Test` and `Debug Chat Test` — modify `filter.name`, `args[0]`, and `--ignored` flag per test function.

### Key patterns

**Adding a new provider adapter:** Implement `ChatAdapter`, add a variant to `AdapterType`, register in `get_chat_adapter()`.

**Adding a new provider:** Insert into `providers`, add credentials, insert models — no code changes.

**MCP tool flow:** `AgentDefinitionData.mcp_servers` (enabled `AgentMcpBinding`s) → `batch_get_by_ids` → server names → `list_tools_by_names()` → filter by `is_tool_allowed()` (allowed/denied tool lists) → merged with built-in agent tools (main role only) via `helper::build_agent_tools()` → `tool_calls` in response → `partition_tool_calls_by_policy()` splits by the **topic's** policy → auto-approved execute immediately, manual-review yield `ApprovalRequired` → `TaskManager::resume()` on resume → results as context → loop.

**Agent tool flow:** LLM calls `agent_list_agents` or `agent_spawn_agent` → `AgentRuntime` identifies `agent_`-prefixed calls → `parse_agent_action()` coalesces duplicate `list_agents` → `AgentHost::spawn_agent()` sends `SupervisorRequest::SpawnAgent` → `TopicFsm` → `Effect::SpawnChild` → `TaskManager::spawn_child()` starts a child `SyncTask` → when the child completes, `TopicRuntime::handle_pending` 通过 `TaskManager::take_pending()` 取出待完成的 `PendingChild`，并把结果沿其 `oneshot` 回给等待中的父实例

**`ToolApprovalPolicy`**: 每个 **topic** 一个策略，控制工具执行：`AllowAll`（默认 —— 全部自动执行）、`AllowList(Vec<String>)`（名单内的工具名自动执行）、`Manual`（全部需要审批）。以 JSON 存于 `topics.tool_approval_policy`

**`create()` returns the record:** `storage.xxx().create(...)` returns the full object (e.g. `Topic`, `Model`, `AgentInstance`) with its `.id` populated — there is no separate "return `i64`, then `get(id)`" round-trip.

**Topic / instance / message scoping**: there is exactly one `TopicRuntime` and one main Agent per topic; a topic holds multiple `AgentInstance`s (`list_instances_by_topic`, 其中 `role = Main` 的至多一个). Agent-to-agent isolation is **not** achieved by wrapping each agent in its own sub-topic — instead every `Message` carries an `instance_id`, so an instance's conversation lives in the messages filtered by that instance (`MessageStorage::list_by_instance` / `list_contexts`). **每次 spawn 都新建实例，不复用空闲实例** — 同一个 `AgentDefinition` 在一个 topic 下会累积多条 instance，靠各自的 `instance_id` 隔离消息。`topics.parent_id` remains in the schema but no code creates child topics today.

**`From<Message> for UpdateMessage`** (`models/message.rs`) preserves fields from the source message. Do NOT set optional fields to `None` unless you intend to clear them.

**Fork mode context**: When spawning an agent in `Fork` mode, `helper::create_fork_contexts()` copies the main agent's message history as the child's starting context — enabling the child to have full visibility of the parent's conversation.

## Agent skills

### Issue tracker

GitHub Issues on `evilArsh/windai` — use the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout — one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
