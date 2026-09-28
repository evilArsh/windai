# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                           # Build entire workspace
cargo build -p wind-core              # Build a specific crate
cargo test                            # Run all tests (SQLite, no external DB)
cargo test --workspace                # Every crate's tests in one run（当前 359 passed / 0 failed / 18 ignored）
cargo test -p wind-core               # Tests for a specific crate
cargo test -p wind-http               # HTTP route/facade/DTO tests (no .env needed)
cargo test -p wind-core -- --ignored  # .env-gated integration tests
cargo test -p wind-core --test core_chat -- --include-ignored --test-threads=1  # one file's ignored tests
# 换存储驱动（feature 二选一，同时开会 compile_error!）
cargo build -p wind-core --no-default-features --features postgres
```

Copy `.env.example` to `.env` and fill in `TEST_*` values for the `.env`-gated tests（`windai/core/tests/chat.rs`、`windai/core/tests/core_chat.rs`）。

**测试清单**（数字是当前快照，会随代码变化）。所有非 `.env` 用例都跑在 `sqlite::memory:` 上，不需要外部数据库：

| File | Tests | Content |
|------|-------|---------|
| `windai/core/tests/storage.rs` | 32 | 各 `*Storage` 的 CRUD、校验、级联、批量 |
| `windai/core/tests/schema.rs` | 15 | schema↔model 列契约：13 张表各一条断言 + `dropped_columns_are_absent` + `dropped_tables_are_absent` |
| `windai/core/tests/autoincrement.rs` | 7 | 自增主键契约：id 非空且落在 JS 安全整数内、不复用（守护 `AUTOINCREMENT`）、严格递增、`create()` 返回值与库中一致、批量插入的自然键关联与分块、布尔列往返 |
| `windai/core/tests/message_content.rs` | 9 | `message_contents` 契约（追加成行、按 id 排序、级联删除、老 schema 写入报错）与上下文获取（展平顺序、`max_context` 按消息条数截断、`is_excluded` 隔离） |
| `windai/core/tests/agent_runtime.rs` | 3 | runtime 生命周期、终态事件先于关流到达订阅者、终态内容按 `MessageContent` 落库 |
| `windai/core/tests/agent_flow.rs` | 9 | 本地假 SSE 服务驱动真实 runtime（无 `.env`、无网络）：审批恢复后块按 id 追加、SSE `index` 是本次运行内的块序号、取消不落内容块、历史遗留工具调用不重放、错误文本不被 JSON 包装、空响应不落空白块、用户输入与子实例首轮输入的内容块经 SSE 下发 |
| `windai/core/src/chat/runner.rs` (cfg test) | 8 | `pending_tool_calls` 的待办工具调用判定，含「只扫描最后一条用户简单消息之后」 |
| `windai/core/src/storage/message.rs` (cfg test) | 3 | `list_contexts` 的排除/删除/boundary 语义（crate 内部 API，集成测试访问不到） |
| `windai/core/src/storage/utils.rs` (cfg test) | 21 | SQL 宏单元测试 |
| `windai/core/src/chat/rule.rs` (cfg test) | 13 | JSON 规则的 `$ctx` 注入与各 adapter 的默认端点 |
| `windai/core/src/env.rs` (cfg test) | 4 | 目录初始化与 `WIND_ROOT_DIR` 覆盖 |
| `windai/core/src/models/mcp.rs` (cfg test) | 5 | `ServerParams` 的转换与校验 |
| `windai/core/tests/core_chat.rs` | 1（`#[ignore]`） | `test_agent_chat`：seeds providers/agents/能力映射，订阅 topic 事件，驱动 `create_task` |
| `windai/core/tests/chat.rs` | 1（`#[ignore]`） | `test_handle_chat`：AI adapter 往返 |
| `windai/http/tests/*` | 85 | `app(state)` + `tower::ServiceExt::oneshot`；`agent_instance.rs` 17、`facade.rs` 17、`mcp_registry.rs` 14、`mcp_runtime.rs` 10、`openapi.rs` 7、`routes.rs` 5、`envelope.rs` 5、`extractor.rs` 3、`topic_children.rs` 3、`dto.rs` 2、`middleware.rs` 2 |
| `windai/http/src/sse.rs` (cfg test) | 2 | SSE 帧格式与取消语义 |
| `windai/skills/tests/{meta,scan}.rs` | 8 + 7 | `SKILL.md` frontmatter 解析与目录扫描 |

`windai/core/tests/common/lib.rs` 与 `windai/http/tests/common/mod.rs` 是共享 helper（不是测试目标）。core 侧提供 `init_test_pool()` / `init_test_core()` / `init_test_core_with_registry()` / `seed_definition()` / `seed_chat_fixture()` / `McpTestEnv`；http 侧提供 `test_core()` / `test_core_with_pool()`。

## Architecture

```
wind-http   ──depends-on──>  wind-core, wind-ai, wind-mcp, wind-rule
wind-tui    ──depends-on──>  wind-core, wind-ai, wind-mcp, wind-rule
wind-core   ──depends-on──>  wind-ai, wind-mcp, wind-rule
wind-mcp    ──depends-on──>  wind-skills
wind-ai / wind-rule / wind-skills ──depends-on──>  仅第三方 crate
```

`wind-core` is the shared business core; `wind-ai`/`wind-mcp`/`wind-rule`/`wind-skills` are capability crates. **Nothing depends on `wind-http`** — it is just one adapter over the core (future targets: napi-rs, Android)。`wind-http` 声明了 `wind-rule` 依赖但当前源码零引用。

The vocabularies of this file and `README.md` are kept in sync with the code — if you find a mismatch, trust the code and fix the doc.

### 已知死代码与不一致（待清理）

- `wind-http/Cargo.toml` 声明了 `wind-rule` 依赖但源码零引用；`tower-http` 的 `cors` / `set-header` feature 启用未使用。
- `wind_mcp::init_builtin(&mut Registry)` 是空壳（函数体只有注释），且 `Registry` 无法从外部构造 —— 实际注册内建 server 走 `RegistryHandle::acquire_builtin`（由 `WindCore::init_with_pool_and_registry` 调用）。
- `MCP_TOOL_IDENTIFIER`（`"0m0"`）是 `wind-mcp` 的私有常量，未导出。
- `wind_rule::Error::Type` 从未被构造；`RuleSet::new` / `clear` / `append_rule_str` 在仓库内无调用方（core 只用 `from_json` + `apply`）。`cond.rs` / `compile.rs` / `path.rs` 的部分文档示例与实现不符（`exists` 要的是字符串路径而非数组；规则 JSON 必须是 `{"rules": [...]}`）。
- `wind-tui` 只渲染 `"hello world"`、任意按键即退出；它声明的 `wind-mcp` / `wind-ai` / `wind-rule` / `tokio` 依赖在源码里完全未使用。
- 根 `Cargo.toml` 的 `regex` / `unicode-segmentation` / `encoding_rs` / `serial_test` 四个 workspace 依赖没有任何 crate 使用。
- `CoreError::Chat` 没有构造方（原本只被已删除的 `find_pending_calls` 使用）。

### `wind-core` — Central orchestration

`WindCore` (`lib.rs`) is a process-level runtime root: it owns a `Storage` (which holds the `DbPool`), a `RegistryHandle` (MCP), a `CancellationToken`, and a `Mutex<HashMap<i64, TopicRuntimeHandle>>` of per-topic runtimes. One instance per process/tests — independent runtimes share nothing. Public surface:

```rust
WindCore::init_memory()                                          // In-memory SQLite (tests/ephemeral)
WindCore::init_local()                                           // File-backed SQLite at app_dirs().db_path()
WindCore::init_with_pool(pool)                                   // Init with external pool (own Registry)
WindCore::init_with_pool_and_registry(pool, registry)            // Init with external pool + shared Registry (tests)

core.storage()              // &Storage — all CRUD access
core.registry()             // &RegistryHandle — MCP client registry
core.fetch_topic(id)        // TopicRuntimeHandle — get-or-create；句柄已停止（is_stopped()）时换成新的运行时（shutdown 后 panic）
core.shutdown().await       // 取消所有 topic 运行时 + 关闭 MCP 客户端 + 关连接池
```

All four init methods converge on `init_with_pool_and_registry`, which runs `schema::init_schema` and registers the two builtin MCP servers (`FsServer`, `SkillsServer`) onto the registry。私有的 `init(db_url)` 只被 `init_local` / `init_memory` 使用。There is **no `core.chat()`** — use `fetch_topic()` + `TopicRuntimeHandle::create_task()`.

`app_dirs()` (`env.rs`) 解析根目录：`WIND_ROOT_DIR` 优先，否则 `~/.windai`；DB 文件 `windai.db`，topic 工作目录 `topics/`，技能目录 `skills/`。

### Storage

`Storage` (`storage.rs`) is `Clone` and holds the executor + 8 sub-storages:

```rust
core.storage().provider()   // &ProviderStorage (incl. credentials + json_rules)
core.storage().topic()      // &TopicStorage
core.storage().model()      // &ModelStorage
core.storage().message()    // &MessageStorage (messages + contents)
core.storage().mcp()        // &McpStorage
core.storage().agent()      // &AgentStorage
core.storage().prompt()     // &PromptStorage
core.storage().approval()   // &ToolApprovalStorage
```

`TableName` (`storage.rs`, `pub(crate)`) centralises the 13 table names as associated constants, and the `*Storage` files build queries from them；列名仍以字面量传入 `select_fields!` / `insert!` 等宏。`storage/utils.rs` 还提供 `now_ts` / `vec_to_str_default` / `vec_to_str_optional` / `map_to_str_default` / `map_to_str_optional` / `de_str_to` / `parse_str_to` / `ensure_affected` / `ensure_lte_one`。

All `create()` methods return the **full record** (`Result<Topic>`, `Result<Model>`, `Result<AgentInstance>`, …) — read `.id` off the result. **id 由数据库自增生成**：SQLite 侧是 `INTEGER PRIMARY KEY AUTOINCREMENT`，`create()` 用 `INSERT ... RETURNING id`（`executor.fetch_one_scalar`）取回；不再有应用层的 id 生成器。自增 id 保证「id 顺序 = 插入顺序」，这正是 `ORDER BY id` 与 `list_contexts` 的 `id > MAX(id)` 所依赖的。

**Messages 与 MessageContent 同属 `MessageStorage`**（`storage/message.rs`）：`MessageStorage` 既管 `messages` 表也管 `message_contents` 表 —— `create` / `update` / `get` / `get_from_msg` / `delete` / `list_by_instance`（消息结构）与 `create_content` / `list_contents(message_id)` / `list_contents_by_messages(&[i64])` / `delete_contents_by_messages` / `delete_contents_by_instances`（正文内容块）。没有单独的 `MessageContentStorage`。

**消息正文在 `message_contents`**：`Message` 只承载会话结构（`from_id` / `model_id` / `instance_id` / `is_boundary` / `is_excluded` / `created_at`），正文按内容块拆到 `MessageContent`（`id` / `message_id` / `data: AiMessage`）。**块顺序由自增 `id` 决定**，`create_content` 只追加、不覆盖（一次模型响应累积成一块、工具调用结果自成一块），**不维护 `messages` 上的任何汇总列** —— token 只落在块上（`AiMessage.input_tokens` / `output_tokens` 平铺成列），读取方按内容块自行累加。模型里没有 `index` 字段：`index` 只存在于 SSE 事件里，是**本次运行内的块序号**。

**Messages hang off an instance, not a topic**: `MessageStorage` queries by instance — `list_by_instance(instance_id)` (all messages of an instance, ordered by id)。`list_contexts(instance_id)`（not `is_excluded`，且只保留最后一个 `is_boundary = true` 之后的消息）是 **`pub(crate)`**，只供 core 内部（`helper::load_contexts`）使用。

**Transactions**: `storage.with_tx(|inner| async { ... }).await`（不是 `.tx()`）。多步事务：`storage.begin().await` → `StorageTx`（绑定事务的 `Storage`），用 `.storage()` / `.commit()` / `.rollback()`。

**取回自增 id 一律用 `INSERT ... RETURNING id`**，不要用 `last_insert_rowid()`（SQLite 驱动专有，PostgreSQL 无对应物）。批量插入时**不要依赖 `RETURNING` 的返回顺序**（两个驱动都不承诺），而要用自然键关联 —— 见 `ToolApprovalStorage::create_requests` 用 `tool_call_id` 回填 id，并按 `input.calls` 原始顺序组装返回值。

**SQL macros** (`storage/utils.rs`): `insert!`, `update!`, `update_fields!`, `insert_fields!`, `delete_by_id!`, `delete_from!`, `select_fields!`, `get_by_id!`。Values are `Option`-wrapped; `None` fields are skipped。`update!` appends `updated_at` and `WHERE id = ?`。（`executor.rs` 的 `with_transaction!` / `with_connection!` 是连接池 plumbing。）

**Serializing `Option` vec/map fields** (`storage/utils.rs`):
- `vec_to_str_default(Some(v))` → JSON string；`vec_to_str_default(None)` → `"[]"` —— 总是写列
- `vec_to_str_optional(v)` → `Some(json)` / `None` —— 为 `None` 时跳过列；`update!` 用它避免写入未提供的字段

`UpdateMessage` 只有 `model_id` —— 正文与 token 都不由客户端直写。Tool approvals 不再存在消息上（见 Tool Approval Flow）。

**`AgentStorage` 的可见性**（`storage/agent.rs`）：`create_definition` / `update_definition` / `delete_definition` / `get_definition` / `get_definition_by_key` / `list_definitions` / `select_definitions` / `clone_definition_for_topic` / `create_instance` / `update_instance` / `delete_instances` / `get_instance` / `get_main_instance` / `list_instances_by_topic` / `list_definitions_by_topic`，以及 4 个能力映射方法（`create_topic_agent_map` / `list_agent_maps_by_topic` / `get_agent_map` / `delete_topic_agent_map`）都是 `pub`；级联用的 `batch_delete_agent_maps_by_topics` 是 `pub(crate)`；`select_instances` / `select_agent_maps` 两个查询构造器是私有的。

### Agent system

The agent system replaces the old public `ChatEngine`. Each `Topic` gets a `TopicRuntime` that runs a **finite-state machine**: every incoming command, task notification, and supervisor request is reduced into effects that the runtime then executes.

#### FSM core (`agent/fsm.rs`)

- `TopicFsm` — a **pure reducer with no side effects**: `reduce(&mut self, event: FsmEvent) -> Vec<Effect>`。它的状态只有 `main_instance_id` 和 `HashMap<i64, TaskFsm>`（按 instance 索引）—— **没有队列**。查询入口：`main_instance_id()` / `task_state(id)` / `is_main_instance(id)` / `is_task_busy(id)` / `is_main_busy()`。
- `TaskFsm` (`fsm/task_fsm.rs`) — per-task state machine whose state is `AgentStatus` (plus `mode`)。**没有单独的 `TopicState` 类型**；`TaskFsm` 不保存 message id。
- `FsmEvent` (`fsm/event.rs`) — `Topic(TopicMsg)` / `Start { spec }` / `ChildResolved { instance_id }` / `Emit(TopicEvent)` / `Signal { instance_id, event: TaskEvent }`。没有 `StartChild` —— 子任务由父任务 `Effect::SpawnChild` 之后的 `FsmEvent::Start` 启动。
- `Effect` (`fsm/effect.rs`) — 全部变体：`PersistStatus`、`PersistContent { message_id, data }`、`Emit(TopicEvent)`、`Start { spec }`、`Resume { instance_id }`、`Cancel { instance_id }`、`SpawnChild { instance_id, call_id, request, reply }`、`Approval { instance_id, allow_ids, deny_ids }`、`StopRuntime`、`CloseEventStream`、`Init { user_input }`、`ApprovalRequest { instance_id, message_id, calls }`、`Finish { instance_id, message_id, status, content }`、`Failed { instance_id, message_id: Option<i64>, status, error }`、`Canceled { instance_id, status, error }`。没有 `PrepareMain` / `StartChild` / `SendChildResponse` / `DrainQueue`。终态副作用只带 id —— 内容由 `PersistContent` 单独下发；`Effect::Failed` 是**唯一**写错误内容块的地方（落一块并广播 `TopicEvent::Error`）。`Effect::Canceled` **不落任何内容块**：取消的语义只是「任务结束」。
- `TopicRuntime::apply()` executes effects **depth-first**: an effect's follow-up events are reduced and executed immediately before the next sibling effect, so each effect's full side-channel (including broadcasts) completes before the round ends.

**任务队列本轮未实现**：`main_status` / `has_queued` / `dequeue` / `Effect::DrainQueue` / `TopicEvent::TaskQueued` 都不存在。一轮 Sync/Fork 结束前不接受新任务。

**并发 `TopicCommand::Start` 不是排队而是拒绝**：`TopicFacade::create_chat` 只确认话题存在且已配置模型；真正的互斥在 FSM 里 —— `TopicFsm::reduce_topic_command` 用 `is_main_busy()` 判断主实例是否处于 `Running` / `WaitingApproval` / `WaitingChild`，忙时直接把 `Err(CoreError::Internal("Instance … is busy"))` 回给调用方（经 `map_core_error` 变成 500）。被拒的请求需要调用方自己重试。

#### Components

| Component | Role |
|-----------|------|
| `TopicRuntime` | Actor per topic — owns `TopicFsm`, `TaskManager`, mailbox and the `app_rx` broadcast sender；reduce 事件并执行副作用（`agent/topic.rs`） |
| `TopicRuntimeHandle` | Cloneable handle — `create_task(Vec<Content>)`、`cancel_task(instance_id)`、`approve(instance_id, allow_ids, deny_ids)`、`subscribe() -> broadcast::Receiver<TopicEvent>`、`shutdown()`、`is_stopped()`（`agent/topic.rs`） |
| `TopicMailbox` / `TopicMsg` | mpsc sender carrying `Command(TopicCommand)` / `Task(TaskNotification)` / `Supervisor(SupervisorRequest)`（`agent/event.rs`） |
| `TaskManager` | Per-topic task registry（`agent/task.rs`）—— `instance_map: HashMap<instance_id, TaskEntry>` + `pending: Vec<PendingChild>`；owns `init` / `resume` / `start` / `cancel` / `spawn_child` / `persist_content` / `persist_approval_state` / `persist_approval_record` |
| `PendingChild` | Links parent/child instances while a spawned agent is pending；子任务结束时由 `TaskManager::take_pending()` 取出 |
| `SyncTask` / `SyncTaskHandler` | Task actor per agent instance（`agent/task/sync.rs`）—— `SyncTask::spawn(ctx, instance_id, topic_id, topic_tx, storage, mcp_registry)` 返回 `SyncTaskHandler`，其 `start(&self, spec)` / `cancel(&self)` 通过 `mpsc` 投递；任务循环里构造 `AgentRuntime` |
| `AgentRuntime` | The LLM loop — runs `ChatRunner`, partitions tool calls, handles approval/agent tools（`agent/runtime.rs`） |
| `ChatRunner` | The low-level LLM interaction engine（`chat/runner.rs`） |
| `AgentHost` | `async_trait` — how `AgentRuntime` reaches the outside world（`execute_tool_calls` / `spawn_agent` / `list_agents` / `list_approvals` / `emit`）（`agent/host.rs`） |
| `SyncHost` | `SyncTask` 使用的 `AgentHost` 实现；`agent/task/sync.rs` 内私有，通过 `TopicMailbox` 回到 `TopicRuntime` |
| `AgentRuntime` 的 `BlockState` | `{ data: Message, index: i64 }` —— partial 缓存块 + **本次运行内的块序号**（`AgentRuntime::run` 里创建，因此每次 resume 都从 0 重新开始） |

#### Lifecycle

```
TopicRuntimeHandle::create_task(user_input)
  → TopicCommand::Start { user_input }
  → TopicFsm::reduce → Effect::Init { user_input }
  → TaskManager::init(): get_or_create_main_instance()（已存在则复用；不存在时以 agent_id = None 创建），
    在事务内创建绑定到主实例的 user + assistant 消息，返回 CreatedContexts + TaskSpec；
    CreatedContexts { user, assistant, user_content } 把用户输入块一并交给上层
  → Effect::Init 与 Effect::SpawnChild 都按 InstanceCreated → MessageCreated(user) →
    Message(user content, index: 0) → MessageCreated(assistant) → Start 的顺序下发（`launch_events`）
  → TaskManager::start(): SyncTask::spawn → 登记 TaskEntry → handler.start(spec) 启动 AgentRuntime
  → AgentRuntime::run(): run_chat() → ChatEvent stream
      Partial        → TaskNotification::Message { partial: true } → Effect::Emit(TopicEvent::Message)
      块结束          → close_block：空块丢弃，否则 TaskNotification::Message { partial: false }
                        → Effect::PersistContent → MessageContent 落库（不广播）
      Finish（带 tool_calls）→ make_tool_plan() → partition_tool_calls_by_policy() →
                       待审批的调用 → TaskNotification::ApprovalRequired → Effect::ApprovalRequest
                       已批准/自动批准的调用 → 执行 → 工具结果块整块下发 + 落库 → 继续下一轮
      Finish         → TaskNotification::Finish → TaskFsm → Effect::Finish → Emit(MessageFinished)
      error          → TaskNotification::Failed → Effect::Failed → 落错误块 + Emit(TopicEvent::Error)
  主实例进入终态（Finished / Failed / Cancelled）或等待审批 → 本轮 apply() 结束后关流
```

`ChatRunner::run()` 的短路：上下文里还有未执行的工具调用时（`has_pending_calls`）直接 `yield ChatEvent::Finish { contexts, error: None }`，不请求模型。**审批恢复就靠这条路径** —— `AgentRuntime` 在 Finish 分支用 `pending_tool_calls(&contexts)` 判定待办调用（不是看本轮块里有没有 `tool_calls`），把已审批的调用执行掉再继续下一轮；`handle_await_tool_call` 返回 `false`（有调用在等待审批）时本轮直接 `Stop`，避免短路循环里反复重发审批请求。

`AgentStatus` transitions: `Idle → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`。

#### 事件流关闭时机

关流是一个普通 effect。`TopicFsm::close_main_stream_guard`（`agent/fsm.rs`）在**主实例**收到 `Cancelled` / `ApprovalRequired` / `Failed` / `Finish` 时追加 `Effect::CloseEventStream`，`TopicRuntime::close_event_stream()` 把 `app_rx: Option<broadcast::Sender<TopicEvent>>` 置 `None`，此后 `emit()` 不再广播。

该 effect 由 `TaskFsm::reduce` 的返回值追加在同一次 `Signal` 归约里，而 `apply()` 是深度优先的，因此终端 `Effect::Emit` 一定先于关流执行 —— 主实例的 `MessageFinished` / `Error` / `ApprovalRequired` 不会被丢弃（`windai/core/tests/agent_runtime.rs` 覆盖了该时序）。`TopicRuntime::shutdown()` 也会关流。代价是：主实例一旦进入终态或等待审批，事件流立即关闭，再次对话必须重新 `subscribe()`。**子实例**等待审批与进入终态都不关流。

#### Agent modes

`AgentMode`: `Sync`（父实例阻塞等待）、`Fork`（子实例通过 `helper::create_fork_contexts()` 复制主实例的消息历史作为初始上下文）、`Background`（**未实现** —— `agent/task/background.rs` 是空文件，`TaskManager::spawn_child` 会在开启事务前以 `CoreError::Validation` 拒绝）。

`AgentDefinitionData.permission_policy`（`can_spawn_agents` / `can_spawn_sync` / `can_spawn_background` / `can_spawn_fork` / `can_spawn_recursive` / `max_spawn_depth`）与 `runtime_limits` 目前**没有任何调用方**，spawn 不受它们约束。只有 `context_policy.max_context` 生效（`helper.rs` 内私有函数 `flatten_contexts`）。`active` 只在 `get_def_by_id` / `get_def_by_key`（以及 `TopicStorage::create` 校验绑定的 agent）里被检查 —— `agent_list_agents` 与 `list_definitions_by_topic` 都不过滤。

#### Agent tools (virtual tools injected for the LLM)

Agent tools use the `agent_` prefix（`AGENT_TOOL_PREFIX = "agent_"`，`agent/tool.rs`）—— 注意是**下划线**，不是点。它们不是 MCP 工具，`AgentRuntime` 在进程内拦截。

| Tool | Purpose |
|------|---------|
| `agent_list_agents` | 列出当前 topic 能力映射（`topic_agent_maps`）中的全部 AgentDefinition，**不做任何过滤**（`active = false` 的定义同样返回） |
| `agent_spawn_agent` | Spawn a sub-agent with `agent_key`、`mode`（`sync` / `fork`）与 `task` |

Only the main role gets these built-ins（`helper::build_agent_tools`，FIXME 注明是为避免递归创建 Agent）。一批里多个 `list_agents` 调用会经 `parse_agent_action()` 合并成一次查询。Spawning sends `SupervisorRequest::SpawnAgent` → `Effect::SpawnChild` → `TaskManager::spawn_child()` → 子 `SyncTask`；子任务结束时 `TopicRuntime::handle_pending` 通过 `TaskManager::take_pending()` 取出 `PendingChild`，把结果沿它携带的 `oneshot` 回给等待中的父实例。

#### Topic events (broadcast)

`TopicEvent`（`agent/event.rs`）—— 通过 `TopicRuntimeHandle::subscribe()` 消费，`#[serde(tag = "type", content = "data")]`：

- `Error` — `instance_id: Option<i64>` + `message_id: Option<i64>` + `error: String`
- `MessageCreated` — 新消息已落库（topic + `instance_id` + `data: Message`）
- `Message` — 内容块事件（`instance_id` + `message_id` + `index` + `data: AiMessage`）。`index` 是**本次运行内的块序号**（run 开始时为 0，每落一块 +1，跨运行不连续）：相同 `index` 的分片属于同一块，消费方按增量拼接。该变体同时承载**整块到达的非流式块** —— 用户输入（`Effect::Init` / `Effect::SpawnChild` 在 user 消息的 `MessageCreated` 之后以 `index: 0` 下发一次）与工具结果（runtime 执行完调用后整块下发）。SSE 载荷里没有 `partial` 标志，消费方只能按 `index` 拼接。块落库由 `PersistContent` 完成（落库不再广播）
- `MessageFinished` — 消息完成（topic + `instance_id` + `message_id`）
- `TaskStatusChanged` — 实例状态迁移（`instance_id` + `status` + `mode`）
- `ApprovalRequired` — 工具调用需要用户审批（`instance_id` + `message_id` + `requests`）
- `InstanceCreated` — 实例已被采用（`data: AgentInstance`）；在 `TaskManager::init` / `resume` 成功后与 `MessageCreated` 一同发出，因此创建 topic 时预先建好的主实例也经它下发给订阅方

The broadcast channel is closed after the main instance reaches a terminal state or waits for approval, and when the runtime stops — re-subscribe per conversation.

### Tool Approval Flow

`ToolApprovalPolicy`（`Manual` / `AllowList(Vec<String>)` / `AllowAll` default）存在 **`Topic.tool_approval_policy`**（`Option`，`None` 视同 `AllowAll`）—— 没有 per-instance 策略。

When the model requests tool calls:

1. `AgentRuntime::make_tool_plan()` 先查 `ToolApprovalStorage` 里当前 message 上的审批行。
2. `partition_tool_calls_by_policy()` 按 topic 的策略切分：自动批准的工具经 MCP 执行；Agent 工具走 `parse_agent_action()`；未处理的调用成为 `ApprovalRequired`。
3. `helper::save_approval_state()` 落 `ToolApprovalRequest` 行（status `Pending`），随后 `Effect::ApprovalRequest` 广播 `TopicEvent::ApprovalRequired`（助手消息的正文已由 `PersistContent` 按块落库，不再整条回写）。
4. 调用方回 `TopicRuntimeHandle::approve(instance_id, allow_ids, deny_ids)` → `TopicCommand::Approval` → FSM（非 `WaitingApproval` 一律拒绝并 warn）→ `Effect::Approval` → `batch_set_status()` → `TaskEvent::ApprovalResolved` → `Effect::Resume` → `TaskManager::resume()` 重新加载消息对与上下文、续跑 `AgentRuntime`；新一轮的 SSE 块序号从 0 重新开始，新块按自增 id 追加在已落库的块之后，不覆盖它们。

**Rejection markers**: Denied tools get `{"error": "tool call denied", "tool": "..."}` in their `FunctionCallOutput`。

### Chat runner (low-level LLM interaction)

`ChatRunner` (`chat/runner.rs`) is the internal engine used by `AgentRuntime`, not called directly by external code.

**`ChatContext`**: `topic`, `model`, `provider`, `credential`, `rule_set: Option<JsonRule>`, `tools: Option<Vec<Tools>>`。

`run_chat(&ChatContext, Vec<Message>) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + '_>>`。`ChatEvent`（`chat/events.rs`）只有两个变体：`Partial { delta }` 与 `Finish { contexts, error: Option<String> }` —— 没有 `Created` / `Completed` / `Failed`。流式：`handle_chat()` → `client::request_sse()`（加 `Accept: text/event-stream`）→ `response.bytes_stream().eventsource()` → 逐事件 `chat_adapter.parse_stream_chunk(&event)`，流结束（EOF）时补一个 `ResEvent::new_finish()`。`eventsource/`（fork 自 `eventsource-stream`，README 见该目录）是**行级 nom 状态机**：只有空行才 dispatch 事件（等价于按 `\n\n` 切），`Utf8Stream` 保证跨 chunk 的多字节字符不被截断，`data: [DONE]` 被静默丢弃（带 FIXME）。

`pending_tool_calls(contexts) -> Option<Vec<&FunctionCall>>` 只扫描**最后一条用户简单消息之后**的内容块，并只统计 `Content::FunctionCall`。全展平之后必须限定范围，否则上一轮遗留的未执行工具调用会被新任务重新执行、模型被绕过。`run_chat` 的短路与 `AgentRuntime` 的待办判定共用这一个实现（`chat/runner.rs`，cfg test 8 条）。

### `wind-core` models

**`Message`** (`models/message.rs`) — `id` / `from_id`（`None` 即用户消息）/ `model_id` / `instance_id` / `is_boundary` / `is_excluded` / `created_at`。**`MessageContent`** — `id` / `message_id` / `data: AiMessage`（`FromRow` 时把平铺的 `role` / `content` / `reasoning_content` / `tool_calls` / `input_tokens` / `output_tokens` / `created_at` 组装回 `AiMessage`；`created_at` 是 `AiMessage` 的透传值，不用于排序或过滤）。`CreateMessage` / `CreateMessageContent` / `UpdateMessage { model_id }`。

**`Topic`** (`models/topic.rs`) — `id` / `parent_id`（树结构保留，但**没有代码创建子话题**）/ `label` / `model_id` / `tool_approval_policy` / `icon` / `created_at`；`CreateTopic.agent_id` 指定时在同一事务内校验定义存在且 `active`，并创建 `role = Main` 的实例（`CreateInstance::new_main`），校验失败则整个创建被拒，不留孤儿 topic；未指定时**不创建实例**，主实例留给 `helper::get_or_create_main_instance` 在首次 `create_task` 时懒创建（`agent_id = None`）。`ToolApprovalPolicy` 是 `#[serde(tag = "type", content = "tools")]` 的枚举，默认 `AllowAll`。

**`AgentDefinition`** (`models/agent/definition.rs`) — what an agent *can do*: `id` / `key` / `name` / `description` / `owner_topic_id`（`None` = 全局）/ `cloned_from_id` / `active` / `data: AgentDefinitionData` / `created_at`。`data` 含 `prompt_modules`、`mcp_servers`（`AgentMcpBinding`：`mcp_server_id` + `allowed_tools` + `denied_tools` + `enabled`）、`builtin_mcp_servers`（`BuiltinMcpBinding`：`name` + 名单 + `enabled`）、`context_policy`（`max_context: Option<i32>`）、`permission_policy`、`runtime_limits`。没有 `scope` 字段 —— 全局 vs topic 私有由 `owner_topic_id` 表达。

**`key` 由系统生成，调用方无法指定**：`CreateAgentDefinition` 不含 `key`，`AgentStorage::generate_key`（`KEY_LEN = 12`）取首字符小写字母 + 其余 nanoid 的 URL 安全字母表；`UpdateAgentDefinition` 与 `update_definition` 都不含 key，生成后不可修改；`clone_definition_for_topic` 同样走 `create_definition` 生成全新 key，来源关系由 `cloned_from_id` 记录。

**`AgentInstance`** (`models/agent/instance.rs`) — `id` / `parent_id`（`None` 即主实例）/ `topic_id` / `agent_id`（`None` = 回退为普通对话）/ `mode: Option<AgentMode>` / `role: AgentRole`（`Main`/`Child`）/ `status: AgentStatus` / `created_at`。**`model_id` 与 `tool_approval_policy` 不在实例上 —— 它们在 `Topic` 上**；实例没有 `enabled` / `chat_config_id` 字段。`AgentStatus`: `Idle → Running → (WaitingApproval | WaitingChild) → Finished | Failed | Cancelled`。

**`TopicAgentMap`** (`models/agent/topic_map.rs`) — 能力映射：`id` / `topic_id` / `agent_id` / `created_at`（**无 `role` 列**，`CreateTopicAgentMap` 同形，没有 `UpdateTopicAgentMap`）。映射只表达「这个 topic 拥有该 AgentDefinition 能力」，用户只能添加或删除。`AgentStorage::list_definitions_by_topic(topic_id)` 返回该 topic 映射到的**全部**定义，不做任何过滤。

**`PromptModule`** (`models/agent/prompt.rs`) — 可复用 prompt 片段，被 `AgentDefinitionData.prompt_modules`（`PromptModuleBinding { prompt_module_id, enabled }`）引用；展示名同时体现在字段与列名上（`alias`）。

**`ToolApprovalRequest`** (`models/agent/approval.rs`) — 每个 tool call 一条持久化审批记录（`instance_id` / `topic_id` / `message_id` / `tool_call_id` / `tool_name` / `arguments` / `status`：`Pending`/`Approved`/`Denied`）。

**`Model`** (`models/model.rs`) — `id` / `name` / `provider_id` / `alias` / `adapter: AdapterType` / `modalities` / `active` / `icon` / `endpoint` / `config: Option<ModelConfig>` / `frequency` / `created_at`。`ModelConfig` 只含 `stream: Option<bool>` 与 `reasoning: Option<ReasonEffort>`（`None`/`Low`/`Medium`/`High`/`Xhigh`）—— 旧 `chat_configs` 表与 `ReqConfig` 都已删除，请求配置按**模型**配置。`Provider` / `Credentials` / `JsonRule` / `McpServerParam` 见各自模型文件。

### Database tables (SQLite, `schema.rs`)

**驱动在编译期由 cargo feature 选定**（`windai/core/Cargo.toml`）：`sqlite`（默认）或 `postgres`，二者互斥 —— 同时开启会 `compile_error!`。`db.rs` 用 `mod driver_impl` 的两个 `#[cfg(feature = ...)]` 分支导出 `DbPool` / `DbDriver` / `DbRow` / `DbTransaction`（SQLite 侧 `create_pool` 打开 WAL、`foreign_keys`、5s `busy_timeout`、`max_connections(5)`）。storage 层只认这几个别名。

`providers`, `models`, `credentials`, `topics`, `messages`, `message_contents`, `mcp_servers`, `json_rule`, `prompt_modules`, `agent_definitions`, `agent_instances`, `topic_agent_maps`, `tool_approval_requests` —— 共 13 张表。`chat_configs` 与 `topic_agent_bindings` 已被删除。

Column notes: `topics` 有 `parent_id` / `model_id` / `tool_approval_policy`；`messages` 按自己的 `instance_id` 归属（**没有 `topic_id` 列**），排除标志列名与模型字段同名 `is_excluded`；`message_contents` 的 `role` / `content` / `reasoning_content` / `tool_calls` / `input_tokens` / `output_tokens` / `created_at` 是把 `AiMessage` 平铺后的列（`content` 与 `tool_calls` 序列化成字符串，`reasoning_content` 与 `tool_calls` 可空）；`agent_instances` 按 `topic_id` 归属；`topic_agent_maps` 按 `topic_id` 归属且**没有 `role` 列**；`prompt_modules` 的展示名列名是 `alias`；`tool_approval_requests` 带 `topic_id` + `message_id` + `instance_id`。

**主键 DDL**：13 张表的 `id` 一律是 `INTEGER PRIMARY KEY AUTOINCREMENT`（SQLite 侧）。必须是 `INTEGER` 而非 `BIGINT` —— 只有前者才是 rowid 别名，用 `BIGINT` 时省略 id 插入**不报错、而是静默写入 NULL**；必须带 `AUTOINCREMENT` —— 否则删除最大 id 行后新行会复用该 id，破坏「id 顺序 = 插入顺序」。`message_contents` 正是靠它：**块顺序就是 `id` 顺序**，没有单独的索引列。

**驱动分支**：`init_schema` 按 feature 分支。`sqlite` 执行 `SCHEMA_SQLITE`；`postgres` 目前返回 `CoreError::Internal("postgres schema is not implemented yet")` —— 接入时新增一份**独立的** `SCHEMA_POSTGRES`（主键 `BIGINT GENERATED BY DEFAULT AS IDENTITY`），不要复用 SQLite 的 DDL。存储层无需改动：`INSERT ... RETURNING`、`push_bind(bool)`、`BOOLEAN` 列在两个驱动上均已实测成立。

**布尔列**：一律 `BOOLEAN`，比较用 `push_bind(bool)` 或 `= TRUE` 字面量，**不要写 `= 0` / `= 1`** —— SQLite 的 BOOLEAN 只有 NUMERIC 亲和性所以能跑，PostgreSQL 会报 `operator does not exist: boolean = integer`。

**No migrations**: `schema.rs` 只跑 `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`。已存在的库文件永远不会被改写，因此任何列/索引变更后必须删掉本地库文件（`~/.windai/windai.db`，或 `WIND_ROOT_DIR` 下的那个）让它重建 —— 否则查询会撞上旧 schema。把 `messages.content` 拆成 `message_contents` 属于这类改动。反之，只是从表里去掉新代码不再读写的列（如 `messages.input_tokens` / `output_tokens`）时旧库仍可继续使用，无需删库。

**Key constraints**: `messages` 有 `idx_messages_instance (instance_id, id)`；`message_contents` 只有普通索引 `idx_message_contents_message (message_id, id)`（用于 `list_contents` / `list_contents_by_messages` 的过滤与排序），`message_id` 上**没有外键**，写入不校验父消息是否存在（无孤儿内容行保护）；`agent_instances` 有 topic / agent / parent / (topic, role) 普通索引，以及 `UNIQUE (topic_id) WHERE role = 'main'` ——「一个 topic 至多一个主实例」由 partial unique index 与应用层 `get_main_instance` 的 `ensure_lte_one` 共同保证；`topic_agent_maps` 只有 `UNIQUE (topic_id, agent_id)`。

**Delete cascades**: `AgentStorage::delete_instances` 删实例时连带 `tool_approval_requests` → `message_contents`（按 `message_id IN (SELECT id FROM messages WHERE instance_id IN …)`）→ `messages` → `agent_instances`；`MessageStorage::delete` 删单条消息时同事务删它的全部内容块，并把配对消息标记为 `is_excluded`。`TopicStorage::delete_topics` 只删该 topic 自己的 `agent_definitions` + `topic_agent_maps` + 其下实例（含上面那几张表）+ 残留审批行 + topic 行；**不**级联子 topic（callers must pass child ids explicitly）。

### `wind-http` — HTTP service

Axum 服务，把 core 用 REST + SSE 暴露出来。分层是纯协议适配（router / middleware / DTO / facade）：handler 调 facade，facade 调 `WindCore`/storage，core 从不依赖 axum/http/tower。

- **模块约定**：没有 `mod.rs` —— 一律「同名文件 + 目录」（`routes.rs` + `routes/…`）。`src/` 下 11 个模块：`app`, `config`, `dto`, `error`, `extractor`, `facade`, `middleware`, `openapi`, `routes`, `sse`, `state`。
- **`app(state) -> Router<()>`**（`app.rs`）：`build_router()` 组装子路由 + layer，最后统一 `.with_state`。`app(state)` = `build_router().with_state(state)`。
- **layer 顺序**：`api` 子 router（8 组 CRUD 路由）上依次 `.layer(TimeoutLayer)` → `.layer(propagate)` → `.layer(set_id)` → `.layer(trace_layer())`；axum 里后 add 的在外层，故实际执行顺序是 **trace → set_id → propagate → timeout → handler**（timeout 最内，只包 handler，`CRUD_TIMEOUT = 30s`，超时返回 408 + 空 body —— tower-http 0.7 的 `TimeoutLayer` 不接受错误处理闭包，塞不进 JSON envelope）。**这三层只覆盖 CRUD 子 router**：`/api-docs/openapi.json` 与两条 SSE 路由都 merge 在外层，不套任何 layer。`fallback_404` 返回**真实的 HTTP 404** + `ApiResponse::<()>::not_found("route not found")`（body 的 `code` 也是 404）。
- **`AppState`**（`state.rs`）：`config: AppConfig`、`core: Arc<WindCore>`、`cancel: CancellationToken`（进程停机信号 —— SSE 流 `select!` 它，因此不会阻塞优雅停机）。构造器 `AppState::new(config, core)` / `AppState::with_cancel(config, core, cancel)`；`FromRef` 只实现了 `AppConfig` 与 `Arc<WindCore>`（`CancellationToken` 没有，两条 SSE handler 直接取整个 `AppState`）。
- **Middlewares**（`middleware/`）：`trace`（`TraceLayer::new_for_http`）、`request_id`（`request_id_layers()` 返回 `(SetRequestIdLayer<MakeRequestUuid>, PropagateRequestIdLayer)`，header 名 `x-request-id`；生成必须在回传外侧）、`timeout`（常量 `CRUD_TIMEOUT`，仅 CRUD 路由）。
- **Facade 层**（`facade/`）：`TopicFacade`（`topic.rs`：topics、子话题、实例消息、实例内容块、单条消息、chat、cancel、approve）、`TopicMapFacade`（`topic_map.rs`：topic 能力映射的增删查）、`McpRuntimeFacade`（`mcp_runtime.rs`：server 生命周期 + tool/prompt/resource 发现，session_id 即 `topic_id`，`topic_id == 0` 映射为 `BUILTIN_SESSION`），以及按资源划分的存储子 facade（`facade/storage/`）：`ProviderStorageFacade`、`ModelStorageFacade`、`McpStorageFacade`、`PromptStorageFacade`、`AgentStorageFacade`、`ToolApprovalFacade`（只读）。Facade 做 HTTP 侧预校验、调 storage/runtime、映射成 DTO、把 `CoreError` 收敛成 `ApiResponse`。（`McpStorageFacade::list_mcp_servers_builtin` 是同步方法，其余都是 async。）
- **DTO / envelope**（`dto.rs`）：`ApiResponse<T> { code: u16, data: Option<T>, msg: String }`（**没有 `Deserialize`**），helper 有 `ok`（200）/ `not_found`（404）/ `bad_request`（400）/ `internal`（500）/ `without_data::<U>()`（保留 code/msg，清空 data）/ `map_core_error`（`RowNotFound` → 404 `"not found"`；`Validation(msg)` → 400 `msg`；其余 → 500）。业务成功/失败都返回 HTTP 200，由 body 的 `code` 区分；真实 HTTP 状态码只用于协议层错误（extractor 拒绝、中间件、404 fallback）。`dto.rs` 里只有 3 个自定义类型：`ApiResponse<T>`、`CreateChatRequest { content: Vec<Content> }`、`ApproveToolCallsRequest { allow_ids, deny_ids }`；其余请求/响应体直接复用 wind-core / wind-mcp 的类型（核心模型自己在 `wind-core/src/models/**` 派生 `utoipa::ToSchema`，wind-http 不再有 `XSchema` 镜像类型）。查询参数 DTO 就地写在各自 route 文件里（`AttachMcpQuery`、`ToolsByNamesQuery`、`ProviderIdQuery`、`ByAdapterQuery`）。
- **OpenAPI**（`openapi.rs`）：`utoipa` 聚合 —— `#[derive(OpenApi)]` 的 `paths(...)` 逐条列出 66 个 handler，`components(schemas(...))` 手工列 40 个 schema；`GET /api-docs/openapi.json` 由 `serve_openapi_json` 直接返回缓存的 JSON（`OnceLock`）—— 不依赖 `utoipa-swagger-ui`（它的 build script 要下载 UI 资源）。SSE 两条路由的注解单独写成 `text/event-stream`，不套 `ApiResponse`。
- **Extractors**（`extractor.rs` + `error.rs`）：`ApiQuery<T>` / `ApiPath<T>`（`Rejection = ApiError`，失败统一收敛成 `ApiResponse{code:500}`，**HTTP 状态码仍是 200**）、`json_body()` 让 handler 保留原生 `Result<Json<T>, JsonRejection>`（utoipa 才能从签名识别 requestBody），失败分支同样收敛。两者只实现 `FromRequestParts`。
- **Env vars**（`config.rs`）：`WIND_HTTP_HOST`（默认 `127.0.0.1`）、`WIND_HTTP_PORT`（默认 `7324`，缺失或解析失败都用默认值）。`main.rs` 用 `env_logger`（`RUST_LOG`，缺省 `info`）初始化日志，`WindCore::init_local()` 建库（数据目录来自 core 的 `WIND_ROOT_DIR`），`axum::serve(...)` + `with_graceful_shutdown`（Ctrl-C / SIGTERM → `cancel.cancel()` → `core.shutdown()`）。
- **路由清单**（共 75 条 `/api/v1`）：
  - 话题：`GET|POST /api/v1/topics`、`GET|PUT|DELETE /api/v1/topics/{topic_id}`、`GET /api/v1/topics/{topic_id}/children`
  - 消息与对话：`POST /api/v1/topics/{topic_id}/messages`（提交输入，返回 `ApiResponse<()>`）、`GET /api/v1/agent-instances/{instance_id}/messages`、`GET /api/v1/agent-instances/{instance_id}/contents`、`GET|PUT /api/v1/messages/{message_id}`（PUT 只改 `model_id`，空 body 也受理）、`GET /api/v1/messages/{message_id}/contents`、`POST /api/v1/topics/{topic_id}/agent-instances/{instance_id}/cancel`、`POST /api/v1/topics/{topic_id}/tool-approvals/{message_id}/approve`
  - Agent 定义：`GET|POST /api/v1/agent-definitions`、`GET /api/v1/agent-definitions/by-key/{key}`、`GET|PUT|DELETE /api/v1/agent-definitions/{agent_definition_id}`、`GET /api/v1/agent-definitions/topics/{topic_id}`、`POST /api/v1/agent-definitions/topics/{topic_id}/clone/{agent_definition_id}`
  - Agent 实例（**全部只读**，不区分主/子）：`GET /api/v1/agent-instances/{instance_id}`、`.../tool-approvals/pending`、`GET /api/v1/topics/{topic_id}/agent-instances`（含主实例）
  - 能力映射：`GET /api/v1/topics/{topic_id}/agent-maps`、`POST /api/v1/agent-maps`（请求体是 core 的 `CreateTopicAgentMap`，自带 `topic_id`，故走集合资源）、`DELETE /api/v1/agent-maps/{map_id}`（映射没有角色概念，故没有 PUT）
  - 审批查询：`GET /api/v1/messages/{message_id}/tool-approvals`、`GET /api/v1/topics/{topic_id}/tool-approvals/pending`
  - Provider / 凭证 / 规则：`GET|POST /api/v1/providers`、`GET /api/v1/adapters`、`GET /api/v1/providers/by-name/{name}`、`GET|PUT|DELETE /api/v1/providers/{provider_id}`、`GET|POST /api/v1/credentials`、`DELETE /api/v1/credentials/{credential_id}`、`GET|POST /api/v1/json-rules`、`GET /api/v1/json-rules/by-adapter`、`GET|PUT|DELETE /api/v1/json-rules/{json_rule_id}`
  - 模型：`POST /api/v1/models`、`GET /api/v1/models/provider/{provider_id}`、`GET|PUT|DELETE /api/v1/models/{model_id}`
  - Prompt：`GET|POST /api/v1/prompt-modules`、`GET|PUT|DELETE /api/v1/prompt-modules/{prompt_module_id}`
  - MCP：`GET|POST /api/v1/mcp-servers`、`GET /api/v1/mcp-servers/by-name/{name}`、`GET /api/v1/mcp-servers/builtin`、`GET|PUT|DELETE /api/v1/mcp-servers/{mcp_server_id}`、`POST /api/v1/topics/{topic_id}/mcp-servers/{mcp_server_id}/start|stop`、`POST /api/v1/topics/{topic_id}/mcp-servers/attach`、`GET /api/v1/mcp-servers/clients[/*]`、`GET /api/v1/mcp-servers/tools`、`GET /api/v1/mcp-servers/tools-by-names`
  - 文档与兜底：`GET /api-docs/openapi.json`
- **SSE**：两条路由都不套 `TimeoutLayer`，都带 `KeepAlive`（15s，`"keep-alive"`）—— `GET /api/v1/topics/{topic_id}/events`（订阅 `TopicEvent`：先校验 topic 存在，存在性检查用真实 404/500 返回；成功时 `Sse::new(event_stream(rx, cancel))`，不做 get-or-create）与 `GET /api/v1/mcp-servers/events`（订阅 `ClientEvent`）。`sse.rs::event_stream<T>(rx, cancel)` 用 `BroadcastStream` + `filter_map` 组帧：`event: <T::as_ref()>`（变体名 snake_case）/ `id: <递增序号>`（从 1 起，被丢弃的帧也消耗序号）/ `data: <JSON>`；`Lagged` / 序列化失败只跳帧不终止；外层 `poll_fn` 先 poll 取消 token，`is_ready()` 就立刻结束流。

### `wind-ai` — Provider abstraction

`ChatAdapter` trait（`provider/adapter.rs`，继承 `Adapter::get_type()`）：`build_request(model, contexts, tools)` / `parse_response(data)` / `parse_stream_chunk(event)`。两个实现都在 `provider/adapter/` 下：`openai_completion.rs` 的 `OpenAICompletionAdapter` 与 `openai_responses.rs` 的 `OpenAIResponseAdapter`，由 `get_chat_adapter(AdapterType)` 分发；`AdapterType` 变体只有 `OpenAICompletion` / `OpenAIResponse`，默认端点由 `get_default_endpoint(adapter)` 给出（`/chat/completions`、`/responses`）。拼写一律是 `Adapter`。`client.rs` 提供 `request()` / `request_sse()` / `handle_response()`（`ClientError`）；`chat.rs` 提供 `build_request()` / `handle_chat()` 与 `ResEvent`（`new_partial` / `new_finish` / `new_error`）；`eventsource/` 是自带的 SSE 解析器（`event.rs` / `event_stream.rs` / `parser.rs` / `traits.rs` / `utf8_stream.rs`）。

`wind_ai::model::Model` 只有 `name` / `adapter` / `endpoint` / `config: Option<JsonObject>`（**没有 `ReqConfig`**）；`build_request(chat_adapter, model, contexts, tools)` 生成请求体。`config` 里当前只读取 `stream` 与 `reasoning` 两个键（两个 adapter 各读一次，`chat.rs` 也读 `stream`）。

`Message`（`message.rs`）携带 role、content（`Content` 变体：Text / Image / File / Audio / FunctionCall）、reasoning_content、token 计数、tool_calls。`append_chunk()` 合并流式分片（**不复制 `role`**，只按内容类型拼接文本、累加 reasoning 与 tool_calls），`is_simple()` = 无 tool_calls 且 role 是 User/Assistant，`is_tool_request()` = Assistant 且 tool_calls **非空**，`is_tool_result()` = role 是 Tool。`tool.rs` 定义 `Tools` / `FunctionTool` / `FunctionCall` / `FunctionCallOutput`。

### `wind-mcp` — MCP client registry

Actor pattern: `Registry::new()` spawns a tokio task; all interaction via `RegistryHandle` (cloneable, `mpsc`)。`Registry`/`RegistryHandle` 在 `client/registry.rs`。

- `acquire(session_id, params)` —— 启动/复用 server（跨 session 引用计数，`session_id` 由调用方决定，`wind-http` 用 `topic_id`）
- `release(session_id, name)` / `attach_session(...)` —— 增减 session 引用
- `acquire_builtin(server)` —— 注册进程内内建 server（`FsServer`、`SkillsServer`，实现 `BuiltinMcp`）
- `list_all_tools()` / `list_tools(name)` / `list_tools_by_names(&[names])` / `call_tool(param)` —— 发现与执行
- `list_prompts(name)` / `list_resources(name)` —— prompt 与 resource 发现
- `terminate(name)` / `list_clients()` / `get_client(name)` / `subscribe()` / `shutdown()` —— 生命周期与状态快照（`ClientSnapshot` / `ClientEvent`）

Transports（`TransportType`）：`Stdio`（`rmcp::TokioChildProcess`，先过 `cmd_normalizer` 把 node / python 启动器改写成 bun / uv）与 `Streamable`（`StreamableHttpClientTransport`，HTTP）。`BUILTIN_SESSION = "builtin_session"` 是内建 server 的会话 id。工具名：`{server_name}0m0{tool_name}`（`MCP_TOOL_IDENTIFIER = "0m0"`，私有常量），由 `McpTool::parse_name` 按**首个** `"0m0"` 反向拆解。

内建 server（`builtin/`，布局是「入口文件 + 同名目录」—— `fs.rs` + `fs/{ops,sandbox,error}.rs`，`skills.rs` + `skills/ops.rs`；入口文件只管 rmcp 集成，功能层不知道 rmcp）：`wind-mcp-fs`（`FsServer`：`list_dir` / `read_file` / `write_file` / `exec`，所有路径经 `Sandbox::resolve` 限制在配置的 roots 内，读上限 1 MiB、输出上限 32 KiB）与 `wind-mcp-skills`（`SkillsServer`：`skills_list`，转 `wind_skills::scan`）。`BUILTIN_SERVERS` / `is_builtin_name()` 在 `builtin.rs`。

### `wind-rule` — JSON rule engine

Rules 存在 `json_rule` 表，按 `(provider_id, adapter)` 索引；每次 API 调用前作用在请求体上。入口是 `RuleSet`（`compile.rs`）：`new()` / `from_json(&str)` / `append_rule_str(&str)` / `apply(&mut Value, &EvalContext)` / `clear()`；`EvalContext` 是 `$ctx` 的载体（`new()` / `with(key, value)` / `get(key)`）。

| Op | Purpose |
|----|---------|
| `set` | 在 JSON 路径上设置值（自动创建中间对象） |
| `remove` | 删除 JSON 路径上的字段 |
| `map_value` | 用查表把字段值映射后**合并到请求体根** |
| `compute` | 用 `evalexpr` 对 `$value` + `$ctx.*` 求值 → 替换字段 |
| `when` | 条件分支：`cond`（`eq`/`neq`/`exists`/`and`/`or`/`not`）+ `then`/`else` 子规则 |

`$ctx` 自动注入 `provider` / `model` / `adapter` / `endpoint`（`EvalContext` 是**扁平**查表：`$ctx.a.b` 查的是字面键 `"a.b"`）。路径语法只支持点分对象键，没有数组下标与转义。

### `wind-skills` — SKILL.md parser

`SkillsMeta`（`meta.rs`：`skill_dir` / `name` / `description` / `license` / `compatibility` / `metadata` / `allowed_tools`，其中 `allowed_tools` 的 serde 名是 `allowed-tools`，`skill_dir` 由代码填 canonicalize 后的绝对路径）、`from_path(dir: PathBuf) -> Result<SkillsMeta>`、`scan(dir: PathBuf, recursive: Option<bool>, max_depth: Option<u16>) -> Vec<SkillsMeta>` —— 解析技能目录下的 `SKILL.md` YAML frontmatter（`SKILL_NAME = "SKILL.md"`，`lib.rs`），暴露 `Error` / `Result`。`name` / `description` 必须存在且非空，其余字段宽松处理，超长按字符截断（64 / 1024）。`scan` 的规则：目录命中 `SKILL.md` 即为 skill 根，不再向更深层找新 skill；同名 skill 先到先得，解析失败只告警跳过。被 `wind-mcp` 的内建 skills server（`builtin/skills.rs` 的 `skills_list`）消费。

### `wind-tui` — Terminal UI

Skeleton TUI application using `ratatui` + `crossterm`。目前只渲染 `"hello world"` —— app loop 与事件处理结构已就位，供后续开发。

## Key patterns

**Adding a new provider adapter:** Implement `ChatAdapter`, add a variant to `AdapterType`, register in `get_chat_adapter()`.

**Adding a new provider:** Insert into `providers`, add credentials, insert models — no code changes.

**MCP tool flow:** `AgentDefinitionData.mcp_servers`（enabled 的 `AgentMcpBinding`）→ `batch_get_by_ids` → server names → `list_tools_by_names()` → 按 `is_tool_allowed()` 过滤（allowed/denied 名单）→ 与内建 Agent 工具（仅主角色）经 `helper::build_agent_tools()` 合并 → 请求体里的 `tool_calls` → `partition_tool_calls_by_policy()` 按 **topic 的** 策略切分 → 自动批准的直接执行，需人工审批的产生 `ApprovalRequired` → 恢复后 `TaskManager::resume()` → 结果作为上下文 → 循环。

**Agent tool flow:** LLM 调 `agent_list_agents` 或 `agent_spawn_agent` → `AgentRuntime` 识别 `agent_` 前缀 → `parse_agent_action()` 合并重复的 `list_agents` → `AgentHost::spawn_agent()` 发 `SupervisorRequest::SpawnAgent` → `TopicFsm` → `Effect::SpawnChild` → `TaskManager::spawn_child()` 启动子 `SyncTask` → 子任务结束时 `TopicRuntime::handle_pending` 经 `TaskManager::take_pending()` 取出 `PendingChild`，把结果沿其 `oneshot` 回给等待中的父实例。

**`create()` returns the record:** `storage.xxx().create(...)` 返回完整对象（`Topic` / `Model` / `AgentInstance` …）且 `.id` 已填好 —— 没有「先返回 `i64` 再 `get(id)`」的往返。

**Topic / instance / message scoping**: 每个 topic 恰好一个 `TopicRuntime` 与一个主 Agent；一个 topic 下可以有多个 `AgentInstance`（`list_instances_by_topic`，其中 `role = Main` 的至多一个）。Agent 间隔离**不是**给每个 agent 包一层子 topic，而是每条 `Message` 都带 `instance_id`，实例的会话就是按该实例过滤出的消息（`list_by_instance` / `list_contexts`）。**每次 spawn 都新建实例，不复用空闲实例** —— 同一个 `AgentDefinition` 在一个 topic 下会累积多条 instance，靠各自的 `instance_id` 隔离消息。`topics.parent_id` 仍在 schema 里，但今天没有代码创建子话题。

**`MessageContent` 是内容块**：数据库里的块顺序**由自增 `id` 决定**（`create_content` 只追加，一次模型响应累积成一块，工具调用结果自成一块），没有任何索引列。`index` 只存在于 SSE 事件里，是**本次运行内的块序号**（同一块的分片共用，块推进 +1，跨运行不连续）：`AgentRuntime` 每次 `run` 从 0 起算，`Effect::PersistContent` 不带序号，落库顺序交给数据库。运行时的写入在取消后一律丢弃 —— 取消不写任何内容块。前端约定：同一次订阅内同 index 的分片做增量拼接，整块到达的非流式块（用户输入、工具结果）按完整块处理，重订阅后先拉一次 `/contents` 重建完整列表。

**Fork mode context**: spawn 子 agent 且 mode 为 `Fork` 时，`helper::create_fork_contexts()` 把主 agent 的消息历史复制成子实例的起始上下文 —— 子实例因此能完整看到父实例的对话。

**`pending_tool_calls` 的判定范围**：`chat/runner.rs` 的实现只扫描**最后一条用户简单消息之后**的内容块，并只统计 `Content::FunctionCall` 项。全展平之后必须限定范围，否则上一轮遗留的未执行工具调用会被新任务重新执行、模型被绕过。`chat::run_chat` 的短路与 `agent::runtime` 的待办判定共用这一个实现。

**上下文获取**（`helper::load_contexts` / `flatten_contexts`）：先用 `list_contexts(instance_id)` 取消息（已排除 `is_excluded`、只保留最后一次 `is_boundary` 之后的行），按其 id 升序分组；再用 `list_contents_by_messages` 取每组的块（组内按自增 `id` 升序，即块插入顺序）；`max_context` 按**消息条数**截断窗口，截断后前移到第一条用户消息，最后展平成 `Vec<AiMessage>` —— 历史消息的全部内容块（含工具调用与结果）都会进入上下文。`create_context_inner` 创建 user/assistant 消息时把用户输入写成 user 消息的第一块内容，assistant 消息的正文留给运行时逐块追加；返回值是 `CreatedContexts { user, assistant, user_content }`，把落库的 `user_content` 一并交给上层，由 `Effect::Init` / `Effect::SpawnChild` 经 `TopicEvent::Message`（`index: 0`）推送给前端。

## Agent skills

### Issue tracker

GitHub Issues on `evilArsh/windai` — use the `gh` CLI.

### Triage labels

Default five-label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`.

### Domain docs

Single-context layout — one `CONTEXT.md` + `docs/adr/` at the repo root.
