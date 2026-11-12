# 架构

windai 是一个单进程的 agent harness 库。宿主应用装配本仓库的 crate，按话题驱动 agent
实例；模型调用、工具执行、mod 生命周期、脚本执行与事件投递都在同一进程内完成。

本文给出 crate 划分、进程内拓扑、事件与数据库两条写入通路，以及宿主必须接上的位置。
事件的身份、派发与投递细节见「事件」；数据库表与写入路径见「存储」；agent 状态机见
「Agent 树」；模型回合见「LLM 回合」；mod 打包与生命周期见「Mod」；MCP 连接与内建服务见
「MCP」；脚本执行与 HostApi 见「脚本」。

## 范围

- 单进程，不做多实例，不做跨进程广播。
- 宿主按话题建立一个 `Agent<LlmDomain>` 事件循环。同一话题内的树状态变更由该循环串行归约；
  不同话题的 `Agent` 互不阻塞。
- 数据库为 SQLite，连接池最多五个连接，写事务一律 `BEGIN IMMEDIATE`，因此写事务串行；
  读事务走 WAL（write-ahead log）快照，不阻塞写。
- 事件身份由 `(epoch, seq)` 组成。`epoch` 每次 `feed::open` 从数据库领取，`seq` 由本次
  feed 在内存中分配。事件的位置不是 `event_log.id`。
- 流式块内容只在内存中累加。助手消息的块行在消息完成时一次写入；用户消息与工具结果的块行
  在消息创建时写入。写入数据库的块与流式增量不是同一份数据。
- HTTP/SSE、WebSocket 与前端底座不在本仓库内。宿主通过 `FeedHandle::subscribe` 取得实时流，
  自行提供端点；数据库中的领域表与 `event_log` 由宿主自行查询。
- 脚本每次调用新建 QuickJS `Runtime`，在独立的解释器线程上执行；运行时不跨调用保留。

## crate 与依赖

| crate | 依赖的内部 crate | 职责 |
| --- | --- | --- |
| `wind-wire` | 无 | 跨层数据类型：`Message`、`Block`、`Body`、`Role`、工具声明、MCP 定义、事件信封、`FeedEvent`、`AppDirs`，以及 `ModService` 与 `McpService` 两个服务 trait |
| `wind-event` | wire | 事件目录、信封、派发、限流、订阅索引、observe 投递池、mod 事件出口与宿主 trait |
| `wind-storage` | wire | SQLite schema、事务封装、行模型、按领域划分的语句集 |
| `wind-ai` | wire | 提供商适配器、请求构造、SSE 解析、HTTP 发送 |
| `wind-rule` | 无 | 面向请求体的 JSON 规则引擎 |
| `wind-script` | wire | QuickJS 引擎、模块加载、门面、字节码缓存、解释器线程池、超时与熔断；只使用 wire 的脚本路径常量 |
| `wind-mod` | wire, event, storage | 清单、包、安装器、资源读取、启用集合、清单 schema |
| `wind-mcp` | wire, event | MCP 注册表、连接、启动规划、内建 `fs` 与 `skills` 服务 |
| `wind-core` | wire, event, storage, ai, rule, mod, mcp, script | agent 树、LLM 回合、feed、资源适配、脚本接线 |

依赖方向：

```text
                        ┌─────────┐
                        │  wire   │
                        └────┬────┘
          ┌──────────────┬───┴───────────┬──────────────┐
          ▼              ▼               ▼              ▼
     ┌─────────┐   ┌──────────┐    ┌─────────┐   ┌──────────┐
     │ event   │   │ storage  │    │   ai    │   │  script  │
     └────┬────┘   └────┬─────┘    └────┬────┘   └────┬─────┘
          │             │               │             │
     ┌────┴────┐        │               │             │
     ▼         ▼        ▼               │             │
┌─────────┐ ┌──────────────┐            │             │
│   mcp   │ │     mod      │            │             │
└────┬────┘ └──────┬───────┘            │             │
     │             │                    │             │
     └──────┬──────┴───────────┬────────┴─────────────┘
            ▼                  ▼
       ┌──────────────────────────┐      ┌─────────┐
       │         core             │◄─────│  rule   │
       └──────────────────────────┘      └─────────┘
```

`wind-core` 是唯一同时依赖全部内部 crate 的 crate。`wind-script` 只依赖 `wind-wire` 的
路径常量，不依赖 `wind-mod`、`wind-core` 或 `wind-storage`；`wind-mod` 不依赖
`wind-script`。`wind-core::feed` 是 feed 的实现位置，不存在独立的 `wind-feed` crate；
块类型在 `wind-wire`，不存在独立的 `wind-stream` crate；组合根与 HTTP 端点在宿主应用，
不存在 `wind-runtime` crate。

## 进程内拓扑

```text
宿主应用
├─ FeedRuntime（任务）
│  ├─ Hub：命令循环、事件身份、保留窗口、订阅者
│  └─ Writer（任务）：批量写 event_log
├─ Dispatcher（共享 Arc）
│  └─ ObservePool（任务）：observe 事件合批与 mod 调用
├─ RegistryHandle（actor 任务）
│  ├─ Connection × N：外部 MCP 服务
│  └─ 内建 fs / skills：进程内 duplex 连接
├─ ScriptRuntime（共享）
│  └─ 解释器线程 × Config::threads：每次调用新建 QuickJS Runtime
├─ Agent<LlmDomain>（每话题一个任务）
│  ├─ Tree：相位归约
│  └─ Jobs（每次运行一个任务）
├─ LlmRunner（共享）
├─ ResourceService（共享，发布串行化）
└─ Storage（连接池，最多五个连接）
```

| 单元 | 实例数 | 并发方式 | 共享状态 |
| --- | --- | --- | --- |
| `FeedRuntime` | 每个 feed 一个 | 单任务命令循环 | `Hub`、保留窗口、订阅者表 |
| `Writer` | 每个 feed 一个 | 单任务批量写 | `Storage` |
| `Dispatcher` | 每个进程一个 | 同步方法，可被任意任务调用 | 事件目录、订阅索引、限流窗口、`EventOutlet` |
| `ObservePool` | 每个 `Dispatcher` 一个 | 单任务消费，按窗口合批 | 调用队列 |
| `RegistryHandle` | 每个进程一个 | actor 任务；启动与停止派生效果任务，调用在调用方任务里执行 | 服务表、引用集合、工具缓存 |
| `ScriptRuntime` | 每个进程一个 | 事件调用落在 `ObservePool` 或拦截链的调用方任务里 | `ModEmitter` 缓存、`Guards`、`EventSink` |
| 解释器线程 | `Config::threads`，默认 4 | 固定线程池，一次调用占用一个名额 | 字节码内存缓存 |
| `Agent` | 每个话题一个 | 单任务归约 | 树、待消费输入 |
| `Jobs` | 每个运行中的节点一个任务 | `tokio::spawn`，可取消 | 运行中任务的取消令牌 |
| `ResourceService` | 每个进程一个 | 发布操作在 `gate` 上串行 | 话题引用、已发布的内建资源 |
| `Storage` | 句柄可克隆 | 连接池并发 | SQLite 数据库 |

`Agent` 的 `Runner` 是一个 `Arc<dyn Runner<D>>`。本仓库提供的实现是 `LlmRunner`，它把
`wind-ai` 的响应转成块、写入领域表，并通过 `Dispatcher` 发出事件。`Agent` 不直接调用模型。

`ScriptRuntime` 实现 `wind_event::ModCaller`，把 `Dispatcher` 的订阅调用转成
`ScriptService::call`。它不持有 `Dispatcher`，只持有 `EventOutlet`、`Guards` 与
`EventSink`，因此 `Dispatcher` 与 `ScriptRuntime` 之间没有循环引用。

## 两条写入通路

领域行与事件不走同一条路。领域表由各自的拥有者直接写；事件统一经过 feed。feed 的保留窗口
是内存中尚未被 Writer 回报或 `Close` 释放的事件集合；出窗是从该集合移除一条事件。

```text
LlmRunner / Installer / Agent / MCP / ScriptRuntime
   │
   ├─ 领域行 ──────────────────────────────► Storage ──► SQLite
   │  （turn / message / block / agent_* / mods / external_waits …）
   │
   └─ 事件 ──► Dispatcher ──► EventOutlet::submit ──► FeedRuntime
                                                         │
                                   ┌─────────────────────┼─────────────────────┐
                                   ▼                     ▼                     ▼
                              订阅者队列            保留窗口              Writer
                              （前端实时流）       （回补与出窗）        （event_log）
```

两条通路没有共享事务：

- 块行与其 `core/block.committed` 事件不在同一事务。`MessageStream::complete` 先在一个事务里
  写入块并定稿消息，提交成功后才发出 `core/block.committed` 与 `core/message.seal`。
- mod 安装先提交数据库事务，再重建订阅索引并发出 `core/mod.installed` 等事件。
- 脚本的 `ctx.emit` 与 `ctx.notify` 只把事件提交到 `EventOutlet`，不参与领域行事务。
- Writer 写入失败时丢弃该批事件，但 Hub 仍会把它从保留窗口取出并通知观察者。数据库历史可能
  缺行，mod 仍可能收到该事件。
- 事件写入成功也不代表领域行存在。历史读取必须把事件载荷与领域表分别解释。

因此，`event_log` 不是领域表的变更日志，领域表也不是事件日志的投影。两者的唯一共同点是
事件载荷中引用的领域标识。

## 事件通路的分支

`FeedRuntime::Hub::emit` 在同步代码里决定一条事件去往哪里。形态与保留策略共同决定分支。
保留策略的四种取值 `Journal`、`Group`、`Close`、`Live` 的完整语义见「事件」。

| 形态 | 保留策略 | 进保留窗口 | 推订阅者 | 通知 observe 订阅 | 写 `event_log` |
| --- | --- | --- | --- | --- | --- |
| `observe` | `Journal` | 是 | 立即 | Writer 回报后 | 是 |
| `observe` | `Group` | 是 | 立即 | 立即 | 否 |
| `observe` | `Close` | 否，并释放同组 | 立即 | 立即 | 否 |
| `observe` | `Live` | 否 | 立即 | 立即 | 否 |
| `intercept` | `Journal` | 否 | 否 | 从不 | 是 |
| `intercept` | `Live` | 否 | 否 | 从不 | 否 |

`observe` 是对前端可见的唯一形态。`intercept` 的改写发生在调用方同步等待的链里，不作为
观察事件投递。保留策略的完整语义与事件目录见「事件」。

前端订阅只回放内存保留窗口，不回放数据库。`Opening.cut` 给出订阅时刻的 `EventKey` 上界，
宿主若要从 `event_log` 取更早的历史，需自行按 `(epoch, seq)` 拼接并去重。当前
`wind-storage` 没有读取 `event_log` 的语句。

## 顺序与一致性

- `EventKey` 按 `(epoch, seq)` 字典序。`epoch` 每次 `feed::open` 递增，`seq` 在本次 feed 内
  严格递增。`event_log` 对 `(epoch, seq)` 建唯一约束，因此同一数据库内的身份不复用。
- 事件身份只由 Hub 的单任务循环分配，所以分配顺序就是事件序。`Writer` 只负责持久化，
  不重新排序。
- `Journal` 形态的 `observe` 事件在写入数据库提交之后才通知 mod。写入失败也会通知，因此
  mod 看到的事件序与 `event_log` 的行序一致，但可能包含历史中缺失的事件。
- 非 `Journal` 的 `observe` 事件（流式增量、消息定稿、实时块、MCP 日志等）在 Hub 内同步
  投递，不等待数据库。
- 订阅者的回放按 `EventKey` 升序；回放结束后接实时队列。队列满时该订阅被取消，`recv`
  返回 `None`，由宿主重连并从领域表或历史重建。
- 块内容的顺序由 `Block.seq` 与 `Block.revision` 表达。`seq` 在一条消息内从 0 递增；
  可追加块每追加一段 `revision` 递增。块行不保存中间版本，只有定稿值。
- 脚本的 `intercept` 调用串行；后一个 mod 看到前一个改写后的载荷。`observe` 调用按
  `(mod, event, scope)` 合批，组内保持到达顺序。

## 关键接口

| trait | 由谁实现 | 用途 |
| --- | --- | --- |
| `wind_event::EventOutlet` | `wind_core::feed::FeedHandle` | 事件离开派发层的唯一出口；实现只把 `Pending` 入队 |
| `wind_event::Observer` | `wind_event::Dispatcher` | 接收已提交或即时事件；`Journal` 在提交后、其余在投递时调用 |
| `wind_event::ModCaller` | `wind_core::script::ScriptRuntime` | 调用 mod 处理函数 |
| `wind_event::ModRoster` | `wind_mod::roster::Roster` | 按作用域给出启用的 mod 名集合 |
| `wind_script::ScriptService` | `wind_script::quickjs()` | 执行入口模块并调用导出函数 |
| `wind_script::ScriptHost` | `wind_core::script` 内的 `Host` | 脚本访问宿主能力的出口 |
| `wind_wire::ModService` | `wind_core::resources::ResourceService` | 提供提示词片段与 mod 版本 |
| `wind_wire::McpService` | `wind_core::resources::ResourceService` | 列出并调用 MCP 工具 |
| `wind_core::agent::Runner<D>` | `wind_core::llm::LlmRunner` | 执行一次节点运行 |
| `wind_core::agent::tree::Domain` | `wind_core::llm::domain::LlmDomain` | 声明运行的输入、挂起、恢复、输出与错误类型 |

`Dispatcher` 同时是 `Observer` 与事件生产入口。`ResourceService` 同时实现 `ModService` 与
`McpService`，把 mod 资源、内建 MCP 与外部 MCP 汇总成 LLM 可见的工具目录。
`ScriptRuntime` 同时是 `ModCaller` 与脚本 Host 的构造者。

## 宿主装配

仓库内没有组合根。测试的 `Bus::open` 展示了一种可用装配顺序：

| 步 | 动作 |
| --- | --- |
| 1 | `Storage::open(path)` 打开数据库并建表 |
| 2 | `feed::open(storage.clone(), FeedOptions)` 得到 `FeedHandle` 与 `FeedRuntime` |
| 3 | 合并各子系统的事件目录：`wind_event::directory()`、`agent::events::directory()`、`llm::events::directory()`、`wind_mod::events::directory()`、`wind_mcp::events::directory()`、`wind_core::script::directory()` |
| 4 | `Roster::load(&storage)` 读取启用关系 |
| 5 | 构造 `ScriptRuntime`：`ScriptRuntime::quickjs(storage, dirs, outlet, limits)` 使用默认 QuickJS 引擎，或用 `ScriptRuntime::new(..., scripts)` 注入自定义 `ScriptService` |
| 6 | `Dispatcher::new(directory, roster, runtime, outlet, Limits::default())`；`runtime` 是步骤 5 的 `ModCaller`，`outlet` 是步骤 2 的 `FeedHandle` |
| 7 | `wind_mcp::init(McpOptions::new(dirs.mods_root()), dispatcher)` 启动注册表 |
| 8 | `Installer::new(storage, dispatcher, roster, dirs)` |
| 9 | `Resources::new(storage, dirs)` 与 `ResourceService::new(installer, resources, storage, registry, dirs.topics_root())` |
| 10 | `services.restore()` 从数据库恢复订阅与启用集合；它调用 `installer.restore()`，重建 `Dispatcher` 的订阅索引与 `Roster` |
| 11 | `tokio::spawn(runtime.run(vec![Arc::downgrade(&dispatcher)]))` 启动 feed；观察者必须在 `run` 之前挂上 |
| 12 | `LlmRunner::new(storage, dispatcher, services.clone(), services.clone())` |
| 13 | 每个话题 `Agent::start(AgentOptions { topic_id, storage, dispatcher, runner, restored, cancel })` |
| 14 | 用 `storage.create_agent_instance(...)` 建主实例行，再用 `storage.create_pending(...)` 放入输入，最后 `AgentHandle::spawn` 或 `enqueue` |
| 15 | `FeedHandle::subscribe()` 取实时流，宿主自行编码为 SSE/WebSocket 并转发给前端 |
| 16 | 收到 `Outcome::Suspend` 后，宿主保存 `external_wait_id`；用户答复时用 `AgentHandle::resume` 传入 `WaitAnswer` |
| 17 | 关停时先 `services.shutdown()`，再 `FeedHandle::shutdown()`，最后等待 `FeedRuntime::run` 返回 |

宿主必须自行提供的能力：

- 主实例的 `agent_instances` 行。`create_spawn_requests` 只为子实例建行；`Agent` 的相位
  持久化假设行已存在。行不存在时 `update_instance_phase` 返回 `NoRowsAffected`，`Agent`
  只记录日志并发出 `core/agent.persist_failed`。
- 用户输入的入口。`wind-core` 没有“接收用户输入”的函数；宿主写 `agent_pending` 行并把
  `LlmSpec { pending_id }` 交给 `AgentHandle`。
- HTTP/SSE 端点与前端底座。`FeedHandle` 只提供进程内订阅。
- 审批答复的入口。`external_waits` 行由 `LlmRunner` 在 `suspend` 时创建，宿主负责展示与
  回填 `WaitAnswer`。
- 事件历史的查询与分页。`wind-storage` 只写 `event_log`。
- 脚本引擎的装配。标准做法是 `ScriptRuntime::quickjs`；宿主也可以实现自己的
  `ScriptService` 并用 `ScriptRuntime::new` 注入。完全不使用脚本时，给 `Dispatcher` 传一个
  返回 `CallError::Internal` 的 `ModCaller`，脚本 mod 仍可安装与启用，调用会失败。
- `AppDirs` 的根目录。它决定数据库、话题工作区、公共技能根、mod 根与字节码缓存根。

## 数据库

连接参数与连接池设置：

- `filename(path)`、`create_if_missing(true)`
- `PRAGMA journal_mode = WAL`
- `busy_timeout = 8s`
- `foreign_keys = true`
- 连接池上限五个

`Storage::open` 在建表前开启写事务，建表语句全部带 `IF NOT EXISTS`。写事务使用
`BEGIN IMMEDIATE`；只读事务使用 `BEGIN`。

`synchronous` 未显式设置，使用 SQLite 对该 journal 模式的默认值。需要改变持久性保证时，
在 `wind-storage::db::open_pool` 统一设置，不要在各语句集里零散设置。

## 当前实现边界

以下是代码事实，不代表期望行为。子系统文档会给出更细的边界。

| 项 | 现状 |
| --- | --- |
| `wind-script` | 已实现；`ScriptRuntime` 提供 `ModCaller`。HostApi 只有 `emit`、`notify` 与本地 `log`；`state`、`storage`、`http`、`agent`、`fs` 未实现 |
| mod request | 清单 `requests` 只做校验；没有 `ToolOrigin::ModRequest`，没有工具来源，也没有调用入口 |
| 事件历史读取 | `event_log` 只写不读；`Opening.cut` 只提供拼接口 |
| 全局作用域的事件订阅 | `Roster::enabled` 只为话题作用域返回 mod 集合；`core/mcp.*` 等全局事件不会命中任何 mod 订阅 |
| 后台子实例结果 | `PendingKind::BackgroundResult`、`spawn_calls.result_delivered_at` 与 `AgentHandle::deliver` 均无生产者 |
| 主实例行 | 由宿主创建；库内没有创建主实例的语句 |
| `wind_spawn_agent` 的 `fork` | 参数被解析，但 `create_spawn_requests` 不使用它 |
| mod 状态 | `Capability::State` / `Capability::Storage` 已声明，数据库没有对应表，宿主也没有注入实现 |
| 脚本能力声明 | `capabilities` 只参与清单校验，不注入、不裁剪 |
| UI | 只识别 `ui/index.js` 并在 `core/mod.installed` / `core/mod.updated` 中给出 `ui_entry`；资源的实际服务由前端底座负责 |
