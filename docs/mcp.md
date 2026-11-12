# MCP

`wind-mcp` 是 MCP（Model Context Protocol）客户端与内建服务的宿主。它按引用管理服务
实例，完成握手、工具枚举与工具调用，并把服务生命周期发布为事件。

本文描述配置类型、注册表状态机、启动规划、连接行为、内建 `fs`、`skills` 与 `workspace`
服务，以及它与 `wind-core` 资源适配的接缝。mod 如何声明 `mcpServers` 见「Mod」；事件派发见
「事件」。

## 概念

| 词 | 含义 |
| --- | --- |
| 服务定义 | `McpDefinition { name, description?, config }`，一个具名服务 |
| 连接参数 | `McpConfig`：子进程 stdio 或 Streamable HTTP |
| 引用 | 一个字符串 `ref_id`。服务被一个或多个引用持有，最后一个引用释放时停止 |
| 服务名 | `McpDefinition.name`，注册表的主键，也是工具归属名 |
| 工具目录 | 所有处于 `Running` 状态的服务提供的工具 |
| 内建服务 | 进程内启动的 `fs`、`skills` 与 `workspace`，通过 duplex 与注册表连接 |

```rust
pub enum McpConfig {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        cwd: PathBuf,
    },
    StreamableHttp {
        url: String,
    },
}

pub struct McpDefinition {
    pub name: String,
    pub description: Option<String>,
    pub config: McpConfig,
}
```

`McpConfig::Stdio.cwd` 必须是绝对路径，且位于 `mods_root` 内；从清单转换时取版本目录。
`command` 为裸命令名时从 `PATH` 查找；包含路径分隔符时必须是 `mods_root` 内的路径。

## 入口与选项

```rust
pub fn init(options: McpOptions, bus: Arc<Dispatcher>) -> RegistryHandle;

pub struct McpOptions {
    pub mods_root: PathBuf,
    pub connect_timeout: Duration,
    pub list_timeout: Duration,
    pub call_timeout: Duration,
    pub shutdown_grace: Duration,
}
```

默认值：

| 字段 | 默认 |
| --- | --- |
| `connect_timeout` | 10s |
| `list_timeout` | 10s |
| `call_timeout` | 60s |
| `shutdown_grace` | 3s |

`McpOptions::new(mods_root)` 会规范化根路径。`init` 派生一个 actor 任务，返回可克隆的句柄。

```rust
impl RegistryHandle {
    pub async fn acquire_builtin<S>(&self, ref_id: String, server: S) -> Result<()>;
    pub fn acquire(&self, ref_id: String, definition: McpDefinition);
    pub fn release(&self, ref_id: String);
    pub async fn list_tools(&self) -> Catalog;
    pub async fn list_tools_by_ref(&self, ref_id: &str) -> Catalog;
    pub async fn call(
        &self,
        server: &str,
        tool: &str,
        arguments: Value,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> CallOutcome;
    pub async fn shutdown(&self);
}
```

`acquire` 与 `release` 是即发命令，不等待结果；`list_tools`、`list_tools_by_ref`、`call`、
`shutdown` 通过 oneshot 等待应答。句柄关闭后，等待类方法返回默认值或失败结果，不阻塞。

```rust
pub struct BoundTool {
    pub server: String,
    pub tool: ToolDescriptor,
}

pub struct ToolDescriptor {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

pub struct Catalog(Arc<[BoundTool]>);
```

## 注册表状态机

每个服务名对应一个 `Entry`：

```rust
struct Entry {
    definition: Definition,      // External(McpDefinition) 或 Builtin
    refs: BTreeSet<String>,
    state: State,
    tools: Vec<ToolDescriptor>,
    connection: Option<Arc<Connection>>,
    server_task: Option<JoinHandle<()>>,
    want: Option<PendingStart>,
}

enum State {
    Starting(u64),
    Running,
    Stopping(u64),
    Failed,
}
```

`generation` 单调递增；启动与停止的回执带有 generation，过期回执被丢弃。`want` 在停止
期间记录下一次要启动的定义。启动锁是一张按 `LaunchPlan.startup_key` 索引的全局表；
Stdio 启动会先取得该锁，同一个包或入口不会并发启动两次。Streamable HTTP 启动不取锁。

### 获取

`acquire(ref_id, definition)`：

| 当前状态 | 同名定义 | 行为 |
| --- | --- | --- |
| 不存在 | — | 建 `Starting` 条目，发 `core/mcp.starting`，派生启动 |
| `Running` | 相同 | 只加入引用 |
| `Starting` | 相同 | 只加入引用 |
| `Running` | 不同 | 转 `Stopping`，停止旧连接；停止完成后启动新定义；没有连接时直接启动 |
| `Starting` | 不同 | 更新定义并重新启动 |
| `Stopping` | 任意 | 更新 `want`，停止完成后启动 |
| `Failed` | 任意 | 重置为 `Starting` 并启动 |

同名同定义的重复获取不重复启动。`refs` 是集合，同一引用重复获取只保留一次。

### 释放

`release(ref_id)` 从所有持有该引用的条目中移除：

- 仍有其他引用：保留服务。
- `Running` 且无引用：转 `Stopping`，停止连接。
- `Starting` 且无引用：不立即停止；启动回报到达时发现引用为空，丢弃新连接。
- `Stopping` 且无引用：清空 `want`。
- `Failed` 且无引用：删除条目。

### 启动与停止

启动在派生任务中执行：

```text
rewrite：
  有 previous → decide(core/mcp.replaced, McpReplaced { name, refs, old, new })
  decide(core/mcp.launch, McpLaunch { name, refs, definition })
强制 definition.name = name
start：
  Stdio → runner::plan → Connection::open
  StreamableHttp → Connection::open
  list_tools（list_timeout）
回报 Command::Report { name, generation, result }
```

`Report` 到达时：

- generation 不是当前 `Starting`：丢弃结果；若连接已建立则关闭。
- 引用为空或注册表已关闭：删除条目并关闭连接。
- 成功：转 `Running`，保存连接与工具，发 `core/mcp.started`。
- 失败：转 `Failed`，发 `core/mcp.failed`；无引用时删除条目。

停止：

```text
abort 内建 server_task（若有）
connection.close()
发 core/mcp.stopped { reason }
若 want 存在 → 转 Starting，发 starting，派生新启动
否则删除条目
```

`StopReason` 取 `released`、`shutdown`、`replaced`。

### 关闭

`shutdown` 关闭注册表入口，停止全部条目，等待所有在飞效果结束。条目清空且
`outstanding == 0` 时，应答所有等待中的关闭请求。

### 内建服务

`acquire_builtin` 在调用方任务里先用 `tokio::io::duplex(64 KiB)` 建立连接并完成握手，
再把连接、工具与服务端任务交给 actor：

- 同名服务已经在运行且是内建：只加入引用，丢弃新连接。
- 否则替换旧条目：停止旧连接，登记新的 `Running` 条目，发 `core/mcp.started`。
- actor 已停止（命令发送失败）：关闭新连接，返回 `McpError::ShuttingDown`。
- `closed` 标志已置但 actor 仍在：丢弃新连接，返回成功。

内建服务没有 `McpDefinition`，`Definition::Builtin` 只用于标记来源。

## 启动规划

`runner::plan(command, args, cwd, mods_root)` 返回：

```rust
struct LaunchPlan {
    program: PathBuf,
    args: Vec<String>,
    cwd: PathBuf,
    startup_key: String,
}
```

规则：

| 输入 | 结果 |
| --- | --- |
| `cwd` 不在 `mods_root` 内 | `InvalidPath` |
| 带路径分隔符的 `command` 不在 `mods_root` 内 | `InvalidPath` |
| 裸命令名 | 交给 `Command::new`，从 `PATH` 查找 |
| `npx <pkg>` / `bunx <pkg>` | 改写为 `bun x -y <原参数>`，启动键为包名 |
| `bun x <pkg>` | 保留原参数，启动键为包名 |
| `uvx <pkg>` | 改写为 `uv tool run <原参数>`，启动键为包名 |
| `uv tool run <pkg>` | 保留原参数，启动键为包名 |
| `bun` / `node` / `python` / `python3` / `deno` 且参数含绝对路径入口 | 入口必须在 `mods_root` 内，启动键为入口路径 |
| 其他 | 启动键为 `program + 第一个参数` |

包名的提取规则：

- `npx`：`--package=<name>`、`--` 后的第一个参数，或第一个非选项且形如包名的参数。
- `bunx` / `bun x`：`-p <name>`、`--package <name>`，或第一个非选项且形如包名的参数。
- `uvx` / `uv tool run`：`--from <name>`，或第一个非选项且形如 Python 包名的参数。

Python 包名排除以 `./`、`/` 开头、包含反斜杠、以 `.py`、`.whl`、`.tar.gz` 结尾的参数。

这些规则只决定如何启动与如何加锁，不改变服务声明的语义。

## 连接

`Connection` 持有 `rmcp` 的 `Peer`、底层 `RunningService`、在飞调用计数与（Unix）进程组 id。

### 握手

- 子进程：`TokioChildProcess` 启动，stderr 设为管道，Unix 下使用进程组。
- 远程：`StreamableHttpClientTransport`。
- 内建：`Connection::attach(reader, writer, options)`。
- 生命周期：优先协商最新协议，失败时回退到 `initialize`。
- 整个握手受 `connect_timeout` 限制。

### 工具枚举

`list_tools` 调 `peer.list_all_tools()`，受 `list_timeout` 限制。结果转换为
`ToolDescriptor`，保留名称、描述与输入 schema。

### 工具调用

```text
closed → Failed
arguments 不是 JSON 对象 → Failed
deadline 已过 → Failed
budget = min(deadline 剩余, call_timeout)
select! {
    取消 → Failed("the call was cancelled")
    timeout(budget, peer.call_tool_once(params)) → 结果
}
```

- `CallToolResponse::Complete` 序列化为 JSON，返回 `CallOutcome::Ok`。
- 其他响应类型返回 `CallOutcome::Failed("the server returned an unsupported tool response")`。
- 取消与超时都以 `Failed` 返回，不区分错误类型。

### 关闭

`Connection::close`：

```text
closed = true
等待在飞调用归零，最多 shutdown_grace
关闭客户端，最多 shutdown_grace
Unix 下 kill 整个进程组
```

`Drop` 在 Unix 下也会 kill 进程组。子进程以 `kill_on_drop(true)` 启动。

### stderr

子进程的 stderr 由独立任务读取：

- 单行上限 64 KiB，超出部分丢弃。
- 按批累积，批上限 3 KiB；200ms 无新数据时冲刷。
- 保留 `\r\n` 转 `\n`；非法 UTF-8 用替换字符。
- 每批转换为 `core/mcp.log { name, stream: "stderr", line }`；注册表侧再把单行截断到
  4096 字节。

`core/mcp.log` 的 `log = false`，只推给当前订阅者，不写 `event_log`。

## 内建服务

### 命名

```rust
pub enum BuiltinScope {
    Global,
    Topic(i64),
}
```

| 作用域 | `fs` | `skills` | `workspace` |
| --- | --- | --- | --- |
| `Global` | `wd_fs` | `wd_skills` | `wd_workspace` |
| `Topic(id)` | `wd_fs_<id>` | `wd_skills_<id>` | `wd_workspace_<id>` |

`builtin::refs(scope)` 返回三个引用名；`builtin::release(scope)` 释放三个引用。
`builtin::sync` 比较前后资源，只重建发生变化的服务：

- `fs`：首次同步，或 `roots()` 变化时重建。启用集合或 mod 版本变化都算。
- `skills`：只在首次同步时启动；后续通过 `SkillRoots::replace` 原地替换根集合，不重启服务。
- `workspace`：只在首次同步时启动；后续通过 `WorkspaceInfo::replace` 原地替换载荷，不重启服务。

内建服务通过 `tokio::io::duplex(64 KiB)` 与注册表连接，服务端任务由注册表持有。

`BuiltinResources`：

```rust
pub struct BuiltinResources {
    pub workspace: PathBuf,
    pub public_skills: PathBuf,
    /// 仅含本话题启用的 mod
    pub mods: Vec<ModDir>,
    pub skill_roots: SkillRoots,
    pub workspace_info: WorkspaceInfo,
}

impl BuiltinResources {
    pub fn roots(&self) -> Vec<PathBuf>;
}
```

`roots()` 为 `workspace` + 公共技能根 + 每个已启用 mod 的版本根；`fs` 的沙箱根即此列表，
`workspace` 的 `roots` 字段与它相同。

### `fs`

工具：

| 工具 | 参数 | 行为 |
| --- | --- | --- |
| `list_dir` | `path`、`recursive?`、`max_depth?` | 递归列出目录，返回相对路径、类型、大小与修改时刻 |
| `read_file` | `path`、`offset?`、`limit?` | 按字节读取文本；默认上限 1 MiB；按 UTF-8 字符边界对齐 |
| `write_file` | `path`、`data` | 写文本，自动创建父目录 |
| `exec` | `command`、`cwd`、`args?`、`env?`、`timeout` | 在沙箱内执行子进程，超时单位毫秒 |

`list_dir` 的忽略规则：

- 隐藏目录只放行 `.skills` 与 `.agents`。
- 忽略 `node_modules`、`target`、`dist`、`build`、`__pycache__`、`vendor`、`coverage`、
  `out`、`venv`。

`read_file` 对空文件返回失败结果，内容为 `empty content`；`offset` 越界与 `limit = 0`
返回空内容；`truncated` 表示读取是否在文件末尾之前停止。`meta` 在 Unix 下由
`file --brief` 给出，在 Windows 下由扩展名给出。

`exec` 的 `cwd` 必须解析到沙箱内且是目录；stdin 为 null，stdout/stderr 分别上限
32 KiB；超时后返回 `timed_out = true`。

### 沙箱

`Sandbox` 在构造时 canonicalize 全部根；空路径被过滤。`Snapshot::resolve`：

1. 路径存在时 canonicalize，并要求落在某个根内。
2. 路径不存在时，向上找最近存在的祖先，canonicalize 后检查在根内。
3. 逐段重放缺失段；遇到符号链接时解析目标并递归检查，深度上限 40。

任何逃逸都返回 `FsError::NotAllowed` 或 `InvalidPath`。`FsError` 的编码通过
`ErrorData` 的 `data.code` 返回，取值为大写下划线名，如 `NOT_ALLOWED`、`NOT_FOUND`。

### `skills`

工具 `skills_list`：

```rust
pub struct SkillsListRequest {
    pub dir: String,
    pub recursive: Option<bool>,
    pub max_depth: Option<u16>,
}
```

`dir` 必须 canonicalize 后落在某个技能根内。扫描规则：

- 目录中存在 `SKILL.md` 时解析该文件，并把当前目录作为技能根；不再向下递归。
- 没有 `SKILL.md` 时，按 `recursive`（默认 true）与 `max_depth`（默认不限）遍历子目录。
- 每个技能归属到最长匹配的技能根，`source` 为根名。
- frontmatter 必须以 `---` 开始、以 `---` 结束；`name` 与 `description` 必填。
- `name` 截断到 64 字符，`description` 截断到 1024 字符，`compatibility` 截断到 500 字符。
- 解析失败的技能进入 `errors`，不影响其他技能。

`skills_list` 只返回元数据，不返回正文。

### `workspace`

工具 `workspace_info`，无参数，只读，返回本话题的运行环境路径：

| 字段 | 含义 |
| --- | --- |
| `summary` | 一句陈述：工作空间、公共技能根、已启用 mod 的 `name@version` |
| `workspace` | 话题工作空间 |
| `roots` | `fs` 接受的目录根 |
| `skills` | 技能根，每项 `{ source, path }`，`source` 为 `public` 或 mod 名 |
| `mods` | 本话题启用的 mod，每项 `{ name, version, root, skills, ui }` |

`mods` 只含本话题启用的版本；未启用的 mod 的版本根不在 `roots` 中，`mods` 根本身也不是
沙箱根。

## 与核心的集成

`wind_core::resources::ResourceService` 是 MCP 的主要使用方：

- `publish(topic_id)` 把话题启用的 mod 的 `mcpServers` 转成服务定义，服务名改为
  `mod_<mod_name>_<local>`，挂在 `topic_<topic_id>@<generation>` 引用下。
- 首次需要某话题的工具时，`ensure_topic` 触发一次发布。
- `list_tools(topic_id)` 读取主引用与内建 `wd_fs_<topic>`、`wd_skills_<topic>`、
  `wd_workspace_<topic>` 引用下的工具，按 `(server, tool)` 去重。
- 工具在模型侧的名字是 `<server>0m0<tool>`，由 `wind_core::llm::tools::mcp_tool_name`
  生成。
- `tool_call` 按服务名与工具名查找连接；`tool_calls` 并发执行一批调用，单个失败转成
  `{ "error": reason }`。两者都使用调用方给出的截止时间与取消令牌。
- `release_topic(topic_id)` 释放主引用与内建引用；`shutdown` 关闭注册表。

`McpService` 定义在 `wind-wire`，当前有 `list_tools`、`tool_call`、`tool_calls` 三个
方法。`wind_core::llm::tools::execute_plan` 使用批量方法，一轮只产生一次注册表调用。

当前 `ResourceService` 只为话题作用域发布内建服务；`BuiltinScope::Global` 没有调用方。

## 事件

事件来源固定为 `Source::core("mcp")`，作用域固定为 `Scope::Global`。观察类事件预算 10 秒，
改写类事件预算 2 秒。

| 事件 | 形态 | `log` | 载荷 |
| --- | --- | --- | --- |
| `core/mcp.launch` | intercept | 是 | `McpLaunch { name, refs, definition }` |
| `core/mcp.replaced` | intercept | 是 | `McpReplaced { name, refs, old, new }` |
| `core/mcp.starting` | observe | 是 | `McpStarting { name, refs }` |
| `core/mcp.started` | observe | 是 | `McpStarted { name, refs, tools }` |
| `core/mcp.failed` | observe | 是 | `McpFailed { name, refs, error }` |
| `core/mcp.stopped` | observe | 是 | `McpStopped { name, refs, reason }` |
| `core/mcp.log` | observe | 否 | `McpLog { name, stream, line }` |

`launch` 与 `replaced` 的改写链查不到任何 mod 订阅：`Roster` 只为话题作用域返回启用集合，
全局作用域得到空集合。因此当前这两个拦截点不会改变服务定义。

## 当前边界

| 项 | 现状 |
| --- | --- |
| 全局作用域订阅 | MCP 事件为 `Scope::Global`，mod 订阅无法命中 |
| `BuiltinScope::Global` | 已实现，无调用方 |
| HTTP 服务 | 只支持 Streamable HTTP；没有 SSE 旧传输 |
| 工具结果 | 只接受 `CallToolResponse::Complete`；其他响应类型判为失败 |
| 取消与超时 | 都返回 `CallOutcome::Failed`；没有错误分类 |
| `list_dir` 截断 | `truncated` 恒为 `false`，扫描不设条目上限 |
| `read_file` 二进制 | 不做二进制检测；内容按 lossy UTF-8 解码 |
| 环境隔离 | 子进程继承宿主环境；`CommandWrap` 中的 `env_clear` 被注释 |
| 服务定义校验 | `McpDefinition` 本身没有路径或 URL 校验；校验由 `mod` 与 `runner` 完成 |
| 启动锁 | 只覆盖 Stdio 启动；Streamable HTTP 不取 `startup_key` 锁 |
