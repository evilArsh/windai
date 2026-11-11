# Mod

mod 是对宿主能力的扩展包。一个 mod 由一个版本目录、一份 `mod.json` 与若干资源目录组成；
宿主安装它、在话题上启用它，并按启用关系加载提示词、技能、MCP 服务与前端组件。

本文描述包布局、清单字段、安装与启用生命周期、资源读取与订阅装配。脚本服务
`wind-script` 已实现；本文只描述宿主如何使用它，不描述其内部。事件派发语义见「事件」；
MCP 服务连接见「MCP」；数据库表见「存储」。

## 包布局

```text
<root>/mods/<name>/<version>/         版本目录
    mod.json                          清单
    scripts/index.js                  脚本入口（由 wind-script 加载）
    prompts/                          提示词文件
        *.md
        *.txt
        *.html
    skills/                           技能根
        <skill>/SKILL.md
    mcp/                              MCP 资源目录（当前没有加载器）
    ui/index.js                       前端组件入口
<root>/mods/<name>/data/              跨版本私有目录
```

`<root>` 由 `AppDirs` 决定：

| 路径 | 取值 |
| --- | --- |
| 根 | `WIND_ROOT_DIR`；缺省 `~/.windai` |
| 数据库 | `<root>/windai.db` |
| 公共技能根 | `<root>/skills` |
| 话题根 | `<root>/topics` |
| mod 根 | `<root>/mods` |
| mod 版本目录 | `<root>/mods/<name>/<version>` |
| mod 私有目录 | `<root>/mods/<name>/data` |

安装时 `Package::open` 与 `Installer::check_location` 都要求包位于 `mods/<name>/<version>`，
且清单中的 `name` 与 `version` 与路径一致。`data/` 与版本目录同级，升级时保留，卸载时不自动
删除。

## 清单

清单文件为 `mod.json`，由 `Manifest::parse` 解析并校验。

```rust
pub struct Manifest {
    pub name: String,
    pub version: u32,
    pub description: Option<String>,
    pub capabilities: Vec<Capability>,
    pub mcp_servers: HashMap<String, McpServer>,
    pub requests: HashMap<String, Request>,
    pub events: Events,
    pub defs: HashMap<String, Value>,
}
```

### 字段

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `name` | string | `^[a-z][a-z0-9_-]{1,62}$`，全局唯一 |
| `version` | integer | 大于 0，单调递增 |
| `description` | string? | 至多 500 字符 |
| `capabilities` | string[] | `state`、`storage`、`http`、`agent`、`fs`，不得重复 |
| `mcpServers` | object | 键为符号名；值为 `McpServer` |
| `requests` | object | 键为符号名；值为 `Request` |
| `events.on` | object | 键必须以 `core/` 开头；值为 `EventSub` |
| `events.emit` | object | 键为符号名；值为 `EmitDecl` |
| `defs` | object | 键为符号名；值为本地 JSON Schema |

符号名为 `^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$`。处理函数名为
`^[a-zA-Z_$][a-zA-Z0-9_$]*$`，即脚本可以导出的名字。

### `McpServer`

```rust
pub struct McpServer {
    pub command: Option<String>,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub url: Option<String>,
    pub description: Option<String>,
}
```

`command` 与 `url` 必须恰好给出一个：

- `command`：可执行程序。以 `./` 开头时相对版本目录解析，解析结果必须仍是版本目录内的文件。
- `url`：绝对 URL，至少 8 个字符；声明 `url` 时不得同时声明 `args` 或 `env`。

运行期转换为 `wind_wire::McpDefinition`：

| 清单 | `McpConfig` |
| --- | --- |
| `command` | `Stdio { command, args, env, cwd: 版本目录 }` |
| `url` | `StreamableHttp { url }` |

### `Request`

```rust
pub struct Request {
    pub handler: String,
    pub description: Option<String>,
    pub input_schema: Option<Value>,
    pub output_schema: Option<Value>,
    pub callable_by: Vec<CallableBy>,
    pub timeout_ms: Option<u64>,
}
```

`handler` 必填，其余可选。`timeoutMs` 范围为 1 到 300000。`callableBy` 取 `agent` 或 `ui`。
`inputSchema`、`outputSchema` 在清单解析时按 Draft 2020-12 校验，且只能引用本地
`#/...` 与 `defs` 中的定义。当前没有调用边界，因此这些 schema 只做静态校验，不在运行期
强制。

### `events.on`

```rust
pub struct EventSub {
    pub handler: String,
    pub timeout_ms: Option<u64>,
}
```

键必须是 `core/` 开头的事件名，且必须存在于宿主的事件目录中。目录检查由
`Installer::validate` 与 `Manifest::subscriptions` 完成；单独调用 `Manifest::parse` 不查目录。
`timeoutMs` 范围为 1 到 60000；缺省取 `DEFAULT_SUB_TIMEOUT_MS = 1000`。

`Manifest::subscriptions(directory, default_timeout_ms)` 把每条声明转换为：

```rust
Subscription {
    mod_name,
    mod_version,
    event: meta.name,        // 目录中的 &'static str
    handler,
    timeout_ms: declared.unwrap_or(default),
}
```

未知事件返回 `ModError::ManifestUnknownEvent`。订阅不检查事件形态：`observe` 事件进入
observe 投递池，`intercept` 事件在调用方同步等待的改写链里执行。

### `events.emit`

```rust
pub struct EmitDecl {
    pub retention: Retention,   // journal | live
    pub event_key: Option<String>,
    pub payload: Option<Value>,
    pub description: Option<String>,
}
```

- `payload` 必填，且必须是合法的本地 schema。
- `retention = journal` 时 `eventKey` 必填，且同一 mod 内不得重复。
- `retention = live` 时 `eventKey` 可选。

频道名是 `events.emit` 的键。`eventKey` 是持久化事件名中的键：`retention = journal` 时事件名
为 `m_<mod_name>/<eventKey>`，`retention = live` 时为 `m_<mod_name>/<channel>`。宿主在调用
脚本时按清单构造 `ModEmitter`，频道、事件名与保留策略都由宿主固定。

### `defs`

`defs` 是共享的 JSON Schema 字典。`requests.*.inputSchema`、`requests.*.outputSchema`、
`events.emit.*.payload` 与 `defs.*` 自身都会用该字典做本地引用校验。片段不得声明顶层
`defs` 关键字，也不得使用外部 `$ref`。

### 生成 schema

`wind_mod::manifest_schema()` 由 `schemars::schema_for!(Manifest)` 加额外约束生成，输出在
`docs/manifest.schema.json`。清单结构变化后应重新生成该文件；`wind-mod` 的测试会把生成的
schema 与解析器行为对齐。

## 安装与启用

```rust
pub struct Installer { /* ... */ }

pub struct ModChange {
    pub affected_topics: Vec<i64>,
}
```

`Installer` 持有数据库、`Dispatcher`、`Roster`、`AppDirs`、可选的 `ScriptService` 与一把写互斥。
所有变更在数据库事务中准备，提交与内存发布在派生任务里完成；调用方取消只放弃等待，操作
继续走完。

### 安装

```text
Package::open(root)               读 mod.json，校验路径与清单
check_location(root)              必须等于 dirs.mod_version(name, version)
version == 1                      首次安装只接受版本 1
validate()                        校验 events.on 能找到目录项
写事务:
  mod_by_name 不存在                否则 AlreadyInstalled
  install_mod(name, manifest_raw)
  snapshot()                      读全部 mod，生成订阅与启用集合
提交
rebuild_index(subscriptions)
roster.publish(roster_rows)
notify core/mod.installed
```

`install_mod` 还要求清单中的名称等于参数、版本等于 1。安装不解析脚本。

### 更新

```text
Package::open / check_location / validate
写事务:
  mod_by_name 必须存在
  新区版本必须大于当前版本
  update_mod(id, expected_version = 当前版本, new_version, manifest)
  topics_with_mod(id)
  snapshot()
提交
rebuild_index / roster.publish
notify core/mod.updated
返回 affected_topics
```

`update_mod` 校验清单名称与版本、期望版本与版本单调；`WHERE id = ? AND version = ?` 的
受影响行数不为 1 时返回 `Conflict`。

### 删除

```text
写事务:
  mod_by_name 必须存在
  delete_mod(id)                   删除 mods 行；topic_mods 级联删除
  snapshot()
提交
rebuild_index / roster.publish
notify core/mod.deleted
返回受影响话题
```

磁盘上的版本目录不会删除。

### 启用与停用

```text
enable(topic_id, name):
  mod_by_name 必须存在
  enable_mod(topic_id, mod_id)      写 topic_mods
  snapshot / 提交 / 发布
  返回 affected_topics = [topic_id]

disable(topic_id, name):
  mod_by_name 必须存在
  disable_mod(topic_id, mod_id)
  snapshot / 提交 / 发布
  返回 affected_topics = [topic_id]
```

`enable_mod` 与 `disable_mod` 先检查话题与 mod 都存在。启用不要求 mod 已经启用过；重复启用
不改变关系，`rows_affected` 为 0 时返回 `false`。

### 恢复

`Installer::restore` 读取全部 mod，重建订阅索引与 `Roster`。订阅只由清单生成，不依赖脚本
服务是否存在。

### 发布

```text
commit
rebuild_index(subscriptions)   整体替换 Dispatcher 的订阅索引
roster.publish(roster_rows)    整体替换话题启用集合
notify completion              安装 / 更新 / 删除事件
```

`finish` 在派生任务里执行提交与发布。任务 panic 或 JoinError 时，`Installer` 重新从数据库
调用 `restore`，返回 `ModError::Background`。

## 资源

```rust
pub struct ModResources {
    pub name: String,
    pub version: u32,
    pub root: PathBuf,
    pub manifest: Manifest,
    pub prompts: Vec<String>,
    pub skills: Option<PathBuf>,
    pub ui: Option<PathBuf>,
    pub mcp: BTreeMap<String, McpDefinition>,
    pub requires_scripts: bool,
}
```

`Resources::topic(topic_id)` 读取该话题启用的全部 mod，逐个加载并排序。`Resources::version`
要求版本是数据库中的当前版本。

加载规则：

| 资源 | 规则 |
| --- | --- |
| 版本根 | `canonicalize` 后作为所有相对路径的边界 |
| `prompts/` | 目录缺失时为空；只读 `.md`、`.txt`、`.html`（扩展名大小写不敏感）；按文件名升序；内容整体读入 |
| `skills/` | 可选目录；存在时 canonicalize 并检查仍在版本根内 |
| `ui/index.js` | 可选文件；存在时 canonicalize 并检查仍在版本根内 |
| `mcpServers` | 每个声明转成 `McpDefinition`；`./` 命令解析为版本根内的文件 |

所有资源路径都经过 `ensure_within`：canonicalize 后必须以版本根为前缀。符号链接逃逸会被
拒绝。

### 发布到话题

`ResourceServices` 把资源翻译成两类 MCP 引用与一组内建服务：

```text
publish(topic_id):
  mods = Resources::topic(topic_id)
  definitions = 每个 mod 的 mcpServers，服务名改为 mod_<mod_name>_<local>
  workspace = topics.workspace 或 <topics_root>/<topic_id>，确保目录存在
  shared_roots = 公共技能根 + mods 根
  skill_roots = 公共技能根 + 每个 mod 的 skills 目录
  generation += 1
  main_ref = topic_<topic_id>@<generation>
  在 main_ref 上 acquire 全部 definitions
  release 旧的 main_ref
  builtin::sync(Topic(topic_id), previous, resources)
```

同名服务由多个 mod 声明时，先声明者保留，后者记录警告。服务名中的 `local` 是清单
`mcpServers` 的键，不是模型的工具名。

内建服务的命名：

| 作用域 | `fs` | `skills` |
| --- | --- | --- |
| `Global` | `wd_fs` | `wd_skills` |
| `Topic(id)` | `wd_fs_<id>` | `wd_skills_<id>` |

`ResourceServices` 只为话题作用域发布内建服务。`BuiltinScope::Global` 存在，但当前没有
调用方。

`ResourceServices::release_topic` 释放该话题的主引用与内建引用；`shutdown` 标记关闭并等待
注册表停止全部连接。

### 提示词

`ModService::prompt_fragments(topic_id)` 返回已启用 mod 的 `prompts` 内容，按 mod 名与文件
名排序。`LlmRunner` 把它们交给 `core/prompt.collect` 的改写链，再用两个换行连接成系统提示
词。

### 工具

`ResourceServices::agent_tools` 当前返回空表。`ToolOrigin::ModRequest` 的命名规则
`m_<mod_name>__<request>` 只存在于工具目录类型中；mod request 尚未接入工具目录，不会出现
在模型可见的工具清单里。

`ResourceServices::call` 对 mod request 返回 `Denied`，即使目录中出现该类工具。

### 技能

每个 mod 的 `skills/` 目录作为技能根加入内建 `skills` 服务的根集合。公共技能根
`<root>/skills` 始终排在第一位。`skills_list` 只返回元数据，不返回技能正文；技能目录的
`SKILL.md` 由内建 MCP 与文件工具读取，不进入宿主提示词。

### UI 入口

安装或更新完成后，若版本目录中存在 `ui/index.js`，`core/mod.installed` 与
`core/mod.updated` 的 `ui_entry` 字段为：

```text
windai://mods/<name>/<version>/ui/index.js
```

`ui_entry` 缺失表示该版本没有前端组件。入口的加载、模块格式与挂载由前端底座决定；
本仓库只负责识别文件并给出地址。

## 订阅装配

`Installer::snapshot` 遍历全部 mod（不限于已启用），为每个 mod 生成订阅：

- 无脚本要求的 mod：没有 `events.on`，订阅为空。
- 有脚本要求：用清单的 `events.on` 生成订阅，事件名必须在目录中。没有脚本服务时只记录
  警告，订阅照常生成，调用时返回运行时错误。

生成的订阅写入 `Dispatcher` 的索引。是否真正调用由 `Roster` 按话题过滤；未启用 mod 的
订阅留在索引里，但永远不会命中。

订阅顺序：

1. `list_mods()` 按 `mods.name` 升序。
2. 单个 mod 的 `events.on` 是 `BTreeMap`，按事件名升序。

同一事件的调用顺序因此是 mod 名升序。索引没有优先级字段。

## 事件

| 事件 | 载荷 |
| --- | --- |
| `core/mod.installed` | `ModInstalled { name, version, manifest, ui_entry? }` |
| `core/mod.updated` | `ModUpdated { name, from_version, to_version, ui_entry? }` |
| `core/mod.deleted` | `ModDeleted { name, version }` |

三条事件都是 `observe`，`log = true`，进入 `event_log` 与前端流。作用域与来源来自调用方
传入的 `DispatchCx`；`ResourceServices` 原样转发。

安装事件中的 `manifest` 是解析后的规范化 JSON，缺省字段被补齐。

## 脚本边界

`wind-script` 已实现。宿主通过 `Option<Arc<dyn ScriptService>>` 接入：

```rust
#[async_trait]
pub trait ScriptService: Send + Sync {
    fn compile(&self, root: &Path, handlers: &HashSet<String>) -> Result<(), ScriptsError>;

    async fn call(&self, call: ScriptCall) -> Result<Option<Value>, ScriptsError>;
}
```

`Installer` 只在构造时保存该服务，不调用 `compile`。安装与更新只校验清单，不读取
`scripts/`；`compile` 的调用由持有服务的启动流程决定。宿主还必须把
`wind_core::script::directory()` 并入事件目录，否则 mod 无法订阅 `core/mod.notify`。

没有脚本服务时：

| 场景 | 结果 |
| --- | --- |
| 安装仅含资源的 mod | 成功 |
| 安装含 `requests` 或 `events.on` 的 mod | 成功，记录警告 |
| 启用含脚本要求的已装 mod | 成功 |
| 恢复全部 mod | 订阅照常生成 |
| 调用脚本处理函数 | `CallError::Internal`，仓储层报告没有脚本服务 |

## 当前边界

| 项 | 现状 |
| --- | --- |
| `wind-script` | 已实现 `ScriptService`；`ModCaller` 由 `wind-core::script::ScriptRuntime` 提供 |
| `scripts/` | 由 `wind-script` 的加载器读取，入口固定为 `scripts/index.js` |
| `mcp/` | 只有路径常量，没有资源加载器；MCP 服务只来自清单 `mcpServers` |
| `events.emit` | 清单校验完整；`ModEmitter` 由 `wind-core::script` 按清单构造，`retention` 参与投递 |
| `requests` | 不会出现在模型工具目录；调用入口返回 `Denied` |
| `callableBy` | 已解析，未在任何调用边界强制 |
| `inputSchema` / `outputSchema` | 只做清单期校验，运行期不强制 |
| `state` / `storage` 能力 | 没有宿主实现，也没有数据库表 |
| 脚本处理函数校验 | `compile` 接收 `handlers`，实现只校验入口语法 |
| `ui` | 只识别 `ui/index.js` 并给出 `ui_entry`；资源服务由前端底座负责 |
| 版本目录清理 | 删除 mod 不删除磁盘上的版本目录与 `data/` |
