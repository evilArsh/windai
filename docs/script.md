# 脚本

`wind-script` 是 mod 脚本的执行引擎。它把版本目录下的 `scripts/index.js` 装入全新的
QuickJS 运行时，调用清单指定的导出函数，把 JSON 输入与返回值在宿主与脚本之间转换；
同时提供 `ctx` 门面、模块加载、字节码缓存、超时、线程池与连续超时熔断。

本文描述公开契约、一次调用的阶段、模块加载规则、缓存与熔断，以及脚本可以访问的
HostApi。脚本如何声明订阅与 `emit` 频道见「Mod」；事件派发与改写链见「事件」；核心
如何把脚本接入 `ModCaller` 见「架构」。

## 职责边界

`wind-script` 只负责执行。它不解析清单、不安装 mod、不查询数据库、不分配事件身份，也
不知道工具目录。

| 归属 | 内容 |
| --- | --- |
| `wind-script` | QuickJS 运行时、模块加载、`ctx` 门面、字节码缓存、解释器线程池、截止时间与熔断 |
| `wind-core::script` | `ModCaller` 实现、清单解析、`ModEmitter` 构造、`DispatchCx`、事件出口 |
| `wind-mod` | 包布局、清单校验、安装与启用；不依赖 `wind-script` |
| 宿主 | 提供 `ScriptService`；默认实现由 `wind_script::quickjs` 构造 |

`wind-script` 依赖 `wind-wire` 的路径常量 `MOD_SCRIPT_DIR` 与 `MOD_SCRIPT_ENTRY`，不依赖
其他内部 crate。`wind-core` 依赖 `wind-script`；`wind-mod` 不依赖它，脚本不作为安装
门禁。

## 公开契约

```rust
pub struct Config {
    /// 解释器线程数
    pub threads: usize,
    /// 单个运行时的内存上限，字节
    pub memory_limit: usize,
    /// 字节码缓存根目录，None 表示只留在内存
    pub cache: Option<PathBuf>,
    /// 引擎标识覆盖，None 表示使用 engine_id()
    pub engine: Option<String>,
}

pub fn quickjs(config: Config) -> Result<Arc<dyn ScriptService>, ScriptsError>;
pub fn engine_id() -> String;

pub enum ScriptScope {
    Global,
    Topic { topic_id: i64, instance_id: i64 },
}

pub struct ScriptCall {
    pub mod_name: String,
    pub mod_version: u32,
    pub scope: ScriptScope,
    pub root: PathBuf,
    pub handler: String,
    pub deadline: std::time::Instant,
    pub cancel: CancellationToken,
    pub input: Value,
    pub host: Arc<dyn ScriptHost>,
}

pub trait ScriptHost: Send + Sync {
    fn emit(&self, channel: &str, payload: Value) -> Result<(), ScriptsError>;
    fn notify(&self, level: &str, text: &str) -> Result<(), ScriptsError>;
}

#[async_trait::async_trait]
pub trait ScriptService: Send + Sync {
    async fn call(&self, call: ScriptCall) -> Result<Option<Value>, ScriptsError>;
}
```

`Config` 的默认值：

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `threads` | 4 | 解释器线程数；0 使构造失败 |
| `memory_limit` | 64 MiB | 每次调用新建的运行时上限；0 使构造失败 |
| `cache` | `None` | 磁盘字节码缓存根目录 |
| `engine` | `None` | 覆盖 `engine_id()`，只影响缓存目录与摘要 |

`QuickJsEngine` 是私有类型，构造入口只有 `quickjs`。`engine_id()` 返回：

```text
rquickjs-sys=<version>;arch=<ARCH>;ptr=<32|64>;endian=<le|be>
```

`rquickjs-sys` 的版本在构建时从 `Cargo.lock` 读取。同一 mod 在不同引擎构建下的字节码
不共享。

## 一次调用

```text
ScriptService::call
  ├─ 熔断器进入          连续超时停摆中 → Suspended
  ├─ 预算检查            截止时间已过 → Timeout
  ├─ 线程池取名额        没有空闲线程 → Busy
  ├─ 新建 Runtime        内存上限、中断处理器
  ├─ 取得入口模块        内存缓存 → 磁盘字节码 → 源码编译
  ├─ 新建 Context        新的 realm，每次调用独立
  ├─ 求值入口模块        顶层代码执行，导入按需加载
  ├─ 取导出函数          缺失或不是函数 → Handler
  ├─ 构造 ctx            mod、scope、emit、notify、log
  ├─ 调用 handler        传 JSON 输入与 ctx
  └─ 读出返回值          undefined/null → None，其余序列化为 JSON
```

取消与截止时间在整个调用期间生效。`call.cancel` 触发时返回
`ScriptsError::Internal("the call was cancelled")`；截止时间到达时返回
`ScriptsError::Timeout`。解释器线程上的中断处理器也检查同一个截止时间，超时后脚本不会
继续运行。

每次调用都新建 `Runtime` 与 `Context`。模块顶层代码每次调用都会执行，模块级变量不在
调用之间保留；跨调用状态需要由宿主能力提供，当前 HostApi 尚未实现这部分（见
「HostApi 实现状态」）。

## 模块加载

入口固定为 `scripts/index.js`。所有模块名都是相对 mod 包根目录的规范路径：

```text
scripts/index.js
scripts/bar.js
scripts/lib/util.js
```

加载规则：

- 只接受以 `./` 或 `../` 开头的说明符。裸说明符、绝对路径、含 `:` 的说明符一律拒绝。
- 必须显式写 `.js`；不自动补扩展名，不解析目录下的 `index.js`。
- 按路径段折叠 `..` 后，模块名必须以 `scripts/` 开头。
- 文件 `canonicalize()` 后必须仍在 `<root>/scripts/` 内；符号链接逃逸被拒绝。
- 只支持静态导入。动态 `import()` 没有专门接口；相对说明符按同一加载器解析，裸名、绝对
  路径与越界说明符仍被拒绝。
- 导入逐个模块加载；入口与每个被导入模块各自成为缓存条目。

失败分类：

| 情况 | 错误 |
| --- | --- |
| 入口或导入文件缺失 | `ScriptsError::Handler` |
| 入口或导入模块语法错误 | `ScriptsError::Handler` |
| 说明符越界、裸名、绝对路径、缺少 `.js` | `ScriptsError::Handler` |
| 符号链接逃逸 | `ScriptsError::Handler` |
| 入口模块顶层代码抛错 | `ScriptsError::Handler` |
| 清单指定的导出不存在或不是函数 | `ScriptsError::Handler` |

`ScriptsError::Compile` 保留在错误枚举中，当前没有构造点：编译在首次加载时按模块进行，
失败按 `Handler` 报告。

## 字节码缓存

磁盘缓存把入口与每个已导入模块的编译产物按内容寻址存放：

```text
<cache>/<mod_name>/<engine_id>/<key>.bin
key = sha256(module_name || 0x00 || source_bytes)
```

文件内容依次为 8 字节标识 `WINDBC01`、32 字节摘要、QuickJS 字节码。摘要为
`sha256(engine_id || 0x00 || key || bytecode)`；读取时同时校验标识、摘要与非空载荷。
摘要不匹配或字节码加载失败时删除该文件、改读源码，并在本次服务生命周期内停用该产物。

内存缓存按 `(mod 根目录, mod 名, 模块名)` 索引。首次取得某个模块时读取源码并计算键；
磁盘上已有可用产物则直接使用，不重新编译，也不重写文件。之后的调用直接使用内存中的
产物，不读文件、不计算键、不编译。

| 情况 | 行为 |
| --- | --- |
| 首次加载，磁盘无产物 | 编译并写文件；写入失败只使用内存产物 |
| 首次加载，磁盘有产物 | 复用文件；源码仍未变化时不重写 |
| 产物损坏、被截断或为空 | 删除文件，改用源码；本服务内不再尝试该文件 |
| 缓存目录不可写 | 调用继续成功，产物只留在内存 |
| `Config::cache = None` | 不读写磁盘；内存缓存继续生效 |
| 同版本目录内修改源码 | 本服务仍执行已缓存的旧模块；重启服务后使用新源码 |
| 新版本目录，源码相同 | 复用同一磁盘产物 |
| 删除 mod | `Installer` 在数据库提交成功后删除 `<root>/bytecode/<mod_name>`；自定义 `Config::cache` 不由库清理 |

产物只写文件，不写数据库。缓存目录只用于本地可信编译产物，不接受外部分发的字节码。

## 线程、期限与取消

`threads` 是固定的解释器线程数；所有调用共用这个池。线程名为 `wind-script-<n>`。

- 空闲线程不足时立即返回 `ScriptsError::Busy`，不排队。
- 每次调用在池内执行；超时后调用方返回 `Timeout`，解释器线程仍会占用到中断处理器
  结束当前脚本为止。因此超时后的短时间内可能出现 `Busy`。
- 解释器任务 panic 被线程池捕获，返回 `ScriptsError::Internal`。
- 截止时间同时由线程池的超时与 QuickJS 的中断处理器执行。`call.cancel` 只影响等待方；
  已提交到解释器线程的任务照常运行到截止时间。

## 连续超时熔断

熔断按 mod 名记录连续超时，阈值 3 次，停摆冷却 30 秒。

| 结果 | 计数 |
| --- | --- |
| 正常返回 | 清零 |
| `Handler`、`PayloadTooLarge` | 清零 |
| `Timeout` | 加一；达到阈值后进入停摆 |
| `Busy`、`Internal`、取消 | 不改变 |
| 冷调用超时 | 不计数（首次加载、等待初始化或从源码编译） |

停摆期间调用返回 `ScriptsError::Suspended`。冷却结束只放行一个探测调用：

- 探测成功：清零并恢复正常。
- 探测超时：重新进入冷却。
- 探测在冷却结束后尚未完成，或取消、提前退出：恢复冷却，不留下永久开放的入口。

## 门面

处理函数签名为：

```js
export function onEvent(input, ctx) {
  // input 是事件载荷；observe 合批时可能是数组
  // ctx.mod   { name, version }
  // ctx.scope { kind: "global" } 或 { kind: "topic", topic_id, instance_id }
  // ctx.emit(channel, payload)
  // ctx.notify({ text, level? })
  // ctx.log(level, ...args)
}
```

`input` 是宿主传给 `ScriptCall.input` 的 JSON 值。`observe` 订阅由投递池按
`(mod, event, scope)` 合批：组内只有一条时是值本身，多于一条时是数组。处理函数必须同时
接受两种形态。

返回值：

- `undefined` 与 `null` 返回 `None`。
- 其余值必须是可 JSON 序列化的值；返回 `Some(Value)`。
- `intercept` 的返回值会替换载荷；无法重新解析为事件载荷时，派发层记录
  `core/dispatch.failed` 并保留原载荷。
- `observe` 的返回值被丢弃。

`ctx` 每次调用新建。模块级变量不得缓存 `ctx`。

### 已实现

| 成员 | 行为 |
| --- | --- |
| `ctx.mod` | `{ name, version }`，只读；`version` 是数字 |
| `ctx.scope` | `global` 或 `topic`；`topic` 带 `topic_id` 与 `instance_id` |
| `ctx.emit(channel, payload)` | 经 `ScriptHost::emit` 交给 `ModEmitter` |
| `ctx.notify({ text, level? })` | 经 `ScriptHost::notify` 产生 `core/mod.notify` |
| `ctx.log(level, ...args)` | 写宿主日志，目标名 `mod` |

`ctx.emit`：

- `channel` 必须是清单 `events.emit` 中声明的键；未知频道抛错。
- `payload` 必须可 JSON 序列化；序列化后的 UTF-8 字节数上限 65536。
- 是否持久化由该频道声明的 `retention` 决定，脚本不能覆盖。
- 事件名、去向与校验规则见「事件」的 mod 事件一节。

`ctx.notify`：

- `options` 必须是对象，`text` 必填且是字符串；`level` 缺省 `info`，允许 `info`、
  `warn`、`error`。
- `text` 的 UTF-8 字节数上限 65536。
- 产生一条 `core/mod.notify`，载荷为 `{ level, text }`，来源是当前 mod，
  保留策略固定为 `Journal`。

`ctx.log`：

- `level` 允许 `debug`、`info`、`warn`、`error`；其他值抛错。
- 参数逐个转成文本；字符串原样，其他值序列化为 JSON。
- 只写本地日志，不产生事件，不经过事件目录或限流。

### HostApi 实现状态

`ScriptHost` 目前只有 `emit` 与 `notify` 两个方法。清单中的 `capabilities` 会被解析与
校验，但运行期不注入任何能力。`ctx` 上除上表五个成员外的属性都不存在，读取得到
`undefined`。

| HostApi | 计划用途 | 状态 |
| --- | --- | --- |
| `ctx.state` | 对外键值状态，写入广播给订阅方 | 未实现 |
| `ctx.storage` | mod 私有键值存储，不广播 | 未实现 |
| `ctx.http` | 同步出网，`intercept` 不可用 | 未实现 |
| `ctx.agent` | 追加输入、派生实例、读取对话 | 未实现 |
| `ctx.fs` | 读写 `<root>/mods/<name>/data` | 未实现 |

这些能力的宿主实现留到后续版本。当前脚本需要网络、文件或持久化时，只能通过清单声明的
MCP 服务使用宿主提供的工具，或在事件载荷允许的范围内完成。

`capabilities` 与调用模式的关系同样未实现：`intercept` 不会裁剪 I/O 能力，因为没有 I/O
能力可裁剪；`CallMode::Request` 也没有生产者。

### 异常与错误映射

脚本侧的错误以 JS 异常抛出；引擎优先返回宿主记录的 `ScriptsError`，否则按
`Handler(CaughtError)` 返回。门面调用失败与处理后抛错因此都归到 `Handler`，只有超时、
取消、池忙与引擎内部错误使用其他变体。

`ScriptsError` 到 `wind-event::CallError` 的映射：

| `ScriptsError` | `CallError` |
| --- | --- |
| `Handler`、`PayloadTooLarge`、`Suspended`、`Compile` | `Handler` |
| `Timeout`、`Busy`、`Internal`、`Config` | `Internal` |

`ScriptRuntime` 在宿主侧另有三类 `Internal`：mod 未安装、订阅版本与当前版本不一致、
清单无法解析。

## 与核心的接线

`wind_core::script::ScriptRuntime` 实现 `wind_event::ModCaller`，是脚本与事件总线之间的
唯一适配点。

```rust
ScriptRuntime::quickjs(storage, dirs, outlet, limits)
ScriptRuntime::new(storage, dirs, outlet, limits, scripts)
```

`quickjs` 用 `AppDirs::bytecode_root()` 作为缓存目录，其他配置取默认值。

`ModCaller::call` 的流程：

```text
按订阅的 mod_name 读当前 Mod 行
版本与订阅不一致 → Internal（stale subscription）
解析清单
按 (mod_name, version) 取或建 ModEmitter
构造 DispatchCx：Origin::Internal、订阅作用域、Source::Mod、剩余预算
构造 Host：ModEmitter、EventSink、DispatchCx、Source
构造 ScriptCall：版本目录、handler、截止时间、取消、载荷、Host
调用 ScriptService，映射错误
```

`ModEmitter` 按 `(mod_name, version)` 缓存。更新后新版本重新构造；旧条目留在缓存里，
不再被新订阅使用。仍在投递中的旧订阅因版本不一致返回 `Internal`，不执行旧版本。

`ScriptRuntime` 自带一个 `Guards`；`Dispatcher` 另有自己的 `Guards`，两者限流参数相同但
计数独立。每次 `ModCaller::call` 都以 `call_cx.remaining()` 新建一个 `DispatchCx`，因此
`max_events_per_trigger` 只约束单次调用内产生的事件，不跨 observe 批次累计。

宿主必须把 `wind_core::script::directory()` 并入事件目录，否则 `core/mod.notify` 不会
被接受，mod 也无法订阅它。

## 事件出口

`ctx.emit` 与 `ctx.notify` 都在解释器线程上同步调用宿主，宿主只把事件提交到
`EventOutlet`，不等待写库或投递。

`ctx.emit` 的约束：

- 频道名与保留策略由清单固定；脚本只给出频道与载荷。
- 载荷先做顶层 `type` 检查：清单 schema 的 `type` 为字符串或字符串数组时，载荷必须匹配
  其中之一；schema 没有 `type` 时不做检查。完整 JSON Schema 尚未接入。
- 事件经过 `Guards::admit` 与每 mod 的速率窗口（默认 3 秒 100 条）。
- `journal` 频道的事件名是 `m_<mod>/<eventKey>`，写入 `event_log`；`live` 频道的事件名是
  `m_<mod>/<channel>`，只推给当前订阅者。
- 去向是 `Target::Mod { name, channel }`。
- 这些事件不在事件目录中，`Dispatcher::on_event` 不会把它们派发给任何 mod。

`ctx.notify` 的产生路径：

```text
ctx.notify → ScriptHost::notify → EventSink::notify
  → Guards::admit
  → Envelope { name: core/mod.notify, shape: observe }
  → Pending { retention: Journal, target: Core }
  → EventOutlet::submit
```

## 当前边界

| 项 | 现状 |
| --- | --- |
| HostApi | 只有 `emit`、`notify`；`log` 由引擎本地写日志。`state`、`storage`、`http`、`agent`、`fs` 未实现 |
| 能力声明 | `capabilities` 只做清单校验，不注入、不裁剪 |
| request | 清单 `requests` 只做校验；没有工具来源、没有调用入口 |
| 调用模式 | `intercept` 与 `observe` 已接入；`CallMode::Request` 没有生产者 |
| 导入 | 只支持静态、相对、显式 `.js`；不支持裸名、绝对路径、目录解析、动态导入 |
| 载荷校验 | 只检查顶层 `type`；完整 JSON Schema 未接入 |
| 模块状态 | 每次调用新建运行时，模块级变量不跨调用保留 |
| 字节码信任 | 只接受本引擎产出并通过摘要校验的本地文件 |
| 超时后的线程 | 调用方已返回，解释器线程可能仍在收尾，期间可能 `Busy` |
| 熔断粒度 | 按 mod 名；不同版本共享同一计数 |
| UI | 本 crate 不涉及；`ui/index.js` 的加载由前端底座负责 |
