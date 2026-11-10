/**
 * windai Mod 门面契约
 * ============================================================================
 *
 * 本文描述 mod 的脚本侧能看到什么。宿主侧的脚本运行时由 `wind-scripts` 提供，该 crate
 * 尚未接入本仓库；因此本文同时标注“已由代码固定”的部分与“目标形状、尚未实现”的部分。
 *
 * 已由代码固定：
 *
 * - 清单的字段、校验与生成 schema：`docs/manifest.schema.json`。
 * - 宿主调用 mod 的入口：`wind_event::ModCaller::call`。
 * - 调用模式、取消、截止时间与订阅记录：`wind_event::{CallMode, CallCx, Subscription}`。
 * - 事件形态、改写链与 observe 合批：`docs/events.md`。
 * - mod 事件出口：`wind_event::ModEmitter`，名称 `m_<mod>/<channel>`，保留策略 `Journal`。
 * - 工具命名：MCP 为 `<server>0m0<tool>`，mod request 为 `m_<mod>__<request>`。
 * - 包布局与生命周期：`docs/mod.md`。
 *
 * 尚未实现：
 *
 * - 脚本编译与执行（`wind_mod::Compiler`、`wind_event::ModCaller` 没有实现者）。
 * - `ctx.state`、`ctx.storage`、`ctx.http`、`ctx.agent`、`ctx.fs` 的宿主注入。
 * - `ctx.notify` 与 `ctx.log` 的宿主事件与实现。
 * - mod request 作为模型工具暴露；当前 `ResourceServices::agent_tools` 返回空。
 * - UI 侧的宿主运行时；本仓库只识别 `ui/index.js` 并给出 `ui_entry`。
 *
 * 文中未标“已实现”的接口都是目标形状，不是当前可调用的 API。
 */

/** 宿主与 mod 之间传递的结构化值。 */
export type ModValue =
  | null
  | boolean
  | number
  | string
  | ModValue[]
  | { [key: string]: ModValue };

// ═══════════════════════════════════════════════════════════════════════════
// 1. 调用形状
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 宿主调用 mod 事件处理函数的唯一入口。
 *
 * 宿主先读清单的 `events.on.<event>.handler` 得到脚本导出名，再取模块导出并调用。
 * `Subscription` 是订阅索引中的一条记录。request 处理函数经由
 * `wind_core::llm::ModService::call`，当前 `ResourceServices` 对任何 request 返回 `Denied`。
 */
export type CallMode = "intercept" | "observe" | "request";

export interface CallCx {
  /** 本次调用的截止时间。 */
  readonly deadline: unknown;
  /** 取消信号。宿主取消时触发。 */
  readonly cancel: unknown;
  readonly mode: CallMode;
  readonly scope: Scope;
}

export interface Subscription {
  readonly mod_name: string;
  readonly mod_version: number;
  /** 宿主事件名，例如 `core/llm.toolcall`。 */
  readonly event: string;
  /** 脚本导出的处理函数名。 */
  readonly handler: string;
  /** 单次调用的时间预算，毫秒。 */
  readonly timeout_ms: number;
}

/**
 * 拦截处理函数。返回改写后的载荷；返回 `undefined` 或 `null` 表示不修改。
 *
 * 拦截链串行：后一个 mod 看到前一个改写后的载荷。载荷始终是单值，不合批。
 */
export type InterceptHandler<P> = (payload: P, ctx: Ctx) => P | null | undefined | void;

/**
 * 观察处理函数。返回值被丢弃。
 *
 * 宿主按 5ms 窗口、以 `(mod, event, scope)` 为键合批投递：
 * 组内一条时载荷是 `P`，多于一条时是 `P[]`。处理函数必须同时处理两种形态。
 */
export type ObserveHandler<P> = (payload: P | P[], ctx: Ctx) => void;

/** request 处理函数。输入与输出由清单的 `inputSchema` / `outputSchema` 描述。 */
export type RequestHandler<I, O> = (input: I, ctx: Ctx) => O;

/**
 * 三条总则（适用于宿主侧 `Ctx`）：
 *
 * 1. 全同步。宿主侧方法没有 Promise/await；调用阻塞解释器直至返回。
 * 2. 失败以异常表示。没有 `{ ok: false }` 形态。
 * 3. 能力按声明注入。`ctx` 上未声明的属性不存在，访问产生 `TypeError`。
 *
 * 第 1 条来自“每次处理函数调用新建解释器”的执行模型；该模型属于 `wind-scripts`，尚未
 * 在本仓库落地。
 */

// ═══════════════════════════════════════════════════════════════════════════
// 2. 身份与作用域
// ═══════════════════════════════════════════════════════════════════════════

export interface ModIdentity {
  /** mod 名，全局唯一，取值受 `docs/manifest.schema.json` 的 `name` 约束。 */
  readonly name: string;
  /** 版本号，单调递增。 */
  readonly version: number;
}

/**
 * 作用域。与 `wind_wire::event::Scope` 相同。
 *
 * `topic` 同时带话题与实例；实例是当前 agent 树节点。
 */
export type Scope =
  | { readonly kind: "global" }
  | { readonly kind: "topic"; readonly topic_id: number; readonly instance_id: number };

// ═══════════════════════════════════════════════════════════════════════════
// 3. Ctx
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 处理函数的第二个参数。每次调用获得一个新实例；不得在模块级变量中缓存。
 *
 * 跨调用保留的数据只能存入 `ctx.state` 或 `ctx.storage`。这两项当前没有宿主实现，
 * 也没有数据库表；其语义是目标契约。
 */
export interface Ctx {
  /** 本 mod 的身份。只读。 */
  readonly mod: ModIdentity;

  /** 本次调用的作用域。只读。 */
  readonly scope: Scope;

  /**
   * 对外状态。需要 `"state"` 能力。
   *
   * 目标语义：对订阅方可见的键值存储；写入广播给订阅者，包括前端；agent 可读取。
   * 当前没有宿主实现。
   */
  readonly state: StateApi;

  /**
   * 私有键值存储。需要 `"storage"` 能力。
   *
   * 目标语义：mod 内部持久化；不广播，agent 不可读取。当前没有宿主实现。
   */
  readonly storage: StorageApi;

  /**
   * 出网。需要 `"http"` 能力。
   *
   * `intercept` 调用中该属性不存在。当前没有宿主实现。
   */
  readonly http: HttpApi;

  /**
   * 与 agent 系统交互。需要 `"agent"` 能力。
   *
   * `intercept` 调用中该属性不存在。当前没有宿主实现。
   */
  readonly agent: AgentApi;

  /**
   * mod 私有目录内的文件读写。需要 `"fs"` 能力。
   *
   * 目标范围：`<root>/mods/<name>/data/`，跨版本共享。`intercept` 调用中该属性不存在。
   * 当前没有宿主实现；mod 若需要文件能力，可通过清单声明的 MCP 服务使用内建 `fs` 工具。
   */
  readonly fs: FsApi;

  /**
   * 产生本 mod 的一条自定义事件。
   *
   * `name` 必须是清单 `events.emit` 中声明的键。宿主把它变成
   * `m_<mod>/<channel>`，载荷按该声明的 `payload` schema 校验，保留策略固定为
   * `Journal`，展示去向为 `Target::Mod { name, channel }`。
   *
   * 该调用受 `Guards::admit` 与每 mod 速率上限约束：默认 3 秒内 100 条。
   *
   * 清单的 `retention` 字段（`journal` / `live`）当前只参与校验，`ModEmitter` 不读取它。
   * 因此声明 `live` 的频道在实际投递时仍是 `Journal`。
   */
  emit(name: string, payload: ModValue): void;

  /**
   * 向用户发出提醒。
   *
   * 只有 `Prop::Notify` 这一声明存在；宿主没有对应的 `EventDef`、事件名或实现。
   * 该形状未定稿，调用它不会产生事件。
   */
  notify(opts: NotifyOptions): void;

  /**
   * 写入宿主日志。
   *
   * 只有 `Prop::Log` 这一声明存在；没有宿主实现。该形状未定稿。
   */
  log(level: LogLevel, ...args: ModValue[]): void;
}

// ─── state / storage ──────────────────────────────────────────────────────

/**
 * 对外状态。目标语义：内存读取、调用结束时落库、写入广播。
 * 当前没有宿主实现，也没有 `mod_state` 表。
 */
export interface StateApi {
  get(key: string): ModValue | undefined;
  set(key: string, value: ModValue): void;
  delete(key: string): void;
  keys(): string[];
}

/**
 * 私有键值存储。目标语义与 `StateApi` 相同，但不广播。
 * 当前没有宿主实现。
 */
export interface StorageApi {
  get(key: string): ModValue | undefined;
  set(key: string, value: ModValue): void;
  delete(key: string): void;
  keys(): string[];
}

// ─── http ─────────────────────────────────────────────────────────────────

/** 出网接口的目标形状。当前没有宿主实现。 */
export interface HttpApi {
  fetchText(url: string, init?: HttpInit): string;
  fetchMany(urls: string[], init?: HttpInit): string[];
}

export interface HttpInit {
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  headers?: Record<string, string>;
  body?: string;
  /** 缺省取所属 request 的 `timeoutMs`。 */
  timeoutMs?: number;
  /** `fetchMany` 中单个请求失败的处理，默认 `throw`。 */
  onError?: "throw" | "null";
}

// ─── agent ────────────────────────────────────────────────────────────────

/**
 * 与 agent 系统交互的目标形状。当前没有宿主实现。
 *
 * 模型侧已有的派生工具是 `wind_spawn_agent`；它由 `LlmRunner` 执行，不经过 `ctx.agent`。
 */
export interface AgentApi {
  /** 追加一条用户输入。立即返回，实际处理由宿主排队。 */
  prompt(text: string): void;
  /** 把一段文本放入输入区域，等待用户确认。 */
  draft(text: string): void;
  /** 派生子实例。`mode` 缺省为 `sync`。 */
  spawn(opts: SpawnOptions): void;
  /** 读取当前话题的最近消息。 */
  transcript(limit?: number): TranscriptEntry[];
}

export interface SpawnOptions {
  task: string;
  mode?: "sync" | "background";
}

export interface TranscriptEntry {
  readonly message_id: number;
  readonly role: "system" | "user" | "assistant" | "tool" | "developer";
  readonly text: string;
}

// ─── fs ───────────────────────────────────────────────────────────────────

/**
 * mod 私有目录的文件接口目标形状。当前没有宿主实现。
 *
 * 私有目录是 `AppDirs::mod_data(name)`，即 `<root>/mods/<name>/data`。
 */
export interface FsApi {
  /** 私有目录的绝对路径。 */
  readonly dir: string;
  readText(path: string): string;
  writeText(path: string, data: string): void;
  list(path?: string): string[];
  remove(path: string): void;
}

// ─── notify / log ─────────────────────────────────────────────────────────

export interface NotifyOptions {
  text: string;
  level?: "info" | "warn" | "error";
}

export type LogLevel = "debug" | "info" | "warn" | "error";

// ═══════════════════════════════════════════════════════════════════════════
// 4. 能力矩阵
// ═══════════════════════════════════════════════════════════════════════════

/**
 * `ctx` 属性在各类调用模式下的可用性。`capabilities` 中声明后属性才存在。
 *
 * | `ctx.*`   | 需要 capability | intercept | observe | request |
 * |-----------|------------------|-----------|---------|---------|
 * | `state`   | `"state"`        | 可用       | 可用     | 可用     |
 * | `storage` | `"storage"`      | 可用       | 可用     | 可用     |
 * | `http`    | `"http"`         | 不存在     | 可用     | 可用     |
 * | `agent`   | `"agent"`        | 不存在     | 可用     | 可用     |
 * | `fs`      | `"fs"`           | 不存在     | 可用     | 可用     |
 * | `emit`    | 无需声明          | 可用       | 可用     | 可用     |
 * | `notify`  | 无需声明          | 未实现     | 未实现   | 未实现   |
 * | `log`     | 无需声明          | 未实现     | 未实现   | 未实现   |
 *
 * `intercept` 的“不存在”是属性不存在，不是运行时拒绝。该裁剪不受 manifest 声明影响：
 * 即使声明 `"http"`，`intercept` 中 `ctx.http` 也不存在。
 *
 * 依据：`intercept` 阻塞调用方，预算为毫秒量级；使 I/O 能力不存在优于使其超时。
 * 代码中的对应判据是 `CallMode::allows_io`：`Intercept` 返回 false，`Observe` 与
 * `Request` 返回 true。
 *
 * 能力的取值集合以 `docs/manifest.schema.json` 的 `Capability` 为准：`state`、`storage`、
 * `http`、`agent`、`fs`。
 */

// ═══════════════════════════════════════════════════════════════════════════
// 5. 工具与事件
// ═══════════════════════════════════════════════════════════════════════════

/**
 * mod 可以出现在模型工具目录中的两类入口：
 *
 * - `requests.<name>`：工具名为 `m_<mod>__<request>`。当前 `ResourceServices::agent_tools`
 *   返回空，因此不会出现；`ResourceServices::call` 也会拒绝。
 * - `mcpServers.<local>`：服务名为 `mod_<mod>_<local>`，工具名为
 *   `<服务名>0m0<原始工具名>`。
 *
 * 清单的 `requests.<name>.callableBy` 取 `agent` / `ui`，但当前没有调用边界强制它。
 *
 * 事件处理：
 *
 * - `events.on.<event>.handler`：`core/` 开头的事件名，必须在宿主目录中存在。
 * - `intercept` 订阅走同步改写链；`observe` 订阅走 observe 投递池。
 * - `events.emit.<channel>`：`ctx.emit` 的频道声明，必须带 `payload` schema；
 *   `retention = journal` 时还必须带唯一的 `eventKey`。
 *
 * 审批挂起由 `core/llm.toolcall` 的 `suspend` 字段完成：处理函数把它设为 `true` 时，本批
 * 调用不执行，宿主写入 `external_waits` 并把节点转为 `Suspended`。答复由宿主调用
 * `AgentHandle::resume(node, expect, WaitAnswer { external_wait_id, decisions })` 恢复；
 * 未表态的调用默认放行。等待项保存的是改写前的调用。
 */

// ═══════════════════════════════════════════════════════════════════════════
// 6. UI 侧
// ═══════════════════════════════════════════════════════════════════════════

/**
 * UI 组件的入口是版本目录下的 `ui/index.js`。
 *
 * 安装或更新完成时，若该文件存在，`core/mod.installed` 与 `core/mod.updated` 的
 * `ui_entry` 字段为：
 *
 * ```text
 * windai://mods/<name>/<version>/ui/index.js
 * ```
 *
 * 加载、模块格式、挂载点与 `UiCtx` 的注入方式由前端底座决定，不在本仓库内。后端只负责
 * 识别入口并给出地址。
 *
 * 以下形状是前端底座的目标契约，当前没有实现：
 */
export interface UiCtx {
  readonly mod: ModIdentity;
  readonly scope: Scope;
  /** 后端接口集合，由前端底座按 manifest 注入；成员未定。 */
  readonly host: HostApi;
}

/** 后端接口集合。成员与传输层一并定稿，当前为空。 */
export interface HostApi {}

// ═══════════════════════════════════════════════════════════════════════════
// 7. 异常
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 以下异常是目标契约。`wind-scripts` 尚未实现，当前没有宿主侧异常类型。
 *
 * 处理函数不得捕获这些异常后返回“通过”；该做法会把超时或取消转成 mod 自己的结论。
 */
export declare class FacadeError extends Error {}
export declare class CapabilityError extends FacadeError {}
export declare class TimeoutError extends FacadeError {}
export declare class CancelledError extends FacadeError {}
export declare class PathError extends FacadeError {}
export declare class NotFoundError extends FacadeError {}
export declare class PayloadTooLargeError extends FacadeError {}
export declare class TooLargeError extends FacadeError {}

export declare class HttpError extends FacadeError {
  readonly status?: number;
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. 不包含的能力
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 刻意排除，不属于遗漏：
 *
 * - 不存在 `ctx.hook(...)`。拦截通过订阅 `intercept` 事件实现。
 * - mod 之间不互相调用，也不能订阅另一个 mod 的 `m_<mod>/` 事件；清单只接受 `core/`
 *   开头的订阅键，目录中也没有动态 mod 事件。
 * - 不存在宿主侧异步接口。目标模型是每次调用新建解释器并同步执行。
 * - 不存在 `mount`。挂载属于前端底座。
 * - 不存在注册新能力类型的接口。
 * - 不存在长期子进程接口。需要长期进程时，用清单的 `mcpServers` 声明 stdio 服务。
 * - 不存在二进制传输通道。宿主与 mod 之间的数据是结构化 JSON 值。
 * - 不存在逐项能力授权。全部能力声明即生效。
 */
