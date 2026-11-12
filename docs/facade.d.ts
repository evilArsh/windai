/**
 * windai Mod 门面契约
 * ============================================================================
 *
 * 本文描述 mod 脚本侧当前能看到什么，以及哪些宿主能力已经实现。
 *
 * 实现状态：
 *
 * - `wind-script` 已实现：QuickJS 运行时、`scripts/index.js` 加载、导出函数调用、
 *   `ctx.emit`、`ctx.notify`、`ctx.log`。
 * - 事件派发已接入：`intercept` 与 `observe` 订阅都会调用脚本处理函数。
 * - 清单的字段、校验与生成 schema 见 `docs/manifest.schema.json`。
 *
 * 未实现（留到后续版本）：
 *
 * - `ctx.state`、`ctx.storage`、`ctx.http`、`ctx.agent`、`ctx.fs` 的宿主注入。
 * - 清单 `requests` 的调用入口与模型工具暴露。
 * - `intercept` 形态对 I/O 能力的裁剪（当前没有 I/O 能力可裁剪）。
 * - UI 侧的宿主运行时；本仓库只识别 `ui/index.js` 并给出 `ui_entry`。
 *
 * `ctx` 上未实现的属性不存在；读取得到 `undefined`，调用其成员才产生 `TypeError`。
 * 本文中标为“未实现”的接口是目标形状，不是当前可调用的 API。
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
 * 一次调用的模式。
 *
 * - `intercept`：宿主同步等待处理函数返回改写后的载荷。
 * - `observe`：事件已经提交，处理函数只观察；返回值被丢弃。
 * - `request`：已声明，尚未接入脚本调用。
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
 * 返回的对象无法重新解析为该事件的载荷时，派发层记录 `core/dispatch.failed`，
 * 并保留前一个载荷。
 */
export type InterceptHandler<P> = (payload: P, ctx: Ctx) => P | null | undefined | void;

/**
 * 观察处理函数。返回值被丢弃。
 *
 * 宿主按 5ms 窗口、以 `(mod, event, scope)` 为键合批投递：
 * 组内一条时载荷是 `P`，多于一条时是 `P[]`。处理函数必须同时处理两种形态。
 */
export type ObserveHandler<P> = (payload: P | P[], ctx: Ctx) => void;

/**
 * request 处理函数。未实现。
 *
 * 清单 `requests` 只做结构校验；当前没有工具来源或调用入口。目标形状中的输入与输出由
 * `inputSchema` / `outputSchema` 描述，完整 JSON Schema 校验留到调用边界实现时接入。
 */
export type RequestHandler<I, O> = (input: I, ctx: Ctx) => O;

// ═══════════════════════════════════════════════════════════════════════════
// 2. 身份与作用域
// ═══════════════════════════════════════════════════════════════════════════

export interface ModIdentity {
  /** mod 名，全局唯一，取值受清单 `name` 约束。 */
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
// 3. Ctx（当前实现）
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 处理函数的第二个参数。每次调用获得一个新实例；不得在模块级变量中缓存。
 *
 * 当前只有下列成员。清单声明的 `capabilities` 不会增加任何属性；读取
 * `ctx.state`、`ctx.storage`、`ctx.http`、`ctx.agent`、`ctx.fs` 得到 `undefined`。
 */
export interface Ctx {
  /** 本 mod 的身份。只读。 */
  readonly mod: ModIdentity;

  /** 本次调用的作用域。只读。 */
  readonly scope: Scope;

  /**
   * 产生本 mod 的一条自定义事件。
   *
   * `channel` 必须是清单 `events.emit` 中声明的键。载荷必须是可 JSON 序列化的值，
   * 序列化后的 UTF-8 字节数上限为 65536。
   *
   * 事件名、保留策略与去向由宿主按清单固定：
   *
   * - `journal` 频道：事件名 `m_<mod>/<eventKey>`，写入 `event_log`。
   * - `live` 频道：事件名 `m_<mod>/<channel>`，只推给当前订阅者。
   * - 去向统一为 `Target::Mod { name, channel }`。
   *
   * 调用受派发预算与每 mod 速率上限约束（默认 3 秒 100 条）。未知频道、载荷不可
   * 序列化或超限时抛出 JS 异常；引擎返回 `ScriptsError::Handler`。
   */
  emit(channel: string, payload: ModValue): void;

  /**
   * 向用户发出提醒。
   *
   * `text` 必填；`level` 缺省 `info`，允许 `info`、`warn`、`error`。`text` 的 UTF-8
   * 字节数上限为 65536。宿主产生 `core/mod.notify`，载荷 `{ level, text }`，
   * 保留策略固定为 `Journal`。
   *
   * 缺少 `text`、`level` 非法或超限时抛出 JS 异常。
   */
  notify(opts: NotifyOptions): void;

  /**
   * 写入宿主日志。
   *
   * `level` 允许 `debug`、`info`、`warn`、`error`。日志目标名为 `mod`，消息前缀为
   * `<mod>: `。该调用不产生事件。
   */
  log(level: LogLevel, ...args: ModValue[]): void;
}

export interface NotifyOptions {
  text: string;
  level?: "info" | "warn" | "error";
}

export type LogLevel = "debug" | "info" | "warn" | "error";

// ═══════════════════════════════════════════════════════════════════════════
// 4. HostApi（未实现）
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 宿主能力的全集。当前实现只有 `Ctx` 上的 `emit`、`notify`、`log`；下列成员全部未
 * 注入，留到后续版本。
 *
 * 清单的 `capabilities` 取 `state`、`storage`、`http`、`agent`、`fs`。声明只影响清单
 * 校验，不影响运行期。
 */
export interface HostApi {
  /** 对外状态。未实现。 */
  readonly state: StateApi;
  /** 私有键值存储。未实现。 */
  readonly storage: StorageApi;
  /** 出网。未实现。 */
  readonly http: HttpApi;
  /** 与 agent 系统交互。未实现。 */
  readonly agent: AgentApi;
  /** mod 私有目录的文件读写。未实现。 */
  readonly fs: FsApi;
}

/**
 * 对外状态的目标形状。未实现。
 *
 * 计划语义：对订阅方可见的键值存储；写入广播给订阅者，包括前端；agent 可读取。
 */
export interface StateApi {
  get(key: string): ModValue | undefined;
  set(key: string, value: ModValue): void;
  delete(key: string): void;
  keys(): string[];
}

/**
 * 私有键值存储的目标形状。未实现。
 *
 * 计划语义：mod 内部持久化；不广播，agent 不可读取。
 */
export interface StorageApi {
  get(key: string): ModValue | undefined;
  set(key: string, value: ModValue): void;
  delete(key: string): void;
  keys(): string[];
}

/**
 * 出网接口的目标形状。未实现。
 *
 * 计划语义：同步调用；`intercept` 形态不注入该能力，因为拦截链的预算在毫秒量级。
 */
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

/**
 * agent 交互的目标形状。未实现。
 *
 * 模型侧现有的派生工具是 `wind_spawn_agent`；它由 `LlmRunner` 执行，不经过 `ctx.agent`。
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

/**
 * mod 私有目录的文件接口目标形状。未实现。
 *
 * 计划范围是 `AppDirs::mod_data(name)`，即 `<root>/mods/<name>/data`，跨版本共享。
 * 当前需要文件能力的 mod 应通过清单 `mcpServers` 使用内建 `fs` 工具。
 */
export interface FsApi {
  /** 私有目录的绝对路径。 */
  readonly dir: string;
  readText(path: string): string;
  writeText(path: string, data: string): void;
  list(path?: string): string[];
  remove(path: string): void;
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. 能力矩阵
// ═══════════════════════════════════════════════════════════════════════════

/**
 * `ctx` 属性的可用性。
 *
 * | `ctx.*`   | 需要 capability | intercept | observe | request |
 * |-----------|------------------|-----------|---------|---------|
 * | `state`   | `"state"`        | 未实现     | 未实现   | 未实现   |
 * | `storage` | `"storage"`      | 未实现     | 未实现   | 未实现   |
 * | `http`    | `"http"`         | 未实现     | 未实现   | 未实现   |
 * | `agent`   | `"agent"`        | 未实现     | 未实现   | 未实现   |
 * | `fs`      | `"fs"`           | 未实现     | 未实现   | 未实现   |
 * | `emit`    | 无需声明          | 可用       | 可用     | 未实现   |
 * | `notify`  | 无需声明          | 可用       | 可用     | 未实现   |
 * | `log`     | 无需声明          | 可用       | 可用     | 未实现   |
 *
 * `request` 模式没有调用入口，因此上表中的“未实现”表示不会发生调用，而不是运行期拒绝。
 *
 * 目标判据是 `CallMode::allows_io`：`Intercept` 返回 false，`Observe` 与 `Request`
 * 返回 true。该裁剪在 HostApi 接入后生效。
 */

// ═══════════════════════════════════════════════════════════════════════════
// 6. 工具与事件
// ═══════════════════════════════════════════════════════════════════════════

/**
 * mod 可以出现在模型工具目录中的入口：
 *
 * - `mcpServers.<local>`：服务名为 `mod_<mod>_<local>`，工具名为
 *   `<服务名>0m0<原始工具名>`。这是当前唯一已实现的 mod 工具来源。
 * - `requests.<name>`：目标工具名为 `m_<mod>__<request>`。未实现；当前工具目录只包含
 *   MCP 工具与内置 `wind_spawn_agent`。
 *
 * 清单的 `requests.<name>.callableBy` 取 `agent` / `ui`，当前没有调用边界强制它。
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
// 7. UI 侧
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
  readonly host: UiHostApi;
}

/** 前端底座的后端接口集合。成员与传输层尚未定稿，当前为空。 */
export interface UiHostApi {}

// ═══════════════════════════════════════════════════════════════════════════
// 8. 异常
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 当前实现的异常行为：
 *
 * - 门面调用失败以普通 `Error` 抛出，消息是英文短句。
 * - 宿主记录到 `ScriptsError` 时，引擎优先返回该错误，而不是 JS 异常本身；因此
 *   `PayloadTooLarge`、`Handler` 等分类在宿主侧可见。
 * - 处理函数不得捕获异常后返回“通过”；这会把超时、取消或载荷错误转成 mod 自己的结论。
 *
 * 以下类型化异常是目标契约，当前没有对应的宿主侧类型：
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
// 9. 不包含的能力
// ═══════════════════════════════════════════════════════════════════════════

/**
 * 刻意排除，不属于遗漏：
 *
 * - 不存在 `ctx.hook(...)`。拦截通过订阅 `intercept` 事件实现。
 * - mod 之间不互相调用，也不能订阅另一个 mod 的 `m_<mod>/` 事件；清单只接受 `core/`
 *   开头的订阅键，目录中也没有动态 mod 事件。
 * - 不存在宿主侧异步接口。每次调用新建解释器并同步执行。
 * - 不存在 `mount`。挂载属于前端底座。
 * - 不存在注册新能力类型的接口。
 * - 不存在长期子进程接口。需要长期进程时，用清单的 `mcpServers` 声明 stdio 服务。
 * - 不存在二进制传输通道。宿主与 mod 之间的数据是结构化 JSON 值。
 * - 不存在逐项能力授权。全部能力声明即生效。
 */
