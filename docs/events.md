# windai 事件目录与事件模型

本文定义 windai 宿主的**事件模型**与**核心事件目录**。它是 mod 订阅、宿主派发、
以及子系统改造三方的共同依据。

---

## 1. 范围

本文规定：

1. 事件的信封结构与字段语义
2. 事件的两种形态及其派发语义
3. 消息树的三层结构（`turn` / `message` / `block`）
4. 宿主核心事件的完整目录，含每条事件的形态、唯一入口与载荷
5. 事件日志的内容、保留策略与历史读取接口
6. 改写留痕、闭环防护、覆盖判据

本文不规定：Mod 的包格式（见 `docs/manifest.schema.json`）、Mod 可用的宿主 API
（见 `docs/facade.d.ts`）。

---

## 2. 术语

| 术语 | 定义 |
| --- | --- |
| **事件** | 宿主状态变化或即将发生的变化的一次结构化陈述。 |
| **形态** | 事件的两分属性，取值 `intercept` 或 `observe`。决定订阅者能否干预，以及派发是否阻塞。 |
| **订阅者** | 声明了对某条事件感兴趣的一方。包括 Mod（可干预）与内建观察者（只读）。 |
| **内建观察者** | 宿主自带的、不可干预的事件消费者。前端事件流、持久化日志、遥测均属此类。 |
| **唯一入口** | 某个子系统中，所有状态变化都必须经过的唯一函数或代码位置。 |
| **轮（`turn`）** | 一次用户输入所引发的全部产出的分组。 |
| **消息（`message`）** | 一次模型 API 往返的分组。 |
| **块（`block`）** | 消息内的一个内容单元，是事件系统中可寻址的最小内容粒度。 |

**不变量**：Mod 与内建观察者消费的是**同一条事件流**。二者的差别仅在于过滤规则
（见「订阅者筛选」），不构成两条不同的流。

---

## 3. 事件信封

所有事件共享同一信封结构：

```ts
interface EventEnvelope<P = unknown> {
  /** 事件身份。全局单调递增，同一进程生命周期内唯一。 */
  seq: number;

  /** 事件产生时刻。Unix 纪元毫秒。 */
  time: number;

  /** 事件名。宿主事件以 `core/` 开头，Mod 事件以 `m_<mod>/` 开头。 */
  name: string;

  /** 触发本次操作的作用域。 */
  scope: Scope;

  /** 触发本次操作的路径来源。 */
  origin: Origin;

  /** 事件发出方。 */
  source: Source;

  /** 起因事件的 `seq`。顶层操作没有起因，此字段缺省。 */
  cause?: number;

  /** 事件载荷。结构由「事件目录」中该事件的条目规定。 */
  payload: P;
}

type Scope =
  | { kind: 'global' }
  | { kind: 'topic'; topic_id: number };

type Origin = 'agent' | 'ui' | 'internal';

type Source =
  /** 宿主子系统。`subsystem` 取值如 `'mcp'`、`'agent'`、`'fs'`。 */
  | { kind: 'core'; subsystem: string }
  /** 某个 Mod。 */
  | { kind: 'mod'; name: string; version: number };
```

**信封在事件产生时冻结。** 后续的改写只修改 `payload`，不修改信封；改写的记录另见「改写留痕」。

### 3.1 `seq` 的语义

`seq` 是事件的身份，承担三项用途：

| 用途 | 说明 |
| --- | --- |
| 身份 | 改写留痕需要指认被改写的事件的身份 |
| 定序 | 消费方按 `seq` 排序，**不依赖到达顺序** |
| 起因链 | `cause` 指向另一事件的 `seq`；闭环防护据此计算深度（见「闭环防护」） |

**每条事件都必须有 `seq`，且序列号不得出现空洞。** 因此事件产生路径不可跳过（见「快速路径」）。

### 3.2 `time` 的语义

`time` 是**事件产生时刻**，不是投递时刻。

一条事件可以被投递给多个消费者，各次投递发生在不同时刻（见「投递追踪（默认关闭）」）。投递时刻属于投递记录，不属于信封。信封在产生时即固定。

### 3.3 `source` 的语义

`source` 表示事件由谁发出：

- `core/` 事件填宿主子系统名
- `m_<mod>/` 事件填 Mod 名与版本

消费方据此区分一条事件由宿主产生还是由某个 Mod 产生。

**Mod 不得直接产生 `core/` 前缀的事件。** 若可伪造，消费方将无法区分来源。需要产生
`core/` 事件的意图通过门面方法表达（例如 `ctx.agent.draft` 产生 `core/input.draft`，
`ctx.notify` 产生 `core/mod.notify`），由宿主翻译为对应事件。

### 3.4 `scope` 的语义

`scope` 表示**触发本次操作的上下文**，而不是事件定义在何处。

宿主实体可以具有全局性质（例如 provider），而触发它的操作仍发生在某个话题内。
此时事件的 `scope` 为话题，而非全局。

`scope` 参与两项判定：

1. 决定唤醒哪些 Mod（见「订阅者筛选」）
2. 决定状态数据的隔离边界

### 3.5 `origin` 的语义

| 取值 | 含义 |
| --- | --- |
| `agent` | 由 agent 循环发起（工具调用、模型请求等） |
| `ui` | 用户直接操作发起 |
| `internal` | 宿主内部任务发起（心跳、崩溃恢复等） |

`origin` 为 `ui` 时，**`intercept` 事件不派发给任何 Mod**；`observe` 事件照常派发（见「`origin` 为 `ui` 时的派发」）。

---

## 4. 事件形态

每条事件在目录中声明形态，取值仅有两种。

| 形态 | 订阅者可做的操作 | 派发方式 | 可否丢弃 |
| --- | --- | --- | --- |
| **`intercept`** | 改写载荷 | 内联同步；决定必须在操作继续之前给出 | 否 |
| **`observe`** | 只读；返回值被忽略 | 异步；在决定作出之后派发 | 是 |

### 4.1 拦截只有改写

`intercept` 的订阅处理函数是载荷上的一个全函数：返回一个新载荷，或者什么都不返回。
没有独立的"否决"结果。系统的主流程始终拿到一个合法的 `T`，不需要知道是否有人拒绝
过、也不必为拒绝发明领域表示。处理函数破坏载荷结构是 Mod 的责任，宿主不做额外校验。

需要在领域里表达"不执行"的信息（例如工具调用的挂起）时，它属于载荷本身的字段，
不属于派发协议。

### 4.2 事件名不携带形态后缀

形态由目录声明。命名不得暗示形态。

### 4.3 订阅处理函数的返回值

`intercept` 事件的订阅处理函数返回以下之一：

- 不返回、返回 `undefined` 或 `null`：不修改载荷；
- 返回一个 JSON 值：该值替换载荷，交给下一个订阅者。

返回旧版的 `{ action: ... }` 包装会被宿主拒绝并报错。

`observe` 事件的订阅处理函数返回值被忽略。

宿主内部不再有 `Outcome` / `Verdict` 两种类型：折叠后的结果就是载荷本身。

---

## 5. 派发语义

### 5.1 两个接口

```rust
// 干预：阻塞，返回改写后的载荷
fn decide<P: EventPayload>(event: &EventDef<P>, cx: &DispatchCx, payload: P) -> P;

// 观测：入队，不阻塞，允许丢弃
fn notify<P: EventPayload>(event: &EventDef<P>, cx: &DispatchCx, payload: P);
```

`decide` 是唯一会阻塞的接口，也是唯一有返回值的接口。它的返回值就是最终载荷。
**它不返回错误**：派发失败只记录并派发 `core/dispatch.failed`，返回值保持当前累积载荷；
`notify` 同理，失败时丢弃这次派发。

### 5.2 派发上下文

顶层操作的入口创建 `DispatchCx`。嵌套派发继承该上下文。

```rust
struct DispatchCx {
    origin: Origin,
    scope: Scope,
    source: Source,                 // 事件由谁发出
    guard: Arc<DispatchGuard>,      // 同一次顶层操作的所有嵌套派发共享同一份；内含剩余预算与取消令牌
    depth: u32,                     // 嵌套深度，顶层为 0
    cause: Option<EventSeq>,        // 起因事件的序号，顶层操作为 None
}
```

**入口与预算的对应关系：**

| 入口 | `origin` | 预算 |
| --- | --- | --- |
| 用户直接操作 | `ui` | 宽松。`intercept` 不唤醒任何 Mod，`observe` 照常派发 |
| agent 工具循环 | `agent` | 严格。每次工具调用分配一份 |
| 宿主内部任务 | `internal` | 宽松 |

嵌套派发将 `depth` 加一，并从**同一个** `guard` 的预算中扣除。一次操作触发的事件数量
不改变其总时间上界。

### 5.3 `origin` 为 `ui` 时的派发

`origin` 为 `ui` 时，**旁路只作用于 `intercept`**：`intercept` 事件不派发给任何 Mod，
操作按无 Mod 情形执行；**`observe` 事件照常派发**。

依据：Mod 约束的是 agent 的行为，不约束用户的行为。**约束是 `intercept` 才有的能力**——
`observe` 只读，谈不上约束，旁路一条只读的派发没有理由，且会让"用户输入可被 Mod 观测"
这句话无法判定。用户直接操作不经过 agent，因此在 `intercept` 上也不经过 Mod 层。
**用户操作与 agent 操作是两条不同的路径，不是同一条路径的两个入口。**

**该规则是常规机制，不是过渡措施。**

**用户不被自身配置所限制的最终保障**是：Mod 的启用、禁用与删除不经过任何拦截（见「Mod 生命周期」）。旁路与不变量解决的是两个不同的问题：旁路让用户**不必**经过 Mod，不变量让用户**能够**关掉 Mod。

### 5.4 订阅者筛选

```
1. 取该 scope 的启用集合：enabled(scope)
2. 若该事件的形态是 intercept 且 origin == 'ui'，返回空集合
3. 保留声明过该事件名的订阅者
```

启用集合不具继承关系，亦无全局回退。新话题创建时复制默认集合，此后两者独立演进。

### 5.5 订阅者排序与折叠

`intercept` 订阅按相位（`phase`）分层执行：

| 相位 | 用途 |
| --- | --- |
| `guard` | 安全与权限。最先执行 |
| `policy` | 业务策略。在 `guard` 之后执行 |

同一相位内按注册顺序执行。

**Mod 声明自己落在哪个相位，这是数据；相位之间的先后是常量。** 前者让 Mod 表达
"我属于安全层还是策略层"而不必争夺位置；后者若是可变的，就只是给"某个相位不在表里"
留了一条静默排到末尾的失败路径。

`observe` 事件不参与相位排序。它没有可争的决定，位置固定在决定作出之后。

**折叠算法：**

```
payload = 原始载荷
for sub in 排序后的订阅者序列:
    t = min(sub.timeout_ms, budget.remaining)
    if t == 0:
        记录 skipped；继续
    rewrite = host.call(sub, payload, deadline = now + t)
        超时          → 跳过该订阅者；记录 timeout 并派发 core/dispatch.timeout
        处理函数抛错   → 跳过该订阅者
        宿主自身出错   → 记录 critical；跳过该订阅者
        返回空         → 记录 pass；继续
        返回一个载荷   → payload = 返回值；记录 rewrite；继续
return payload
```

**挂起不是一种订阅结果。** 需要外部答复的调用由载荷自身携带：`core/llm.toolcall` 的
`ToolCalls.suspended` 列出需要转交用户的调用 id。宿主读到非空的 `suspended` 后把它
作为一次外部等待持久化并派发 `core/approval.request`；答复经 `host` 上被暴露的对应
接口回来。挂起时长的上界不存在——用户可能数小时后才答复。但**处理函数自身仍受
`timeoutMs` 约束**。

**性能要求**：事件名无任何订阅者时，派发必须退化为一次哈希查找，不得触碰任何 Mod
运行时。

### 5.6 调用模式与能力裁剪

事件的派发模式决定订阅处理函数可用的能力：

| 形态 | 调用模式 | 可用 I/O 能力 |
| --- | --- | --- |
| `intercept` | 内联同步 | **不可用** |
| `observe` | 异步 | 可用 |

`intercept` 处理函数的作用域中不注入 I/O 能力（出网、agent 交互）。此限制在能力
注入阶段生效，即相应属性不存在，而非运行时拒绝。

依据：`intercept` 阻塞系统，其预算为毫秒量级，任何 I/O 都必然超时。使该能力不存在
优于使其超时。

完整的可用性矩阵见 `docs/facade.d.ts`。

### 5.7 批量派发

`observe` 事件投递到 Mod 时由**宿主按窗口合批**：同一 `(mod, event, scope)` 在时间
窗口内的多条事件合成一次调用。**manifest 不声明合批**，任何 `observe` 订阅都按同一
窗口处理。组内只有一条时，载荷就是该事件的值本身；多于一条时，载荷是这些值组成的
数组。处理函数用 `Array.isArray(payload)` 判断拿到的是哪一种，两种情况都必须处理。

一次 Mod 调用可能覆盖 N 个事件，`timeout_ms` 预算与 `core/dispatch.timeout`、投递
记录都覆盖整组，并采用组内第一个 `seq`。

`intercept` 事件不支持合批。决定必须在操作继续之前给出。

### 5.8 消费者与投递类别

事件总线有两个消费者族：

| 族 | 可做的操作 | 未安装 Mod 时 |
| --- | --- | --- |
| Mod | 干预、改写 | 不存在 |
| 内建观察者 | 只读 | 必须照常工作 |

前端事件流属于内建观察者，不是 Mod 系统的输出。在未安装任何 Mod 时，会话内容流
必须完整可用。

内建观察者与 Mod 消费同一份事件流，二者不得存在特权路径差异。

**投递类别完全由形态决定。**

| 形态 | 投 Mod | 投前端事件流 | 落日志 |
| --- | --- | --- | --- |
| `intercept` | 有订阅才投 | **否** | 是 |
| `observe` | 有订阅才投 | **是** | 是 |

**一处显式例外**，在事件目录中标注：

| 事件 | 偏离 | 理由 |
| --- | --- | --- |
| `core/block.delta` | `observe`，但**不落日志** | 它是草稿，不是事实 |

`intercept` 不投前端事件流的理由：它们描述"宿主即将做什么"，属于控制面；前端需要的
是"已经发生了什么"。**没有任何 `intercept` 事件需要到达前端**——可能被误认为需要的
那一处（审批）已改为在挂起时派发一条 `observe` 事实，因此不需要例外。

该例外的性质与投递范围无关，它只描述 `core/block.delta` 不进入日志。事件日志的写入
是正确性所需，而草稿不是事实。

前端事件流的过滤只有两个维度，均为查询条件，不引入新机制：

| 维度 | 规则 |
| --- | --- |
| 作用域 | 只投给订阅了该话题的连接 |
| 类别 | 按上表；例外按事件自身标注 |

### 5.9 快速路径

**当某条事件没有任何 Mod 订阅时，宿主跳过整个 Mod 投递路径。**

需要精确区分被跳过的与不被跳过的部分：

| 环节 | 是否跳过 |
| --- | --- |
| 事件产生（分配 `seq`、写入日志） | **否**。跳过会使日志不完整、序列号出现空洞 |
| 查 Mod 订阅索引 | 是 |
| 投递给内建观察者 | **否**。按「消费者与投递类别」中的类别决定 |
| 构造调用上下文、实例化解释器 | 是 |

判定条件：

```
Mod 订阅索引中不存在该事件名 且 该事件不投前端事件流
```

**内建观察者是常驻的。** 前端事件流对展示面事件始终有订阅，因此那些事件永远走完整
路径。这不是快速路径失效，而是设计如此：会话内容流在未安装任何 Mod 时也必须可用。

换言之：**"没有 Mod 订阅"与"没有消费者"是两件不同的事。** 前者可以被优化掉，后者
不存在。

### 5.10 投递追踪（默认关闭）

消费方的事件面板需要展示"一条事件投给了谁、结果如何"。这些信息不在信封里，因为
**一次投递不是事件的一部分**：一条事件可以有零个、一个或多个投递。

投递记录作为独立事件产生：

| 事件 | 形态 | 载荷 |
|---|---|---|
| `core/dispatch.delivered` | observe | `{ event_seq, target, outcome, duration_ms, error? }` |

```
target:  { kind: 'mod'; name: string } | { kind: 'observer'; name: string }
outcome: 'pass' | 'rewrite' | 'observed' | 'skipped' | 'timeout' | 'error'
```

**默认关闭。**

| | 关闭 | 打开 |
| --- | --- | --- |
| 可见内容 | 事件流本身（含 `seq` / `time` / `source` / `cause`） | 加上每次投递的目标与结果 |
| 数据量 | 事件数 | **事件数 × 订阅者数** |
| 用途 | 常规运行 | 排查"某个 Mod 为何未被唤醒""某个决定被谁改写" |

默认关闭的理由：对高频事件而言，投递记录的量级是事件本身的数倍。常开会使一项调试
能力成为常态成本。

**它是一个开关，不是一项被禁用的能力。** 用户开启后即获得完整的投递链路视图，代价是
上述数据量。

**一条必需的排除规则**：**`core/dispatch.*` 事件自身不产生投递记录。** 否则任何订阅
了它的事件都会引发递归。

### 5.11 默认开关的取值原则

**影响性能的开关一律默认关闭，由用户开启并承担后果。**

判据是该能力的**数据量或计算量是否随负载放大**：

| 能力 | 随负载放大 | 默认 |
| --- | --- | --- |
| 投递追踪（见「投递追踪（默认关闭）」） | 是（事件数 × 订阅者数） | **关闭** |
| 事件日志 | 线性于事件数，且是正确性所需 | 开启 |
| 合批（见「批量派发」） | 否（减少调用次数） | **固定启用**（宿主窗口，无开关，manifest 不声明） |

**开启一个默认关闭的开关时，其代价必须有文档记载且可量化。** "由用户承担后果"的前提
是后果可见；不可见的后果不构成可选择的选项。

**凡是能默认关闭的，就必须有对应的开关，而不是只保留关闭状态。** 默认值是策略，
硬编码不是。

### 5.12 帧与事件

**前端的传输通道上存在两类消息，二者不是同一种东西。**

| 类别 | 何时发送 | 收件人 | 是否落日志 | 是否属于事件目录 |
| --- | --- | --- | --- | --- |
| **事件帧** | 事件产生时 | 按「消费者与投递类别」 | 是（除显式例外） | 是 |
| **快照帧** | 订阅建立时发送（零到多帧） | **仅该订阅** | 否 | **否** |

**快照帧不是事件。** 判据：**它不是"发生了什么"的陈述，而是一次握手的内容。** Mod
订阅它没有定义的含义——它携带的是未提交块的基线，只对刚刚建立连接的展示端有意义。

由该区分得到两条结果：

1. 快照帧不受「消费者与投递类别」约束，因此该表不需要为它增设例外。
2. 快照帧不属于事件目录，不分配 `seq`，不参与起因链。

**重连机制不受影响**，见「重连」：它本来就由"首帧（回补出的历史段 + 快照帧）+ 后续
事件帧"构成。

### 5.13 订阅握手

**核心要求：基线的产生与订阅的注册必须在同一个临界区内完成。**

这不是优化项。缺少它会产生缺口或重叠，且部分后果不可自愈（见「问题」）。

#### 5.13.1 问题

订阅方建立连接时需要两部分输入：

- **基线**：订阅建立时刻的当前值
- **增量**：此后产生的变更

若二者不在同一时刻切分，就会出现一个窗口。设注册为 `R`，读取基线为 `S`：

| 顺序 | 窗口内（`R` 与 `S` 之间）产生的变更 | 后果 |
| --- | --- | --- |
| 先 `S` 后 `R` | 既不在基线中，也未被转发给该订阅方 | **缺口** |
| 先 `R` 后 `S` | 既在基线中，又已被转发 | **重叠** |

两种顺序的后果严重程度取决于通道类别：

| 通道 | 缺口 | 重叠 |
| --- | --- | --- |
| **分片类**（如 `core/block.delta`） | 内容缺失，但最终值（`core/block.committed`）到达时整体替换 | 由 `revision` 排除 |
| **状态类**（`stateful` 投递） | **值停留在旧版本，直至下一次变更** | **旧值覆盖新值** |

**分片类可自愈，状态类不可。** 因此本要求对状态类通道是必需的。

#### 5.13.2 机制

```
临界区 {
    boundary = 当前进度
    baseline = 当前状态基线()
    订阅表.注册(订阅方, 首帧 = baseline)
}

临界区外 :
    发送(baseline)                          // 快照帧
    此后产生的变更按序入该订阅方的帧队列        // 事件帧
```

**首帧是快照帧，这不是无条件的。** 带历史回补的订阅在它之前还有一段历史事件帧，
顺序见「事件日志与历史读取」；既不带历史、又没有快照的订阅，首帧就是事件帧。

由该结构得到三项性质：

| 性质 | 保证 |
| --- | --- |
| 临界区之前产生的变更 | 已包含在基线中 |
| 临界区之内 | 不产生变更——临界区排他 |
| 临界区之后产生的变更 | 进入帧队列，排在基线之后 |

**因此不需要额外的缓冲。** 唯一需要满足的条件是：**帧队列的入队顺序等于变更的产生顺序。**

#### 5.13.3 实现方式

| 方式 | 做法 | 评价 |
| --- | --- | --- |
| **单所有者串行化** | 状态持有者是唯一所有者；"取基线并注册"作为一条消息发给它，与变更的产生走同一条串行路径 | 无需锁。与宿主既有的 actor 结构一致 |
| **临界区** | 状态读写与订阅表更新共用一把互斥量 | 可行，但把互斥量放进了变更产生的路径 |

#### 5.13.4 帧队列边界

帧队列必须有界。超出上限时按通道区分处置：

| 通道 | 处置 | 依据 |
| --- | --- | --- |
| 分片类 | 丢弃队列里最旧的帧 | 幂等且自愈——最终值到达时替换全部内容 |
| 状态类 | 丢弃队列里最旧的帧 | 队列看不到 key，无法据 key 合并；「每 key 仅保留最新值」由持有该 key 的投递方负责 |
| 其余事件 | 断开该连接 | 客户端可重连，握手幂等 |

状态类的按 key 合并发生在投递方，不在队列这一层：**状态键不在帧里**，队列无从据
key 归并。队列只管帧的进出，它面对两类可丢的帧做同一件事——挤出最旧的一帧。

**断开是安全操作**：重连即重新执行一次握手。

#### 5.13.5 适用范围

本要求适用于所有"基线 + 增量"形式的通道：

| 位置 | 基线 | 增量 |
| --- | --- | --- |
| 消息流（见「重连」） | 历史事件帧 + 快照帧中的未提交块 | 事件帧中的块事件 |
| `stateful` 事件投递 | 该键的当前值 | 后续同键变更 |

文档其余各处描述"先收基线再收增量"时，均以本节为机制依据。

### 5.14 事件日志与历史读取

#### 5.14.1 日志内容

事件日志是内建观察者之一，默认开启。它记录每一条产生的事件。

| 项 | 内容 |
| --- | --- |
| 记录粒度 | 一条事件一条记录 |
| 记录字段 | 信封全部字段（`seq` / `time` / `source` / `scope` / `origin` / `cause`）、形态、完整 `payload` |
| 覆盖范围 | `core/*` 与 `m_<mod>/*` 的全部事件 |

不落日志的情形只有一处显式例外：`core/block.delta`（见「消费者与投递类别」）。

改写留痕（见「改写留痕」）与投递追踪（见「投递追踪（默认关闭）」）自身都是事件，
因此只要它们产生，就同样进入日志。

**日志是事件的完整记录。** 「快速路径」明确不跳过此处：分配 `seq` 与写入日志位于
事件产生路径上，不可省略。这保证 `seq` 无空洞，日志与事件一一对应。

#### 5.14.2 保留策略：随话题删除

每条日志记录携带其 `scope`。记录的生命周期由 `scope` 决定。

| `scope` | 保留 |
| --- | --- |
| `topic` | **随该话题删除一并删除** |
| `global` | 不随任何话题删除 |

`scope` 的取值规则见「`scope` 的语义」：它是触发本次操作的上下文。因此在话题内触发的、
作用于全局实体的操作（例如在该话题中创建一个 provider），其记录随该话题删除。这是
预期行为——该记录陈述的是"该话题中发生了什么"。

`global` 记录仅由全局操作产生，其数量与话题无关，因此不适用随话题删除的策略。
**其保留期限不在本规范内定义**，由部署配置决定。

**不设基于时间或容量的轮转。** 记录的生命周期完全由话题的生命周期决定。

#### 5.14.3 读取接口

**读取能力是订阅握手的参数，不是独立接口。**

历史读取与实时订阅在语义上属于同一件事——"取得从某个位置开始的全部事件"。区别仅在于
该位置位于过去还是当前。

| 参数 | 含义 |
| --- | --- |
| `since_seq` | 可选。起始位置。省略时只接收实时事件，不回补历史 |
| `until_seq` | 可选。终止位置。省略时在追上当前进度后转入增量 |
| `blocks` | 可选。是否随握手给出未提交块的快照。线上形状见 `docs/event-stream-wire.md` 的「握手的请求参数」 |

**返回内容为原始事件记录**：信封全部字段、形态、完整 `payload`。不做聚合、不做改写、
不做投影。

**按 `scope` 过滤。** 订阅了某话题的连接只收到该话题的记录，不收到 `global` 记录。
读取 `global` 记录需要建立全局订阅。

`seq` 是全局序号，因此按话题过滤后返回的记录在 `seq` 上不连续。序号本身仍作为游标使用。

**分页。** 单次回补有上限（默认 1000 条）。该上限数的是与作用域匹配的日志**行**，
数在可见性过滤之前，因此一页可能给出少于 1000 帧，也可能一帧都不给。**客户端可用的
下一个游标是这一页最后一帧的 `seq`**：页尾的记录若被可见性过滤掉，客户端的游标就
落在它之前。

#### 5.14.4 与订阅握手的复用

**边界的取得与注册订阅必须在同一个临界区内完成**，机制同「订阅握手」。否则产生缺口
或重叠。

```
临界区 {
    boundary = 当前进度
    baseline = 当前状态基线()
    订阅表.注册(订阅方, 首帧 = 历史段 + baseline)
}

临界区外 {
    history = 日志查询(scope, since_seq, limit, until = min(until_seq, boundary))
    发送(history)                          // 事件帧，按序号升序
    发送(baseline)                         // 快照帧
    此后产生的事件按序进入该订阅方的帧队列      // 事件帧
}
```

**上下界与注册出自同一个临界区**：`boundary` 是注册那一刻的进度，回补的上界取
`min(until_seq, boundary)`，因此注册之后产生的事件不会落进历史段；注册之前产生的
事件由历史段与快照承担。查询本身在临界区之外执行，它读的是一个已经定下的边界。
**这里说的「进度」以事件到达观察者为准，不是以取号为准**：序号在事件交给观察者之前
就已分配，并发之下序号小的可以晚于序号大的到达观察者，注册正好落在两条之间时，
同一个序号可能既在回补里又在实时队列里。这正是客户端要用事件水位丢弃重复帧的
理由（见「消息与块」的客户端合并规则）。

与「订阅握手」的差别仅在基线的形状与它在首帧里的位置：

| 通道 | 基线 |
| --- | --- |
| 状态类（`stateful` 投递） | 该键的当前值 |
| 分片类（消息流） | 未提交块的快照 |
| **事件历史** | **一个 `seq` 区间内的原始事件** |

三者的共同点是"订阅建立时必须一次性给出该订阅方错过的内容"，临界区规则因此通用。
**事件历史排在其余基线之前**：历史按时间顺序填充过去，快照是此刻的状态，紧跟实时
增量。客户端因此可以顺序处理——先补历史，再用快照建立当前状态，最后消费增量。

**回补帧与实时帧是同一形状。** 客户端不需要区分二者，也不需要为历史读取实现第二套
处理逻辑。

#### 5.14.5 可见性边界

**读取范围不超过订阅方已有的话题可见性。** 日志查询按 `scope` 过滤，订阅方无法通过
指定 `seq` 区间读取其他话题的记录。

`seq` 为全局序号，因此可从中推知"其他话题产生了多少事件"。该信息属于序号机制的固有
性质，不在本规范的可见性边界内。

---

## 6. 消息树

会话内容采用三层结构。

| 层 | 名称 | 语义 | 承载的独有属性 |
| --- | --- | --- | --- |
| 1 | `turn` | 一轮对话 | `from_id`（本轮的用户输入）、`is_boundary` |
| 2 | `message` | 一次模型 API 往返 | `usage`、`finish_reason`、`model_id` |
| 3 | `block` | 内容单元 | `kind`、`call_id`、`revision` |

### 6.1 层次结构的必要性

**第三层不可省略**：`AiMessage` 的 `tool_calls` 为数组类型，`content` 中可包含多个
结果项。在消息层，`call_id` 不是标量字段，配对操作需要遍历列表元素。降入块层后，
`call_id` 成为标量字段，配对成为字段比较。

**第二层不可省略**：存储结构必须比 `AiMessage` 更细，否则 `AiMessage` 是存储结构
本身而非投影。此外 `usage` 与 `finish_reason` 是每次 API 往返的事实，只能归属该层。

**第一层不可省略**：`from_id` 与 `is_boundary` 是轮级属性。

### 6.2 块的定义

```ts
interface Block {
  /** 块标识。全局唯一，单调递增。用于合并、补发与去重。 */
  id: string;

  /** 所属消息。 */
  of_message: number;

  /** 该块在消息中所属的角色。 */
  role: 'system' | 'user' | 'assistant' | 'tool' | 'developer';

  /** 块类型。 */
  kind: BlockKind;

  /** 工具调用配对键。仅 ToolCall 与 ToolResult 两类具有。 */
  call_id?: string;

  /** 修订号。每次追加内容递增。用于流式去重。 */
  revision: number;

  /** 文本内容。Text 与 Reasoning 两类使用。 */
  content?: string;

  /** 结构化载荷。其余类型使用。取值随 kind 而定，见下 */
  data?: unknown;

  /** 是否已提交。已提交的块不可再修改。 */
  finished: boolean;
}

type BlockKind =
  | 'text'
  | 'reasoning'
  | 'tool_call'
  | 'tool_result'
  | 'media';
```

**`content` 与 `data` 的取值随 `kind` 而定。** 前端据此渲染不同种类的块；同样的形状
适用于快照里的块与 `core/block.committed` 的载荷：

| `kind` | `content` | `data` |
| --- | --- | --- |
| `text` | 正文文本 | 不出现 |
| `reasoning` | 推理文本 | 不出现 |
| `tool_call` | 参数原文；参数解析进载荷之后为空，解析失败时保留原文 | `{ name, arguments }`，`arguments` 是解析后的参数对象。未提交的块上通常只有 `{ name }`；解析失败时无 `arguments`；工具名未知时无 `name` |
| `tool_result` | 不出现 | 工具返回的完整值；本次调用被拒绝时是 `{ refused: true, reason }` |
| `media` | 媒体载荷 | `{ mime }`，`mime` 是 `image`、`file`，音频则是它的编码格式 |

### 6.3 草稿与事实

块的产生分为两条通道：

| | 草稿通道 | 事实通道 |
| --- | --- | --- |
| 事件 | `core/block.delta` | `core/block.committed` |
| 是否落库 | 否 | 是 |
| 是否进入日志 | 否 | 是 |
| 可否丢弃 | 是 | 否 |
| 丢失后果 | 界面内容滞后；重连时由快照补齐 | 数据缺失 |

两条通道分离后，同一内容先后到达两次不属于异常：草稿先到，事实后到，客户端按块
标识覆盖。

### 6.4 重连

新订阅者建立连接时，宿主按「订阅握手」的顺序发送：**回补出的历史事件帧在前，快照帧
紧随其后**，快照帧包含当前所有未提交的块；实时事件帧在最后。两者都排在实时增量之前，
因此客户端先补齐过去，再拿到此刻的状态，最后消费增量。不请求历史回补时没有第一段，
快照帧即首帧；既不回补又不请求快照时，首帧就是实时事件帧。

```
服务端
  │
  ├─ 内存累加器（仅保存未提交的块）
  │
  ├── 每次追加 ──────────────► core/block.delta
  │
  └── 订阅建立时取一次 ──────► 快照帧
```

已提交的块不进入快照，随历史查询一并加载。

**快照帧的读取与订阅的注册必须同序**，否则会产生缺口或重叠。机制见「订阅握手」——本节不另行规定顺序。

**客户端合并规则：**

```
onOpen(o):
    blocks[o.block_id] = { of_message: o.of_message, role: o.role,
                           kind: o.kind, call_id: o.call_id,
                           content: "", revision: 0, finished: false }

onSnapshot(s):
    for b in s.blocks: blocks[b.id] = b
    // 快照是订阅建立那一刻全部未提交块的权威：本地还有 finished 为假、
    // 标识不在 s.blocks 里的块，一并丢弃

onDelta(d):
    if d.revision > (blocks[d.block_id].revision 或 0):
        blocks[d.block_id].content += d.fragment
        blocks[d.block_id].revision = d.revision

onCommitted(c):
    blocks[c.block_id].kind     = c.kind
    blocks[c.block_id].call_id  = c.call_id 或 空
    blocks[c.block_id].content  = c.content 或 ""
    blocks[c.block_id].data     = c.data 或 空
    blocks[c.block_id].revision = c.revision
    blocks[c.block_id].finished = true
```

`onOpen` 不可省：一个在连接内开块、又在连接内提交的块从不出现在任何快照里，它的
`of_message` 与 `role` 只有 `core/block.open` 给得出。提交帧的载荷本身不带这两个
字段（见 `docs/event-stream-wire.md` 的「客户端的合并规则」），身份因此来自此前的
`core/block.open` 或快照。客户端还须记下已应用过的最大 `seq`，并忽略 `seq` 不大于
它的任何事件帧——快照帧不带序号，不参与这条规则。

**快照对未提交块是权威的**：收到快照时丢弃本地一切 `finished` 仍为假、而标识不在
快照里的块。在别处已经提交掉的块由此消失，不会留成一条永远
「进行中」的幽灵；**块标识可以安全复用，依据也是这条规则，而不是巧合**。已提交的
块（`finished` 为真）不在快照里，不受这条丢弃影响。

上述规则对分片丢失、分片重复、快照与分片到达顺序颠倒三类情形均产生一致结果。
客户端的合并状态不需要与宿主对账；出现不一致时重新拉取快照即可恢复。

### 6.5 存储结构

```sql
-- 第 1 层：一轮对话
turn(id PK, instance_id, topic_id, from_id, is_boundary, created_at)

-- 第 2 层：一次 API 往返
message(id PK, turn_id FK, seq,
        role, model_id, finish_reason, input_tokens, output_tokens)

-- 第 3 层：内容块
block(id PK, message_id FK, seq,
      kind, call_id, revision, content, data, finished)
```

未提交的块仅存在于内存累加器中，不落库。进程终止时未提交的块丢失。此项为已接受的
取舍。

**约束**：内存累加器不得被任何历史查询路径读取。由于未提交的块不在数据库中，该
约束由存储结构本身保证。

---

## 7. 事件目录

各表中各列含义：

- **形态**：`intercept` 或 `observe`
- **唯一入口**：该子系统所有状态变化必须经过的位置。绕过该位置即违反本规范
- **载荷**：事件载荷的结构。字段后标注 `?` 者为可选

**事件名的缩写约定。** 表中形如 `` `core/a.b.c` / `.d` `` 的写法，`.` 后的部分
**替换最后一个段**，展开为 `core/a.b.d`。该约定不能跨越两个及以上段，因此
`core/mcp.server.list` 与 `core/mcp.tool.list` 必须分别写全，不得缩写为
`` `core/mcp.server.list` / `.tool.list` ``。

**两处不列入本目录的事件：**

| 事件 | 位置 | 原因 |
| --- | --- | --- |
| `core/<被改写的事件名>.rewritten` | 「改写留痕」 | 参数化家族，名字由被改写的事件决定，无法逐条列举 |
| 快照帧 | 「帧与事件」 | 不是事件，是握手内容 |

其余事件一律在本目录中列出。任何在正文出现而未列入本目录的事件名，均视为规范缺陷。

**本节只列 `core/` 事件。** Mod 自定义事件（`m_<mod>/…`）由各 Mod 在 manifest 的
`events.emit` 中声明，不在本目录内。它们的唯一入口是门面方法 **`ctx.emit(name, payload)`**：
`name` 必须是已声明的名字，宿主加前缀后投递。语义见「Mod 通知」一节中的对照。

### 7.1 输入

| 事件 | 形态 | 唯一入口 | 载荷 |
|---|---|---|---|
| `core/input.draft` | observe | 门面方法 `ctx.agent.draft` | `{ topic_id, text }` |
| `core/input.accepted` | observe | `Assembly::submit` 把输入送入待消费队列时 | `{ topic_id, instance_id, pending_id }` |

**用户输入的内容没有独立的入向事件；「已进入待消费队列」这一事实有。**

内容仍经会话内容的常规通道进入：它成为一轮中的一条消息，其内容以块的形式提交，
对应 `core/block.committed`（见「消息与块」）。`core/input.accepted` 只说明"这条输入
已被受理、进入待消费队列"：`Assembly::submit` 总是先写队列行，节点忙时派发它；
节点空闲、输入直接开启一轮时不派发。载荷带 `topic_id`、`instance_id` 与
`pending_id`，不带内容，因此不与块事件重复。

这个事件没有可争的决定：`Assembly::submit` 以 `Origin::Internal` 发起这次操作，而
本事件是 `observe`，照常派发给订阅方。它没有 `intercept` 形态，因此「`origin` 为
`ui` 时旁路 `intercept`」的规则在这里不适用；"排队"这一状态对订阅方可见。

`core/input.draft` 的语义由前端定义。宿主只负责产生并投递该事件，不规定前端如何处置
（填入输入框、作为建议展示、丢弃等均属前端决定）。

**该事件不落库、不进模型上下文。** 它描述的是"一个可能的输入"，不是"一次已发生的
输入"。只有用户确认后才真正进入会话。

**该事件的投递受「闭环防护」中的每 Mod 速率上限约束**，以避免单个 Mod 持续改写用户
输入区域。

### 7.2 配置实体

**一组 CRUD 是三条事件，不是两条。** 创建不覆盖编辑。

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/topic.create` | intercept | 话题用例层 | `{ parent_id?, label, model_id?, tool_approval_policy?, icon? }` |
| `core/topic.update` | intercept | 话题用例层 | `{ topic_id, data }` |
| `core/topic.delete` | intercept | 话题用例层 | `{ topic_id }` |
| `core/topic.created` / `.updated` / `.deleted` | observe | 话题用例层 | `{ topic_id }` |
| `core/provider.create` | intercept | provider 用例层 | `{ name, alias?, description?, base_url, doc? }` |
| `core/provider.update` | intercept | provider 用例层 | `{ provider_id, data }` |
| `core/provider.delete` | intercept | provider 用例层 | `{ provider_id }` |
| `core/provider.created` / `.updated` / `.deleted` | observe | provider 用例层 | `{ provider_id }` |
| `core/credential.create` | intercept | 凭证用例层 | `{ id, provider_id, key, created_at, active }` |
| `core/credential.update` | intercept | 凭证用例层 | `{ credential_id, data }` |
| `core/credential.delete` | intercept | 凭证用例层 | `{ credential_id }` |
| `core/credential.created` / `.updated` / `.deleted` | observe | 凭证用例层 | `{ credential_id }` |
| `core/model.create` | intercept | 模型用例层 | `{ provider_id, name, alias?, adapter, modalities?, icon?, endpoint?, config? }` |
| `core/model.update` | intercept | 模型用例层 | `{ model_id, data }` |
| `core/model.delete` | intercept | 模型用例层 | `{ model_id }` |
| `core/model.created` / `.updated` / `.deleted` | observe | 模型用例层 | `{ model_id }` |

**`core/*.create` 的载荷是新建实体的字段，`core/*.created` 一族的载荷只带实体标识。**
观测事件描述的是"这个实体变了"，订阅方据此回读；它不重复实体的字段，因此不会在实体
列变化时跟着漂移。

**载荷中的 `adapter` 是枚举，只接受 `openai_completion` 与 `openai_response`。**
其余取值在反序列化阶段即被拒绝。

**`core/credential.create` 的载荷是凭证实体本身，其中 `key` 是按原文长度打星号的掩码。**
明文密钥不入事件载荷，只交给存储。

**三条 `intercept` 不可合并为一条带动作字段的事件。** 订阅按事件名进行；合并后，
只关心删除的 Mod 必须在每次派发时自行判断动作字段。这与本规范要消除的隐式约定同类。

**`update` 的载荷携带 `data`，其中只含本次提交的字段。** 缺省字段不写入，载荷据此
无法判断变更前后的差异。

三个决定回答三个不同问题，互不蕴含：

| 事件 | 该决定回答的问题 |
| --- | --- |
| `create` | 该实体是否允许存在 |
| `update` | 该实体是否允许变成那样 |
| `delete` | 该实体是否允许消失 |

凭证属于安全边界，其三项操作均必须可拦截。话题删除为不可逆操作，同样必须可拦截。

**这四组事件均已落地，四类配置实体的用例层是它们共同的唯一入口。** 配置实体的读写都
收敛在用例层，用例层之外不存在直接写配置的路径；绕过用例层直接写存储即违反本规范。

### 7.3 模型调用

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/prompt.collect` | intercept | 组装系统提示词的位置 | `{ topic_id, instance_id, fragments }` |
| `core/rule.apply` | intercept | 规则引擎的应用点。位于提供商结构侧 | `{ adapter, body }` |
| `core/llm.request` | intercept | 适配器发出请求之前。位于统一结构侧 | `{ model_id, request }` |
| `core/llm.response` | intercept | 完整响应组装完成之后 | `{ model_id, message, usage }` |
| `core/llm.toolcall` | intercept | 模型返回的工具调用被采纳之前 | `{ instance_id, calls, suspended? }` |

`core/llm.response` 在完整响应组装完成后派发，不逐分片派发。逐分片改写会破坏流式
传输与提供商侧的缓存复用。**它派发时分片已经经块累加器落库，因此它的改写影响的是
下一轮上下文与工具采纳，改不了已经进流的块。**

`core/llm.toolcall` 与 `core/mcp.tool.call` 是两个不同的事件：前者对应"模型请求调用"，
后者对应"宿主即将执行"。

**`core/llm.toolcall` 的载荷带 `instance_id`，它是实例标识，不是话题标识。** 这个字段
是订阅方判明这批调用出自哪个实例的唯一依据。**载荷可改写**：订阅方可以改名字、改参数、
增删调用或调整顺序；宿主按改写后的列表执行。改写破坏调用与结果的配对是 Mod 的责任。

**`core/llm.toolcall` 是工具放行的代码档挂载点。** Mod 通过改写载荷表态：

| 改写 | 效果 |
| --- | --- |
| 不返回 / 返回 `null` | 不修改，交给链上其余订阅者；到链尾仍无人修改即按原样放行 |
| 改 `calls` | 这一批调用按新值执行 |
| 在 `suspended` 里列出调用 id | 这些调用转交用户；宿主不执行，实例转入"等外部答复" |

**`suspended` 是逐调用的**：一批里可以部分挂起、部分放行。唯一的例外是同一批里还
包含派生调用：派生必须独占这一轮的 `Outcome`，因此只要同批有挂起，整批挂起，
放行的调用与派生都等到答复之后再执行。挂起后宿主派发 `core/approval.request`
（`observe`），答复落库后派发 `core/approval.resolved`。

**审批载荷来自改写后的调用。** 挂起载荷同时保存 `pending_call_ids` 与 `pending_calls`
（改写后的完整调用）；宿主只把 `call_id` 呈现给用户，但答复之后执行的是
`pending_calls` 里的版本。`external_waits` 仍以 `message_id` 与这批调用标识定位等待。

**声明式白名单先于本事件且截断。** 一次调用在到达本事件之前先过白名单：

```
白名单命中 → 直接执行（终局，不派发给任何 Mod）
其它       → 派发 core/llm.toolcall
```

名单由话题的 `tool_approval_policy` 声明（`{ allow: string[] }`）。**没有黑名单**——
拒绝只由用户表达：把调用写进 `suspended`，用户答复不允时宿主合成一条
`{ refused: true, reason }` 结果。声明式档只承担"这些不用问"，不承担"这些不许"。

**截断是有意的**：它保证用户永远有一条不经过任何 Mod 的放行路径，因此 Mod 无法通过
挂起把系统锁死。

**未命中的调用派发给本事件；无人表态即放行。** 核心不承担「无人表态就问用户」：
没有订阅者表态时，调用直接执行。问用户由**默认审批 mod** 承担——它在
`core/llm.toolcall` 里把需要确认的调用写进 `suspended`，并订阅宿主随后派发的
`core/approval.request`，把待批调用呈现给 UI。次序不能颠倒：先派发给 Mod，
直接问会把 Mod 挡在门外。全新安装的系统省略审批 mod 时，第一个工具调用不会停住
——没有订阅者就没有挂起。

截断的代价：Mod 看不到白名单里的调用。想"放行 fetch 但拦掉指向某域名的 fetch"的
Mod 做不到——它必须在名单里把该工具的整体放行交还给用户。

### 7.4 工具编排

工具编排没有独立事件。`core/llm.toolcall` 是唯一的采纳点；一批调用被采纳之后
直接进入执行，不再有第二次整批判定。

### 7.5 MCP

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/mcp.attach` / `.detach` | intercept | 将服务绑定到话题的用例层 | `{ topic_id, server_name }` |
| `core/mcp.start` / `.stop` | intercept | 启停服务的函数，含自动重连路径 | `{ server_name, reason }` |
| `core/mcp.crash` | observe | 连接断开处 | `{ server_name, error }` |
| `core/mcp.server.list` | intercept | 列举可用服务的位置 | `{ servers }` |
| `core/mcp.tool.list` | intercept | 列举可用工具的位置 | `{ tools }` |
| `core/mcp.tool.call` | intercept | MCP 服务的工具执行点 | `{ server_name, tool, arguments, scope }` |
| `core/mcp.tool.result` | intercept | 同上，取得结果之后 | `{ server_name, tool, call_id, result }` |

`core/mcp.start` 与 `.stop` 一律为 `intercept`。自动重连同样触发启动，因此不得仅对
用户主动触发的路径开放拦截。

**本组事件不区分服务的来源。** 宿主自带的 MCP 服务与 Mod 声明或用户配置的服务走
同一组事件、同一套拦截点、同一套审批规则。**不存在"内置"这一类别。**

**宿主自带的服务的名字是稳定的公开标识。** 当前为 `fs`（文件读写与命令执行）与
`skills`（技能目录扫描与读取）。Mod 可以依据 `server_name` 判别一次调用是否指向它们。

**自带服务对全部话题生效，也不参与按名字的话题绑定。**

因为任何 MCP 服务都可能执行命令或写入文件，**判别只能依据 `server_name` / `tool` /
`arguments` 三个声明字段**，不得依据"这个调用来自哪里"——不存在这个区分。

**MCP 服务的去重规则**：多个 Mod 声明同一个服务时，宿主按服务的**连接方式**判定身份，
而非按名称。名称仅用作引用键。

| 连接方式 | 身份判据 | 去重键 |
| --- | --- | --- |
| stdio，由包管理器启动（`uvx` / `npx` / `bun` / `uv` 等） | `command` + `args` | 包名 |
| stdio，由本包内的相对路径启动 | `command` 以 `./` 开头 | 资产内容指纹 |
| Streamable HTTP | 声明了 `url` | 归一化后的地址 |

因此两个 Mod 声明同名服务但连接方式不同时，宿主建立两条独立连接。该行为符合预期：
按名称合并会将两个不同的实现错误地视为同一服务。

### 7.6 Agent 实例树

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/agent.create` | intercept | hub 的入口，位置在状态机之前 | `{ topic_id, parent_id?, mode, task }` |
| `core/agent.terminate` | intercept | hub 的入口 | `{ instance_id, reason }` |
| `core/agent.cancel` | intercept | hub 的入口。一次取消请求在这里被拦截一次，可改写；级联由状态机自己完成，每个被取消的节点经 `core/agent.status` 逐个可观 | `{ instance_id, origin }` |
| `core/agent.spawn` | observe | hub 的创建入口。实例诞生不是相位变化，翻译层不产出它；运行模式只有创建入口知道 | `{ instance_id, parent_id, mode }` |
| `core/agent.status` | observe | 状态机事件的翻译层 | `{ instance_id, phase, phase_payload? }` |
| `core/agent.error` | observe | 状态机事件的翻译层 | `{ instance_id, message_id?, error }` |
| `core/agent.tree` | observe | hub 的快照读取入口。树结构是 `stateful` 基线，由订阅通道投递 | 树结构快照 |

`core/agent.status` 的载荷携带内部相位的**判别式与载荷**，不携带投影出的对外状态
枚举。对外状态是一个全函数，可由相位算出；两者并存会产生两个真相。

载荷同时携带相位而非仅携带一个对外枚举，是为了保住投影时会丢失的信息：相位里的
"等外部答复"带挂起载荷，"被祖先取消"带取消来源，而对外枚举两者都表达不了。

三条 `intercept` 事件位于状态机之外；`core/agent.status` 与 `core/agent.error` 位于状态机
事件的唯一翻译出口；`core/agent.spawn` 由创建入口直接产生，因为节点的运行模式不在状态机里；
`core/agent.tree` 由 hub 的快照读取入口产生，它读的是树本身而不是某一次相位变化，作为
`stateful` 基线经「订阅握手」投递。状态机本身不感知事件系统。

### 7.7 外部等待

**本组不是"审批"这一件事的专用机制。** 任何使 agent 实例转入"等外部输入"的操作
都经由它；审批是当前唯一的用户。

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/approval.request` | observe | 实例转入"等外部答复"的挂起点 | `{ instance_id, handle, calls }` |
| `core/approval.resolved` | observe | 答复落库之后 | `{ instance_id, handle, decisions, by }` |

`calls` 的每一项为 `{ call_id, server_name, tool, arguments }`——即待放行的调用。这些
调用来自等待行 `payload_json` 里的 `pending_calls`（拦截链改写后的版本），不是从
`tool_call` 块重新读出的原始调用；订阅方只点名标识，不提供调用内容。

`decisions` 的每一项为 `{ call_id, allow }`——即对该次调用的表态。`call_id` 指认它表态的是
`calls` 里的哪一次调用，`allow` 为假表示否决该次调用。

`handle` 是挂起句柄，与 `external_waits` 表的主键对应。答复经 `host` 上被暴露的对应接口
回到宿主，凭 `handle` 定位挂起。

**两条都是 `observe`。** 因此：

- 前端按「消费者与投递类别」的默认规则即可收到 `core/approval.request`，**不需要例外**；
- **没有任何 Mod 能改变用户的答复**。决定"要不要问用户"的位置在
  `core/llm.toolcall`：那里改写调用、把调用写进 `suspended` 挂起；一旦挂起，
  答复就不可被拦。

重复回答同一 `handle` 无效果。挂起载荷不回显比对的任务由 agent 状态机的
`Event::Resume{expect}` 承担。

### 7.8 消息与块

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/message.seal` | observe | 一次 API 往返定稿时 | `{ message_id, finish_reason, usage }` |
| `core/block.open` | observe | 一个块开始产出时；派生任务块在派生事务提交后 | `{ block_id, of_message, role, kind, call_id? }` |
| `core/block.delta` | observe | 流式分片产出时 | `{ block_id, revision, fragment }` |
| `core/block.committed` | observe | 块落库之后 | 块的最终值，附加 `revision` |

`core/block.committed` 的 `content` 与 `data` 按块的 `kind` 取形：`tool_call` 的参数
此时已解析进 `data` 的 `arguments`，`tool_result` 的结果值与 `media` 的类型标签也都
在 `data` 里；逐类的形状见「消息与块」的块定义表。用户拒绝的工具调用表现为一个
`tool_result` 块，其 `data` 是 `{ refused: true, reason }`。

**块内容在落库前不经过 Mod。** 定稿发生在内存累加器里：`candidate` 应用最终内容并
完成 `tool_call` 的参数归一化，内容确定后才写库，写库成功后才派发
`core/block.committed`。派生任务消息是例外：它的块直接构造，不进入累加器，随派生
事务一起落库，事务提交后才派发 `core/block.open` 与 `core/block.committed`。
`core/block.commit` 已由 2026-10-24 的块事务设计删除——块定稿
不再是一次 `intercept`，因此这一族只有 `observe` 事件。

**本组不包含 `core/message.open`，也不包含任何 `turn` 级事件。**

| 不含的开合事件 | 替代来源 |
| --- | --- |
| `turn` 开始 | 由该轮首条消息的块落库体现，见 `core/block.committed` |
| `turn` 结束 | 由实例进入静息相位体现，见 `core/agent.status` |
| `message` 开始 | 由 `core/block.open` 体现。该事件携带 `of_message` 与 `role`，订阅方可据此按需建立分组 |

`core/message.seal` 不可省略，理由有二：

1. 该事件的载荷包含 `finish_reason` 与 `usage`，二者是模型输出的组成部分，无法从
   其他事件取得。
2. 该事件是流式状态结束的唯一显式信号。若省略，订阅方只能依据实例相位推断；而工具
   执行期间实例同样处于运行相位，推断将产生错误结果。

本组事件在形态上不对称，此为刻意设计：开始状态可由内容事件推导，结束状态不可。

**未提交块的基线不走事件，走帧。** 见「帧与事件」。

**投递类别例外**：`core/block.delta` 为 `observe` 形态，但**不落日志**。理由见
「消费者与投递类别」。

### 7.9 Mod 生命周期

| 事件 | 形态 | 唯一入口 | 载荷 |
| --- | --- | --- | --- |
| `core/mod.install` | observe | Mod 安装器 | `{ name, version, manifest }` |
| `core/mod.installed` | observe | Mod 安装器 | `{ name, version, manifest, ui_entry? }` |
| `core/mod.update` | observe | Mod 安装器 | `{ name, from_version, to_version }` |
| `core/mod.updated` | observe | Mod 安装器 | `{ name, from_version, to_version, ui_entry? }` |
| `core/mod.delete` / `.deleted` | observe | Mod 安装器 | `{ name, version }` |

`ui_entry` 是前端组件入口的地址。**`core/mod.installed` 与 `core/mod.updated` 带它**：
地址由安装器在包落地后算出，Mod 没有 `ui/` 组件时该字段不出现。前端在安装与更新两条
路径上都要拿到它——组件地址不在清单里，只能由宿主给出。`core/mod.install` 与
`core/mod.update` 不带它：它们描述操作本身，落地的包由对应的完成事件描述。

`manifest` 是清单的规范化形式，缺省字段已被补齐，与包内那份原文只在缺省字段上不同形。

`core/mod.update` 对应发布新版本。它与 `.install` 是两个不同的操作：安装是首次引入，
更新是替换已有版本的代码。二者都不触发数据迁移（见 `docs/manifest.schema.json` 的
`version` 字段）。

**本组事件在任何情况下都不是 `intercept`。**

理由：Mod 的启用、禁用、删除与因超时触发的自动禁用均属不可干预的内核操作，
它们没有 `intercept` 入口，因此不违反"系统不得被 Mod 锁定"这一约束。

`core/mod.delete` 的用途是使其他 Mod 有机会接管被删除 Mod 遗留的外部资源。

### 7.10 Mod 通知

| 事件 | 形态 | 唯一入口 | 载荷 |
|---|---|---|---|
| `core/mod.notify` | observe | 门面方法 `ctx.notify` | `{ level, text }` |

**本事件提供的是统一的用户提醒通道。**

它与 `ctx.emit` 产生的事件是两件事，区别不在数据结构而在**消费者构成**：

| | `m_<mod>/...`（`ctx.emit`） | `core/mod.notify` |
| --- | --- | --- |
| 前端是否需要知道具体 Mod | **需要**（按名订阅） | **不需要**（订阅一次即可处理全部 Mod 的提醒） |
| 宿主能否统一控制 | 否 | 是 |
| 是否用于打断用户 | 否 | **是** |

`ctx.emit` 的载荷是某个面板的数据，不应用于提醒用户。`ctx.notify` 用于提醒，因此宿主
必须能够对它实施统一控制——限流、去重、降级、统一呈现。

**该事件不落库。** 需要长期可见的信息应由 Mod 写入状态（`ctx.state`），提醒只承担
"请现在看一眼"的作用。这样提醒丢失不导致信息丢失。

**载荷不带可执行动作。** 需要动作时，动作应放在该 Mod 自己的面板中。

**该事件的投递受「闭环防护」中的每 Mod 速率上限约束。**

### 7.11 总线自身

| 事件 | 形态 | 唯一入口 | 载荷 |
|---|---|---|---|
| `core/dispatch.timeout` | observe | 一次投递超过该订阅的时间预算 | `{ event, event_seq, target, duration_ms }` |
| `core/dispatch.failed` | observe | 一次派发失败 | `{ event, failure, target?, event_seq?, reason }` |
| `core/dispatch.delivered` | observe | 派发器。每次投递产生一条 | `{ event_seq, target, outcome, duration_ms, error? }` |

`core/dispatch.timeout` 在超时发生时立即产生，`event` 是被超时的订阅事件名，
`event_seq` 是那次事件的序号（同时作为本事件的 `cause`），`target` 是超时的订阅方。
它不受投递追踪开关控制，始终派发给内置观察者；`core/dispatch.*` 自身不产生投递记录。

`core/dispatch.failed` 在派发失败时立即产生，并始终派发给内置观察者，因此前端可见。
`failure` 取 `bad_rewrite` / `handler_error` / `host_error` / `too_deep` /
`too_many_events` / `bad_payload` / `wrong_shape` / `not_in_directory`。
`target` 只在失败归属于某个订阅方时出现；`event_seq` 在序号尚未分配时缺席。
派发失败不会让 `decide` 或 `notify` 中止：`decide` 返回当前累积载荷，`notify` 丢弃
这次派发，操作继续。

```
target:  { kind: 'mod'; name: string } | { kind: 'observer'; name: string }
outcome: 'pass' | 'rewrite' | 'observed' | 'skipped' | 'timeout' | 'error'
```

本事件是"一次投递"的记录。投递不是事件的一部分——一条事件可以产生零次、一次或多
次投递，因此投递记录作为独立事件产生。完整语义见「投递追踪（默认关闭）」。

**默认不产生。** 由「默认开关的取值原则」控制。开启后每次投递产生一条。

**本事件自身不产生投递记录。** 否则订阅它的消费者将引发递归。该规则覆盖整个
`core/dispatch.*` 名字空间，不限于本事件。

**不适用草稿例外。** 它与其余事件一样进入日志（见「事件日志与历史读取」）。默认关闭
时它不产生记录；开启后的数据量见「投递追踪（默认关闭）」。

---

## 8. 改写留痕

每次 `intercept` 事件的载荷被改写时，宿主产生一条 `observe` 记录：

| 事件 | 载荷 |
|---|---|
| `core/<被改写的事件名>.rewritten` | `{ target_id, mod_name, from, to }`，或等价的改动摘要 |

缺少该项记录时，被改写内容的来源不可追溯。改写本身必须作为事件被记录。

---

## 9. 闭环防护

Mod 代码可能构造事件环。宿主实施三项限制：

| 限制 | 阈值 | 落点 |
| --- | --- | --- |
| 起因链深度 | 8 | 派发入口检查 |
| 单次操作派发的事件总数 | 1000 | 附加在派发上下文上的计数器，跨嵌套累加 |
| 单 Mod 的事件产生速率 | 按时间窗口分桶 | 派发器 |

三项限制全部在派发器中实施。Mod 不持有事件总线句柄，无法绕过。

速率上限对**门面方法产生的 `core/` 事件**同样生效，其中 `core/input.draft` 与
`core/mod.notify` 需要单独关注：前者改写用户输入区域，后者打断用户。二者都是 Mod
可以直接作用于用户体验的通道，因此逐 Mod 计数，不与其他事件合并计算。

超出限制的事件被丢弃并记录告警。`observe` 事件允许丢弃；`intercept` 事件超出限制
时跳过该订阅者。

---

## 10. 覆盖判据

判定某子系统是否已完成事件化，使用以下三项检验。

### 10.1 唯一入口

**每个子系统必须存在唯一的变更入口，所有状态变化必须经过该入口。**

| 实现强度 | 结构 | 可否遗漏 |
| --- | --- | --- |
| 弱 | 状态变更后另行调用派发 | 可遗漏，且遗漏不产生任何可见信号 |
| 强 | 状态变更经由派发路径内部完成 | 不可遗漏 |

强实现下，遗漏会表现为编译错误或加载期错误，而非运行期的静默丢失。

### 10.2 三项检验

1. **调用路径检验**：能否检索到绕过唯一入口直接访问存储层的代码。能检索到即不满足。
2. **摘除检验**：移除事件派发后，系统应无法运行。若仅"缺少 Mod 功能"而系统行为不变，
   则该子系统的事件化属于弱实现。
3. **入口一致性检验**：所有调用入口（用户界面、agent 工具、内部任务）必须经过同一
   唯一入口。判据不是"每个入口都有对应事件"，而是"不存在第二条路径"。
