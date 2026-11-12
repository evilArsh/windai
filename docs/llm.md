# LLM 回合

`wind_core::llm` 把一次 agent 运行翻译成若干轮模型调用：组装上下文、构造请求、读取流式
响应、把响应拆成块、执行工具、写入领域表，并在每个阶段发出事件。一轮指一次模型调用
及其后续的工具执行；一次运行可以包含多轮。

本文描述 `LlmRunner` 的完整路径、块的拆分与合并规则、工具目录与工具执行。树如何调度
`LlmRunner` 见「Agent 树」；适配器与 SSE 见「AI」；请求改写规则见「规则」；数据库写入见
「存储」；事件目录见「事件」。

## 入口

```rust
pub struct LlmRunner {
    pub(crate) bus: Arc<Dispatcher>,
    pub(crate) storage: Storage,
    pub(crate) max_rounds: u32,
    pub(crate) mod_service: Arc<dyn ModService>,
    pub(crate) mcp_service: Arc<dyn McpService>,
}

impl Runner<LlmDomain> for LlmRunner { /* ... */ }
```

`max_rounds` 默认 `DEFAULT_MAX_ROUNDS = 40`。每次运行的预算由 `round_cx` 给出：

```rust
DispatchCx::root(
    Origin::Agent,
    Scope::Topic { topic_id, instance_id },
    Source::core("llm"),
    Duration::from_secs(120),
)
```

每轮重新建立 `DispatchCx`，因此 120 秒是单轮预算，不是整次运行的预算。

## 输入

`Runner::run` 收到 `Input<LlmDomain>`，先与取消令牌竞速；取消时返回
`Outcome::Failed(LlmFailure { error: "run cancelled" })`。其余输入映射为：

| `Input` | 行为 |
| --- | --- |
| `Fresh(spec)` | 一条任务单；`message_id` 与 `turn_id` 来自任务单 |
| `Children` | 以 `reentry = true` 进入，只有 `instance_id`；用于子实例结束后回到父节点 |
| `External(answer)` | 走恢复路径，见「恢复」 |
| `Idle(specs)` | 多条待消费输入合成一条用户消息 |

## 一次运行的主干

```text
serve(instance_id, specs, message_id, turn_id, reentry, cancel)
  ├─ topic = 实例所在话题
  ├─ cx = round_cx(topic, instance)
  ├─ compose_pending(specs)           把 agent_pending 合成为用户消息
  │    有写入 → 逐块发 core/block.committed（Live）
  ├─ report_spawns(instance_id)       回补子实例的工具结果
  │    有写入 → 逐块发 core/block.committed（Live）
  ├─ contexts::assemble(instance_id)  读取并规范化全部历史块
  ├─ 确定 turn_id
  └─ rounds(topic, instance, turn_id, contexts, None, cancel)
```

`turn_id` 的选择顺序：

| 条件 | 取值 |
| --- | --- |
| 显式给出 `turn_id` | 用它 |
| 给出 `message_id` | `get_turn_of(message_id)`；找不到则失败 |
| `compose_pending` 新建了用户消息 | 新 turn 的 id |
| `reentry` 且 `report_spawns` 产生了结果 | 结果所属的最大 turn id |
| 其余 | 新建 turn |

### 合成待消费输入

`compose_pending` 读取 `LlmSpec.pending_id` 指向的 `agent_pending` 行，过滤出属于当前实例的
行，把每行的 `payload`（`Vec<Content>`）顺序拼接成一条 `Role::User` 消息。

一个写事务内完成：

```text
INSERT turn
INSERT message(role = user)
INSERT block × N
DELETE agent_pending WHERE id IN (...)
COMMIT
```

返回 `WrittenBlocks { message_id, blocks, block_ids }` 与 `ComposedInput { turn_id, message_id }`。
待消费行在事务提交后才算消费；没有匹配行时直接返回，不建 turn。

### 回补子实例结果

`report_spawns` 找出当前实例所有还没有对应 `tool_result` 的 `wind_spawn_agent` 调用，
通过 `spawn_calls` 读取子实例状态，把每个调用的结论写成 `tool_result` 块。

一个写事务内，按 `(turn_id, message_id)` 分组：

```text
INSERT message(role = tool)
INSERT block × N（每块一个 tool_result）
```

结果内容按子实例相位：

| 子实例相位 | `tool_result.content` |
| --- | --- |
| `Done(reply)` | 子实例当前消息的全部 `text` 块按 `seq` 拼接成的字符串 |
| `Suspended(wait)` | `{ "instance_id", "phase": "suspended", "phase_payload": wait }` |
| `Running` / `WaitingChildren` | `{ "instance_id", "phase": "running" }` |
| `Failed` / `Cancelled` / `Aborted` | `{ "error": "child task did not complete" }` |
| 无子实例 | `{ "error": "spawn call <id> has no child" }` |
| 相位无法解析 | `{ "error": "child <id> has an unreadable phase: ..." }` |

后台子实例（`mode = background`）即使在 `report_spawns` 时已进入终态，也只返回
`{ "instance_id", "phase": "running" }`：父实例不等它的结果。
后台实例终止后由 `wind-runtime` 的终态观察者把结果写入父实例的待消费输入
（`kind = background_result`），并在同一事务里写 `spawn_calls.result_delivered_at`；
话题会话首次建立时还会扫描未交付的后台终态结果。父实例随后以一次普通输入消费该结果。

后台结果的文本：子实例相位为 `done` 且回复非空时取回复文本，其余情况为
`background child <id> ended with phase <phase>`。

### 组装上下文

`contexts::assemble` 读取 `list_instance_context` 的全部块，按 `message_id` 分组为
`Message`，再做四步规范化：

1. 丢弃没有任何内容的 `Message`。有工具调用、非空推理、非空文本、媒体或函数调用结果
   之一即视为有内容。
2. 丢弃开头的连续 `assistant` 消息。开头是助手消息通常说明上一次运行中断在半途。
3. 修复工具配对：
   - 助手消息的 `tool_calls` 建立期望的 `call_id` 集合。
   - 工具消息只保留期望集合中的 `FunctionCall`，其余内容原样保留；集合为空时整条工具消息丢弃。
   - 期望集合在遇到下一条非工具消息时若仍非空，插入一条合成的工具消息，内容为
     `{ "error": "the call did not complete" }`。
   - 走到结尾仍有未配对调用时，同样补一条合成工具消息。
4. 合并相邻的同类消息：
   - 连续 `user` 消息合并内容。
   - 连续 `assistant` 消息在没有工具调用时合并内容与推理；`reasoning_item_id` 取第一个
     非空值；用量取逐字段最大值。

规范化只影响发给模型的上下文，不修改数据库。

## 准备阶段

`turn::prepare` 在每次 `rounds` 调用开始时执行一次：

```text
model_id = topic.model_id                      话题必须指定模型
ModelConfig = get_model_config(model_id)       模型、提供商、第一条启用凭证
provider                                      必须存在
rules = compile_rules(list_rules(provider.id, adapter))
fragments = mod_service.prompt_fragments(topic.id)
prompt = decide(core/prompt.collect, fragments).fragments.join("\n\n")
from_mcp  = mcp_service.list_tools(topic.id)
catalog = ToolCatalog::assemble(from_mcp)
```

`prompt` 的来源是话题启用的 mod 的 `prompts/` 文件。`ResourceService` 按 mod 名升序返回
片段，`Resources` 在单个 mod 内按文件名升序读取 `.md`、`.txt`、`.html`。

`Prepared` 保存适配器、模型、提供商、目标地址与凭据、规则集、提示词与工具目录。`rounds`
在进入循环前把提示词作为一条 `Role::System` 消息插入上下文首位。

`core/prompt.collect` 是 `intercept` 事件，载荷是 `PromptCollect { topic_id, instance_id,
fragments }`。mod 可以替换片段列表；返回的列表按顺序用两个换行连接。

## 模型回合

```text
loop:
    rounds += 1
    rounds > max_rounds → 失败
    message_id = INSERT message(role = assistant, model_id)
    stream = MessageStream::new(message_id, bus, cancel, cx)

    body = decide_without_payload(
        core/llm.request,
        LlmRequest { contexts, tools: Some(catalog.declarations()) },
    )
    body = adapter.build_request(model, body.contexts, body.tools)

    apply_rules(rules, body, EvalContext { provider, model })
    body = decide_without_payload(
        core/rule.apply,
        RuleApply { adapter, body },
    ).body

    absorber = exchange(prepared, body, |block| stream.delta(block))
    usage = absorber.usage()
    finish_reason = absorber.finish_reason()
    contexts.push(absorber.compose())
    blocks = absorber.settle()
    calls = blocks 中的 tool_call

    stream.complete(storage, blocks, finish_reason, usage)

    calls 为空 → Outcome::Completed
    calls → decide(core/llm.toolcall, ToolCalls { ... })
            suspend == true → 创建 external_wait，Outcome::Suspend
            过滤改写后的调用
            execute_plan
            写入 tool 消息并发出块事件
            有 spawn → Outcome::Spawn
            没有工具结果且没有 spawn → Outcome::Completed
```

`core/llm.request` 与 `core/rule.apply` 使用 `decide_without_payload`：mod 收到完整载荷，
但事件信封的 `payload` 为 `NULL`，因此不把整份上下文或请求体写入 `event_log`。

`core/llm.toolcall` 使用 `decide`，完整载荷写入 `event_log`。载荷是
`ToolCalls { instance_id, message_id, calls, suspend: Option<bool> }`。mod 返回
`suspend = true` 时，本批调用不执行，宿主创建一行 `external_waits` 并结束本次运行：

```rust
create_external_wait(instance_id, message_id, json!(original_calls))
```

等待项保存的是**改写前**的调用列表。

## 流式响应与块累加

`turn::exchange` 根据 `model.config.stream` 选择路径：

- `stream = true`：`wind_ai::chat::stream_body`，逐事件解析。
- 其余：`wind_ai::chat::once_body`，把完整消息包装成一次 `Chunk::Delta`，随后 `Chunk::Done`。

处理规则：

```text
Chunk::Done        → finish_reason = "stop"，结束
Chunk::Delta(msg)  → absorber.absorb(msg)，每个变更块调用 emit_delta
Err(provider)      → finish_reason = 错误文本；把错误文本作为 assistant 文本吸收；结束
结束时 finish_reason 为空 → "stop"
```

提供商错误不会让本次运行失败；错误文本成为助手消息的一段文本，结束原因记录为错误文本。
只有存储错误、适配器错误、工具执行错误等才会以 `LlmFailure` 结束。

`BlockAbsorb` 是块的唯一累加器：

```rust
pub struct BlockAbsorb {
    blocks: Vec<Block>,
    seq: i64,
    input_tokens: i32,
    output_tokens: i32,
    created_at: i64,
    finish_reason: String,
}
```

`absorb(partial)` 把一条 `Message` 拆成 `Body` 列表，逐个并入已有块或新开块，返回本次的
增量块。`compose()` 把当前块组装成完整 `Message`；`settle()` 取走块并把序号与用量归零；
`usage()` 与 `finish_reason()` 给出本次往返的结束信息。

### 拆分规则

一条 `Message` 按以下顺序拆成 `Body`：

1. `reasoning_content` 非空 → `Reasoning { item_id: reasoning_item_id, content }`。
2. `content` 中每个非空项按类型转换：文本、图片、文件、音频；`FunctionCall` 转成
   `ToolResult`。
3. `tool_calls` 中每个调用转成 `ToolCall`。

空文本、空图片、空文件、空音频被跳过；空的工具参数不跳过。`FunctionCall` 即使
`content` 为 `null` 也保留，作为一次工具结果。

### 落位规则

| `Body` | 落到哪个块 | 追加方式 |
| --- | --- | --- |
| `Reasoning` | 末尾块若是 `Reasoning` 则并入，否则新开 | 追加文本；非空片段使 `revision + 1` |
| `Text` | 末尾块若是 `Text` 则并入，否则新开 | 追加文本；非空片段使 `revision + 1` |
| `Audio` | 先按 `item_id` 找同 id 块；找不到则看末尾是否为兼容的音频块；否则新开 | 追加内容；非空片段使 `revision + 1`；首次有 `item_id` 时补上 |
| `ToolCall` | 有 `item_id` 按 id 找；否则按非空 `call_id` 找；都没有则找最后一个 `ToolCall`；否则新开 | 追加参数；非空片段使 `revision + 1`；补空字段 |
| `Image` / `File` | 总是新开 | 不追加 |
| `ToolResult` | 总是新开 | 不追加 |

新开块时分配 `seq`，从 0 开始递增。构造函数给出的初始 `revision`：

- `Reasoning`、`Text`、`ToolCall`：0。
- `Image`、`File`、`Audio`、`ToolResult`：1。

`absorb` 返回的增量块带有目标块的 `role`、`seq` 与追加后的 `revision`，但 `body` 只含本次
片段，不是累计值。前端必须按 `seq` 定位块并追加，不能把增量块当成完整块。

## 完成与事件

`MessageStream::complete` 先在一个事务里写入全部块并定稿消息：

```text
INSERT block × N
UPDATE message SET finish_reason, input_tokens, output_tokens
```

提交成功后：

1. 为每个块发 `core/block.committed`，`block` 字段为空，只带 `block_id` 与 `seq`。
2. 发 `core/message.seal`，带结束原因与用量。

两步都使用 `Retention::Group` / `Close(Message)`，不写 `event_log`。`Close` 会释放该消息
在 feed 保留窗口里的全部增量与实时块。

提交失败时：

- `MessageStream` 标记为终态，`Drop` 不再发中断事件。
- 本次运行返回 `LlmFailure`。

未完成消息由 `MessageStream::Drop` 发 `core/message.interrupted`：

| 条件 | `reason` |
| --- | --- |
| 取消令牌已取消 | `cancelled` |
| 其余 | `aborted` |
| 显式调用 `interrupted(Failed)` | `failed` |

`interrupted(Failed)` 在 `exchange` 返回错误时调用。

## 工具执行

### 工具目录

```rust
pub enum ToolOrigin {
    Agent,
    Mcp,
}
```

模型可见名字：

| 来源 | 名字 |
| --- | --- |
| Agent | `wind_spawn_agent` |
| MCP | `<server>0m0<tool>` |

没有 `ModRequest` 来源。清单 `requests` 只做结构校验，当前不会出现在工具目录中，也没有
调用入口。

`ToolCatalog::assemble(from_mcp)` 先放入 MCP 工具；同名工具出现两次即返回
`CoreError::ToolNameConflict`。最后追加内置的 `wind_spawn_agent`；若同名已存在，则保留已存在
的那一项。

`wind_spawn_agent` 的参数：

```rust
pub struct SpawnAgentSchema {
    pub mode: AgentMode,   // sync | background
    pub task: String,
    pub fork: bool,        // 当前被解析但不使用
}
```

`fork` 不参与子实例创建：子实例总是只收到一条包含 `task` 的用户消息，不继承父实例上下文。

### 改写与过滤

`core/llm.toolcall` 返回后，`filter_calls` 按索引对齐原始调用与改写结果：

- 同一索引上的 `call_id` 与 `name` 必须都相同，才采用改写后的调用。
- 其余情况保留原始调用。

因此 mod 可以改写参数，不能借这个事件增删调用或改变顺序。`suspend = true` 时本批调用整体
不执行；等待项保存的是改写前的调用。

### 执行

```text
execute_plan(calls, catalog, cx, mcp_service)
  Agent 调用        → 收集，稍后解析为 SpawnAgentAction
  MCP 调用          → 参数必须是 JSON 对象；加入同一批
  未知工具          → { "error": "unknown tool <name>" }
  MCP 批            → mcp_service.tool_calls(deadline, cancel, params)
```

一批 MCP 调用只发一次 `McpService::tool_calls`。返回的 `FunctionCallOutput` 按 `call_id` 回填；
某个调用没有对应输出时写入 `{ "error": "the call produced no result" }`。整批失败时记录警告，
每个调用都得到 error 结果，本轮运行继续。参数不是 JSON 对象、工具名无法拆出服务名的调用
不进入批次，直接得到 error 结果。

`ResourceService` 是 `McpService` 的实现：

- `list_tools(topic_id)` 首次调用时发布话题服务，然后读取主引用与内建 `wd_fs_<topic>`、
  `wd_skills_<topic>`、`wd_workspace_<topic>` 引用下的工具，按 `(server, tool)` 去重。
- `tool_call` 与 `tool_calls` 按服务名、工具名查找当前 `Running` 的连接，并传
  `cx.deadline()` 与 `cx.cancel()`。
- `ModService::prompt_fragments` 返回话题启用 mod 的提示词片段；`version_of` 返回当前版本。
  `ModService` 没有工具目录或调用方法。

### 工具结果

每次执行计划中产生的结果写入一条 `Role::Tool` 消息，每个结果一块 `ToolResult`：

```text
INSERT message(role = tool)
INSERT block × N
```

写入后逐块发 `core/block.committed`（`block` 有值，`Retention::Live`）。工具结果消息与
产生调用的助手消息由 `call_id` 关联，不与工具结果消息共享消息 id。

### 派生

`create_spawn_requests` 对每个 `SpawnAgentAction` 在一个事务里：

```text
INSERT agent_instances(parent_id = 当前实例, role = child, mode, phase = running)
INSERT spawn_calls(parent_id, call_id, child_id)
INSERT turn
INSERT message(role = user, model_id = topic.model_id)
INSERT block × N（任务文本）
```

返回的 `SpawnRequest` 带子实例 id 与 `LlmSpec { instance_id: child, message_id, turn_id }`。
`Outcome::Spawn` 交给树处理；树插入子节点并立即启动。

## 恢复

`Input::External(answer)` 走 `resume`：

```text
写事务:
  读 external_wait；不存在 → 失败
  读该等待消息所属的 turn；不存在或不属于当前实例 → 失败
  等待已答复 → 失败
  answer_external_wait(id, decisions)；未写入 → 失败
解析等待项里保存的原始调用
prepare（模型、规则、提示词、工具目录）
按 decisions 过滤：
  allow = false → 生成拒绝结果
  allow = true 或没有表态 → 执行
合并结果：
  原始调用的顺序就是结果顺序
  执行结果中找不到且不是 spawn → { "error": "the call produced no result" }
写入工具结果
有 spawn → Outcome::Spawn
否则 contexts::assemble，进入 rounds（复用已准备的材料）
```

未表态的调用默认放行。拒绝结果的内容是
`{ "refused": true, "reason": "the user did not allow this call" }`。等待项保存的是改写前的
调用，因此 mod 在 `core/llm.toolcall` 中改写过的参数不会进入审批载荷。

## 事件与保留策略

| 事件 | 形态 | `log` | 保留策略 | 载荷 |
| --- | --- | --- | --- | --- |
| `core/prompt.collect` | intercept | 是 | `Journal` | `PromptCollect`，信封携带载荷 |
| `core/llm.request` | intercept | 是 | `Journal` | `LlmRequest`，信封不携带载荷 |
| `core/rule.apply` | intercept | 是 | `Journal` | `RuleApply`，信封不携带载荷 |
| `core/llm.toolcall` | intercept | 是 | `Journal` | `ToolCalls`，信封携带载荷 |
| `core/block.delta` | observe | 否 | `Group(Message)` | `BlockDelta { message_id, ...Block }` |
| `core/block.committed` | observe | 否 | `Group(Message)` 或 `Live` | `BlockCommitted { message_id, seq, block_id, block? }` |
| `core/message.seal` | observe | 否 | `Close(Message)` | `MessageSeal { message_id, finish_reason, usage }` |
| `core/message.interrupted` | observe | 否 | `Close(Message)` | `MessageInterrupted { message_id, reason }` |

`BlockCommitted.block` 的两种取值：

- 实时流式消息：`None`。前端已经从 `block.delta` 收到内容。
- 非实时消息（用户输入、工具结果、子实例任务）：`Some(Block)`。前端直接建块，不等待增量。

## 当前边界

| 项 | 现状 |
| --- | --- |
| Mod request 工具 | 没有 `ToolOrigin::ModRequest`；`requests` 不进入工具目录，也没有调用入口 |
| 脚本 HostApi | 只有 `emit`、`notify` 与本地 `log`；`state`、`storage`、`http`、`agent`、`fs` 未实现 |
| 审批载荷 | 保存的是改写前的调用；mod 改写后的参数不会出现在等待项里 |
| `fork` | 已解析，未使用 |
| 后台结果 | 父实例经由 `agent_pending` 的 `background_result` 行消费；后台子实例的终态不以 `tool_result` 回写 |
| `core/input.accepted` | 由 `Runtime::submit` 发出，不携带用户内容 |
| 非流式响应 | 走一次完整 `Delta`，随后 `core/block.committed` 的 `block` 仍为 `None` |
| 提供商错误 | 作为助手文本写入，不以 `LlmFailure` 结束 |
| 取消 | `Runner::run` 与 `MessageStream` 都观察取消令牌；取消产生 `cancelled` 中断事件 |
| `suspend` | 只有 `Some(true)` 触发挂起；`Some(false)` 与 `None` 都按未挂起处理 |
