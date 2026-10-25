# windai 事件流线格式

前端是独立项目，读不到宿主源码。本文档与 `docs/events.md`、`docs/facade.d.ts`、
`docs/manifest.schema.json` 同级，是同一套契约的一部分：任何消费帧流的端据此实现
事件流，宿主据此断言自己发出的东西。改动其中一份时须检查其余各份是否需要同步。

本文档只回答一个问题：**事件以什么形状、按什么顺序到达前端**。事件自身的语义
（信封、形态、目录、日志、可见性）见 `docs/events.md`；帧里的 `name` 指向哪里、
`observe` 与 `intercept` 的区别、哪些事件属于目录，本文档不重复定义。

本文档描述的是已经落地的行为。传输端点本身不在其中，见「不在本文档里的东西」。

---

## 1. 帧的两类

前端流上只有两类消息：

| 类别 | 何时发送 | 收件人 | 是否带序号 |
| --- | --- | --- | --- |
| **快照帧** | 订阅建立时发送（零到多帧） | 仅该订阅 | 否 |
| **事件帧** | 事件产生时 | 按作用域 | 是 |

**快照帧不是事件。** 它不是「发生了什么」的陈述，而是一次握手的内容。由该区分
得到四条结果：它不分配序号；不参与起因链；不属于事件目录；不使用 `core/` 这类
事件名前缀（它没有名字）。它只属于建立它的那个订阅，不会被转发给任何其他订阅方。

**事件帧是一条完整的事件信封。** 回补出来的历史帧与实时帧是同一形状的事件帧，客户端
不需要区分二者，也不需要为历史实现第二套处理逻辑。二者的边界见「历史回补的边界」。

```json
{ "kind": "snapshot", "baseline": { "baseline": "blocks", "blocks": [] } }
```

```json
{
  "kind": "event",
  "envelope": {
    "seq": 41,
    "time": 1760000000000,
    "name": "core/block.open",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": { "block_id": "5127", "of_message": 2, "role": "assistant", "kind": "text" }
  }
}
```

---

## 2. 帧的线上形状

帧的最外层判别式是 `kind`，取值只有 `snapshot` 与 `event` 两种。**其余取值不是
本协议的一部分**：解析到未知的 `kind` 是协议错误，不是可以静默跳过的输入。

### 快照帧

快照帧的形状是 `{"kind": "snapshot", "baseline": {...}}`。`baseline` 自身带判别式
`baseline`，取值只有两种：

- `"blocks"`：携带订阅建立时刻全部未提交的块。
- `"stateful"`：携带某个状态键的当前值。

```json
{
  "kind": "snapshot",
  "baseline": {
    "baseline": "blocks",
    "blocks": [
      {
        "id": "5127",
        "of_message": 2,
        "role": "assistant",
        "kind": "text",
        "revision": 17,
        "content": "你好呀",
        "finished": false
      }
    ]
  }
}
```

```json
{
  "kind": "snapshot",
  "baseline": {
    "baseline": "stateful",
    "key": "items",
    "value": [1, 2]
  }
}
```

块各字段的含义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | string | **块标识。是字符串，不是数字**，见下 |
| `of_message` | integer | 所属消息 |
| `role` | string | `system` / `user` / `assistant` / `tool` / `developer` |
| `kind` | string | `text` / `reasoning` / `tool_call` / `tool_result` / `media` |
| `call_id` | string | 工具调用配对键。仅 `tool_call` 与 `tool_result` 两类具有，其余块不出现该字段 |
| `revision` | integer | 被追加到第几版，等于已追加的片段数 |
| `content` | string | 文本内容。为空时不出现该字段，客户端按空串处理 |
| `data` | 任意 | 结构化载荷。没有时不出现该字段。取值随 `kind` 而定，见下 |
| `finished` | boolean | 是否已提交。已提交的块不再修改 |

**`data` 的取值随 `kind` 而定。** 前端是独立项目，读不到宿主源码，没有这张表就画不出
工具调用与拒绝结果。下表的形状同时适用于快照里的块与 `core/block.committed` 事件的
载荷（后者的块标识字段叫 `block_id`）：

| `kind` | `content` | `data` |
| --- | --- | --- |
| `text` | 正文文本 | 不出现 |
| `reasoning` | 推理文本 | 不出现 |
| `tool_call` | 参数原文；参数解析进载荷之后为空，解析失败时保留原文 | 工具名与解析后的参数：`{ "name": "...", "arguments": <解析后的对象> }`。未提交的块上通常只有 `{ "name": ... }`，参数原文还在 `content` 里；解析失败时 `arguments` 不出现；工具名未知时 `name` 不出现 |
| `tool_result` | 不出现 | 工具返回的完整值，原样一个 JSON 值；本次调用被拒绝时是 `{ "refused": true, "reason": "..." }` |
| `media` | 媒体载荷 | `{ "mime": "<类型标签>" }`。标签是 `image` 或 `file`，音频则是它的编码格式 |

**块标识是字符串。** 它不是「数字被写成字符串」，而是标识本身就是一个十进制字符串：
它的取值范围超过 JavaScript 的安全整数范围，用数字承载会在前端静默丢精度。块标识
全局唯一、单调递增，是合并、补发与去重的锚点。

### 事件帧

事件帧的形状是 `{"kind": "event", "envelope": {...}}`。信封的字段与
`docs/events.md` 的「事件信封」一致：

| 字段 | 说明 |
| --- | --- |
| `seq` | 事件的全局单调递增序号，裸数字 |
| `time` | 产生时刻，epoch 毫秒 |
| `name` | 事件名，带 `core/` 或 `m_<mod>/` 前缀 |
| `scope` | 触发这次操作的作用域：`{"kind":"topic","topic_id":N}` 或 `{"kind":"global"}` |
| `origin` | `agent` / `ui` / `internal` |
| `source` | `{"kind":"core","subsystem":"..."}` 或 `{"kind":"mod","name":"...","version":N}` |
| `cause` | 起因事件的序号。顶层操作没有起因，此时整个字段不出现 |
| `payload` | 载荷，内容随事件名而定 |

```json
{
  "kind": "event",
  "envelope": {
    "seq": 42,
    "time": 1760000000000,
    "name": "core/block.committed",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "cause": 41,
    "payload": {
      "block_id": "5119",
      "kind": "text",
      "content": "先前的回答",
      "revision": 4
    }
  }
}
```

本单元产出的事件名与载荷见 `docs/events.md` 的「事件目录」。其中与块流直接相关的
三条是 `core/block.open`、`core/block.delta` 与 `core/block.committed`，三条都会
出现在本流上。

---

## 3. 握手的请求参数

建立订阅是一次握手。请求参数如下：

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `scope` | — | 作用域。**由端点路径决定，不在帧里，也不在请求体里** |
| `since_seq` | integer，可选 | 回补的起点。**不含**该序号 |
| `until_seq` | integer，可选 | 回补的终点。**含**该序号 |
| `blocks` | boolean，可选 | 是否随握手给出未提交块的快照 |

**参数名一律下划线。** 信封与信封内的全部字段，以及这里的请求参数，全部是蛇形：
`since_seq` 不写成 `sinceSeq`，`until_seq` 不写成 `untilSeq`，块里的 `of_message`
不写成 `ofMessage`。整个协议没有一处驼峰。

```json
{ "since_seq": 40, "until_seq": 100, "blocks": true }
```

上述参数的含义：

- `since_seq` 省略时**不回补任何历史**：订阅只收到握手之后产生的事件。此时该连接
  也没有发出任何一次日志查询。
- `until_seq` 省略时补到「当前进度」为止，此后转入实时增量。它不能越过握手时刻的
  进度，细节见「历史回补的边界」。
- `blocks` 为真时握手给出未提交块的快照；省略或为假时不给块快照，首帧要么是回补出
  的历史事件帧（同时给了 `since_seq` 时），要么是实时事件帧。

---

## 4. 响应的顺序

握手之后，帧按以下顺序到达，三类之间不交错：

1. **历史事件帧**：按序号升序，一次回补的全部结果。
2. **快照帧**：零到多帧。块基线（若有）在最前，其后是各状态键的基线；状态键之间
   的先后不作规定。
3. **实时事件帧**：握手之后产生、作用域匹配的事件，按产生顺序。

**快照排在历史之后，而不是之前。** 历史按时间顺序填充过去，快照是此刻的状态，
紧跟实时增量。客户端因此可以顺序处理：先补历史，再用快照建立当前状态，最后消费
增量。

省略 `since_seq` 时没有第 1 段。**有快照时，首帧即快照帧**，且一次握手可能给出多帧
快照；既不回补又没有快照时，该连接的第一帧就是实时事件帧——`blocks` 省略或为假、
且没有任何状态键时正是如此。因此「快照是首帧」不是一条无条件的规定，条件的两种取值
见「握手的请求参数」。

**一个完整的时序例子。** 客户端在 1 号话题上以 `since_seq: 40` 握手，日志里已有
序号 41、42、43 三条记录，此刻有两个未提交的块（5127 已有内容，5131 刚开），握手
之后又产生了一条分片：

```json
{
  "kind": "event",
  "envelope": {
    "seq": 41,
    "time": 1760000000000,
    "name": "core/block.open",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": { "block_id": "5127", "of_message": 2, "role": "assistant", "kind": "text" }
  }
}
```

```json
{
  "kind": "event",
  "envelope": {
    "seq": 42,
    "time": 1760000000000,
    "name": "core/block.committed",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": { "block_id": "5119", "kind": "text", "content": "先前的回答", "revision": 4 }
  }
}
```

```json
{
  "kind": "event",
  "envelope": {
    "seq": 43,
    "time": 1760000000000,
    "name": "core/block.open",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": { "block_id": "5131", "of_message": 3, "role": "assistant", "kind": "text" }
  }
}
```

```json
{
  "kind": "snapshot",
  "baseline": {
    "baseline": "blocks",
    "blocks": [
      {
        "id": "5127",
        "of_message": 2,
        "role": "assistant",
        "kind": "text",
        "revision": 17,
        "content": "你好呀",
        "finished": false
      },
      {
        "id": "5131",
        "of_message": 3,
        "role": "assistant",
        "kind": "text",
        "revision": 0,
        "finished": false
      }
    ]
  }
}
```

```json
{
  "kind": "event",
  "envelope": {
    "seq": 44,
    "time": 1760000000100,
    "name": "core/block.delta",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": { "block_id": "5127", "revision": 18, "fragment": "！" }
  }
}
```

---

## 5. 历史回补的边界

**区间是左开右闭。** `since_seq` 不含，`until_seq` 含。回补只返回日志里存在、作用域
匹配、且序号落在该区间内的记录。

**上界取 `min(until_seq, 握手时刻的进度)`。** 握手时刻的进度是注册那一刻已产生的
最大序号，注册与进度在同一临界区内取得。因此：

- `until_seq` 省略时，回补覆盖到进度为止——这就是「追上当前进度后转入增量」，
  进度之后的帧由实时路径承担。
- `until_seq` 给得再大也不会越过进度；注册之后产生的事件不在回补里。

**单次回补的上限是 1000 条记录。** 该上限数的是与作用域匹配的日志**行**，数在
可见性过滤之前，因此一页可能给出少于 1000 帧，也可能一帧都不给。**客户端可用的
下一个游标是这一页最后一帧的 `seq`**：要接着读，就以该序号为 `since_seq` 重新握手。
客户端不得假定「一页必有 1000 帧」，也不得假定「页尾一定是某个固定的序号」。

**一页若一条可见帧都没有，客户端手上就没有可推进的游标。** 这是本单元的已知边界：
上限按行计，而过滤掉的行（`intercept` 形态）客户端看不见，因此它无法从这个位置自行
前进。今天事件目录里带 `intercept` 形态的事件（如 `core/mcp.tool.call`）都不以这个
密度出现，要连续占满一页并不现实。

```json
{
  "kind": "event",
  "envelope": {
    "seq": 1000,
    "time": 1760000000000,
    "name": "core/message.seal",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": {
      "message_id": 2,
      "finish_reason": "stop",
      "usage": { "input_tokens": 128, "output_tokens": 32 }
    }
  }
}
```

以上是以 `since_seq: 0` 起步、覆盖到序号 1000 的那一页的页尾；继续读的请求是：

```json
{ "since_seq": 1000, "blocks": true }
```

**序号是全局的，按作用域过滤后并不连续。** 话题订阅只收到该话题的记录，收不到
`global` 记录；要读 `global` 记录须建立全局订阅。日志里可以存在的序号空洞也不会
被填平——回补只给出日志里真正有的记录，不会为一个不存在的序号停下等待。

**回补与实时用同一条可见性判据。** 只有 `observe` 形态的事件进入前端流；`intercept`
形态的事件（例如 `core/mcp.tool.call`）两条路都不给。判据相同这一点是刻意的：否则
客户端重连一次就会看到实时流永远不给它的控制面事件，「回补帧与实时帧是同一形状」
就成了一句空话。

**`core/block.delta` 不会出现在回补里。** 它有实时帧，但不进日志（它是草稿，不是
事实，见 `docs/events.md` 的「草稿与事实」）。因此**重连的客户端不得期待草稿事件被
重放**：断线期间丢失的分片由握手给出的块快照补齐，靠的是快照里的
`content` 与 `revision`，不是靠回补。

**回补取不到就不建立订阅。** 握手不会退化成「给一段有缺口的历史」：日志读取失败时
该次订阅直接失败，客户端重新握手。

---

## 6. 客户端的合并规则

前端的块状态是一张按块标识索引的表。**四条规则覆盖全部块输入**（开块、快照、分片、
落库），第五条是事件水位：

1. **收到 `core/block.open` 时**：为这个块标识建立一条空状态，并记下它的身份——
   `of_message`、`role`、`kind`、`call_id`，内容为空串、`revision` 为 0、`finished`
   为假。一个块只开一次，因此这条规则不会覆盖已有的状态。
2. **收到块快照时**：快照是订阅建立那一刻**全部未提交块**的集合，因此它对于这些块
   是权威的。除了对快照里的每个块用它整体建立或替换状态（身份字段一并写入），还要
   **丢弃本地一切 `finished` 仍为假、而标识不在这次快照里的块**。被拒绝的块、已经在
   别处提交掉的块都由此消失，而不是留成一条永远「进行中」的幽灵。已提交的块
   （`finished` 为真）不在快照里，也不受这条丢弃影响。**块标识可以安全复用，依据
   正是这条规则**，而不是巧合。
3. **收到 `core/block.delta` 时**：若该帧的 `revision` 大于持有的 `revision`
   （尚未持有任何状态时按 0 计），把 `fragment` 追加到持有的内容后面，并把持有的
   `revision` 更新为帧里的值；否则整帧忽略。`finished` 不变。
4. **收到 `core/block.committed` 时**：该块已成事实，用帧的载荷取代持有的 `kind`、
   `call_id`、`content`、`data` 与 `revision`，并把 `finished` 置为 `true`；
   **载荷里未出现的可选字段按空处理**——`call_id` 与 `data` 清空，`content` 置为空串。

规则 1 是身份的来源之一，不可省：一个在连接内开块、又在连接内提交的块，从不出现在
任何快照里，它的 `of_message` 与 `role` 只有 `core/block.open` 给得出。提交帧的载荷
本身不带这两个字段，身份因此来自此前到达的 `core/block.open` 或块快照。若一个块在
本条连接建立之前就已开块、客户端又没有请求块快照，它的提交帧会早于任何身份信息到达：
此时客户端只能按 `block_id` 记下这条状态，身份在本流上不再补得。

`revision` 的回写是规则 3 的一半：只追加片段而不更新版本号，下一个分片会与刚应用
过的那个比较，比较结果失去意义。**分片丢失、分片重复、快照与分片到达顺序颠倒**这
三类情形在上述规则下都得到一致结果，客户端因此不需要与宿主对账。

**第五条规则是事件水位。** 客户端记下自己**已应用过的最大 `seq`**，并忽略任何
`seq` 不大于它的**事件帧**。

```json
{
  "kind": "event",
  "envelope": {
    "seq": 42,
    "time": 1760000000000,
    "name": "core/block.committed",
    "scope": { "kind": "topic", "topic_id": 1 },
    "origin": "agent",
    "source": { "kind": "core", "subsystem": "conversation" },
    "payload": { "block_id": "5119", "kind": "text", "content": "先前的回答", "revision": 4 }
  }
}
```

上例中的帧在水位已是 42 时被整帧忽略；在水位是 41 时被应用，水位随之推到 42。

**快照帧不带序号，不参与这条规则**：它每次都必须按规则 2 应用，无论水位走到了哪里。
块合并规则里的顺序不重要（见上），水位规则的顺序却重要——两者互不干扰，正是因为
快照帧不参与水位。

**为什么需要它。** 握手保证的是**不漏**（前提是客户端按游标把回补页读完）：注册与
进度在同一临界区取得，注册之后产生的事件进入实时队列，此前的由历史段与快照承担。
但这个保证有一个前提会被并发派发打破：**序号在事件交给观察者之前就已分配**，因此
并发之下序号 9 可以晚于序号 10 到达观察者。订阅若正好注册在这两条之间，先到的落在
水位之下由回补给出，后到的落在水位之上进实时队列——**同一个序号可以既在回补里又在
实时队列里**。这正是不重不能只靠握手的原因，也是水位规则的存在理由：客户端只要丢弃
`seq` 不大于已应用序号的帧，重复到达就不会改变状态。
此处还有一个由「历史回补的边界」单独交代的例外：`core/block.delta` 不进日志，它既
不在历史段里，握手之后也不会被补发，只能以快照里当下的内容间接到达。此外握手**不**
保证不重：客户端保存的游标记的是它上次读到哪，而不是它应用过哪些，重新握手时回补段
因此可能把上一条连接里已经应用过的事件再发一遍。**「不漏」由握手承担，「不重」由
水位承担**。

---

## 7. 队列溢出

每个订阅方有一个**有界**的帧队列。队列满时按事件的投递类别处置，类别由事件名决定：

| 类别 | 事件 | 队列满时的处置 | 依据 |
| --- | --- | --- | --- |
| **分片类** | `core/block.delta` | 丢掉队列里最旧的一帧，新帧入队 | 幂等且自愈——最终值到达时整体替换全部内容 |
| **状态类** | 声明为状态投递的事件 | 同分片类：丢掉队列里最旧的一帧，新帧入队 | 队列看不到 key，无法据 key 合并；按 key 只保留最新值由持有 key 的投递方负责（见下） |
| **其余事件** | 日志类，即除上述两者外的全部事件 | 新帧不入队，**断开该连接** | 这些事件一条都不能丢 |

队列容量的具体数值不是本契约的一部分，客户端不得依赖它；能被依赖的是「有界」以及
上表的处置。

状态类的**按 key 合并发生在持有 key 的一侧**（投递方），不在队列这一层：状态键不在
事件帧里，队列无法据 key 合并。本单元产出的事件里只有分片类与日志类两种；状态类由
Mod 声明为状态投递的事件产生，mod 宿主已经上线这条投递。

**断开是安全操作。** 客户端重连即重新执行一次握手：快照给回当前状态，历史按游标
补齐缺的那一段，水位丢弃重复的。断开时队列的既有内容不再交付，客户端看到的是流的
结束，而不是流的错误。

---

## 8. 不在本文档里的东西

本文档只描述帧本身。以下各项属于传输层，随 `wind-http` 重写时另定：

- **端点路径与路由**：订阅建立在哪个 URL、路径如何决定作用域、查询参数长什么样。
- **鉴权**：谁能订阅哪个作用域。
- **心跳与保活**：连接空闲时如何维持。
- **重连与退避**：客户端隔多久重试、重试多少次。

**本单元交付的是帧的产出侧。** 帧被交给进程内的订阅方，**SSE 路由本身尚未建立**；
把 HTTP 请求映射到「握手的请求参数」那一步、以及把帧渲染成 SSE 文本的那一层，随
端点一并到来。因此可以确定的是帧的形状、顺序、回补边界与溢出处置；尚不确定的是
它们如何被搬上一个 HTTP 连接。

```json
{ "kind": "heartbeat" }
```

上例**不是**本协议的一帧：`kind` 只有 `snapshot` 与 `event` 两种取值，未知取值是
协议错误。心跳若将来存在，它在传输层，不在帧流上。
