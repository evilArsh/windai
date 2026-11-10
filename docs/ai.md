# AI

`wind-ai` 负责把系统内的 `Message` 变成提供商的请求体，并把提供商返回的 JSON 或
SSE（Server-Sent Events）转回 `Message`。它不决定何时调用模型，也不写数据库；调用方是 `wind_core::llm::turn::exchange`。

本文描述适配器接口、请求发送、SSE 解析与两个适配器的映射。回合如何组装上下文见
「LLM 回合」；提供商与模型的数据库行见「存储」。

## 模块

| 模块 | 职责 |
| --- | --- |
| `adapter` | `ChatAdapter` trait 与 `adapter_of` |
| `chat` | 请求发送、一次性响应、流式响应与 `Chunk` |
| `client` | 复用的 `reqwest::Client` |
| `eventsource` | 字节流到 SSE 事件的解析 |
| `model` | `ChatModel` 与 `Target` |
| `schema` | 适配器使用的请求/响应类型 |
| `openai_completion` | OpenAI Chat Completions 适配器 |
| `openai_responses` | OpenAI Responses 适配器 |

## 适配器接口

```rust
pub trait ChatAdapter: Send + Sync {
    fn path(&self) -> &'static str;

    fn build_request(
        &self,
        model: &ChatModel,
        contexts: &[Message],
        tools: Option<&[Tools]>,
    ) -> Result<serde_json::Value, AdapterError>;

    fn parse_response(&self, data: &[u8]) -> Result<Message, AdapterError>;

    fn parse_stream_chunk(&self, event: &Event) -> Result<Option<Message>, AdapterError>;
}

pub fn adapter_of(kind: AdapterType) -> Box<dyn ChatAdapter>;
```

`AdapterType` 取 `openai_completion` 或 `openai_response`，定义在 `wind-wire`。数据库中的
`models.adapter` 解析为该枚举，`adapter_of` 返回对应实现。

`parse_stream_chunk` 返回 `None` 表示该 SSE 事件不产生消息；返回 `Some(Message)` 表示产生了
一段增量。适配器不负责判断流是否结束，结束由 `chat::Chunk::Done` 表达。

## 模型与目标

```rust
pub struct ChatModel {
    pub name: String,
    pub adapter: AdapterType,
    /// 专属端点，缺省时用适配器的默认端点
    pub endpoint: Option<String>,
    pub config: Option<serde_json::Value>,
}

pub struct Target {
    pub base_url: String,
    pub api_key: Option<String>,
}
```

`ChatModel` 来自 `models` 行；`Target` 来自 `providers.base_url` 与第一条启用凭证。
`config` 同时承载两类信息：

- `stream`：布尔值，决定 `exchange` 走流式还是一次性请求。
- `reasoning`：字符串，作为 OpenAI 的 `reasoning_effort` / `reasoning.effort`。
- 其余键在请求体构造完成后逐项覆盖同名字段。`stream` 与 `reasoning` 不参与覆盖。

## 请求发送

```rust
pub async fn once(adapter: &dyn ChatAdapter, request: &Request) -> Result<Message, ProviderError>;
pub async fn stream(
    adapter: &dyn ChatAdapter,
    request: &Request,
) -> Result<impl Stream<Item = Result<Chunk, ProviderError>>, ProviderError>;

pub async fn once_body(
    adapter: &dyn ChatAdapter,
    model: &ChatModel,
    target: &Target,
    body: Value,
) -> Result<Message, ProviderError>;

pub async fn stream_body(
    adapter: &dyn ChatAdapter,
    model: &ChatModel,
    target: &Target,
    body: Value,
) -> Result<impl Stream<Item = Result<Chunk, ProviderError>>, ProviderError>;
```

`once` / `stream` 先调 `build_request`；`once_body` / `stream_body` 接收已经构造好的请求体，
供调用方在请求体上应用规则与事件改写之后发送。

地址：

```text
base = target.base_url.trim_end_matches('/')
path = model.endpoint.unwrap_or(adapter.path())
path 不以 '/' 开头时补 '/'
url  = Url::parse(base + path)
```

请求固定为 `POST`，带两个头：

| 头 | 值 |
| --- | --- |
| `Content-Type` | `application/json; charset=utf-8` |
| `Accept` | `text/event-stream` |

`api_key` 非空时追加 `Authorization: Bearer <key>`。即使是一次性请求也发送
`Accept: text/event-stream`；实际的响应处理由调用的函数决定。

HTTP 客户端是进程内复用的一份 `reqwest::Client`，只设置 120 秒的读取空闲上限，不限制整段
生成的时长。

状态码非 2xx 时返回 `ProviderError::Status { code, body }`，响应体读为文本。

## 一次性与流式

`once_body` 读完整个响应体后交给 `adapter.parse_response`。`stream_body` 把
`response.bytes_stream()` 接上 `eventsource()`，再逐事件调用 `parse_stream_chunk`。

`Chunk`：

```rust
pub enum Chunk {
    Delta(Message),
    Done,
}
```

流式路径的映射：

| 情况 | 结果 |
| --- | --- |
| 适配器返回 `Some(Message)` | `Chunk::Delta` |
| 适配器返回 `None` | 继续读下一个事件 |
| SSE 解析错误 | `Err(ProviderError::Transport)`，流终止 |
| 适配器解析错误 | `Err(ProviderError::Adapter)`，流终止 |
| 字节流自然结束 | `Chunk::Done` |

OpenAI 的 `data: [DONE]` 在 SSE 解析层被跳过，不产生事件；流的结束就是字节流的结束。

## SSE 解析

`eventsource` 从任意字节流构造 `Event`：

```rust
pub struct Event {
    pub event: String,
    pub data: String,
    pub id: String,
    pub retry: Option<Duration>,
}
```

解析遵循 HTML 规范的字段规则：

- `event:` 设置事件名；没有事件名时，分发后为 `"message"`。
- `data:` 追加字段值并补一个换行；多行 `data` 以换行连接，分发时去掉末尾换行。
- `id:` 的值不含 NUL 时设置事件 id；id 在后续事件中保持。
- `retry:` 能解析为 `u64` 时取毫秒值；解析失败时忽略。
- 以 `:` 开头的注释行忽略。
- 空行触发分发。`data` 为空的事件不分发。
- `data` 的值为 `[DONE]` 时跳过该字段；只有 `[DONE]` 的事件因 data 为空而不分发。

UTF-8 解码是增量的：多字节字符被切在两个字节块之间时会拼接后一起解码，不会截断。字节流
错误、UTF-8 错误与解析错误分别映射为 `EventStreamError::{Transport, Utf8, Parser}`。

## OpenAI Chat Completions

`OpenAICompletion`：

- 默认端点 `"/chat/completions"`。
- 请求类型见 `wind_ai::schema::openai_completion`。

消息转换：

| `Message` | 请求项 |
| --- | --- |
| 有非空 `tool_calls` 的助手消息 | 一条带 `tool_calls` 的 assistant 消息；文本作为 `content` |
| `Role::Tool` | 每个 `Content::FunctionCall` 生成一条 `role = tool` 的消息，`tool_call_id` 取 `FunctionCallOutput.id` |
| 其余角色 | 一条消息；文本、图片、音频、文件分别转为对应的内容段 |

`Content::FunctionCall` 出现在非工具消息中时被过滤并记录日志。请求字段中 `n` 固定为 1；
`stream` 与 `stream_options.include_usage` 由 `config.stream` 决定；`reasoning_effort` 取
`config.reasoning`。其余 `config` 键在请求体构造后覆盖同名字段。

响应解析：

- 非流式：取 `choices[0].message`，用量取 `usage.prompt_tokens` / `usage.completion_tokens`。
- 流式：每次取 `choices[0].delta`；没有 `choices` 时返回一条只带用量与角色的空消息。
- `finish_reason` 不进入 `Message`；结束原因由 `exchange` 依据 `Chunk::Done` 或错误决定。
- `audio.transcript` 在文本为空时作为文本内容；`audio.data` 转为音频内容。
- 工具调用的 `index` 写入 `FunctionCall.item_id`，用于跨增量的配对。

## OpenAI Responses

`OpenAIResponse`：

- 默认端点 `"/responses"`。
- 请求类型见 `wind_ai::schema::openai_responses`。

输入转换：

- 助手消息拆成输入项：同时有 `reasoning_item_id` 与非空推理时生成 `reasoning` 项；文本
  转成助手消息内容；工具调用转成 `function_call` 项。
- 工具消息的 `FunctionCall` 转成 `function_call_output` 项。
- 其他角色转成普通 `message` 输入项；文本、图片、文件、音频分别转成 `input_text`、
  `input_image`、`input_file`。音频按文件内容提交。

请求字段中 `model` 取 `ChatModel.name`；`stream` 取 `config.stream`；`reasoning.effort` 取
`config.reasoning`；其余 `config` 键覆盖同名字段。

响应解析：

- 非流式：遍历 `output`，把 `message` 内容转为文本/媒体，把 `reasoning` 转为推理文本与
  `reasoning_item_id`，把 `function_call` 转为 `FunctionCall`。
- 流式：按事件类型处理 `response.output_text.delta`、`response.reasoning_text.delta`、
  `response.function_call_arguments.delta` 等增量；`item_id` 写入 `FunctionCall.item_id`
  或 `reasoning_item_id`，用于跨增量配对。

## 错误

```rust
pub enum AdapterError {
    Shape(serde_json::Error),
    ContentType(String),
    Kind(AdapterTypeError),
    Transfer(String),
    Io(std::io::Error),
}

pub enum ProviderError {
    Status { code: u16, body: String },
    Transport(String),
    Url(url::ParseError),
    Adapter(AdapterError),
}
```

`ProviderError::Adapter` 在流式路径中会终止流；`exchange` 把错误文本作为助手消息的一部分，
而不是把整次运行判为失败。

## 当前边界

| 项 | 现状 |
| --- | --- |
| 重试 | 没有重试、退避或限流；每次调用只发送一次 |
| 连接复用 | 进程内一份 `reqwest::Client`；没有按提供商区分超时 |
| 代理 | 未设置；使用 `reqwest` 的默认环境行为 |
| `finish_reason` | 适配器不返回；由 `exchange` 在流结束时设为 `"stop"` 或错误文本 |
| 响应类型 | `ChatAdapter` 只覆盖文本/工具调用；没有独立的媒体响应接口 |
| 流式取消 | `stream_body` 产生的流不持有取消令牌；取消由调用方在 `exchange` 外层处理 |
