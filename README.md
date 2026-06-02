# windai

AI 引擎核心库，提供数据库驱动的多轮对话、工具调用、多供应商适配和声明式请求改写。

## 快速开始

```bash
cargo build
cargo test                   # SQLite 内存库，无需外部依赖
```

```toml
[dependencies]
wind-core = { git = "https://github.com/evilArsh/windai" }
```

## 使用

### 初始化

```rust
use wind_core::WindCore;

let core = WindCore::init_memory().await?;
let core = WindCore::init_local(Some("db.path")).await?;
```

### 注册供应商和模型

```rust
use wind_core::models::{CreateProvider, CreateCredentials, CreateModel};
use wind_ai::model::AdaptorType;

let storage = core.storage();

let pid = storage.provider().create(CreateProvider {
    name: "deepseek".into(), base_url: "https://api.deepseek.com".into(),
    description: None, doc: None, alias: None,
}).await?;

storage.provider().create_credentials(CreateCredentials {
    provider_id: pid, key: "sk-xxx".into(),
}).await?;

let mid = storage.model().create(CreateModel {
    name: "deepseek-chat".into(), provider_id: pid,
    adaptor: AdaptorType::OpenAICompletion,
    alias: None, modalities: None, active: Some(true),
    icon: None, endpoint: None,
}).await?;
```

### 创建话题并发起对话

```rust
use wind_core::models::{CreateTopic, CreateMessage};
use wind_ai::message::{Message, Content, Role, ReqConfig};

let storage = core.storage();
let tid = storage.topic().create(CreateTopic {
    parent_id: None, chat_config_id: 0,
    label: "聊天".into(), icon: None, max_context: None,
}).await?;

storage.topic().create_chat_config(tid, ReqConfig {
    temperature: Some(0.7), stream: Some(true), ..Default::default()
}).await?;

// 用户消息
let uid = storage.message().create(CreateMessage {
    from_id: None, stream: false, is_boundary: false,
    content: vec![Message::new_simple(Role::User, vec![
        Content::new_text("你好".into())
    ], None)],
    topic_id: tid, model_id: mid, input_tokens: 0, output_tokens: 0,
}).await?;

// 助手消息
let aid = storage.message().create(CreateMessage {
    from_id: Some(uid), stream: false, is_boundary: false,
    content: vec![],
    topic_id: tid, model_id: mid, input_tokens: 0, output_tokens: 0,
}).await?;

// 流式对话
use futures::StreamExt;
use wind_core::chat::ChatEvent;

let mut stream = core.chat().start(tid, mid, uid, aid);
while let Some(event) = stream.next().await {
    match event {
        ChatEvent::Created { .. } => {},
        ChatEvent::Partial { delta, .. } => {
            if let Some(c) = delta.content.first() {
                print!("{}", c.text());
            }
        }
        ChatEvent::Finish { error, .. } => {
            if let Some(e) = error { eprintln!("错误: {e}"); }
        }
    }
}
```

### JSON 规则引擎

用 JSON 定义请求改写规则，存到数据库后自动生效。例如将 DeepSeek 的 `reasoning_effort` 转成 `thinking` 参数：

```rust
use wind_core::models::CreateJsonRule;

storage.provider().create_json_rule(CreateJsonRule {
    provider_id: pid, adaptor: AdaptorType::OpenAICompletion,
    json_rule: r#"{
        "rules": [{
            "type": "map_value",
            "path": "reasoning_effort",
            "mappings": {
                "medium": {"thinking": {"type": "enabled"}},
                "high":   {"thinking": {"type": "enabled"}}
            },
            "default": {"thinking": {"type": "disabled"}},
            "remove_source": true
        }]
    }"#.into(),
}).await?;
```

支持 `set`、`remove`、`map_value`、`compute`、`when` 五种操作。条件支持 `eq`/`neq`/`gt`/`lt`/`contains`/`and`/`or`/`not`/`in`。上下文变量 `$ctx.provider` / `$ctx.model` / `$ctx.adaptor` 自动注入。

### MCP 工具调用

```rust
use wind_core::models::CreateMcpServer;
use wind_core::storage::mcp::McpStorage;
use wind_mcp::client::TransportType;

let mcp = McpStorage::new(/* pool */);
let sid = mcp.create(CreateMcpServer {
    r#type: TransportType::Stdio, name: "fetch".into(),
    command: Some("uvx".into()), args: Some(vec!["mcp-server-fetch".into()]),
    url: None, description: None, env: None, auto_approves: None,
}).await?;

storage.topic().set_mcp_servers(tid, vec![sid]).await?;
// chat().start() 会自动发现工具并循环执行工具调用
```

### 实体管理

```rust
let s = core.storage();
s.provider().list_all().await?;
s.model().list_by_provider().await?;
s.topic().list_topics().await?;
s.message().list_by_topic(tid).await?;
s.topic().delete_topics(&[tid]).await?;
s.provider().delete(pid).await?;  // 级联删除 credentials + json_rules

core.shutdown().await;  // 优雅关闭
```

## Crate 结构

| Crate       | 职责                                               |
| ----------- | -------------------------------------------------- |
| `wind-core` | 编排层 — SQLite 存储、对话引擎、规则应用、MCP 协调 |
| `wind-ai`   | 供应商抽象 — 流式/非流式、adaptor 模式、SSE 解析   |
| `wind-mcp`  | MCP 客户端 — actor 模式、stdio/HTTP 传输、工具发现 |
| `wind-rule` | JSON 规则引擎 — 声明式请求变换、表达式编译         |

## 环境变量

| 变量              | 用途                          |
| ----------------- | ----------------------------- |
| `WINDAI_ROOT_DIR` | 数据目录（默认 `~/.windai/`） |
| `RUST_LOG`        | 日志级别                      |

## License

MIT OR Apache-2.0
