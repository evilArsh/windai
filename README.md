# windai

An embeddable AI core library built in Rust. Designed as a shared engine for desktop (Electron/Tauri), web (WASM), and mobile apps.

## Crates

| Crate | Purpose |
|-------|---------|
| **wind-core** | Central orchestration — DB-backed chat engine, multi-turn tool calling, JS request hooks, MCP client coordination |
| **wind-ai** | Provider-agnostic chat layer — streaming/non-streaming requests, adaptors for OpenAI Completion & Response APIs, SSE parser |
| **wind-mcp** | MCP (Model Context Protocol) client — stdio & streamable transports, shared server registry, tool discovery and calling |
| **wind-js** | JavaScript engine (rquickjs) — user-defined `transform(body, context)` hooks to rewrite requests per provider/model at runtime |

## Quick Start

```bash
# Build
cargo build

# Test
cargo test

# Integration test (requires API key)
API_KEY=sk-xxx cargo test -p wind-core -- test_chat_mcp_env
```

## Embedding

```rust
use wind_core::WindCore;

let core = WindCore::init("sqlite:windai.db").await?;

// CRUD services
core.provider().create(...);
core.model().create(...);
core.topic().create(...);

// Chat (streaming)
let stream = core.chat().send(topic_id, model_id, from_msg_id, msg_id);
```

## License

MIT OR Apache-2.0
