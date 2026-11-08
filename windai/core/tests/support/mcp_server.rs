//! 资源适配测试使用的 stdio MCP 服务

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    record_start();
    wait_for_gate().await;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let output = Arc::new(Mutex::new(tokio::io::stdout()));
    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let output = Arc::clone(&output);
        tokio::spawn(async move {
            handle(message, output).await;
        });
    }
}

async fn handle(message: Value, output: Arc<Mutex<tokio::io::Stdout>>) {
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    let Some(id) = message.get("id").cloned() else {
        return;
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let response = match method {
        "initialize" => {
            let version = params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or("2025-11-25");
            reply(
                &id,
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "wind-core-test-server", "version": "0.1.0"},
                }),
            )
        }
        // 让客户端的自动探测回退到 initialize 握手
        "server/discover" => error(&id, -32601, "method not found"),
        "ping" => reply(&id, json!({})),
        "tools/list" => {
            if std::env::var_os("WIND_TEST_LIST_FAIL").is_some() {
                error(&id, -32603, "the tool list failed")
            } else {
                record_list();
                wait_for_list_gate().await;
                reply(&id, json!({"tools": tools()}))
            }
        }
        "tools/call" => call(&id, &params).await,
        other => error(&id, -32601, &format!("method {other} is unknown")),
    };
    let mut output = output.lock().await;
    let mut line = response.to_string();
    line.push('\n');
    let _ = output.write_all(line.as_bytes()).await;
    let _ = output.flush().await;
}

async fn call(id: &Value, params: &Value) -> Value {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    match name {
        "echo" => structured(id, arguments),
        "context" => structured(
            id,
            json!({
                "cwd": std::env::current_dir()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                "mark": std::env::var("WIND_TEST_MARK").unwrap_or_default(),
            }),
        ),
        "wait" => {
            let millis = arguments
                .get("millis")
                .and_then(Value::as_u64)
                .unwrap_or(30_000);
            tokio::time::sleep(Duration::from_millis(millis)).await;
            reply(
                id,
                json!({
                    "content": [{"type": "text", "text": "done"}],
                    "isError": false,
                }),
            )
        }
        "failing" => {
            let mut response = reply(
                id,
                json!({
                    "content": [{"type": "text", "text": "boom"}],
                    "isError": true,
                }),
            );
            response["result"]["isError"] = json!(true);
            response
        }
        other => error(id, -32602, &format!("tool {other} is unknown")),
    }
}

fn structured(id: &Value, value: Value) -> Value {
    reply(
        id,
        json!({
            "content": [{"type": "text", "text": value.to_string()}],
            "structuredContent": value,
            "isError": false,
        }),
    )
}

fn reply(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error(id: &Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
    })
}

fn tools() -> Vec<Value> {
    ["echo", "context", "wait", "failing"]
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "description": format!("the {name} tool"),
                "inputSchema": {"type": "object"},
            })
        })
        .collect()
}

fn record_start() {
    let Some(path) = std::env::var_os("WIND_TEST_START_LOG") else {
        return;
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("the start log must open");
    use std::io::Write;
    writeln!(file, "started").expect("the start log must accept a line");
}

async fn wait_for_gate() {
    let Some(path) = std::env::var_os("WIND_TEST_HANDSHAKE_GATE") else {
        return;
    };
    while !std::path::Path::new(&path).exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn record_list() {
    let Some(path) = std::env::var_os("WIND_TEST_LIST_LOG") else {
        return;
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("the list log must open");
    use std::io::Write;
    writeln!(file, "listed").expect("the list log must accept a line");
}

async fn wait_for_list_gate() {
    let Some(path) = std::env::var_os("WIND_TEST_LIST_GATE") else {
        return;
    };
    while !std::path::Path::new(&path).exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
