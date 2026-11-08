//! stdio 与 HTTP 测试服务共用的实现

#![allow(dead_code)]

use std::io::Write;
use std::time::Duration;

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorCode, ErrorData,
    Implementation, InputRequiredResult, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use serde_json::{Map, Value, json};

/// 每页工具数
const PAGE_SIZE: usize = 2;

/// 测试服务的工具定义
pub fn tools() -> Vec<Tool> {
    vec![
        Tool::new(
            "echo",
            "returns the arguments",
            object_schema(json!({"type": "object"})),
        ),
        Tool::new(
            "context",
            "returns the working directory and environment",
            object_schema(json!({"type": "object"})),
        ),
        Tool::new(
            "wait",
            "waits for a long time",
            object_schema(json!({"type": "object"})),
        ),
        Tool::new(
            "unsupported",
            "returns an intermediate response",
            object_schema(json!({"type": "object"})),
        ),
        Tool::new(
            "failing",
            "returns a tool level error",
            object_schema(json!({"type": "object"})),
        ),
    ]
}

fn object_schema(value: Value) -> Map<String, Value> {
    value
        .as_object()
        .cloned()
        .expect("the tool schema must be an object")
}

/// 提供 echo、context、wait、unsupported、failing 五个工具
#[derive(Clone, Copy, Default)]
pub struct TestServer;

impl ServerHandler for TestServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("wind-test-server", "0.1.0"))
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if std::env::var_os("WIND_TEST_LIST_FAIL_FILE")
            .is_some_and(|path| std::path::Path::new(&path).exists())
        {
            return Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                "the tool list failed",
                None,
            ));
        }
        if std::env::var_os("WIND_TEST_LIST_FAIL").is_some() {
            return Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                "the tool list failed",
                None,
            ));
        }
        record_list();
        wait_for_list_gate().await;
        let all = tools();
        let start = request
            .and_then(|params| params.cursor)
            .map(|cursor| {
                cursor
                    .parse::<usize>()
                    .expect("the cursor must be a page index")
            })
            .unwrap_or(0);
        let start = start.min(all.len());
        let end = (start + PAGE_SIZE).min(all.len());
        record_page(start);
        let mut result = ListToolsResult::with_all_items(all[start..end].to_vec());
        result.next_cursor = (end < all.len()).then(|| end.to_string());
        Ok(result)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let arguments = request.arguments.unwrap_or_default();
        match request.name.as_ref() {
            "echo" => Ok(CallToolResult::structured(Value::Object(arguments)).into()),
            "context" => {
                let cwd = std::env::current_dir()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default();
                let mark = std::env::var("WIND_TEST_MARK").unwrap_or_default();
                let host = std::env::var("WIND_TEST_HOST").unwrap_or_default();
                Ok(CallToolResult::structured(json!({
                    "cwd": cwd,
                    "mark": mark,
                    "host": host,
                }))
                .into())
            }
            "wait" => {
                let millis = arguments
                    .get("millis")
                    .and_then(Value::as_u64)
                    .unwrap_or(30_000);
                tokio::time::sleep(Duration::from_millis(millis)).await;
                Ok(CallToolResult::success(vec![ContentBlock::text("done")]).into())
            }
            "unsupported" => Ok(InputRequiredResult::from_request_state("state").into()),
            "failing" => Ok(CallToolResult::error(vec![ContentBlock::text("boom")]).into()),
            other => Err(ErrorData::new(
                ErrorCode::INVALID_PARAMS,
                format!("unknown tool {other}"),
                None,
            )),
        }
    }
}

fn record_page(start: usize) {
    let Some(path) = std::env::var_os("WIND_TEST_PAGES_FILE") else {
        return;
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("the pages file must open");
    writeln!(file, "{start}").expect("the pages file must accept a page");
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

fn record_start() {
    let Some(path) = std::env::var_os("WIND_TEST_START_LOG") else {
        return;
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("the start log must open");
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

/// 以标准输入输出运行测试服务，供辅助 binary 调用
pub async fn run_stdio() {
    record_start();
    if std::env::var_os("WIND_TEST_SILENT").is_some() {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        return;
    }
    wait_for_gate().await;
    let exit_file = std::env::var_os("WIND_TEST_EXIT_FILE");
    let service = match TestServer
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await
    {
        Ok(service) => service,
        Err(error) => {
            eprintln!("test server failed to start: {error}");
            std::process::exit(2);
        }
    };
    let _ = service.waiting().await;
    if let Some(path) = exit_file {
        let _ = std::fs::write(path, "exited");
    }
}
