#[path = "common/lib.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use rmcp::ServiceExt;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wind_mcp::{
    Connection, ConnectionKey, Connector, Environment, McpError, McpOptions, connection_key,
};
use wind_wire::McpConfig;

fn server_command() -> String {
    env!("CARGO_BIN_EXE_mcp_test_server").to_string()
}

fn stdio(command: &str, cwd: &Path) -> McpConfig {
    McpConfig::Stdio {
        command: command.to_string(),
        args: Vec::new(),
        env: BTreeMap::new(),
        cwd: cwd.to_path_buf(),
    }
}

fn environment(pairs: &[(&str, &str)]) -> Environment {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

async fn connect(connector: &Connector, config: &McpConfig) -> Connection {
    connector
        .connect(config)
        .await
        .expect("the test server must connect")
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

#[tokio::test]
async fn stdio_and_http_complete_handshake() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let connector = Connector::from_host(McpOptions::default());

    let stdio_connection = connect(&connector, &stdio(&server_command(), dir.path())).await;
    let stdio_tools = stdio_connection
        .list_tools(CancellationToken::new())
        .await
        .expect("stdio tools must list");
    assert!(
        stdio_tools.iter().any(|tool| tool.name == "echo"),
        "stdio must expose the echo tool"
    );
    stdio_connection
        .close()
        .await
        .expect("stdio connection must close");

    let http = HttpServer::spawn().await;
    let http_config = McpConfig::StreamableHttp {
        url: http.url.clone(),
    };
    let http_connection = connect(&connector, &http_config).await;
    let http_tools = http_connection
        .list_tools(CancellationToken::new())
        .await
        .expect("http tools must list");
    assert!(
        http_tools.iter().any(|tool| tool.name == "echo"),
        "http must expose the echo tool"
    );
    http_connection
        .close()
        .await
        .expect("http connection must close");
    http.shutdown().await;
}

#[tokio::test]
async fn tools_pagination_is_complete() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let pages = dir.path().join("pages");
    let mut config = stdio(&server_command(), dir.path());
    if let McpConfig::Stdio { env, .. } = &mut config {
        env.insert(
            "WIND_TEST_PAGES_FILE".to_string(),
            pages.to_string_lossy().into_owned(),
        );
    }
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &config).await;
    let tools = connection
        .list_tools(CancellationToken::new())
        .await
        .expect("tools must list");
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["echo", "context", "wait", "unsupported", "failing"],
        "listing must follow every page"
    );
    let pages = std::fs::read_to_string(&pages).expect("the page log must exist");
    assert_eq!(
        pages.lines().collect::<Vec<_>>(),
        vec!["0", "2", "4"],
        "the client must request every page"
    );
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn unsupported_response_is_not_success() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &stdio(&server_command(), dir.path())).await;
    let result = connection
        .call(
            "unsupported",
            json!({}),
            CancellationToken::new(),
            deadline(),
        )
        .await;
    assert!(
        matches!(result, Err(McpError::UnsupportedResponse)),
        "an intermediate response must not be treated as a completed call: {result:?}"
    );
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn arguments_must_be_object() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &stdio(&server_command(), dir.path())).await;
    let result = connection
        .call("echo", json!("text"), CancellationToken::new(), deadline())
        .await;
    assert!(
        matches!(result, Err(McpError::ArgumentsNotObject)),
        "non object arguments must be rejected before the call"
    );
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn command_environment_and_cwd_take_effect() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let mut config = stdio(&server_command(), dir.path());
    if let McpConfig::Stdio { env, .. } = &mut config {
        env.insert("WIND_TEST_MARK".to_string(), "from-config".to_string());
    }
    let connector = Connector::new(
        environment(&[("WIND_TEST_HOST", "from-host")]),
        McpOptions::default(),
    );
    let connection = connect(&connector, &config).await;
    let value = connection
        .call("context", json!({}), CancellationToken::new(), deadline())
        .await
        .expect("the context tool must answer");
    let content = &value["structuredContent"];
    assert_eq!(content["mark"], json!("from-config"));
    assert_eq!(content["host"], json!("from-host"));
    let reported = content["cwd"].as_str().expect("the cwd must be a string");
    assert_eq!(
        std::fs::canonicalize(reported).expect("the reported cwd must resolve"),
        std::fs::canonicalize(dir.path()).expect("the temporary directory must resolve"),
        "the child must run in the configured working directory"
    );
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn http_query_is_preserved() {
    let http = HttpServer::spawn().await;
    let url = format!("{}?token=abc", http.url);
    let key = connection_key(
        &McpConfig::StreamableHttp { url: url.clone() },
        &Environment::new(),
    )
    .expect("the url must form a connection key");
    assert_eq!(
        key,
        ConnectionKey::StreamableHttp { url: url.clone() },
        "the key must keep the query"
    );
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &McpConfig::StreamableHttp { url }).await;
    connection
        .list_tools(CancellationToken::new())
        .await
        .expect("tools must list");
    let queries = http
        .queries
        .lock()
        .expect("the query log must lock")
        .clone();
    assert!(
        queries
            .iter()
            .any(|query| query.as_deref() == Some("token=abc")),
        "the request url must keep the query"
    );
    connection.close().await.expect("connection must close");
    http.shutdown().await;
}

#[tokio::test]
async fn startup_failure_is_reported() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let missing = dir.path().join("missing-server");
    let connector = Connector::from_host(McpOptions::default());
    let result = connector
        .connect(&stdio(&missing.to_string_lossy(), dir.path()))
        .await;
    assert!(
        matches!(result, Err(McpError::CommandUnavailable(_))),
        "a missing command must be reported before spawning"
    );
}

#[tokio::test]
async fn handshake_timeout_is_reported() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let mut config = stdio(&server_command(), dir.path());
    if let McpConfig::Stdio { env, .. } = &mut config {
        env.insert("WIND_TEST_SILENT".to_string(), "1".to_string());
    }
    let connector = Connector::from_host(McpOptions {
        connect_timeout: Duration::from_millis(200),
        ..McpOptions::default()
    });
    let result = connector.connect(&config).await;
    assert!(
        matches!(result, Err(McpError::Timeout)),
        "a silent server must not block the handshake"
    );
}

#[tokio::test]
async fn call_is_cancelled_by_token() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &stdio(&server_command(), dir.path())).await;
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        signal.cancel();
    });
    let result = connection.call("wait", json!({}), cancel, deadline()).await;
    assert!(
        matches!(result, Err(McpError::Cancelled)),
        "cancelling must end the call"
    );
    let echo = connection
        .call(
            "echo",
            json!({"value": 1}),
            CancellationToken::new(),
            deadline(),
        )
        .await
        .expect("the connection must stay usable after a cancelled call");
    assert_eq!(echo["structuredContent"], json!({"value": 1}));
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn call_deadline_is_reported() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &stdio(&server_command(), dir.path())).await;
    let past = Instant::now() + Duration::from_millis(50);
    let result = connection
        .call("wait", json!({}), CancellationToken::new(), past)
        .await;
    assert!(
        matches!(result, Err(McpError::Timeout)),
        "the call must respect its deadline"
    );
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn complete_results_keep_is_error() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let connector = Connector::from_host(McpOptions::default());
    let connection = connect(&connector, &stdio(&server_command(), dir.path())).await;
    let value = connection
        .call("failing", json!({}), CancellationToken::new(), deadline())
        .await
        .expect("a tool level error is still a complete result");
    assert_eq!(value["isError"], Value::Bool(true));
    assert_eq!(value["content"][0]["text"], json!("boom"));
    connection.close().await.expect("connection must close");
}

#[tokio::test]
async fn duplex_completes_handshake() {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        let service = common::TestServer
            .serve((server_read, server_write))
            .await
            .expect("the in-process server must start");
        let _ = service.waiting().await;
    });
    let connector = Connector::from_host(McpOptions::default());
    let connection = connector
        .connect_duplex(client_read, client_write)
        .await
        .expect("the duplex transport must connect");
    let tools = connection
        .list_tools(CancellationToken::new())
        .await
        .expect("tools must list");
    assert!(tools.iter().any(|tool| tool.name == "echo"));
    connection
        .close()
        .await
        .expect("the duplex connection must close");
    let _ = tokio::time::timeout(Duration::from_secs(10), server_task).await;
}

#[tokio::test]
async fn closing_stops_the_child() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let exit_file = dir.path().join("exit");
    let mut config = stdio(&server_command(), dir.path());
    if let McpConfig::Stdio { env, .. } = &mut config {
        env.insert(
            "WIND_TEST_EXIT_FILE".to_string(),
            exit_file.to_string_lossy().into_owned(),
        );
    }
    let connector = Connector::from_host(McpOptions {
        shutdown_grace: Duration::from_secs(5),
        ..McpOptions::default()
    });
    let connection = connect(&connector, &config).await;
    connection
        .list_tools(CancellationToken::new())
        .await
        .expect("the connection must serve a request");
    connection.close().await.expect("connection must close");
    let wait = Duration::from_secs(10);
    let started = Instant::now();
    while std::fs::read_to_string(&exit_file).ok().as_deref() != Some("exited") {
        assert!(
            started.elapsed() < wait,
            "the child must exit after the connection closes"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct HttpServer {
    url: String,
    queries: Arc<Mutex<Vec<Option<String>>>>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl HttpServer {
    async fn spawn() -> Self {
        let cancel = CancellationToken::new();
        let config = StreamableHttpServerConfig::default()
            .with_sse_keep_alive(None)
            .with_cancellation_token(cancel.clone());
        let service: StreamableHttpService<common::TestServer, LocalSessionManager> =
            StreamableHttpService::new(|| Ok(common::TestServer), Default::default(), config);
        let queries: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new().nest_service("/mcp", service).layer(
            axum::middleware::from_fn_with_state(queries.clone(), record_query),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the test server must bind");
        let address = listener.local_addr().expect("a bound address is required");
        let shutdown = cancel.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled_owned().await })
                .await;
        });
        Self {
            url: format!("http://{address}/mcp"),
            queries,
            cancel,
            task,
        }
    }

    async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

async fn record_query(
    State(queries): State<Arc<Mutex<Vec<Option<String>>>>>,
    request: Request,
    next: Next,
) -> Response {
    queries
        .lock()
        .expect("the query log must lock")
        .push(request.uri().query().map(ToOwned::to_owned));
    next.run(request).await
}
