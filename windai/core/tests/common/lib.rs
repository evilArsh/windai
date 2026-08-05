use std::path::PathBuf;
use wind_ai::model::AdapterType;

pub fn setup_env() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let mut path = PathBuf::from(&manifest);
    loop {
        let env_file = path.join(".env");
        if env_file.exists() {
            let _ = dotenvy::from_path(&env_file);
            return;
        }
        if !path.pop() {
            break;
        }
    }
    let _ = dotenvy::dotenv();
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Env {
    // chat
    pub test_stream: bool,
    pub test_base_url: String,
    pub test_key: String,
    pub test_model: String,
    pub test_adapter: AdapterType,
    pub test_endpoint: Option<String>,
    // mcp completion
    pub test_mcp_completion_key: String,
    pub test_mcp_completion_model: String,
    pub test_mcp_completion_base_url: String,
    pub test_mcp_completion_endpoint: Option<String>,
    // mcp responses
    pub test_mcp_responses_key: String,
    pub test_mcp_responses_model: String,
    pub test_mcp_responses_base_url: String,
    pub test_mcp_responses_endpoint: Option<String>,
}

pub fn load_env() -> Env {
    setup_env();
    unsafe {
        std::env::set_var(
            "RUST_LOG",
            var("RUST_LOG").unwrap_or_else(|_| "debug".to_string()),
        );
    }
    let _ = env_logger::builder().is_test(true).try_init();
    let env = Env {
        test_stream: var("TEST_STREAM")
            .map(|s| s.parse().unwrap_or(true))
            .unwrap_or(true),
        test_base_url: var("TEST_BASE_URL").unwrap_or_default(),
        test_key: var("TEST_KEY").unwrap_or_default(),
        test_model: var("TEST_MODEL").unwrap_or_default(),
        test_adapter: var("TEST_ADAPTER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(AdapterType::OpenAICompletion),
        test_endpoint: var("TEST_ENDPOINT").ok().filter(|s| !s.is_empty()),
        test_mcp_completion_key: var("TEST_MCP_COMPLETION_KEY").unwrap_or_default(),
        test_mcp_completion_model: var("TEST_MCP_COMPLETION_MODEL").unwrap_or_default(),
        test_mcp_completion_base_url: var("TEST_MCP_COMPLETION_BASE_URL").unwrap_or_default(),
        test_mcp_completion_endpoint: var("TEST_MCP_COMPLETION_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty()),
        test_mcp_responses_key: var("TEST_MCP_RESPONSES_KEY").unwrap_or_default(),
        test_mcp_responses_model: var("TEST_MCP_RESPONSES_MODEL").unwrap_or_default(),
        test_mcp_responses_base_url: var("TEST_MCP_RESPONSES_BASE_URL").unwrap_or_default(),
        test_mcp_responses_endpoint: var("TEST_MCP_RESPONSES_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty()),
    };
    // log::info!("env: {:#?}", &env);
    env
}

fn var(name: &str) -> Result<String, std::env::VarError> {
    std::env::var(name)
}

// ---------------------------------------------------------------------------
// 共享测试核心初始化
// ---------------------------------------------------------------------------

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use wind_core::WindCore;
use wind_mcp::client::registry::RegistryHandle;

#[allow(dead_code)]
async fn init_test_pool() -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .shared_cache(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        // `sqlite::memory:` is scoped to the physical SQLite connection.
        // Keep test pools single-connection so schema initialization and
        // later queries always operate on the same in-memory database.
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    pool
}

#[allow(dead_code)]
pub async fn init_test_core() -> WindCore {
    let pool = init_test_pool().await;
    WindCore::init_with_pool(pool).await.unwrap()
}

#[allow(dead_code)]
pub async fn init_test_core_with_registry(registry: RegistryHandle) -> WindCore {
    let pool = init_test_pool().await;
    WindCore::init_with_pool_and_registry(pool, registry)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// MCP 测试辅助
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct McpTestEnv {
    pub base_url: String,
    pub key: String,
    pub model: String,
    pub adapter: AdapterType,
    pub endpoint: Option<String>,
}

#[allow(dead_code)]
pub fn mcp_completion_env() -> McpTestEnv {
    let env = load_env();
    McpTestEnv {
        base_url: env.test_mcp_completion_base_url,
        key: env.test_mcp_completion_key,
        model: env.test_mcp_completion_model,
        adapter: AdapterType::OpenAICompletion,
        endpoint: env.test_mcp_completion_endpoint,
    }
}

#[allow(dead_code)]
pub fn mcp_responses_env() -> McpTestEnv {
    let env = load_env();
    McpTestEnv {
        base_url: env.test_mcp_responses_base_url,
        key: env.test_mcp_responses_key,
        model: env.test_mcp_responses_model,
        adapter: AdapterType::OpenAIResponse,
        endpoint: env.test_mcp_responses_endpoint,
    }
}

use wind_core::models::CreateMcpServer;
use wind_mcp::client::{ServerParams, StdioParams, TransportType};

#[allow(dead_code)]
pub fn everything_params() -> ServerParams {
    ServerParams::Stdio(StdioParams {
        name: "everything".to_string(),
        description: None,
        command: "npx".to_string(),
        args: vec![
            "-y".to_string(),
            "@modelcontextprotocol/server-everything".to_string(),
        ],
        env: serde_json::from_str(
            r#"{
            "NPM_CONFIG_REGISTRY": "https://registry.npmmirror.com",
        }"#,
        )
        .ok(),
    })
}

#[allow(dead_code)]
pub fn fetch_params() -> ServerParams {
    ServerParams::Stdio(StdioParams {
        name: "fetch".to_string(),
        description: None,
        command: "uvx".to_string(),
        args: vec!["mcp-server-fetch".to_string()],
        env: serde_json::from_str(
            r#"{
            "UV_DEFAULT_INDEX": "https://pypi.tuna.tsinghua.edu.cn/simple/",
            "PIP_INDEX_URL": "https://pypi.tuna.tsinghua.edu.cn/simple/",
        }"#,
        )
        .ok(),
    })
}
#[allow(dead_code)]
pub fn create_everything_server_params() -> CreateMcpServer {
    CreateMcpServer {
        r#type: TransportType::Stdio,
        name: "everything".into(),
        url: None,
        description: None,
        command: Some("npx".into()),
        args: Some(vec![
            "-y".into(),
            "@modelcontextprotocol/server-everything".into(),
        ]),
        env: None,
    }
}
