use std::path::PathBuf;
use wind_ai::model::AdaptorType;

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
    pub test_adaptor: AdaptorType,
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
        test_adaptor: var("TEST_ADAPTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(AdaptorType::OpenAICompletion),
        test_endpoint: var("TEST_ENDPOINT").ok(),
        test_mcp_completion_key: var("TEST_MCP_COMPLETION_KEY").unwrap_or_default(),
        test_mcp_completion_model: var("TEST_MCP_COMPLETION_MODEL").unwrap_or_default(),
        test_mcp_completion_base_url: var("TEST_MCP_COMPLETION_BASE_URL").unwrap_or_default(),
        test_mcp_completion_endpoint: var("TEST_MCP_COMPLETION_ENDPOINT").ok(),
        test_mcp_responses_key: var("TEST_MCP_RESPONSES_KEY").unwrap_or_default(),
        test_mcp_responses_model: var("TEST_MCP_RESPONSES_MODEL").unwrap_or_default(),
        test_mcp_responses_base_url: var("TEST_MCP_RESPONSES_BASE_URL").unwrap_or_default(),
        test_mcp_responses_endpoint: var("TEST_MCP_RESPONSES_ENDPOINT").ok(),
    };
    // log::info!("env: {:#?}", &env);
    env
}

fn var(name: &str) -> Result<String, std::env::VarError> {
    std::env::var(name)
}
