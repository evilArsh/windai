//! 真实请求测试共用的 `.env` 读取与脱敏打印

use std::path::PathBuf;
use wind_ai::model::AdapterType;

/// 从 `.env` 读到的提供商配置
pub struct Env {
    pub file: PathBuf,
    pub stream: bool,
    pub base_url: String,
    pub key: String,
    pub model: String,
    pub adapter: AdapterType,
    pub endpoint: Option<String>,
}

impl Env {
    /// 从仓库根读 `.env`，缺项立即报出变量名
    pub fn load() -> Self {
        let file = find_env();
        println!("[env] loading {}", file.display());
        dotenvy::from_path(&file).expect("load the .env file");

        let key = required("TEST_KEY");
        assert!(!key.is_empty(), "TEST_KEY is empty");

        Self {
            file,
            stream: optional("TEST_STREAM")
                .map(|value| value.parse().unwrap_or(true))
                .unwrap_or(true),
            base_url: required("TEST_BASE_URL"),
            key,
            model: required("TEST_MODEL"),
            adapter: adapter_from(
                &optional("TEST_ADAPTER").unwrap_or_else(|| "OpenAICompletion".to_string()),
            ),
            endpoint: optional("TEST_ENDPOINT").filter(|value| !value.is_empty()),
        }
    }

    /// 打印配置，密钥只给长度
    pub fn print(&self) {
        println!("[env] file: {}", self.file.display());
        println!("[env] base_url: {}", self.base_url);
        println!("[env] endpoint: {:?}", self.endpoint);
        println!("[env] model: {}", self.model);
        println!("[env] adapter: {}", self.adapter);
        println!("[env] stream: {}", self.stream);
        println!("[env] api_key: <{} chars>", self.key.chars().count());
    }
}

fn find_env() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let file = path.join(".env");
        if file.is_file() {
            return file;
        }
        if !path.pop() {
            panic!(
                "no .env found above {}; copy .env.example to .env",
                env!("CARGO_MANIFEST_DIR")
            );
        }
    }
}

fn required(name: &str) -> String {
    optional(name).unwrap_or_else(|| panic!("{name} is not set in the .env file"))
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn adapter_from(raw: &str) -> AdapterType {
    let name = raw.trim().to_ascii_lowercase().replace('_', "");
    match name.as_str() {
        "openaicompletion" | "openaicompletions" => AdapterType::OpenAICompletion,
        "openairesponse" | "openairesponses" => AdapterType::OpenAIResponse,
        other => panic!("unknown TEST_ADAPTER {other:?}; use OpenAICompletion or OpenAIResponse"),
    }
}
