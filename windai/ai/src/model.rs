use wind_wire::AdapterType;

#[derive(Debug, Clone)]
pub struct ChatModel {
    pub name: String,
    pub adapter: AdapterType,
    /// 专属端点，缺省时用适配器的默认端点
    pub endpoint: Option<String>,
    pub config: Option<serde_json::Value>,
}

/// 一次请求的地址与凭据
#[derive(Debug, Clone)]
pub struct Target {
    pub base_url: String,
    pub api_key: Option<String>,
}
