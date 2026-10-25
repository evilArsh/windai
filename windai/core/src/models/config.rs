use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
pub use wind_ai::model::AdapterType;

/// 工具放行策略
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AllowPolicy {
    /// 无需审批的工具名，`"*"` 表示全部
    pub allow: Vec<String>,
}

impl Default for AllowPolicy {
    fn default() -> Self {
        Self {
            allow: vec!["*".into()],
        }
    }
}

impl_display!(AllowPolicy { allow });

/// 一个话题
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Topic {
    /// 话题标识
    pub id: i64,
    /// 上级话题
    pub parent_id: Option<i64>,
    /// 显示名
    pub label: String,
    /// 使用的模型
    pub model_id: Option<i64>,
    /// 放行策略
    pub tool_approval_policy: AllowPolicy,
    /// 图标
    pub icon: Option<String>,
}

impl_display!(Topic {
    id,
    parent_id,
    label,
    model_id,
    tool_approval_policy,
    icon
});

/// 新建话题
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicCreate {
    pub parent_id: Option<i64>,
    pub label: String,
    pub model_id: Option<i64>,
    /// 放行策略，缺省为全部放行
    #[serde(default)]
    pub tool_approval_policy: AllowPolicy,
    pub icon: Option<String>,
}

/// 修改话题
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_approval_policy: Option<AllowPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
}

impl TopicUpdate {
    pub fn is_empty(&self) -> bool {
        self.parent_id.is_none()
            && self.label.is_none()
            && self.model_id.is_none()
            && self.tool_approval_policy.is_none()
            && self.icon.is_none()
    }
}

/// 一个提供商
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Provider {
    /// 提供商标识
    pub id: i64,
    /// 唯一的提供商名
    pub name: String,
    /// 别名
    pub alias: Option<String>,
    /// 描述
    pub description: Option<String>,
    /// base API 地址
    pub base_url: String,
    /// 官方文档地址
    pub doc: Option<String>,
    /// 是否启用
    pub active: bool,
}

impl_display!(Provider {
    id,
    name,
    alias,
    description,
    base_url,
    doc,
    active
});

/// 新建提供商
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderCreate {
    pub name: String,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub base_url: String,
    pub doc: Option<String>,
}

/// 修改提供商
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl ProviderUpdate {
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.alias.is_none()
            && self.description.is_none()
            && self.base_url.is_none()
            && self.doc.is_none()
            && self.active.is_none()
    }
}

/// 一份凭证
///
/// 明文密钥只留在字段里。`Debug`、`Display`、`Serialize` 三处实现一致地按长度
/// 打星号
#[derive(Deserialize, Clone, schemars::JsonSchema)]
pub struct Credentials {
    /// 唯一标识
    pub id: i64,
    /// 所属提供商
    pub provider_id: i64,
    /// 密钥原文，只在写入数据库与构造请求时使用
    pub key: String,
    /// 创建时刻
    pub created_at: i64,
    /// 是否启用
    pub active: bool,
}

impl Credentials {
    fn masked_key(&self) -> String {
        "*".repeat(self.key.len())
    }
}

impl std::fmt::Display for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Credentials {{ id: {}, provider_id: {}, key: {}, created_at: {}, active: {} }}",
            self.id,
            self.provider_id,
            self.masked_key(),
            self.created_at,
            self.active
        )
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("id", &self.id)
            .field("provider_id", &self.provider_id)
            .field("key", &self.masked_key())
            .field("created_at", &self.created_at)
            .field("active", &self.active)
            .finish()
    }
}

impl Serialize for Credentials {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Credentials", 5)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("provider_id", &self.provider_id)?;
        state.serialize_field("key", &self.masked_key())?;
        state.serialize_field("created_at", &self.created_at)?;
        state.serialize_field("active", &self.active)?;
        state.end()
    }
}

/// 修改凭证
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CredentialUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl CredentialUpdate {
    pub fn is_empty(&self) -> bool {
        self.active.is_none()
    }
}

/// 一个模型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Model {
    /// 模型标识
    pub id: i64,
    /// 所属提供商
    pub provider_id: i64,
    /// 提供商给出的模型名
    pub name: String,
    /// 别名
    pub alias: Option<String>,
    /// 适配器，决定请求与响应的处理方式
    pub adapter: AdapterType,
    /// 模态，存为 JSON 数组
    pub modalities: Option<serde_json::Value>,
    /// 是否启用
    pub active: bool,
    /// 图标
    pub icon: Option<String>,
    /// 专属端点，缺省时用默认端点
    pub endpoint: Option<String>,
    /// 请求配置，存为 JSON 对象
    pub config: Option<serde_json::Value>,
    /// 使用计数
    pub frequency: Option<i64>,
}

impl_display!(Model {
    id,
    provider_id,
    name,
    alias,
    adapter,
    modalities,
    active,
    icon,
    endpoint,
    config,
    frequency
});

/// 新建模型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelCreate {
    pub provider_id: i64,
    pub name: String,
    pub alias: Option<String>,
    pub adapter: AdapterType,
    /// 模态，缺省为空数组
    pub modalities: Option<serde_json::Value>,
    pub icon: Option<String>,
    /// 专属端点，缺省时用默认端点
    pub endpoint: Option<String>,
    /// 请求配置，存为 JSON 对象
    pub config: Option<serde_json::Value>,
}

impl ModelCreate {
    pub fn new(provider_id: i64, name: String, adapter: AdapterType) -> Self {
        Self {
            provider_id,
            name,
            alias: None,
            adapter,
            modalities: None,
            icon: None,
            endpoint: None,
            config: None,
        }
    }
}

/// 修改模型
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter: Option<AdapterType>,
    /// 模态，存为 JSON 数组
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modalities: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// 专属端点，缺省时用默认端点
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// 请求配置，存为 JSON 对象
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
}

impl ModelUpdate {
    pub fn is_empty(&self) -> bool {
        self.provider_id.is_none()
            && self.name.is_none()
            && self.alias.is_none()
            && self.adapter.is_none()
            && self.modalities.is_none()
            && self.active.is_none()
            && self.icon.is_none()
            && self.endpoint.is_none()
            && self.config.is_none()
    }
}

/// 提供商结构规则
#[derive(Debug, Clone, PartialEq)]
pub struct RuleRow {
    /// 规则标识
    pub id: i64,
    /// 所属提供商
    pub provider_id: i64,
    /// 适用的适配器
    pub adapter: AdapterType,
    /// 规则原文
    pub json_rule: String,
}

impl_display!(RuleRow {
    id,
    provider_id,
    adapter,
    json_rule
});

/// 新建提供方结构规则
#[derive(Debug, Clone, PartialEq)]
pub struct RuleCreate {
    pub provider_id: i64,
    pub adapter: AdapterType,
    pub json_rule: String,
    pub active: bool,
}

impl_display!(RuleCreate {
    provider_id,
    adapter,
    json_rule,
    active
});

/// 修改提供方结构规则，缺省字段不写入
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RuleUpdate {
    pub adapter: Option<AdapterType>,
    pub json_rule: Option<String>,
    pub active: Option<bool>,
}

impl RuleUpdate {
    pub fn is_empty(&self) -> bool {
        self.adapter.is_none() && self.json_rule.is_none() && self.active.is_none()
    }
}
