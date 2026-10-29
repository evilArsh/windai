use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use wind_wire::{
    AdapterType, AgentMode, AgentPhase, AgentRole, BlockKind, Content, PendingKind, Role,
};

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

/// 话题
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

/// 提供商
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

/// 模型
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

/// 提供商结构规则
#[derive(Debug, Clone, PartialEq)]
pub struct Rule {
    /// 规则标识
    pub id: i64,
    /// 所属提供商
    pub provider_id: i64,
    /// 适用的适配器
    pub adapter: AdapterType,
    /// 规则原文
    pub json_rule: String,
}

/// 对话轮
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub id: i64,
    pub instance_id: i64,
    pub topic_id: i64,
    pub from_id: Option<i64>,
    pub created_at: Option<i64>,
}

/// 消息
///
/// 一轮对话有多个 Message
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: i64,
    pub turn_id: i64,
    pub role: Role,
    pub model_id: Option<i64>,
    pub finish_reason: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub created_at: Option<i64>,
}

/// 消息块
///
/// 一个 Message 可分为多个消息块
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub id: i64,
    pub message_id: i64,
    pub kind: BlockKind,
    pub finished: bool,
    pub content: Option<String>,
    pub format: Option<String>,
    pub call_id: Option<String>,
    pub call_name: Option<String>,
    pub call_arguments: Option<String>,
    pub call_res: Option<serde_json::Value>,
}

/// mod 基础信息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mod {
    pub id: i64,
    pub name: String,
    pub current_version: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModVersion {
    pub mod_id: i64,
    pub version: i64,
    pub manifest: String,
    pub compiled: Option<serde_json::Value>,
    pub engine_fingerprint: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModStateEntry {
    pub mod_id: i64,
    pub scope_kind: String,
    /// 全局作用域为 0
    pub scope_id: i64,
    /// 区分对外状态与私有存储
    pub kind: String,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInstance {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub topic_id: i64,
    pub role: AgentRole,
    pub mode: Option<AgentMode>,
    pub phase: AgentPhase,
    pub phase_payload: Option<String>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentPending {
    /// 同时是队列顺序
    pub id: i64,
    pub instance_id: i64,
    pub kind: PendingKind,
    /// 待拼进 user 消息的内容
    pub payload: Vec<Content>,
    /// `background` 时是 child_id
    pub source_id: Option<i64>,
    pub created_at: i64,
}
