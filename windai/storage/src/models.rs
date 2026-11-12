use crate::db::DbRow;
use crate::error::StorageError;
use crate::utils::{column_error, decode_adapter, parse_enum, parse_json, parse_optional_json};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use sqlx::types::Json;
use sqlx::{FromRow, Row};
use wind_wire::event::{Origin, Scope, Shape, Source};
use wind_wire::{
    AdapterType, AgentMode, AgentRole, Block as WireBlock, Body, Content, PendingKind, Role,
};

/// 话题
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema, sqlx::FromRow)]
pub struct Topic {
    /// 话题标识
    pub id: i64,
    /// 上级话题
    pub parent_id: Option<i64>,
    /// 显示名
    pub label: String,
    /// 使用的模型
    pub model_id: Option<i64>,
    /// 图标
    pub icon: Option<String>,
    /// 话题工作目录，创建后不可改
    pub workspace: Option<String>,
}

/// 提供商
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema, sqlx::FromRow)]
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
    /// 创建时刻
    pub created_at: i64,
    /// 更新时刻
    pub updated_at: i64,
}

/// 一份凭证
///
/// 明文密钥只留在字段里。`Debug`、`Display`、`Serialize` 三处实现一致地按长度
/// 打星号
#[derive(Deserialize, Clone, schemars::JsonSchema, sqlx::FromRow)]
pub struct Credentials {
    /// 唯一标识
    pub id: i64,
    /// 所属提供商
    pub provider_id: i64,
    /// 密钥原文，只在写入数据库与构造请求时使用
    pub key: String,
    /// 创建时刻
    pub created_at: i64,
    /// 更新时刻
    pub updated_at: i64,
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
            "Credentials {{ id: {}, provider_id: {}, key: {}, created_at: {}, updated_at: {}, active: {} }}",
            self.id,
            self.provider_id,
            self.masked_key(),
            self.created_at,
            self.updated_at,
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
            .field("updated_at", &self.updated_at)
            .field("active", &self.active)
            .finish()
    }
}

impl Serialize for Credentials {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Credentials", 6)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("provider_id", &self.provider_id)?;
        state.serialize_field("key", &self.masked_key())?;
        state.serialize_field("created_at", &self.created_at)?;
        state.serialize_field("updated_at", &self.updated_at)?;
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
    pub modalities: Option<Value>,
    /// 是否启用
    pub active: bool,
    /// 图标
    pub icon: Option<String>,
    /// 专属端点，缺省时用默认端点
    pub endpoint: Option<String>,
    /// 请求配置，存为 JSON 对象
    pub config: Option<Value>,
    /// 使用计数
    pub frequency: Option<i64>,
    /// 创建时刻
    pub created_at: i64,
    /// 更新时刻
    pub updated_at: i64,
}

impl<'r> FromRow<'r, DbRow> for Model {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let adapter: String = row.try_get("adapter")?;
        let modalities: Option<String> = row.try_get("modalities")?;
        let config: Option<String> = row.try_get("config")?;
        Ok(Self {
            id: row.try_get("id")?,
            provider_id: row.try_get("provider_id")?,
            name: row.try_get("name")?,
            alias: row.try_get("alias")?,
            adapter: decode_adapter("adapter", &adapter)?,
            modalities: parse_optional_json("modalities", modalities)?,
            active: row.try_get("active")?,
            icon: row.try_get("icon")?,
            endpoint: row.try_get("endpoint")?,
            config: parse_optional_json("config", config)?,
            frequency: row.try_get("frequency")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
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
    /// 创建时刻
    pub created_at: i64,
    /// 更新时刻
    pub updated_at: i64,
}

impl<'r> FromRow<'r, DbRow> for Rule {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let adapter: String = row.try_get("adapter")?;
        Ok(Self {
            id: row.try_get("id")?,
            provider_id: row.try_get("provider_id")?,
            adapter: decode_adapter("adapter", &adapter)?,
            json_rule: row.try_get("json_rule")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

/// 对话轮
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct Turn {
    pub id: i64,
    pub instance_id: i64,
    pub topic_id: i64,
    pub from_id: Option<i64>,
    pub created_at: Option<i64>,
}

/// 消息块
///
/// 一个 Message 可分为多个消息块
#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    pub id: i64,
    pub message_id: i64,
    pub role: Role,
    /// 块在消息内的位置
    pub seq: i64,
    pub revision: i64,
    pub body: Body,
}

#[derive(Debug, Default)]
pub(crate) struct BlockColumns {
    pub content: Option<String>,
    pub format: Option<String>,
    pub item_id: Option<String>,
    pub call_id: Option<String>,
    pub call_name: Option<String>,
    pub call_arguments: Option<String>,
    pub call_res: Option<Json<Value>>,
}

impl BlockColumns {
    pub(crate) fn of(body: &Body) -> Self {
        match body {
            Body::Reasoning { item_id, content } => Self {
                item_id: item_id.clone(),
                content: present(content),
                ..Self::default()
            },
            Body::Text { content } | Body::Image { content } | Body::File { content } => Self {
                content: present(content),
                ..Self::default()
            },
            Body::Audio {
                item_id,
                format,
                content,
            } => Self {
                item_id: item_id.clone(),
                content: present(content),
                format: present(format),
                ..Self::default()
            },
            Body::ToolCall {
                item_id,
                call_id,
                call_name,
                call_arguments,
            } => Self {
                item_id: item_id.clone(),
                call_id: present(call_id),
                call_name: present(call_name),
                call_arguments: present(call_arguments),
                ..Self::default()
            },
            Body::ToolResult { call_id, call_res } => Self {
                call_id: present(call_id),
                call_res: Some(Json(call_res.clone())),
                ..Self::default()
            },
        }
    }

    fn into_body(self, kind: &str) -> sqlx::Result<Body> {
        Ok(match kind {
            "reasoning" => Body::Reasoning {
                item_id: self.item_id,
                content: self.content.unwrap_or_default(),
            },
            "text" => Body::Text {
                content: self.content.unwrap_or_default(),
            },
            "image" => Body::Image {
                content: self.content.unwrap_or_default(),
            },
            "file" => Body::File {
                content: self.content.unwrap_or_default(),
            },
            "audio" => Body::Audio {
                item_id: self.item_id,
                format: self.format.unwrap_or_default(),
                content: self.content.unwrap_or_default(),
            },
            "tool_call" => Body::ToolCall {
                item_id: self.item_id,
                call_id: self.call_id.unwrap_or_default(),
                call_name: self.call_name.unwrap_or_default(),
                call_arguments: self.call_arguments.unwrap_or_default(),
            },
            "tool_result" => Body::ToolResult {
                call_id: self.call_id.unwrap_or_default(),
                call_res: self
                    .call_res
                    .map(|Json(value)| value)
                    .unwrap_or(Value::Null),
            },
            other => {
                return Err(column_error(
                    "kind",
                    StorageError::MalformedColumn {
                        column: "kind".into(),
                        detail: format!("unknown block kind: {other}"),
                    },
                ));
            }
        })
    }
}

fn present(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

impl<'r> FromRow<'r, DbRow> for Block {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let kind: String = row.try_get("kind")?;
        let role: String = row.try_get("role")?;
        let columns = BlockColumns {
            content: row.try_get("content")?,
            format: row.try_get("format")?,
            item_id: row.try_get("item_id")?,
            call_id: row.try_get("call_id")?,
            call_name: row.try_get("call_name")?,
            call_arguments: row.try_get("call_arguments")?,
            call_res: row.try_get("call_res")?,
        };
        Ok(Self {
            id: row.try_get("id")?,
            message_id: row.try_get("message_id")?,
            role: parse_enum("role", &role)?,
            seq: row.try_get("seq")?,
            revision: row.try_get("revision")?,
            body: columns.into_body(&kind)?,
        })
    }
}

impl From<Block> for WireBlock {
    fn from(block: Block) -> Self {
        Self {
            role: block.role,
            seq: block.seq,
            revision: block.revision,
            body: block.body,
        }
    }
}

/// 上下文投影的一行
#[derive(Debug, Clone, PartialEq)]
pub struct ContextRow {
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub message_created_at: Option<i64>,
    pub block: Block,
}

impl<'r> FromRow<'r, DbRow> for ContextRow {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        Ok(Self {
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            message_created_at: row.try_get("created_at")?,
            block: Block::from_row(row)?,
        })
    }
}

/// 子实例状态的一行
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ChildStateRow {
    pub call_id: String,
    pub child_id: i64,
    /// 运行模式名
    pub mode: Option<String>,
    /// 阶段名
    pub phase: String,
    /// 阶段载荷的 JSON
    pub phase_payload: Option<String>,
    /// 子实例回复的一个文本块
    pub reply_part: Option<String>,
}

/// 一次外部等待
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalWait {
    pub id: i64,
    pub instance_id: i64,
    /// 挂起时所在的消息
    pub message_id: i64,
    /// 模型给出的原始调用
    pub payload: Value,
    pub created_at: i64,
    /// 答复时间
    pub answered_at: Option<i64>,
    /// 工具调用判定
    pub decision: Option<Value>,
}

impl<'r> FromRow<'r, DbRow> for ExternalWait {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let payload_json: String = row.try_get("payload_json")?;
        let decision_json: Option<String> = row.try_get("decision_json")?;
        Ok(Self {
            id: row.try_get("id")?,
            instance_id: row.try_get("instance_id")?,
            message_id: row.try_get("message_id")?,
            payload: parse_json("payload_json", &payload_json)?,
            created_at: row.try_get("created_at")?,
            answered_at: row.try_get("answered_at")?,
            decision: parse_optional_json("decision_json", decision_json)?,
        })
    }
}

/// 一个已安装的 mod
#[derive(Debug, Clone, PartialEq)]
pub struct Mod {
    pub id: i64,
    pub name: String,
    pub version: u32,
    pub manifest: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl<'r> FromRow<'r, DbRow> for Mod {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            version: version_of(row, "version")?,
            manifest: row.try_get("manifest")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

fn version_of(row: &DbRow, column: &str) -> sqlx::Result<u32> {
    let value: i64 = row.try_get(column)?;
    u32::try_from(value).map_err(|_| {
        column_error(
            column,
            StorageError::MalformedColumn {
                column: column.to_string(),
                detail: format!("{value} is outside the u32 range"),
            },
        )
    })
}

// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// pub struct ModStateEntry {
//     pub mod_id: i64,
//     pub scope_kind: String,
//     /// 全局作用域为 0
//     pub scope_id: i64,
//     /// 区分对外状态与私有存储
//     pub kind: String,
//     pub key: String,
//     pub value: Value,
//     pub updated_at: i64,
// }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInstance {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub topic_id: i64,
    pub role: AgentRole,
    pub mode: Option<AgentMode>,
    pub phase: String,
    pub phase_payload: Option<String>,
    pub created_at: Option<i64>,
}

impl<'r> FromRow<'r, DbRow> for AgentInstance {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let role: String = row.try_get("role")?;
        let mode: Option<String> = row.try_get("mode")?;
        let phase: String = row.try_get("phase")?;
        Ok(Self {
            id: row.try_get("id")?,
            parent_id: row.try_get("parent_id")?,
            topic_id: row.try_get("topic_id")?,
            role: parse_enum("role", &role)?,
            mode: mode
                .as_deref()
                .map(|text| parse_enum("mode", text))
                .transpose()?,
            phase,
            phase_payload: row.try_get("phase_payload")?,
            created_at: row.try_get("created_at")?,
        })
    }
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

impl<'r> FromRow<'r, DbRow> for AgentPending {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let kind: String = row.try_get("kind")?;
        let payload_json: String = row.try_get("payload_json")?;
        Ok(Self {
            id: row.try_get("id")?,
            instance_id: row.try_get("instance_id")?,
            kind: parse_enum("kind", &kind)?,
            payload: parse_json("payload_json", &payload_json)?,
            source_id: row.try_get("source_id")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub id: i64,
    /// 产生时刻，epoch 毫秒
    pub create_time: i64,
    pub name: String,
    pub shape: Shape,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    pub payload: Option<Value>,
}

impl<'r> FromRow<'r, DbRow> for LogRecord {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let shape: String = row.try_get("shape")?;
        let origin: String = row.try_get("origin")?;
        let topic_id: Option<i64> = row.try_get("topic_id")?;
        let instance_id: Option<i64> = row.try_get("instance_id")?;
        let scope = match (topic_id, instance_id) {
            (None, None) => Scope::Global,
            (Some(topic_id), Some(instance_id)) => Scope::Topic {
                topic_id,
                instance_id,
            },
            _ => {
                return Err(column_error(
                    "topic_id/instance_id",
                    StorageError::MalformedColumn {
                        column: "topic_id/instance_id".into(),
                        detail: "topic and instance must both be set or both be empty".into(),
                    },
                ));
            }
        };

        let source_kind: String = row.try_get("source_kind")?;
        let source_name: String = row.try_get("source_name")?;
        let source_version: i64 = row.try_get("source_version")?;
        let source = match source_kind.as_str() {
            "core" => Source::core(source_name),
            "mod" => {
                let version = u32::try_from(source_version).map_err(|_| {
                    column_error(
                        "source_version",
                        StorageError::MalformedColumn {
                            column: "source_version".into(),
                            detail: "mod version is out of range".into(),
                        },
                    )
                })?;
                Source::module(source_name, version)
            }
            other => {
                return Err(column_error(
                    "source_kind",
                    StorageError::MalformedColumn {
                        column: "source_kind".into(),
                        detail: format!("unknown source kind: {other}"),
                    },
                ));
            }
        };

        let payload: Option<String> = row.try_get("payload")?;
        Ok(Self {
            id: row.try_get("id")?,
            create_time: row.try_get("create_time")?,
            name: row.try_get("name")?,
            shape: parse_enum("shape", &shape)?,
            scope,
            origin: parse_enum("origin", &origin)?,
            source,
            payload: parse_optional_json("payload", payload)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct SpawnCall {
    pub parent_id: i64,
    pub call_id: String,
    pub child_id: i64,
    pub created_at: i64,
    pub result_delivered_at: Option<i64>,
}
