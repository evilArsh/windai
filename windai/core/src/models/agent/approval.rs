use crate::db::DbRow;
use crate::storage::utils;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// 需要用户审批的 tool call 请求。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ToolApprovalRequest {
    pub id: i64,
    pub binding_id: i64,
    /// 实际产生 tool request 的 Topic id。
    pub topic_id: i64,
    pub parent_topic_id: i64,
    /// 包含 tool request 的 assistant message id。
    pub message_id: i64,
    /// 模型生成的 tool call id。
    pub tool_call_id: String,
    /// 请求调用的工具名称。
    pub tool_name: String,
    /// 工具调用参数。
    pub arguments: serde_json::Value,
    /// 审批状态。
    pub status: ToolApprovalStatus,
    /// 创建时间戳。
    pub created_at: i64,
    /// 更新时间戳。
    pub updated_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for ToolApprovalRequest {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            parent_topic_id: row.get("parent_topic_id"),
            topic_id: row.get("topic_id"),
            message_id: row.get("message_id"),
            binding_id: row.get("binding_id"),
            tool_call_id: row.get("tool_call_id"),
            tool_name: row.get("tool_name"),
            arguments: utils::de_str_to(&row.get::<String, _>("arguments")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize approval arguments: {}", e).into())
            })?,
            status: utils::parse_str_to(&row.get::<String, _>("status")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize approval status: {}", e).into())
            })?,
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }
}

/// ToolApprovalRequest 的审批状态。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display, Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ToolApprovalStatus {
    /// 等待用户处理。
    Pending,
    /// 用户已批准。
    Approved,
    /// 用户已拒绝。
    Denied,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApprovalRecord {
    pub id: i64,
    pub status: ToolApprovalStatus,
}

/// 批量创建审批请求的输入。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateToolApprovalRequests {
    pub binding_id: i64,
    /// 审批所属的 root Topic id。
    pub parent_topic_id: i64,
    /// 实际产生 tool request 的 Topic id。
    pub topic_id: i64,
    /// 包含 tool request 的 assistant message id。
    pub message_id: i64,
    /// 需要创建审批的 tool call 列表。
    pub calls: Vec<CreateToolApprovalCall>,
}

/// 单个 tool call 对应的审批创建参数。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateToolApprovalCall {
    /// 模型生成的 tool call id。
    pub tool_call_id: String,
    /// 请求调用的工具名称。
    pub tool_name: String,
    /// 工具调用参数。
    pub arguments: serde_json::Value,
}
