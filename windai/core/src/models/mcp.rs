use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_mcp::client::TransportType;

use crate::db::DbRow;
use crate::storage;

/// MCP 服务配置，(Stdio, Streamable-HTTP)
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct McpServerParam {
    /// 唯一id
    pub id: i64,
    /// 传输类型
    pub r#type: TransportType,
    /// 服务名称
    pub name: String,
    /// 服务地址
    pub url: Option<String>,
    /// 描述
    pub description: Option<String>,
    /// 启动命令
    pub command: Option<String>,
    /// 启动参数
    pub args: Option<Vec<String>>,
    /// 环境变量
    pub env: Option<HashMap<String, String>>,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for McpServerParam {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        let r#type: TransportType =
            storage::utils::parse_str_to(row.get::<String, _>("type").as_str()).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize type failed: {}", e).into())
            })?;
        let args = storage::utils::de_str_to(row.get::<String, _>("args").as_str())
            .map_err(|e| sqlx::Error::Decode(format!("deserialize args failed: {}", e).into()))?;
        let env = storage::utils::de_str_to(row.get::<String, _>("env").as_str())
            .map_err(|e| sqlx::Error::Decode(format!("deserialize env failed: {}", e).into()))?;
        Ok(McpServerParam {
            id: row.get("id"),
            r#type,
            name: row.get("name"),
            url: row.get("url"),
            description: row.get("description"),
            command: row.get("command"),
            args,
            env,
            created_at: row.get("created_at"),
        })
    }
}

/// 创建 MCP 服务
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct CreateMcpServer {
    /// 传输类型
    pub r#type: TransportType,
    /// 服务名称
    pub name: String,
    /// Streamable-HTTP 服务地址
    pub url: Option<String>,
    /// 描述
    pub description: Option<String>,
    /// Stdio 类型服务启动命令
    pub command: Option<String>,
    /// Stdio 类型服务启动参数
    pub args: Option<Vec<String>>,
    /// Stdio 类型服务环境变量
    pub env: Option<HashMap<String, String>>,
}

/// 更新 MCP 服务
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateMcpServer {
    /// 传输类型
    pub r#type: Option<TransportType>,
    /// 服务名称
    pub name: String,
    /// Streamable-HTTP 服务地址
    pub url: Option<String>,
    /// 描述
    pub description: Option<String>,
    /// Stdio 类型服务启动命令
    pub command: Option<String>,
    /// Stdio 类型服务启动参数
    pub args: Option<Vec<String>>,
    /// Stdio 类型服务环境变量
    pub env: Option<HashMap<String, String>>,
}
