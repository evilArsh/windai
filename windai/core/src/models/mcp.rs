use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_mcp::client::{ServerParams, TransportType};

use crate::db::DbRow;
use crate::error::CoreError;
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

impl TryFrom<McpServerParam> for ServerParams {
    type Error = CoreError;

    fn try_from(p: McpServerParam) -> Result<Self, CoreError> {
        match p.r#type {
            TransportType::Stdio => {
                let command = p.command.filter(|s| !s.is_empty()).ok_or_else(|| {
                    CoreError::Validation("stdio mcp server requires a non-empty command".into())
                })?;
                Ok(ServerParams::new_stdio(
                    p.name,
                    command,
                    p.args.unwrap_or_default(),
                    p.description,
                    p.env,
                ))
            }
            TransportType::Streamable => {
                let url = p.url.filter(|s| !s.is_empty()).ok_or_else(|| {
                    CoreError::Validation("streamable mcp server requires a non-empty url".into())
                })?;
                Ok(ServerParams::new_streamable(
                    p.name,
                    url,
                    p.description.unwrap_or_default(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_param() -> McpServerParam {
        McpServerParam {
            id: 1,
            r#type: TransportType::Stdio,
            name: "srv".to_string(),
            url: None,
            description: None,
            command: Some("npx".to_string()),
            args: Some(vec!["-y".to_string()]),
            env: Some(HashMap::from([("K".to_string(), "V".to_string())])),
            created_at: 0,
        }
    }

    #[test]
    fn stdio_converts_fields_and_passes_env_through() {
        let sp = ServerParams::try_from(base_param()).unwrap();
        match sp {
            ServerParams::Stdio(s) => {
                assert_eq!(s.name, "srv");
                assert_eq!(s.command, "npx");
                assert_eq!(s.args, vec!["-y".to_string()]);
                assert_eq!(s.env.as_ref().unwrap().get("K"), Some(&"V".to_string()));
                assert_eq!(s.description, None);
            }
            _ => panic!("expected ServerParams::Stdio"),
        }
    }

    #[test]
    fn stdio_defaults_missing_args_and_env() {
        let mut p = base_param();
        p.args = None;
        p.env = None;
        let sp = ServerParams::try_from(p).unwrap();
        match sp {
            ServerParams::Stdio(s) => {
                assert!(s.args.is_empty());
                assert!(s.env.is_none());
            }
            _ => panic!("expected ServerParams::Stdio"),
        }
    }

    #[test]
    fn stdio_requires_non_empty_command() {
        let mut p = base_param();
        p.command = None;
        assert!(matches!(
            ServerParams::try_from(p),
            Err(CoreError::Validation(_))
        ));

        let mut p = base_param();
        p.command = Some(String::new());
        assert!(matches!(
            ServerParams::try_from(p),
            Err(CoreError::Validation(_))
        ));
    }

    #[test]
    fn streamable_converts_with_empty_description_default() {
        let p = McpServerParam {
            r#type: TransportType::Streamable,
            url: Some("http://127.0.0.1:9000/mcp".to_string()),
            description: None,
            ..base_param()
        };
        let sp = ServerParams::try_from(p).unwrap();
        match sp {
            ServerParams::Streamable(s) => {
                assert_eq!(s.name, "srv");
                assert_eq!(s.url, "http://127.0.0.1:9000/mcp");
                assert_eq!(s.description, "");
            }
            _ => panic!("expected ServerParams::Streamable"),
        }
    }

    #[test]
    fn streamable_requires_non_empty_url() {
        let p = McpServerParam {
            r#type: TransportType::Streamable,
            url: None,
            ..base_param()
        };
        assert!(matches!(
            ServerParams::try_from(p),
            Err(CoreError::Validation(_))
        ));
    }
}
