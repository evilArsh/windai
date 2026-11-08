use std::collections::BTreeMap;
use std::time::Duration;

use url::Url;
use wind_wire::{McpConfig, McpDefinition};

use crate::error::{McpError, Result};

/// 宿主进程的环境变量
pub type Environment = BTreeMap<String, String>;

/// MCP 连接与调用的时间预算
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOptions {
    /// 建立连接并完成握手的上限
    pub connect_timeout: Duration,
    /// 读取工具清单的上限
    pub list_timeout: Duration,
    /// 单次工具调用的上限
    pub call_timeout: Duration,
    /// 关闭连接时等待对端退出的上限
    pub shutdown_grace: Duration,
}

impl Default for McpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            list_timeout: Duration::from_secs(10),
            call_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}

/// 读取宿主启动时的环境变量，跳过非 UTF-8 的项
pub fn environment_snapshot() -> Environment {
    std::env::vars_os()
        .filter_map(|(key, value)| {
            let key = key.into_string().ok()?;
            let value = value.into_string().ok()?;
            Some((key, value))
        })
        .collect()
}

/// 检查连接配置在连接前就能判定的错误
pub(crate) fn validate(config: &McpConfig) -> Result<()> {
    match config {
        McpConfig::Stdio {
            command,
            args,
            env,
            cwd,
        } => {
            if command.trim().is_empty() {
                return Err(McpError::InvalidConfig(
                    "the command must not be empty".to_string(),
                ));
            }
            if command.contains('\0') {
                return Err(McpError::InvalidConfig(
                    "the command must not contain a nul character".to_string(),
                ));
            }
            if args.iter().any(|arg| arg.contains('\0')) {
                return Err(McpError::InvalidConfig(
                    "an argument must not contain a nul character".to_string(),
                ));
            }
            if env
                .keys()
                .any(|key| key.contains('\0') || key.contains('='))
            {
                return Err(McpError::InvalidConfig(
                    "an environment key must not contain a nul character or '='".to_string(),
                ));
            }
            if !cwd.is_absolute() {
                return Err(McpError::InvalidConfig(format!(
                    "the working directory {} must be an absolute path",
                    cwd.display()
                )));
            }
        }
        McpConfig::StreamableHttp { url } => {
            let parsed = Url::parse(url)
                .map_err(|error| McpError::InvalidConfig(format!("the url is invalid: {error}")))?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(McpError::InvalidConfig(format!(
                    "the url scheme {} is not supported",
                    parsed.scheme()
                )));
            }
            if parsed.host_str().is_none() {
                return Err(McpError::InvalidConfig(
                    "the url must name a host".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// 检查独立服务声明在连接前就能判定的错误
pub(crate) fn validate_definition(definition: &McpDefinition) -> Result<()> {
    if !symbol_ok(&definition.name) {
        return Err(McpError::InvalidConfig(
            "the name must be 1 to 63 characters from [a-zA-Z0-9_-] and start with a letter or underscore"
                .to_string(),
        ));
    }
    validate(&definition.config)
}

fn symbol_ok(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    value.len() <= 63
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::validate_definition;
    use wind_wire::{McpConfig, McpDefinition};

    fn stdio(name: &str, command: &str) -> McpDefinition {
        McpDefinition {
            name: name.to_string(),
            description: None,
            config: McpConfig::Stdio {
                command: command.to_string(),
                args: Vec::new(),
                env: BTreeMap::new(),
                cwd: std::env::temp_dir(),
            },
        }
    }

    #[test]
    fn definition_name_uses_the_symbol_charset() {
        for name in ["files", "_files", "a", &"a".repeat(63)] {
            assert!(
                validate_definition(&stdio(name, "node")).is_ok(),
                "{name} must be accepted"
            );
        }
        for name in ["", "1files", "with space", "files.v2", &"a".repeat(64)] {
            assert!(
                validate_definition(&stdio(name, "node")).is_err(),
                "{name} must be rejected"
            );
        }
    }

    #[test]
    fn nul_and_separator_characters_are_rejected() {
        assert!(validate_definition(&stdio("files", "node\0")).is_err());

        let mut argument = stdio("files", "node");
        if let McpConfig::Stdio { args, .. } = &mut argument.config {
            args.push("--root\0".to_string());
        }
        assert!(validate_definition(&argument).is_err());

        let mut separator = stdio("files", "node");
        if let McpConfig::Stdio { env, .. } = &mut separator.config {
            env.insert("A=B".to_string(), "value".to_string());
        }
        assert!(validate_definition(&separator).is_err());

        let mut nul = stdio("files", "node");
        if let McpConfig::Stdio { env, .. } = &mut nul.config {
            env.insert("A\0B".to_string(), "value".to_string());
        }
        assert!(validate_definition(&nul).is_err());

        let mut valid = stdio("files", "node");
        if let McpConfig::Stdio { env, .. } = &mut valid.config {
            env.insert("PATH".to_string(), "/bin".to_string());
        }
        assert!(validate_definition(&valid).is_ok());

        let absolute = McpDefinition {
            name: "files".to_string(),
            description: None,
            config: McpConfig::Stdio {
                command: "node".to_string(),
                args: Vec::new(),
                env: BTreeMap::new(),
                cwd: PathBuf::from("relative"),
            },
        };
        assert!(validate_definition(&absolute).is_err());
    }
}
