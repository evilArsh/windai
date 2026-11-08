use std::fs;
use std::path::{Path, PathBuf};

use url::Url;
use wind_wire::McpConfig;

use crate::config::{self, Environment};
use crate::error::{McpError, Result};

/// 决定复用同一条连接的配置特征
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConnectionKey {
    Stdio {
        program: PathBuf,
        args: Vec<String>,
        environment: Vec<(String, String)>,
        cwd: PathBuf,
    },
    StreamableHttp {
        url: String,
    },
}

/// 计算连接特征，同时校验并解析配置
pub fn connection_key(config: &McpConfig, host: &Environment) -> Result<ConnectionKey> {
    config::validate(config)?;
    match config {
        McpConfig::Stdio {
            command,
            args,
            env,
            cwd,
        } => {
            let cwd = resolve_cwd(cwd);
            let program = resolve_program(command, &cwd)?;
            let mut environment = host.clone();
            environment.extend(env.iter().map(|(key, value)| (key.clone(), value.clone())));
            Ok(ConnectionKey::Stdio {
                program,
                args: args.clone(),
                environment: environment.into_iter().collect(),
                cwd,
            })
        }
        McpConfig::StreamableHttp { url } => {
            let url = Url::parse(url)
                .map_err(|error| McpError::InvalidConfig(format!("the url is invalid: {error}")))?
                .to_string();
            Ok(ConnectionKey::StreamableHttp { url })
        }
    }
}

fn resolve_cwd(cwd: &Path) -> PathBuf {
    // 目录可能尚未创建，保留原路径让子进程报告失败
    fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf())
}

fn resolve_program(command: &str, cwd: &Path) -> Result<PathBuf> {
    if !command.chars().any(std::path::is_separator) {
        return Ok(PathBuf::from(command));
    }
    let path = Path::new(command);
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let resolved = fs::canonicalize(&candidate)
        .map_err(|_| McpError::CommandUnavailable(candidate.clone()))?;
    if !resolved.is_file() {
        return Err(McpError::CommandUnavailable(resolved));
    }
    Ok(resolved)
}
