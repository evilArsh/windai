use super::StdioParams;

/// 标准化后的命令，可用于进程启动和去重。
pub struct NormalizedCommand {
    /// 标准化后的可执行文件，如 "bun" 替代 "npx"
    pub command: String,
    /// 标准化后的参数
    pub args: Vec<String>,
    /// 用作去重键的包名
    pub dedup_key: String,
}

/// 可标准化并按包名去重的已知运行器命令。
pub fn normalize(params: &StdioParams) -> Option<NormalizedCommand> {
    match params.command.as_str() {
        "npx" => normalize_npx(&params.args),
        "bun" => normalize_bun(&params.args),
        "bunx" => normalize_bunx(&params.args),
        "uvx" => normalize_uvx(&params.args),
        "uv" => normalize_uv(&params.args),
        _ => None,
    }
}

fn is_node_package_name(arg: &str) -> bool {
    if arg.starts_with('@') {
        return arg.contains('/');
    }
    !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
}
fn extract_npx_package_name(args: &[String]) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        // `--` 分隔符：下一个参数是包名
        if arg == "--" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
            return None;
        }
        // --package=<pkg>[@<version>]
        if let Some(value) = arg.strip_prefix("--package=") {
            return Some(value.to_string());
        }
        if arg.starts_with("-") {
            i += 1;
            continue;
        }
        if is_node_package_name(arg) {
            return Some(arg.clone());
        }
        i += 1;
    }
    None
}
fn extract_bun_package_name(args: &[String]) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-p" || arg == "--package" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
            i += 1;
            continue;
        }
        if arg.starts_with("-") {
            i += 1;
            continue;
        }
        if is_node_package_name(arg) {
            return Some(arg.clone());
        }
        i += 1;
    }
    None
}
fn normalize_npx(args: &[String]) -> Option<NormalizedCommand> {
    let dedup_key = extract_npx_package_name(args)?;
    let mut new_args = Vec::with_capacity(args.len() + 2);
    new_args.push("x".to_string());
    new_args.push("-y".to_string());
    new_args.extend(args.iter().cloned());
    Some(NormalizedCommand {
        command: "bun".to_string(),
        args: new_args,
        dedup_key,
    })
}

fn normalize_bunx(args: &[String]) -> Option<NormalizedCommand> {
    let dedup_key = extract_bun_package_name(args)?;
    let mut new_args = Vec::with_capacity(args.len() + 1);
    new_args.push("x".to_string());
    new_args.push("-y".to_string());
    new_args.extend(args.iter().cloned());
    Some(NormalizedCommand {
        command: "bun".to_string(),
        args: new_args,
        dedup_key,
    })
}

fn normalize_bun(args: &[String]) -> Option<NormalizedCommand> {
    if args.is_empty() || args[0] != "x" {
        return None;
    }
    let after_x = &args[1..];
    let dedup_key = extract_bun_package_name(after_x)?;
    Some(NormalizedCommand {
        command: "bun".to_string(),
        args: args.to_vec(),
        dedup_key,
    })
}

fn normalize_uvx(args: &[String]) -> Option<NormalizedCommand> {
    let dedup_key = extract_python_package_name(args)?;
    let mut new_args = Vec::with_capacity(args.len() + 1);
    new_args.push("tool".to_string());
    new_args.push("run".to_string());
    new_args.extend(args.iter().cloned());
    Some(NormalizedCommand {
        command: "uv".to_string(),
        args: new_args,
        dedup_key,
    })
}

fn normalize_uv(args: &[String]) -> Option<NormalizedCommand> {
    if args.len() < 2 || (args[0] != "tool" && args[1] != "run") {
        return None;
    }
    let after_tool = &args[2..];
    let dedup_key = extract_python_package_name(after_tool)?;
    Some(NormalizedCommand {
        command: "uv".to_string(),
        args: args.to_vec(),
        dedup_key,
    })
}

fn extract_python_package_name(args: &[String]) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--from" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
            i += 1;
            continue;
        }
        if arg.starts_with("-") {
            i += 1;
            continue;
        }
        if arg == "--" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
            return None;
        }
        if is_python_package_name(arg) {
            return Some(arg.clone());
        }
        i += 1;
    }
    None
}

fn is_python_package_name(arg: &str) -> bool {
    if arg.starts_with("./") || arg.starts_with("/") || arg.contains('\\') {
        return false;
    }
    if arg.ends_with(".py") || arg.ends_with(".whl") || arg.ends_with(".tar.gz") {
        return false;
    }
    !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '[')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(command: &str, args: &[&str]) -> StdioParams {
        StdioParams {
            name: "test".to_string(),
            description: None,
            command: command.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            env: None,
        }
    }

    #[test]
    fn test_normalize_npx_simple() {
        let p = params("npx", &["-y", "@modelcontextprotocol/server-everything"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.command, "bun");
        assert_eq!(
            n.args,
            vec!["x", "-y", "-y", "@modelcontextprotocol/server-everything"]
        );
        assert_eq!(n.dedup_key, "@modelcontextprotocol/server-everything");
    }

    #[test]
    fn test_normalize_npx_with_extra_args() {
        let p = params("npx", &["-y", "@org/pkg", "arg1", "arg2"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.dedup_key, "@org/pkg");
        assert!(n.args.contains(&"arg1".to_string()));
    }

    #[test]
    fn test_normalize_npx_package_flag() {
        let p = params("npx", &["--package=@org/pkg", "--", "some-cmd"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.dedup_key, "@org/pkg");
    }

    #[test]
    fn test_normalize_npx_short_package_flag() {
        let p = params("npx", &["-p", "@org/pkg", "--", "cmd"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.dedup_key, "@org/pkg");
    }

    #[test]
    fn test_normalize_bunx() {
        let p = params("bunx", &["-y", "@modelcontextprotocol/server-everything"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.command, "bun");
        assert_eq!(n.dedup_key, "@modelcontextprotocol/server-everything");
        assert_eq!(n.args[0], "x");
    }

    #[test]
    fn test_normalize_bun_x() {
        let p = params("bun", &["x", "-y", "@org/pkg"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.command, "bun");
        assert_eq!(n.dedup_key, "@org/pkg");
    }

    #[test]
    fn test_normalize_bun_non_x_returns_none() {
        let p = params("bun", &["run", "server.js"]);
        assert!(normalize(&p).is_none());
    }

    #[test]
    fn test_normalize_uvx() {
        let p = params("uvx", &["fastmcp"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.command, "uv");
        assert_eq!(n.dedup_key, "fastmcp");
        assert_eq!(n.args, vec!["tool", "run", "fastmcp"]);
    }

    #[test]
    fn test_normalize_uvx_with_from_flag() {
        let p = params("uvx", &["--from", "mcp-server", "run"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.dedup_key, "mcp-server");
    }

    #[test]
    fn test_normalize_uv_run() {
        let p = params("uv", &["run", "fastmcp"]);
        let n = normalize(&p);
        assert!(n.is_none());
    }

    #[test]
    fn test_normalize_uv_tool_run() {
        let p = params("uv", &["tool", "run", "fastmcp"]);
        let n = normalize(&p).unwrap();
        assert_eq!(n.command, "uv");
        assert_eq!(n.dedup_key, "fastmcp");
    }

    #[test]
    fn test_normalize_uv_non_run_returns_none() {
        let p = params("uv", &["sync"]);
        assert!(normalize(&p).is_none());
    }

    #[test]
    fn test_normalize_python_returns_none() {
        let p = params("python", &["server.py"]);
        assert!(normalize(&p).is_none());
    }

    #[test]
    fn test_normalize_path_returns_none() {
        let p = params("./my-mcp-server", &[]);
        assert!(normalize(&p).is_none());
    }

    #[test]
    fn test_normalize_node_returns_none() {
        let p = params("node", &["./server.js"]);
        assert!(normalize(&p).is_none());
    }
}
