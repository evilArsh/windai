use std::path::PathBuf;

use crate::error::Result;
use crate::roots::{Command, Roots};
use crate::spec::StdioSpec;

pub struct LaunchPlan {
    pub program: PathBuf,
    pub args: Vec<String>,
    /// 启动互斥键
    pub startup_key: String,
    /// 解释器参数里的入口路径
    pub entry: Option<PathBuf>,
}

pub fn plan(spec: &StdioSpec, roots: &Roots) -> Result<LaunchPlan> {
    let command = roots.check_command(&spec.command)?;
    if let Some(plan) = package_plan(&spec.command, &spec.args) {
        return Ok(plan);
    }
    let program = match command {
        Command::Path(path) => path,
        Command::Name(name) => PathBuf::from(name),
    };
    if let Some(entry) = interpreter_entry(&spec.command, &spec.args) {
        return Ok(LaunchPlan {
            program,
            args: spec.args.clone(),
            startup_key: entry.to_string_lossy().into_owned(),
            entry: Some(entry),
        });
    }
    let startup_key = match spec.args.first() {
        Some(first) => format!("{} {first}", program.to_string_lossy()),
        None => program.to_string_lossy().into_owned(),
    };
    Ok(LaunchPlan {
        program,
        args: spec.args.clone(),
        startup_key,
        entry: None,
    })
}

fn package_plan(command: &str, args: &[String]) -> Option<LaunchPlan> {
    match command {
        "npx" => {
            let startup_key = extract_npx_package_name(args)?;
            Some(bun_x_plan(args, startup_key))
        }
        "bunx" => {
            let startup_key = extract_bun_package_name(args)?;
            Some(bun_x_plan(args, startup_key))
        }
        "bun" if args.first().is_some_and(|arg| arg == "x") => {
            let startup_key = extract_bun_package_name(&args[1..])?;
            Some(LaunchPlan {
                program: PathBuf::from("bun"),
                args: args.to_vec(),
                startup_key,
                entry: None,
            })
        }
        "uvx" => {
            let startup_key = extract_python_package_name(args)?;
            Some(uv_tool_run_plan(args, startup_key))
        }
        "uv" if args.len() >= 2 && args[0] == "tool" && args[1] == "run" => {
            let startup_key = extract_python_package_name(&args[2..])?;
            Some(LaunchPlan {
                program: PathBuf::from("uv"),
                args: args.to_vec(),
                startup_key,
                entry: None,
            })
        }
        _ => None,
    }
}

fn bun_x_plan(args: &[String], startup_key: String) -> LaunchPlan {
    let mut rewritten = Vec::with_capacity(args.len() + 2);
    rewritten.push("x".to_owned());
    rewritten.push("-y".to_owned());
    rewritten.extend(args.iter().cloned());
    LaunchPlan {
        program: PathBuf::from("bun"),
        args: rewritten,
        startup_key,
        entry: None,
    }
}

fn uv_tool_run_plan(args: &[String], startup_key: String) -> LaunchPlan {
    let mut rewritten = Vec::with_capacity(args.len() + 2);
    rewritten.push("tool".to_owned());
    rewritten.push("run".to_owned());
    rewritten.extend(args.iter().cloned());
    LaunchPlan {
        program: PathBuf::from("uv"),
        args: rewritten,
        startup_key,
        entry: None,
    }
}

fn interpreter_entry(command: &str, args: &[String]) -> Option<PathBuf> {
    if !matches!(command, "bun" | "python" | "python3" | "node" | "deno") {
        return None;
    }
    args.iter().map(PathBuf::from).find(|arg| arg.is_absolute())
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
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        // `--` 后的第一个参数是包名
        if arg == "--" {
            return args.get(index + 1).cloned();
        }
        if let Some(value) = arg.strip_prefix("--package=") {
            return Some(value.to_owned());
        }
        if !arg.starts_with('-') && is_node_package_name(arg) {
            return Some(arg.clone());
        }
        index += 1;
    }
    None
}

fn extract_bun_package_name(args: &[String]) -> Option<String> {
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "-p" || arg == "--package" {
            return args.get(index + 1).cloned();
        }
        if !arg.starts_with('-') && is_node_package_name(arg) {
            return Some(arg.clone());
        }
        index += 1;
    }
    None
}

fn extract_python_package_name(args: &[String]) -> Option<String> {
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--from" {
            return args.get(index + 1).cloned();
        }
        if !arg.starts_with('-') && is_python_package_name(arg) {
            return Some(arg.clone());
        }
        index += 1;
    }
    None
}

fn is_python_package_name(arg: &str) -> bool {
    if arg.starts_with("./") || arg.starts_with('/') || arg.contains('\\') {
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
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::plan;
    use crate::error::McpError;
    use crate::roots::Roots;
    use crate::spec::StdioSpec;

    fn stdio(command: &str, args: &[&str], cwd: &Path) -> StdioSpec {
        StdioSpec {
            command: command.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            env: Default::default(),
            cwd: cwd.to_path_buf(),
        }
    }

    #[test]
    fn package_runners_are_rewritten_and_keyed_by_package() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("fence").join("mods"));

        let p = plan(&stdio("npx", &["-y", "@scope/server"], tmp.path()), &roots).unwrap();
        assert_eq!(p.program, PathBuf::from("bun"));
        assert_eq!(p.args[0], "x");
        assert_eq!(p.startup_key, "@scope/server");

        let q = plan(&stdio("uvx", &["mcp-server-git"], tmp.path()), &roots).unwrap();
        assert_eq!(q.startup_key, "mcp-server-git");

        let entry = tmp
            .path()
            .join("mods")
            .join("demo")
            .join("1")
            .join("index.js");
        let r = plan(
            &stdio(
                "bun",
                &["run", entry.to_str().expect("utf-8 path")],
                tmp.path(),
            ),
            &roots,
        )
        .unwrap();
        assert_eq!(r.startup_key, entry.to_string_lossy());
        assert_eq!(r.entry, Some(entry));
    }

    #[test]
    fn package_runner_arguments_keep_the_original_tail() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("fence").join("mods"));

        let p = plan(&stdio("npx", &["-y", "@scope/server"], tmp.path()), &roots).unwrap();
        assert_eq!(p.args, vec!["x", "-y", "-y", "@scope/server"]);

        let q = plan(&stdio("bunx", &["-y", "@scope/server"], tmp.path()), &roots).unwrap();
        assert_eq!(q.program, PathBuf::from("bun"));
        assert_eq!(q.args, vec!["x", "-y", "-y", "@scope/server"]);
        assert_eq!(q.startup_key, "@scope/server");

        let r = plan(
            &stdio("bun", &["x", "-y", "@scope/server"], tmp.path()),
            &roots,
        )
        .unwrap();
        assert_eq!(r.args, vec!["x", "-y", "@scope/server"]);
        assert_eq!(r.startup_key, "@scope/server");
    }

    #[test]
    fn uv_tool_run_requires_both_words() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("fence").join("mods"));

        let p = plan(
            &stdio("uvx", &["--from", "mcp-server", "run"], tmp.path()),
            &roots,
        )
        .unwrap();
        assert_eq!(p.program, PathBuf::from("uv"));
        assert_eq!(p.args, vec!["tool", "run", "--from", "mcp-server", "run"]);
        assert_eq!(p.startup_key, "mcp-server");

        let q = plan(
            &stdio("uv", &["tool", "run", "fastmcp"], tmp.path()),
            &roots,
        )
        .unwrap();
        assert_eq!(q.args, vec!["tool", "run", "fastmcp"]);
        assert_eq!(q.startup_key, "fastmcp");

        let r = plan(&stdio("uv", &["run", "fastmcp"], tmp.path()), &roots).unwrap();
        assert_eq!(r.startup_key, "uv run");
    }

    #[test]
    fn unknown_commands_key_on_the_program_and_first_argument() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("fence").join("mods"));

        let p = plan(&stdio("python", &["server.py"], tmp.path()), &roots).unwrap();
        assert_eq!(p.program, PathBuf::from("python"));
        assert_eq!(p.startup_key, "python server.py");
        assert_eq!(p.entry, None);

        let q = plan(&stdio("node", &[], tmp.path()), &roots).unwrap();
        assert_eq!(q.startup_key, "node");
        assert_eq!(q.entry, None);
    }

    #[test]
    fn interpreter_entries_become_the_startup_key() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("fence").join("mods"));
        let entry = tmp
            .path()
            .join("mods")
            .join("demo")
            .join("1")
            .join("index.js");

        let p = plan(
            &stdio("node", &[entry.to_str().expect("utf-8 path")], tmp.path()),
            &roots,
        )
        .unwrap();
        assert_eq!(p.program, PathBuf::from("node"));
        assert_eq!(p.startup_key, entry.to_string_lossy());
        assert_eq!(p.entry, Some(entry));
    }

    #[test]
    fn path_commands_are_fenced() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path().join("mods");
        let entry = mods.join("demo").join("1").join("server");
        fs::create_dir_all(entry.parent().expect("get the parent")).expect("create the mod dir");
        fs::write(&entry, b"").expect("create the server file");
        let roots = Roots::new(mods);

        let p = plan(
            &stdio(entry.to_str().expect("utf-8 path"), &[], tmp.path()),
            &roots,
        )
        .unwrap();
        assert_eq!(
            p.program,
            entry.canonicalize().expect("canonicalize the entry")
        );
        assert_eq!(p.startup_key, p.program.display().to_string());

        let outside = tmp.path().join("etc").join("server");
        assert!(matches!(
            plan(
                &stdio(outside.to_str().expect("utf-8 path"), &[], tmp.path()),
                &roots
            ),
            Err(McpError::InvalidPath(_))
        ));
    }
}
