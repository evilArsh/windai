use crate::error::{McpError, Result};
use std::path::{Component, Path, PathBuf, is_separator};

pub(crate) struct LaunchPlan {
    /// 实际执行的程序
    pub program: PathBuf,
    pub args: Vec<String>,
    /// 子进程的工作目录
    pub cwd: PathBuf,
    /// 启动互斥键
    pub startup_key: String,
}

pub(crate) fn normalize_root(root: PathBuf) -> PathBuf {
    normalize(&root).unwrap_or(root)
}

pub(crate) fn plan(
    command: &str,
    args: &[String],
    cwd: &Path,
    mods_root: &Path,
) -> Result<LaunchPlan> {
    let mods = normalize(mods_root).unwrap_or_else(|| mods_root.to_path_buf());
    let cwd = check_path(cwd, &mods)?;
    if let Some(mut plan) = package_plan(command, args) {
        plan.cwd = cwd;
        return Ok(plan);
    }
    let program = match check_command(command, &mods)? {
        Command::Path(path) => path,
        Command::Name(name) => PathBuf::from(name),
    };
    if let Some(entry) = interpreter_entry(command, args) {
        let entry = check_path(&entry, &mods)?;
        return Ok(LaunchPlan {
            program,
            args: args.to_vec(),
            cwd,
            startup_key: entry.to_string_lossy().into_owned(),
        });
    }
    let startup_key = match args.first() {
        Some(first) => format!("{} {first}", program.to_string_lossy()),
        None => program.to_string_lossy().into_owned(),
    };
    Ok(LaunchPlan {
        program,
        args: args.to_vec(),
        cwd,
        startup_key,
    })
}

enum Command {
    Path(PathBuf),
    Name(String),
}

fn check_command(command: &str, mods: &Path) -> Result<Command> {
    if command.chars().any(is_separator) {
        check_path(Path::new(command), mods).map(Command::Path)
    } else {
        Ok(Command::Name(command.to_owned()))
    }
}

fn check_path(path: &Path, mods: &Path) -> Result<PathBuf> {
    let resolved = normalize(path).ok_or_else(|| invalid_path(path))?;
    if resolved.starts_with(mods) {
        Ok(resolved)
    } else {
        Err(invalid_path(path))
    }
}

fn invalid_path(path: &Path) -> McpError {
    McpError::InvalidPath(path.display().to_string())
}

fn normalize(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let components: Vec<Component<'_>> = path.components().collect();
    let floor = components
        .iter()
        .position(|component| matches!(component, Component::Normal(_)))
        .unwrap_or(components.len());
    for split in (floor..=components.len()).rev() {
        let prefix: PathBuf = components[..split].iter().collect();
        let Ok(mut resolved) = prefix.canonicalize() else {
            continue;
        };
        for component in &components[split..] {
            match component {
                Component::Normal(name) => resolved.push(name),
                Component::ParentDir => {
                    resolved.pop();
                }
                Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            }
        }
        return Some(resolved);
    }
    None
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
                cwd: PathBuf::new(),
                startup_key,
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
                cwd: PathBuf::new(),
                startup_key,
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
        cwd: PathBuf::new(),
        startup_key,
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
        cwd: PathBuf::new(),
        startup_key,
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

    fn plan_at(command: &str, args: &[&str], cwd: &Path, mods: &Path) -> Option<super::LaunchPlan> {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
        plan(command, &args, cwd, mods).ok()
    }

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn package_runners_are_rewritten_and_keyed_by_package() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path();

        let p = plan_at("npx", &["-y", "@scope/server"], mods, mods).expect("npx must plan");
        assert_eq!(p.program, PathBuf::from("bun"));
        assert_eq!(p.args[0], "x");
        assert_eq!(p.startup_key, "@scope/server");
        assert_eq!(p.cwd, mods.canonicalize().expect("canonicalize the root"));

        let q = plan_at("uvx", &["mcp-server-git"], mods, mods).expect("uvx must plan");
        assert_eq!(q.startup_key, "mcp-server-git");
    }

    #[test]
    fn package_runner_arguments_keep_the_original_tail() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path();

        let p = plan_at("npx", &["-y", "@scope/server"], mods, mods).expect("npx must plan");
        assert_eq!(p.args, args(&["x", "-y", "-y", "@scope/server"]));

        let q = plan_at("bunx", &["-y", "@scope/server"], mods, mods).expect("bunx must plan");
        assert_eq!(q.program, PathBuf::from("bun"));
        assert_eq!(q.args, args(&["x", "-y", "-y", "@scope/server"]));
        assert_eq!(q.startup_key, "@scope/server");

        let r = plan_at("bun", &["x", "-y", "@scope/server"], mods, mods).expect("bun x must plan");
        assert_eq!(r.args, args(&["x", "-y", "@scope/server"]));
        assert_eq!(r.startup_key, "@scope/server");
    }

    #[test]
    fn uv_tool_run_requires_both_words() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path();

        let p =
            plan_at("uvx", &["--from", "mcp-server", "run"], mods, mods).expect("uvx must plan");
        assert_eq!(p.program, PathBuf::from("uv"));
        assert_eq!(
            p.args,
            args(&["tool", "run", "--from", "mcp-server", "run"])
        );
        assert_eq!(p.startup_key, "mcp-server");

        let q =
            plan_at("uv", &["tool", "run", "fastmcp"], mods, mods).expect("uv tool run must plan");
        assert_eq!(q.args, args(&["tool", "run", "fastmcp"]));
        assert_eq!(q.startup_key, "fastmcp");

        let r = plan_at("uv", &["run", "fastmcp"], mods, mods).expect("uv run must plan");
        assert_eq!(r.startup_key, "uv run");
    }

    #[test]
    fn unknown_commands_key_on_the_program_and_first_argument() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path();

        let p = plan_at("python", &["server.py"], mods, mods).expect("python must plan");
        assert_eq!(p.program, PathBuf::from("python"));
        assert_eq!(p.startup_key, "python server.py");

        let q = plan_at("node", &[], mods, mods).expect("node must plan");
        assert_eq!(q.startup_key, "node");
    }

    #[test]
    fn interpreter_entries_become_the_startup_key() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path();
        let entry = mods.join("demo").join("1").join("index.js");

        let p = plan_at("node", &[entry.to_str().expect("utf-8 path")], mods, mods)
            .expect("node must plan");
        assert_eq!(p.program, PathBuf::from("node"));
        assert_eq!(p.startup_key, entry.to_string_lossy());
    }

    #[test]
    fn path_commands_are_fenced() {
        let tmp = TempDir::new().expect("create a temp dir");
        let outside = TempDir::new().expect("create an outside dir");
        let entry = tmp.path().join("demo").join("1").join("server");
        fs::create_dir_all(entry.parent().expect("get the parent")).expect("create the mod dir");
        fs::write(&entry, b"").expect("create the server file");

        let p = plan_at(
            entry.to_str().expect("utf-8 path"),
            &[],
            tmp.path(),
            tmp.path(),
        )
        .expect("the fenced command must plan");
        assert_eq!(
            p.program,
            entry.canonicalize().expect("canonicalize the entry")
        );
        assert_eq!(p.startup_key, p.program.display().to_string());

        let outside_entry = outside.path().join("server");
        fs::write(&outside_entry, b"").expect("create the outside file");
        let args: Vec<String> = Vec::new();
        let result = plan(
            outside_entry.to_str().expect("utf-8 path"),
            &args,
            tmp.path(),
            tmp.path(),
        );
        assert!(matches!(result, Err(McpError::InvalidPath(_))));
    }

    #[test]
    fn a_cwd_outside_the_mods_root_is_rejected() {
        let tmp = TempDir::new().expect("create a temp dir");
        let outside = TempDir::new().expect("create an outside dir");
        let args: Vec<String> = Vec::new();
        let result = plan("node", &args, outside.path(), tmp.path());
        assert!(matches!(result, Err(McpError::InvalidPath(_))));
    }
}
