//! 连接描述与身份规则

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;
use wind_mcp::identity::{AssetRoot, Identity, identity_of, normalize_url};
use wind_mcp::normalizer::normalize;
use wind_mcp::params::{ServerParams, StdioParams, StreamableParams};
use wind_mcp::process::build_command;

#[test]
fn a_stdio_description_round_trips() {
    let params = ServerParams::Stdio(StdioParams {
        name: "fetch".into(),
        description: None,
        command: "uvx".into(),
        args: vec!["mcp-server-fetch".into()],
        env: None,
        cwd: None,
    });
    let value = serde_json::to_value(&params).expect("serialize");
    assert_eq!(value["type"], serde_json::json!("stdio"));
    assert_eq!(value["name"], serde_json::json!("fetch"));
    assert_eq!(value["args"][0], serde_json::json!("mcp-server-fetch"));
    let back: ServerParams = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, params);
}

#[test]
fn a_remote_description_carries_only_a_url() {
    let params = ServerParams::Streamable(StreamableParams {
        name: "remote".into(),
        description: "remote knowledge base".into(),
        url: "https://mcp.example.com/v1".into(),
    });
    let value = serde_json::to_value(&params).expect("serialize");
    assert_eq!(value["type"], serde_json::json!("streamable"));
    assert!(value.get("command").is_none());
    assert!(value.get("args").is_none());
}

#[test]
fn an_unknown_transport_is_refused() {
    let raw = r#"{"type":"in-process","name":"x"}"#;
    assert!(serde_json::from_str::<ServerParams>(raw).is_err());
}

/// 构造 stdio 连接描述
fn stdio(command: &str, args: &[&str]) -> StdioParams {
    StdioParams {
        name: "test".into(),
        description: None,
        command: command.into(),
        args: args.iter().map(|arg| (*arg).to_string()).collect(),
        env: None,
        cwd: None,
    }
}

#[test]
fn a_package_manager_command_yields_a_package_name() {
    let normalized = normalize(&stdio(
        "npx",
        &["-y", "@modelcontextprotocol/server-everything"],
    ))
    .expect("normalize a package manager command");
    assert_eq!(normalized.command, "bun", "npx normalizes to bun");
    assert_eq!(
        normalized.dedup_key,
        "@modelcontextprotocol/server-everything"
    );

    let normalized = normalize(&stdio("uvx", &["fastmcp"])).expect("normalize the command");
    assert_eq!(normalized.command, "uv");
    assert_eq!(normalized.args, vec!["tool", "run", "fastmcp"]);
}

#[test]
fn an_unknown_command_has_no_normal_form() {
    for params in [
        stdio("./my-mcp-server", &[]),
        stdio("node", &["./server.js"]),
        stdio("python", &["server.py"]),
    ] {
        assert!(
            normalize(&params).is_none(),
            "{} is not among the known runners; taking the direct branch",
            params.command
        );
    }
}

#[test]
fn a_child_command_carries_its_program_and_its_arguments() {
    let command = build_command(&stdio("uvx", &["mcp-server-fetch"]));
    let stdio = command.as_std();
    assert_eq!(stdio.get_program().to_string_lossy(), "uvx");
    assert_eq!(
        stdio.get_args().collect::<Vec<_>>(),
        vec![OsStr::new("mcp-server-fetch")]
    );
}

#[test]
fn the_command_asks_for_the_child_to_be_killed_on_drop() {
    let command = build_command(&stdio("uvx", &[]));
    assert!(command.get_kill_on_drop());
}

#[test]
fn the_working_directory_follows_the_description() {
    let dir = env::temp_dir();
    let mut params = stdio("uvx", &[]);
    params.cwd = Some(dir.clone());
    let command = build_command(&params);
    assert_eq!(command.as_std().get_current_dir(), Some(dir.as_path()));
}

#[test]
fn a_declared_environment_is_passed_through() {
    let mut params = stdio("uvx", &[]);
    params.env = Some(
        [("KEY".to_string(), "value".to_string())]
            .into_iter()
            .collect(),
    );
    let command = build_command(&params);
    let pairs: Vec<_> = command
        .as_std()
        .get_envs()
        .map(|(key, value)| {
            (
                key.to_string_lossy().to_string(),
                value.map(|v| v.to_string_lossy().to_string()),
            )
        })
        .collect();
    assert_eq!(pairs, vec![("KEY".to_string(), Some("value".to_string()))]);
}

/// 子进程读取自身环境时要运行的命令行。先输出 PATH，再输出声明的变量
#[cfg(unix)]
fn echo_environment() -> (&'static str, Vec<&'static str>) {
    (
        "sh",
        vec!["-c", "echo ${PATH:-unset}; echo ${WIND_TEST_MARK:-unset}"],
    )
}

/// 子进程读取自身环境时要运行的命令行。先输出 PATH，再输出声明的变量
#[cfg(windows)]
fn echo_environment() -> (&'static str, Vec<&'static str>) {
    ("cmd", vec!["/C", "echo %PATH% & echo %WIND_TEST_MARK%"])
}

/// 未声明的环境变量按宿主继承，声明的叠加在宿主环境之上
#[tokio::test]
async fn a_child_sees_the_hosts_environment() {
    let (program, args) = echo_environment();
    let mut params = stdio(program, &args);
    params.env = Some(
        [("WIND_TEST_MARK".to_string(), "declared".to_string())]
            .into_iter()
            .collect(),
    );

    let output = build_command(&params)
        .output()
        .await
        .expect("spawn the child process");
    assert!(
        output.status.success(),
        "the child process must run: {output:?}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let mut lines = stdout.lines().map(str::trim_end);
    let path = lines.next().expect("read the PATH line");
    let mark = lines.next().expect("read the declared variable");

    assert_eq!(
        path,
        env::var("PATH").expect("read the host PATH"),
        "undeclared environment variables are still inherited"
    );
    assert_eq!(
        mark, "declared",
        "declared variables are appended on top of the host environment"
    );
}

/// 已安装的包，资产根目录下有一个同名资产
fn asset_root(tmp: &TempDir, asset: &str, body: &[u8]) -> AssetRoot {
    let root = tmp.path().join("pkg");
    fs::create_dir_all(root.join("mcp").join(asset)).expect("create dir");
    fs::write(root.join("mcp").join(asset).join("bin"), body).expect("write");
    AssetRoot::new(root)
}

/// 已安装的包，资产目录下另有一个子目录，子目录里也有一个文件
fn asset_root_with_subdir(tmp: &TempDir, asset: &str, body: &[u8]) -> AssetRoot {
    let root = asset_root(tmp, asset, body);
    let sub = root.root().join("mcp").join(asset).join("sub");
    fs::create_dir_all(&sub).expect("create dir");
    fs::write(sub.join("bin"), body).expect("write");
    root
}

#[test]
fn a_package_manager_service_is_identified_by_its_package_name() {
    let params = ServerParams::Stdio(stdio("uvx", &["mcp-server-fetch"]));
    let root = AssetRoot::new(PathBuf::from("/nonexistent"));
    assert_eq!(
        identity_of(&params, &root).expect("decide"),
        Identity::Package {
            package: "mcp-server-fetch".into()
        }
    );
}

#[test]
fn a_packaged_asset_is_identified_by_its_content() {
    let first = TempDir::new().expect("create temp dir");
    let second = TempDir::new().expect("create temp dir");
    let mut params = stdio("./mcp/search/bin", &[]);
    params.name = "search".into();
    let params = ServerParams::Stdio(params);

    let a = identity_of(&params, &asset_root(&first, "search", b"same")).expect("decide");
    let b = identity_of(&params, &asset_root(&second, "search", b"same")).expect("decide");
    assert_eq!(
        a, b,
        "identical content means identical identity, regardless of the package location"
    );

    let c = identity_of(&params, &asset_root(&second, "search", b"other")).expect("decide");
    assert_ne!(a, c, "different content means two independent connections");
}

#[test]
fn a_nested_asset_is_fingerprinted_by_its_whole_tree() {
    let first = TempDir::new().expect("create temp dir");
    let second = TempDir::new().expect("create temp dir");
    let params = ServerParams::Stdio(stdio("./mcp/search/bin", &[]));

    let a =
        identity_of(&params, &asset_root_with_subdir(&first, "search", b"same")).expect("decide");
    let b =
        identity_of(&params, &asset_root_with_subdir(&second, "search", b"same")).expect("decide");
    assert_eq!(
        a, b,
        "the same content including subdirectories gets the same fingerprint at different locations"
    );

    let nested = first
        .path()
        .join("pkg")
        .join("mcp")
        .join("search")
        .join("sub")
        .join("bin");
    fs::write(&nested, b"other").expect("write");
    let c = identity_of(&params, &AssetRoot::new(first.path().join("pkg"))).expect("decide");
    assert_ne!(
        a, c,
        "the content of the file in the subdirectory changed, so the fingerprint must change"
    );
}

/// 指纹覆盖整个资产，不只是命令所在目录
///
/// 资产目录里与命令同级的文件也随包分发，改动它们等同于建立另一条连接
#[test]
fn the_fingerprint_covers_the_whole_asset() {
    let tmp = TempDir::new().expect("create temp dir");
    let params = ServerParams::Stdio(stdio("./mcp/search/bin", &[]));
    let root = asset_root(&tmp, "search", b"same");
    let before = identity_of(&params, &root).expect("decide");

    fs::write(
        root.root().join("mcp").join("search").join("README"),
        b"extra",
    )
    .expect("write");
    let after = identity_of(&params, &AssetRoot::new(root.root())).expect("decide");

    assert_ne!(
        before, after,
        "other files in the asset directory are part of the fingerprint too"
    );
}

/// 指纹只覆盖当前资产
///
/// 同一个包的资产目录里还有其他资产，它们不属于当前资产
#[test]
fn the_fingerprint_covers_only_that_asset() {
    let tmp = TempDir::new().expect("create temp dir");
    let params = ServerParams::Stdio(stdio("./mcp/search/bin", &[]));
    let root = asset_root(&tmp, "search", b"same");
    let before = identity_of(&params, &root).expect("decide");

    let other = root.root().join("mcp").join("other");
    fs::create_dir_all(&other).expect("create dir");
    fs::write(other.join("bin"), b"other").expect("write");
    let after = identity_of(&params, &AssetRoot::new(root.root())).expect("decide");

    assert_eq!(
        before, after,
        "changes outside this asset do not change the identity"
    );
}

/// 指到资产目录之外的链接不进指纹
///
/// 链接指向的内容不随包分发，读取链接会让包外的内容决定这条连接的身份
#[cfg(unix)]
#[test]
fn a_link_out_of_the_asset_directory_stays_out_of_the_fingerprint() {
    let tmp = TempDir::new().expect("create temp dir");
    let outside = TempDir::new().expect("create temp dir");
    fs::write(outside.path().join("secret"), b"outside").expect("write");
    let params = ServerParams::Stdio(stdio("./mcp/search/bin", &[]));
    let root = asset_root(&tmp, "search", b"same");
    let before = identity_of(&params, &root).expect("decide");

    std::os::unix::fs::symlink(
        outside.path(),
        root.root().join("mcp").join("search").join("leak"),
    )
    .expect("create the symlink");
    let linked = identity_of(&params, &AssetRoot::new(root.root())).expect("decide");
    assert_eq!(before, linked, "links do not enter the fingerprint");

    fs::write(outside.path().join("secret"), b"changed").expect("write");
    let changed = identity_of(&params, &AssetRoot::new(root.root())).expect("decide");
    assert_eq!(
        before, changed,
        "changes to content outside the package do not change the identity"
    );
}

/// 由系统 PATH 查找或绝对路径启动的命令按完整的命令行判定身份
#[test]
fn a_command_from_the_path_is_identified_by_its_command_line() {
    let root = AssetRoot::new(PathBuf::from("/nonexistent"));
    let cases = [
        stdio("node", &["server.js"]),
        stdio("/usr/bin/env", &["node"]),
        stdio("python3", &["-m", "server"]),
        // 不是包管理器的运行形式
        stdio("uv", &["sync"]),
    ];
    for params in cases {
        let identity = identity_of(&ServerParams::Stdio(params.clone()), &root)
            .unwrap_or_else(|error| panic!("decide the identity of {}: {error}", params.command));
        assert_eq!(
            identity,
            Identity::Command {
                program: params.command.clone(),
                args: params.args.clone(),
            },
            "the key is the full command line"
        );
    }
}

/// 命令行不同就是两条独立连接，相同则是同一条
#[test]
fn two_commands_that_differ_in_their_arguments_are_two_connections() {
    let root = AssetRoot::new(PathBuf::from("/nonexistent"));
    let one = ServerParams::Stdio(stdio("node", &["one.js"]));
    let two = ServerParams::Stdio(stdio("node", &["two.js"]));
    assert_ne!(
        identity_of(&one, &root).expect("decide"),
        identity_of(&two, &root).expect("decide"),
        "different arguments mean two connections"
    );
    let same = ServerParams::Stdio(stdio("node", &["one.js"]));
    assert_eq!(
        identity_of(&one, &root).expect("decide"),
        identity_of(&same, &root).expect("decide"),
        "the same command line shares one connection"
    );
}

#[test]
fn a_remote_service_is_identified_by_its_normalized_url() {
    let root = AssetRoot::new(PathBuf::from("/nonexistent"));
    let one = ServerParams::Streamable(StreamableParams {
        name: "a".into(),
        description: String::new(),
        url: "https://MCP.Example.com/v1/".into(),
    });
    let two = ServerParams::Streamable(StreamableParams {
        name: "b".into(),
        description: String::new(),
        url: "https://mcp.example.com/v1".into(),
    });
    assert_eq!(
        identity_of(&one, &root).expect("decide"),
        identity_of(&two, &root).expect("decide")
    );
}

#[test]
fn normalizing_a_url_drops_the_parts_that_do_not_identify_it() {
    assert_eq!(
        normalize_url("HTTPS://MCP.Example.com:443/v1/?q=1#frag"),
        "https://mcp.example.com/v1"
    );
    assert_eq!(
        normalize_url("https://mcp.example.com/v1#frag"),
        "https://mcp.example.com/v1"
    );
    assert_eq!(
        normalize_url("http://mcp.example.com:80/v1"),
        "http://mcp.example.com/v1"
    );
    assert_eq!(
        normalize_url("http://mcp.example.com:8080/v1"),
        "http://mcp.example.com:8080/v1"
    );
    assert_eq!(
        normalize_url("https://mcp.example.com"),
        "https://mcp.example.com"
    );
}

#[test]
fn a_name_alone_never_decides_the_identity() {
    let root = AssetRoot::new(PathBuf::from("/nonexistent"));
    let mut packaged = stdio("uvx", &["mcp-server-fetch"]);
    packaged.name = "fetch".into();
    let packaged = ServerParams::Stdio(packaged);
    let mut direct = stdio("./mcp/fetch/bin", &[]);
    direct.name = "fetch".into();
    let direct = ServerParams::Stdio(direct);
    let tmp = TempDir::new().expect("create temp dir");
    assert_eq!(
        packaged.name(),
        direct.name(),
        "this assertion assumes both services have the same name"
    );
    assert_ne!(
        identity_of(&packaged, &root).expect("decide"),
        identity_of(&direct, &asset_root(&tmp, "fetch", b"x")).expect("decide"),
        "same name but a different connection method means two independent connections"
    );
}

#[test]
fn a_relative_command_never_leaves_the_asset_root() {
    let root = AssetRoot::new(PathBuf::from("/assets"));
    assert!(
        root.resolve("mcp/search/bin").is_none(),
        "a command that does not start with ./ is not a relative command"
    );
    assert!(
        root.resolve(".//etc/passwd").is_none(),
        "the command contains an absolute path component"
    );
    #[cfg(windows)]
    assert!(
        root.resolve("./C:/Windows/system32/cmd.exe").is_none(),
        "the command contains a drive letter"
    );
    assert!(
        root.resolve("./../secret").is_none(),
        ".. escapes the asset root"
    );
}

#[test]
fn a_relative_command_may_step_back_inside_the_asset_root() {
    let root = AssetRoot::new(PathBuf::from("/assets"));
    assert_eq!(
        root.resolve("./a/../b")
            .expect("resolve .. inside the root"),
        PathBuf::from("/assets").join("b")
    );
}
