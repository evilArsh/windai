//! 资源适配：mod 与内建 MCP 的发布、工具目录与生命周期

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_core::error::CoreError;
use wind_core::resources::ResourceService;
use wind_event::{
    CallCx, CallError, DispatchCx, Dispatcher, EventOutlet, Limits, ModCaller, ModRoster, Origin,
    Pending, Scope, Source, SubmitError, Subscription,
};
use wind_mcp::{McpOptions, RegistryHandle};
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;
use wind_wire::tool::Tools;
use wind_wire::{AppDirs, McpService, ModService, ToolCallParam};

struct Fixture {
    dir: TempDir,
    dirs: AppDirs,
    storage: Storage,
    /// 复制进 mods 根的测试 MCP 服务
    server: PathBuf,
    registry: RegistryHandle,
    services: ResourceService,
}

impl Fixture {
    async fn new() -> Self {
        Self::build_with_dir(TempDir::new().expect("a temporary directory is required")).await
    }

    async fn topic(&self) -> i64 {
        let workspace = self.dir.path().join("workspace");
        self.storage
            .create_topic(
                None,
                "topic",
                None,
                None,
                Some(workspace.to_string_lossy().as_ref()),
            )
            .await
            .expect("the topic must be created")
    }

    fn manifest(&self, name: &str, version: u32, env: Value) -> Value {
        self.manifest_with(name, version, "tools", env)
    }

    fn manifest_with(&self, name: &str, version: u32, local: &str, env: Value) -> Value {
        json!({
            "name": name,
            "version": version,
            "mcpServers": {
                local: {
                    "command": self.server.to_string_lossy(),
                    "env": env,
                }
            }
        })
    }

    async fn build_with_dir(dir: TempDir) -> Self {
        let dirs = AppDirs::under(dir.path().join("app"));
        fs::create_dir_all(dirs.root()).expect("the application directory must be created");
        let server = dirs.mods_root().join("core_test_mcp");
        fs::create_dir_all(dirs.mods_root()).expect("the mods root must be created");
        fs::copy(env!("CARGO_BIN_EXE_core_test_mcp"), &server)
            .expect("the test server must be copied");
        let storage = Storage::open(&dirs.db_file())
            .await
            .expect("the database must open");
        let roster = Arc::new(Roster::load(&storage).await.expect("the roster must load"));
        let bus = Arc::new(Dispatcher::new(
            wind_event::directory()
                .merge(wind_mod::events::directory())
                .merge(wind_mcp::events::directory())
                .merge(wind_core::script::directory()),
            Arc::clone(&roster) as Arc<dyn ModRoster>,
            Arc::new(NoCaller),
            Arc::new(RecordingOutlet),
            Limits::default(),
        ));
        let registry = wind_mcp::init(McpOptions::new(dirs.mods_root()), Arc::clone(&bus));
        let installer = Installer::new(
            storage.clone(),
            Arc::clone(&bus),
            Arc::clone(&roster),
            dirs.clone(),
        );
        let resources = Resources::new(storage.clone(), dirs.clone());
        let services = ResourceService::new(
            installer.clone(),
            resources,
            storage.clone(),
            registry.clone(),
            dirs.topics_root(),
        );
        Self {
            dir,
            dirs,
            storage,
            server,
            registry,
            services,
        }
    }
}

struct RecordingOutlet;

impl EventOutlet for RecordingOutlet {
    fn submit(&self, _pending: Pending) -> Result<(), SubmitError> {
        Ok(())
    }
}

struct NoCaller;

#[async_trait::async_trait]
impl ModCaller for NoCaller {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: Value,
        _cx: CallCx,
    ) -> Result<Option<Value>, CallError> {
        Err(CallError::Internal("mod calls are unavailable".to_string()))
    }
}

fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Global,
        Source::core("test"),
        Duration::from_secs(60),
    )
}

fn write_mod(
    dirs: &AppDirs,
    name: &str,
    version: u32,
    manifest: Value,
    files: &[(&str, &str)],
) -> PathBuf {
    let root = dirs.mod_version(name, version);
    fs::create_dir_all(&root).expect("the package directory must be created");
    fs::write(
        root.join("mod.json"),
        serde_json::to_vec(&manifest).expect("manifest"),
    )
    .expect("the manifest must be written");
    for (relative, content) in files {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("the parent must be created");
        }
        fs::write(path, content).expect("the file must be written");
    }
    root
}

fn tool_name(tool: &Tools) -> &str {
    match tool {
        Tools::Function(function) => function.name.as_str(),
    }
}

fn name_of(server: &str, tool: &str) -> String {
    format!("{server}0m0{tool}")
}

fn is_from(tool: &Tools, server: &str) -> bool {
    tool_name(tool)
        .split_once("0m0")
        .is_some_and(|(declared, _)| declared == server)
}

fn find_tool(tools: &[Tools], server: &str, name: &str) -> String {
    let expected = name_of(server, name);
    tools
        .iter()
        .map(tool_name)
        .find(|declared| *declared == expected)
        .unwrap_or_else(|| panic!("the {server}/{name} tool must be declared: {tools:?}"))
        .to_string()
}

async fn declarations(services: &ResourceService, topic_id: i64) -> Vec<Tools> {
    McpService::list_tools(services, topic_id)
        .await
        .expect("the declarations must be assembled")
}

async fn prompts(services: &ResourceService, topic_id: i64) -> Vec<String> {
    ModService::prompt_fragments(services, topic_id)
        .await
        .expect("the prompts must be assembled")
}

async fn call_tool(
    services: &ResourceService,
    name: &str,
    arguments: Value,
) -> Result<Value, String> {
    call_named(services, name, arguments, CancellationToken::new()).await
}

async fn call_named(
    services: &ResourceService,
    name: &str,
    arguments: Value,
    token: CancellationToken,
) -> Result<Value, String> {
    let (server, tool) = name
        .split_once("0m0")
        .unwrap_or_else(|| panic!("the tool name {name} names no server"));
    McpService::tool_call(
        services,
        tokio::time::Instant::now() + Duration::from_secs(60),
        token,
        ToolCallParam {
            call_id: "call-1".to_string(),
            server: server.to_string(),
            name: tool.to_string(),
            arguments,
        },
    )
    .await
}

async fn wait_tools<F>(services: &ResourceService, topic_id: i64, mut ready: F) -> Vec<Tools>
where
    F: FnMut(&[Tools]) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let tools = declarations(services, topic_id).await;
        if ready(&tools) {
            return tools;
        }
        assert!(
            Instant::now() < deadline,
            "the tools must appear: {tools:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn enable_publishes_mod_tools_and_call_returns_its_value() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest("demo", 1, json!({"WIND_TEST_MARK": "demo"})),
        &[("prompts/one.md", "prompt one")],
    );
    let installed = fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    assert!(installed.affected_topics.is_empty());

    let enabled = fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");
    assert_eq!(enabled.affected_topics, vec![topic_id]);
    assert_eq!(
        prompts(&fixture.services, topic_id).await,
        vec!["prompt one".to_string()]
    );

    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;
    let context = find_tool(&tools, "mod_demo_tools", "context");
    assert_eq!(context, "mod_demo_tools0m0context".to_string());
    let value = call_tool(&fixture.services, &context, json!({}))
        .await
        .expect("the mod tool must answer");
    assert_eq!(value["mark"], json!("demo"));

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn disable_removes_the_tools() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest("demo", 1, json!({"WIND_TEST_MARK": "demo"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");
    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;
    let context = find_tool(&tools, "mod_demo_tools", "context");
    call_tool(&fixture.services, &context, json!({}))
        .await
        .expect("the tool must answer before the change");

    let disabled = fixture
        .services
        .disable(&cx(), topic_id, "demo")
        .await
        .expect("disable");
    assert_eq!(disabled.affected_topics, vec![topic_id]);
    wait_tools(&fixture.services, topic_id, |tools| {
        !tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;
    let error = call_tool(&fixture.services, &context, json!({}))
        .await
        .expect_err("a disabled service must reject new calls");
    assert!(
        error.contains("not running"),
        "the rejection must name the disabled state: {error}"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_declarations_publish_once() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let start_log = fixture.dir.path().join("start.log");
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest(
            "demo",
            1,
            json!({
                "WIND_TEST_MARK": "demo",
                "WIND_TEST_START_LOG": start_log.to_string_lossy(),
            }),
        ),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");

    let barrier = Arc::new(tokio::sync::Barrier::new(10));
    let mut listings = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let services = fixture.services.clone();
        let barrier = barrier.clone();
        listings.spawn(async move {
            barrier.wait().await;
            McpService::list_tools(&services, topic_id).await
        });
    }
    while let Some(result) = listings.join_next().await {
        result
            .expect("the declaration task must not panic")
            .expect("the concurrent declarations must succeed");
    }
    wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;

    let log = fs::read_to_string(&start_log).expect("the start log must exist");
    assert_eq!(log.lines().count(), 1, "the server must start once: {log}");
    assert!(
        !fixture
            .registry
            .list_tools_by_ref(&format!("topic_{topic_id}@1"))
            .await
            .is_empty(),
        "the first generation must hold the tools"
    );
    assert!(
        fixture
            .registry
            .list_tools_by_ref(&format!("topic_{topic_id}@2"))
            .await
            .is_empty(),
        "the topic must publish exactly once"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

async fn wait_mark(services: &ResourceService, tool: &str, expected: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(value) = call_tool(services, tool, json!({})).await
            && value["mark"] == expected
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "the mark of {tool} must become {expected}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_empty(registry: &RegistryHandle, ref_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !registry.list_tools_by_ref(ref_id).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the reference {ref_id} must be released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn skill_sources(value: &Value) -> Vec<String> {
    let mut sources: Vec<String> = value["skills"]
        .as_array()
        .map(|skills| {
            skills
                .iter()
                .filter_map(|skill| skill["source"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    sources.sort();
    sources
}

async fn wait_skills(
    services: &ResourceService,
    tool: &str,
    dir: &str,
    expected: &[&str],
) -> Vec<String> {
    let mut expected: Vec<String> = expected.iter().map(|name| name.to_string()).collect();
    expected.sort();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(value) = call_tool(services, tool, json!({ "dir": dir })).await
            && skill_sources(&value) == expected
        {
            return expected;
        }
        assert!(
            Instant::now() < deadline,
            "the skills must become {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_skills_rejected(services: &ResourceService, tool: &str, dir: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if call_tool(services, tool, json!({ "dir": dir }))
            .await
            .is_err()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the directory {dir} must be rejected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn update_keeps_unchanged_servers_running() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let alpha_log = fixture.dir.path().join("alpha-start.log");
    let alpha = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        fixture.manifest(
            "alpha",
            1,
            json!({
                "WIND_TEST_MARK": "alpha",
                "WIND_TEST_START_LOG": alpha_log.to_string_lossy(),
            }),
        ),
        &[],
    );
    let beta_v1 = write_mod(
        &fixture.dirs,
        "beta",
        1,
        fixture.manifest("beta", 1, json!({"WIND_TEST_MARK": "beta-v1"})),
        &[],
    );
    let beta_v2 = write_mod(
        &fixture.dirs,
        "beta",
        2,
        fixture.manifest("beta", 2, json!({"WIND_TEST_MARK": "beta-v2"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &alpha)
        .await
        .expect("install alpha");
    fixture
        .services
        .install(&cx(), &beta_v1)
        .await
        .expect("install beta");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable alpha");
    fixture
        .services
        .enable(&cx(), topic_id, "beta")
        .await
        .expect("enable beta");

    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_alpha_tools"))
            && tools.iter().any(|tool| is_from(tool, "mod_beta_tools"))
    })
    .await;
    let alpha_context = find_tool(&tools, "mod_alpha_tools", "context");
    let beta_context = find_tool(&tools, "mod_beta_tools", "context");
    wait_mark(&fixture.services, &beta_context, "beta-v1").await;

    fixture
        .services
        .update(&cx(), &beta_v2)
        .await
        .expect("update beta");
    wait_mark(&fixture.services, &beta_context, "beta-v2").await;

    let value = wait_mark(&fixture.services, &alpha_context, "alpha").await;
    assert_eq!(value["mark"], json!("alpha"));
    let log = fs::read_to_string(&alpha_log).expect("the alpha start log must exist");
    assert_eq!(
        log.lines().count(),
        1,
        "an unchanged server must not restart: {log}"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn duplicate_constructed_names_keep_the_first() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let first = write_mod(
        &fixture.dirs,
        "ab_c",
        1,
        fixture.manifest_with("ab_c", 1, "d", json!({"WIND_TEST_MARK": "ab_c"})),
        &[],
    );
    let second = write_mod(
        &fixture.dirs,
        "ab",
        1,
        fixture.manifest_with("ab", 1, "c_d", json!({"WIND_TEST_MARK": "ab"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &first)
        .await
        .expect("install ab_c");
    fixture
        .services
        .install(&cx(), &second)
        .await
        .expect("install ab");
    fixture
        .services
        .enable(&cx(), topic_id, "ab")
        .await
        .expect("enable ab");
    fixture
        .services
        .enable(&cx(), topic_id, "ab_c")
        .await
        .expect("enable ab_c");

    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_ab_c_d"))
    })
    .await;
    let contexts: Vec<&str> = tools
        .iter()
        .map(tool_name)
        .filter(|name| name.starts_with(&name_of("mod_ab_c_d", "context")))
        .collect();
    assert_eq!(
        contexts.len(),
        1,
        "the duplicate service must be declared once: {tools:?}"
    );
    let value = call_tool(&fixture.services, contexts[0], json!({}))
        .await
        .expect("the kept service must answer");
    assert_eq!(
        value["mark"],
        json!("ab"),
        "the first mod in name order must win"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_failed_mod_service_does_not_block_other_tools() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let missing = fixture.dirs.mods_root().join("missing-server");
    let broken = write_mod(
        &fixture.dirs,
        "broken",
        1,
        json!({
            "name": "broken",
            "version": 1,
            "mcpServers": {
                "tools": {"command": missing.to_string_lossy()}
            }
        }),
        &[],
    );
    let good = write_mod(
        &fixture.dirs,
        "good",
        1,
        fixture.manifest("good", 1, json!({"WIND_TEST_MARK": "good"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &broken)
        .await
        .expect("install broken");
    fixture
        .services
        .install(&cx(), &good)
        .await
        .expect("install good");
    fixture
        .services
        .enable(&cx(), topic_id, "broken")
        .await
        .expect("enable broken");
    fixture
        .services
        .enable(&cx(), topic_id, "good")
        .await
        .expect("enable good");

    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_good_tools"))
    })
    .await;
    let context = find_tool(&tools, "mod_good_tools", "context");
    let value = call_tool(&fixture.services, &context, json!({}))
        .await
        .expect("the healthy service must answer");
    assert_eq!(value["mark"], json!("good"));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let tools = declarations(&fixture.services, topic_id).await;
    assert!(
        !tools.iter().any(|tool| is_from(tool, "mod_broken_tools")),
        "a failed service must not be declared: {tools:?}"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn builtin_fs_serves_the_workspace_and_enabled_mod_roots() {
    let fixture = Fixture::new().await;
    let workspace = fixture.dir.path().join("workspace");
    fs::create_dir_all(&workspace).expect("the workspace must be created");
    fs::write(workspace.join("note.txt"), "hello").expect("the note must be written");
    let outside = fixture.dir.path().join("outside");
    fs::create_dir_all(&outside).expect("the outside directory must be created");
    fs::write(outside.join("secret.txt"), "secret").expect("the secret must be written");
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        json!({"name": "demo", "version": 1}),
        &[(
            "skills/alpha/SKILL.md",
            "---\nname: alpha\ndescription: The alpha skill.\n---\n",
        )],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");

    let server = format!("wd_fs_{topic_id}");
    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, &server))
    })
    .await;
    let list = find_tool(&tools, &server, "list_dir");
    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": workspace.to_string_lossy()}),
    )
    .await
    .expect("the workspace must be listable");
    assert!(
        value["error"].is_null(),
        "the workspace listing must succeed: {value}"
    );
    assert!(
        value["entries"]
            .as_array()
            .is_some_and(|entries| entries.iter().any(|entry| entry["name"] == "note.txt")),
        "the listing must include the note: {value}"
    );

    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": outside.to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer with a failure result");
    assert!(
        value["error"].is_string(),
        "a path outside the workspace must be rejected: {value}"
    );

    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": fixture.dirs.mod_version("demo", 1).to_string_lossy()}),
    )
    .await
    .expect("the enabled mod root must be listable");
    assert!(
        value["error"].is_null(),
        "the enabled mod root listing must succeed: {value}"
    );

    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": fixture.dirs.mods_root().to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer with a failure result");
    assert!(
        value["error"].is_string(),
        "the mods root itself must be rejected: {value}"
    );

    let sibling = fixture.dirs.topics_root().join("other");
    fs::create_dir_all(&sibling).expect("the sibling topic directory must be created");
    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": sibling.to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer with a failure result");
    assert!(
        value["error"].is_string(),
        "another topic's directory must be rejected: {value}"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn builtin_workspace_lists_only_the_enabled_mods() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let alpha = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        json!({"name": "alpha", "version": 1}),
        &[(
            "skills/alpha/SKILL.md",
            "---\nname: alpha\ndescription: The alpha skill.\n---\n",
        )],
    );
    let beta = write_mod(
        &fixture.dirs,
        "beta",
        1,
        json!({"name": "beta", "version": 1}),
        &[],
    );
    for package in [&alpha, &beta] {
        fixture
            .services
            .install(&cx(), package)
            .await
            .expect("install the package");
    }
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable alpha");

    let workspace =
        fs::canonicalize(fixture.dir.path().join("workspace")).expect("the workspace must exist");
    let alpha = fs::canonicalize(&alpha).expect("the alpha root must exist");
    let beta = fs::canonicalize(&beta).expect("the beta root must exist");
    let server = format!("wd_workspace_{topic_id}");
    let fs_server = format!("wd_fs_{topic_id}");

    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, &server))
    })
    .await;
    let info = find_tool(&tools, &server, "workspace_info");
    let value = call_tool(&fixture.services, &info, json!({}))
        .await
        .expect("the workspace facts must be readable");
    let facts = value;
    assert_eq!(facts["workspace"], json!(workspace.to_string_lossy()));
    let mods = facts["mods"].as_array().expect("mods must be an array");
    assert_eq!(mods.len(), 1, "only the enabled mod may be listed: {facts}");
    assert_eq!(mods[0]["name"], json!("alpha"));
    assert_eq!(mods[0]["root"], json!(alpha.to_string_lossy()));
    let roots = facts["roots"].as_array().expect("roots must be an array");
    assert!(roots.contains(&json!(alpha.to_string_lossy())));
    assert!(!roots.contains(&json!(beta.to_string_lossy())));
    let skills = facts["skills"].as_array().expect("skills must be an array");
    assert!(
        skills
            .iter()
            .any(|skill| skill["source"] == json!("public"))
    );
    assert!(skills.iter().any(|skill| skill["source"] == json!("alpha")));

    let fs_tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, &fs_server))
    })
    .await;
    let list = find_tool(&fs_tools, &fs_server, "list_dir");
    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": alpha.to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer");
    assert!(
        value["error"].is_null(),
        "the enabled mod root must be listable: {value}"
    );
    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": beta.to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer");
    assert!(
        value["error"].is_string(),
        "a disabled mod root must be rejected: {value}"
    );

    fixture
        .services
        .enable(&cx(), topic_id, "beta")
        .await
        .expect("enable beta");
    let tools = declarations(&fixture.services, topic_id).await;
    let info = find_tool(&tools, &server, "workspace_info");
    let value = call_tool(&fixture.services, &info, json!({}))
        .await
        .expect("the workspace facts must be readable");
    assert_eq!(
        value["mods"]
            .as_array()
            .expect("mods must be an array")
            .len(),
        2,
        "the newly enabled mod must appear: {value}"
    );
    let list = find_tool(&tools, &fs_server, "list_dir");
    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": beta.to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer");
    assert!(
        value["error"].is_null(),
        "the newly enabled mod root must be listable: {value}"
    );

    fixture
        .services
        .disable(&cx(), topic_id, "beta")
        .await
        .expect("disable beta");
    let tools = declarations(&fixture.services, topic_id).await;
    let info = find_tool(&tools, &server, "workspace_info");
    let value = call_tool(&fixture.services, &info, json!({}))
        .await
        .expect("the workspace facts must be readable");
    assert_eq!(
        value["mods"]
            .as_array()
            .expect("mods must be an array")
            .len(),
        1,
        "the disabled mod must disappear: {value}"
    );
    let list = find_tool(&tools, &fs_server, "list_dir");
    let value = call_tool(
        &fixture.services,
        &list,
        json!({"path": beta.to_string_lossy()}),
    )
    .await
    .expect("the sandbox must answer");
    assert!(
        value["error"].is_string(),
        "the disabled mod root must be rejected: {value}"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn builtin_skills_rebuilds_when_mods_change() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let alpha = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        json!({"name": "alpha", "version": 1}),
        &[(
            "skills/alpha/SKILL.md",
            "---\nname: alpha\ndescription: The alpha skill.\n---\n",
        )],
    );
    let beta = write_mod(
        &fixture.dirs,
        "beta",
        1,
        json!({"name": "beta", "version": 1}),
        &[(
            "skills/beta/SKILL.md",
            "---\nname: beta\ndescription: The beta skill.\n---\n",
        )],
    );
    fixture
        .services
        .install(&cx(), &alpha)
        .await
        .expect("install alpha");
    fixture
        .services
        .install(&cx(), &beta)
        .await
        .expect("install beta");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable alpha");

    let server = format!("wd_skills_{topic_id}");
    let alpha_root = fs::canonicalize(fixture.dirs.mod_version("alpha", 1).join("skills"))
        .expect("the alpha skills root must exist");
    let beta_root = fs::canonicalize(fixture.dirs.mod_version("beta", 1).join("skills"))
        .expect("the beta skills root must exist");
    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, &server))
    })
    .await;
    let skill_tool = find_tool(&tools, &server, "skills_list");
    wait_skills(
        &fixture.services,
        &skill_tool,
        &alpha_root.to_string_lossy(),
        &["alpha"],
    )
    .await;
    assert!(
        call_tool(
            &fixture.services,
            &skill_tool,
            json!({"dir": beta_root.to_string_lossy()})
        )
        .await
        .is_err(),
        "the beta root must be rejected before beta is enabled"
    );

    fixture
        .services
        .enable(&cx(), topic_id, "beta")
        .await
        .expect("enable beta");
    wait_skills(
        &fixture.services,
        &skill_tool,
        &beta_root.to_string_lossy(),
        &["beta"],
    )
    .await;
    wait_skills(
        &fixture.services,
        &skill_tool,
        &alpha_root.to_string_lossy(),
        &["alpha"],
    )
    .await;

    fixture
        .services
        .disable(&cx(), topic_id, "beta")
        .await
        .expect("disable beta");
    wait_skills_rejected(&fixture.services, &skill_tool, &beta_root.to_string_lossy()).await;
    wait_skills(
        &fixture.services,
        &skill_tool,
        &alpha_root.to_string_lossy(),
        &["alpha"],
    )
    .await;
    assert!(
        beta_root.is_dir(),
        "the beta root must stay on disk after disabling"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cancelled_tool_call_stops_waiting() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest("demo", 1, json!({"WIND_TEST_MARK": "demo"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");
    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;
    let wait = find_tool(&tools, "mod_demo_tools", "wait");

    let cancel = CancellationToken::new();
    let calling = tokio::spawn({
        let services = fixture.services.clone();
        let cancel = cancel.clone();
        let wait = wait.clone();
        async move { call_named(&services, &wait, json!({"millis": 30_000}), cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), calling)
        .await
        .expect("the cancellation must end the wait")
        .expect("the call task must not panic");
    let error = result.expect_err("a cancelled call must fail");
    assert!(
        error.to_string().contains("cancel"),
        "the error must report the cancellation: {error}"
    );

    let tools = declarations(&fixture.services, topic_id).await;
    let context = find_tool(&tools, "mod_demo_tools", "context");
    call_tool(&fixture.services, &context, json!({}))
        .await
        .expect("the connection must stay usable");
    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn release_topic_and_shutdown_close_runtime_state() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest("demo", 1, json!({"WIND_TEST_MARK": "demo"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");
    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;
    let context = find_tool(&tools, "mod_demo_tools", "context");

    fixture
        .services
        .release_topic(topic_id)
        .await
        .expect("release");
    wait_empty(&fixture.registry, &format!("wd_fs_{topic_id}")).await;
    wait_empty(&fixture.registry, &format!("topic_{topic_id}@1")).await;
    assert!(
        call_tool(&fixture.services, &context, json!({}))
            .await
            .is_err(),
        "a released topic must reject new calls"
    );

    fixture.services.shutdown().await.expect("shutdown");
    let error = McpService::list_tools(&fixture.services, topic_id)
        .await
        .expect_err("a closed adapter must reject listings");
    assert!(error.contains("closed"), "{error}");
    let topic = fixture.topic().await;
    let error = fixture
        .services
        .enable(&cx(), topic, "demo")
        .await
        .expect_err("a closed adapter must reject management");
    assert!(matches!(error, CoreError::ResourcesClosed), "{error}");
}

#[tokio::test]
async fn release_topic_then_declare_republishes_the_tools() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest("demo", 1, json!({"WIND_TEST_MARK": "demo"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");
    wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;

    fixture
        .services
        .release_topic(topic_id)
        .await
        .expect("release");
    wait_empty(&fixture.registry, &format!("topic_{topic_id}@1")).await;

    let tools = wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;
    let context = find_tool(&tools, "mod_demo_tools", "context");
    let value = call_tool(&fixture.services, &context, json!({}))
        .await
        .expect("a redeclared topic must answer again");
    assert_eq!(value["mark"], json!("demo"));

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_closes_declarations_without_a_release() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "demo",
        1,
        fixture.manifest("demo", 1, json!({"WIND_TEST_MARK": "demo"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "demo")
        .await
        .expect("enable");
    wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;

    fixture.services.shutdown().await.expect("shutdown");
    let error = McpService::list_tools(&fixture.services, topic_id)
        .await
        .expect_err("a closed adapter must reject listings");
    assert!(error.contains("closed"), "{error}");
}

#[tokio::test]
async fn a_failed_topic_does_not_block_other_topics() {
    let fixture = Fixture::new().await;
    let failed_topic = fixture.topic().await;
    let healthy_topic = fixture.topic().await;
    let alpha = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        fixture.manifest("alpha", 1, json!({"WIND_TEST_MARK": "alpha"})),
        &[],
    );
    let beta_v1 = write_mod(
        &fixture.dirs,
        "beta",
        1,
        fixture.manifest("beta", 1, json!({"WIND_TEST_MARK": "beta-v1"})),
        &[],
    );
    let beta_v2 = write_mod(
        &fixture.dirs,
        "beta",
        2,
        fixture.manifest("beta", 2, json!({"WIND_TEST_MARK": "beta-v2"})),
        &[],
    );
    for package in [&alpha, &beta_v1] {
        fixture
            .services
            .install(&cx(), package)
            .await
            .expect("install");
    }
    fixture
        .services
        .enable(&cx(), failed_topic, "alpha")
        .await
        .expect("enable alpha");
    fixture
        .services
        .enable(&cx(), failed_topic, "beta")
        .await
        .expect("enable beta");
    fixture
        .services
        .enable(&cx(), healthy_topic, "beta")
        .await
        .expect("enable beta");

    let tools = wait_tools(&fixture.services, healthy_topic, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_beta_tools"))
    })
    .await;
    let beta_context = find_tool(&tools, "mod_beta_tools", "context");
    wait_mark(&fixture.services, &beta_context, "beta-v1").await;

    fs::remove_dir_all(fixture.dirs.mod_version("alpha", 1))
        .expect("the alpha version directory must be removable");

    let result = fixture.services.update(&cx(), &beta_v2).await;
    assert!(
        result.is_err(),
        "the failed topic must be reported: {result:?}"
    );
    wait_mark(&fixture.services, &beta_context, "beta-v2").await;

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restore_works_without_a_script_service() {
    let fixture = Fixture::new().await;
    fixture.services.restore().await.expect("restore");
}
