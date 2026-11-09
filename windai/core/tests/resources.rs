//! 资源适配：mod 与内建 MCP 的发布、工具目录与生命周期

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use wind_core::error::CoreError;
use wind_core::llm::tools::{DeclaredTool, McpTarget, ToolOrigin};
use wind_core::llm::{McpService as LlmMcpService, ModService};
use wind_core::resources::ResourceServices;
use wind_event::{
    CallCx, CallError, DispatchCx, Dispatcher, EventOutlet, Limits, ModCaller, ModRoster, Origin,
    Pending, Scope, Source, SubmitError, Subscription,
};
use wind_mcp::McpOptions;
use wind_mod::dirs::AppDirs;
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;

struct Fixture {
    dir: TempDir,
    dirs: AppDirs,
    storage: Storage,
    /// 复制进 mods 根的测试 MCP 服务
    server: PathBuf,
    services: ResourceServices,
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
        json!({
            "name": name,
            "version": version,
            "mcpServers": {
                "tools": {
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
                .merge(wind_mcp::events::directory()),
            Arc::clone(&roster) as Arc<dyn ModRoster>,
            Arc::new(NoCaller),
            Arc::new(RecordingOutlet::default()),
            Limits::default(),
        ));
        let registry = wind_mcp::init(McpOptions::new(dirs.mods_root()), Arc::clone(&bus));
        let installer = Installer::new(
            storage.clone(),
            Arc::clone(&bus),
            Arc::clone(&roster),
            dirs.clone(),
            None,
        );
        let resources = Resources::new(storage.clone(), dirs.clone());
        let services = ResourceServices::new(
            installer.clone(),
            resources,
            storage.clone(),
            registry,
            dirs.topics_root(),
        );
        Self {
            dir,
            dirs,
            storage,
            server,
            services,
        }
    }
}

#[derive(Default)]
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

fn target(tool: &DeclaredTool) -> &McpTarget {
    match &tool.origin {
        ToolOrigin::Mcp { target } => target,
        other => panic!("the tool must be an mcp tool: {other:?}"),
    }
}

fn declaration_name(tool: &DeclaredTool) -> String {
    match &tool.declaration {
        wind_wire::tool::Tools::Function(function) => function.name.clone(),
    }
}

fn is_from(tool: &DeclaredTool, server: &str) -> bool {
    matches!(&tool.origin, ToolOrigin::Mcp { target } if target.server == server)
}

fn find_tool<'a>(tools: &'a [DeclaredTool], server: &str, name: &str) -> &'a DeclaredTool {
    tools
        .iter()
        .find(|tool| {
            matches!(
                &tool.origin,
                ToolOrigin::Mcp { target }
                    if target.server == server && target.tool == name
            )
        })
        .unwrap_or_else(|| panic!("the {server}/{name} tool must be declared: {tools:?}"))
}

async fn declarations(services: &ResourceServices, topic_id: i64) -> Vec<DeclaredTool> {
    LlmMcpService::declarations(services, &cx(), topic_id)
        .await
        .expect("the declarations must be assembled")
}

async fn prompts(services: &ResourceServices, topic_id: i64) -> Vec<String> {
    ModService::prompt_fragments(services, topic_id)
        .await
        .expect("the prompts must be assembled")
}

async fn call_tool(
    services: &ResourceServices,
    target: &McpTarget,
    arguments: Value,
) -> Result<Value, CoreError> {
    LlmMcpService::call(services, &cx(), "call-1", target, arguments).await
}

async fn wait_tools<F>(
    services: &ResourceServices,
    topic_id: i64,
    mut ready: F,
) -> Vec<DeclaredTool>
where
    F: FnMut(&[DeclaredTool]) -> bool,
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

fn structured(value: &Value) -> &Value {
    &value["structuredContent"]
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
    assert_eq!(
        declaration_name(context),
        "mod_demo_tools0m0context".to_string()
    );
    let value = call_tool(&fixture.services, target(context), json!({}))
        .await
        .expect("the mod tool must answer");
    assert_eq!(structured(&value)["mark"], json!("demo"));

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
    let context = target(find_tool(&tools, "mod_demo_tools", "context")).clone();
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
        matches!(error, CoreError::Denied(_)),
        "the rejection must name the disabled state: {error}"
    );

    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
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

    let listings: Vec<_> = (0..10)
        .map(|_| {
            let services = fixture.services.clone();
            async move { LlmMcpService::declarations(&services, &cx(), topic_id).await }
        })
        .collect();
    for result in futures::future::join_all(listings).await {
        result.expect("the concurrent declarations must succeed");
    }
    wait_tools(&fixture.services, topic_id, |tools| {
        tools.iter().any(|tool| is_from(tool, "mod_demo_tools"))
    })
    .await;

    let log = fs::read_to_string(&start_log).expect("the start log must exist");
    assert_eq!(log.lines().count(), 1, "the server must start once: {log}");

    fixture.services.shutdown().await.expect("shutdown");
}
