//! 资源适配：mod 与独立 MCP 配置的发布、工具目录与生命周期

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use wind_core::error::CoreError;
use wind_core::llm::tools::{DeclaredTool, ToolCatalog, ToolOrigin, execute_plan};
use wind_core::llm::{McpService as LlmMcpService, ModService};
use wind_core::resources::ResourceServices;
use wind_event::{
    CallCx, CallError, DispatchCx, Dispatcher, EventOutlet, Limits, ModCaller, ModRoster, Origin,
    Pending, Scope, Source, SubmitError, Subscription,
};
use wind_mcp::{
    BuiltinRoots, Connector, McpOptions, McpService as McpManager, Registry, ToolTarget,
};
use wind_mod::Compiler;
use wind_mod::dirs::AppDirs;
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;
use wind_wire::tool::{FunctionCall, FunctionTool, Tools};
use wind_wire::{CompiledScripts, McpConfig, McpDefinition};

struct Fixture {
    dir: TempDir,
    dirs: AppDirs,
    storage: Storage,
    roster: Arc<Roster>,
    bus: Arc<Dispatcher>,
    installer: Installer,
    registry: Arc<Registry>,
    services: ResourceServices,
    compiler: Option<Arc<dyn Compiler>>,
}

impl Fixture {
    async fn new() -> Self {
        Self::build(None).await
    }

    async fn with_compiler(compiler: Arc<dyn Compiler>) -> Self {
        Self::build(Some(compiler)).await
    }

    async fn build(compiler: Option<Arc<dyn Compiler>>) -> Self {
        Self::build_with_dir(
            TempDir::new().expect("a temporary directory is required"),
            compiler,
        )
        .await
    }

    async fn topic(&self) -> i64 {
        self.storage
            .create_topic(None, "topic", None, None)
            .await
            .expect("the topic must be created")
    }

    async fn reopen(self) -> Self {
        self.services.shutdown().await.expect("shutdown");
        let Fixture {
            dir,
            storage,
            roster,
            bus,
            installer,
            registry,
            services,
            compiler,
            ..
        } = self;
        drop(services);
        drop(registry);
        drop(installer);
        drop(bus);
        drop(roster);
        drop(storage);
        let next = Self::build_with_dir(dir, compiler).await;
        next.services
            .restore()
            .await
            .expect("the installer must restore");
        next
    }

    async fn build_with_dir(dir: TempDir, compiler: Option<Arc<dyn Compiler>>) -> Self {
        let dirs = AppDirs::under(dir.path().join("app"));
        fs::create_dir_all(dirs.root()).expect("the application directory must be created");
        let storage = Storage::open(&dirs.db_file())
            .await
            .expect("the database must reopen");
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
        let registry = Arc::new(Registry::new(
            Connector::from_host(McpOptions::default()),
            Arc::clone(&bus),
        ));
        let installer = Installer::new(
            storage.clone(),
            Arc::clone(&bus),
            Arc::clone(&roster),
            dirs.clone(),
            compiler.clone(),
        );
        let resources = Resources::new(storage.clone(), dirs.clone());
        let manager = McpManager::new(storage.clone(), Arc::clone(&registry));
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(&workspace).expect("the workspace must exist");
        let services = ResourceServices::new(
            installer.clone(),
            resources,
            manager,
            Arc::clone(&registry),
            BuiltinRoots {
                workspace: Some(workspace),
                skills: Vec::new(),
            },
        );
        Self {
            dir,
            dirs,
            storage,
            roster,
            bus,
            installer,
            registry,
            services,
            compiler,
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

struct Engine;

impl Compiler for Engine {
    fn compile(
        &self,
        _root: &Path,
        _handlers: &HashSet<String>,
    ) -> Result<Option<CompiledScripts>, String> {
        Ok(Some(CompiledScripts {
            modules: HashMap::from([("main".to_string(), vec![1])]),
            fingerprint: "test".to_string(),
        }))
    }
}

struct NoMods;

#[async_trait::async_trait]
impl ModService for NoMods {
    async fn prompt_fragments(&self, _topic_id: i64) -> Result<Vec<String>, CoreError> {
        Ok(Vec::new())
    }

    async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(Vec::new())
    }

    async fn version_of(&self, _mod_name: &str) -> Result<i64, CoreError> {
        Ok(0)
    }

    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        _input: Value,
    ) -> Result<Value, CoreError> {
        Err(CoreError::Denied("no mod is available".to_string()))
    }
}

#[derive(Default)]
struct RecordingMcp {
    calls: Mutex<Vec<ToolTarget>>,
}

#[async_trait::async_trait]
impl LlmMcpService for RecordingMcp {
    async fn declarations(
        &self,
        _cx: &DispatchCx,
        _topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(Vec::new())
    }

    async fn call(
        &self,
        _cx: &DispatchCx,
        _call_id: &str,
        target: &ToolTarget,
        arguments: Value,
    ) -> Result<Value, CoreError> {
        self.calls.lock().expect("call log").push(target.clone());
        Ok(json!({ "arguments": arguments }))
    }
}

fn server_bin() -> String {
    env!("CARGO_BIN_EXE_core_test_mcp").to_string()
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

fn mod_manifest(name: &str, version: u32, env: Value) -> Value {
    json!({
        "name": name,
        "version": version,
        "mcpServers": {
            "tools": {
                "command": server_bin(),
                "env": env,
            }
        }
    })
}

fn configured(name: &str, mark: &str, cwd: &Path) -> McpDefinition {
    fs::create_dir_all(cwd).expect("the cwd must be created");
    McpDefinition {
        name: name.to_string(),
        description: Some("a configured test server".to_string()),
        config: McpConfig::Stdio {
            command: server_bin(),
            args: Vec::new(),
            env: BTreeMap::from([("WIND_TEST_MARK".to_string(), mark.to_string())]),
            cwd: cwd.to_path_buf(),
        },
    }
}

fn target(tool: &DeclaredTool) -> &ToolTarget {
    match &tool.origin {
        ToolOrigin::Mcp { target } => target,
        other => panic!("the tool must be an mcp tool: {other:?}"),
    }
}

fn declaration_name(tool: &DeclaredTool) -> String {
    match &tool.declaration {
        Tools::Function(function) => function.name.clone(),
    }
}

fn find_tool<'a>(tools: &'a [DeclaredTool], source: &str, name: &str) -> &'a DeclaredTool {
    tools
        .iter()
        .find(|tool| {
            matches!(
                &tool.origin,
                ToolOrigin::Mcp { target }
                    if target.source == source && target.tool == name
            )
        })
        .unwrap_or_else(|| panic!("the {source}/{name} tool must be declared: {tools:?}"))
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

async fn mcp_id(services: &ResourceServices, name: &str) -> i64 {
    services
        .mcp_servers()
        .await
        .expect("the configured servers must be listed")
        .into_iter()
        .find(|record| record.definition.name == name)
        .unwrap_or_else(|| panic!("the {name} server must exist"))
        .id
}

async fn call_tool(
    services: &ResourceServices,
    target: &ToolTarget,
    arguments: Value,
) -> Result<Value, CoreError> {
    LlmMcpService::call(services, &cx(), "call-1", target, arguments).await
}

fn structured(value: &Value) -> &Value {
    &value["structuredContent"]
}

async fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(Instant::now() < deadline, "the file must appear");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn sample_target(source: &str, key: &str, tool: &str) -> ToolTarget {
    ToolTarget {
        topic_id: 1,
        source: source.to_string(),
        key: key.to_string(),
        revision: 1,
        generation: 1,
        tool: tool.to_string(),
    }
}

fn declared(target: ToolTarget) -> DeclaredTool {
    let origin = ToolOrigin::Mcp { target };
    DeclaredTool {
        declaration: Tools::Function(FunctionTool {
            name: origin.exposed(),
            description: Some("a test tool".to_string()),
            parameters: Some(json!({"type": "object"})),
            strict: None,
        }),
        origin,
    }
}

#[tokio::test]
async fn resources_follow_the_full_lifecycle_and_survive_a_restart() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let first = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "v1"})),
        &[("prompts/one.md", "prompt one")],
    );
    let installed = fixture
        .services
        .install(&cx(), &first)
        .await
        .expect("install");
    assert!(installed.affected_topics.is_empty());
    assert!(installed.sync_errors.is_empty());
    let enabled = fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");
    assert_eq!(enabled.affected_topics, vec![topic_id]);
    assert!(enabled.sync_errors.is_empty());
    assert_eq!(
        prompts(&fixture.services, topic_id).await,
        vec!["prompt one".to_string()]
    );

    fixture
        .services
        .create_mcp(&configured("tools", "configured", &fixture.dirs.root()))
        .await
        .expect("create mcp");
    let configured_id = mcp_id(&fixture.services, "tools").await;
    let enabled_config = fixture
        .services
        .enable_mcp(topic_id, configured_id)
        .await
        .expect("enable mcp");
    assert_eq!(enabled_config.affected_topics, vec![topic_id]);

    let tools = declarations(&fixture.services, topic_id).await;
    let mod_context = find_tool(&tools, "mods", "context");
    let config_context = find_tool(&tools, "configured", "context");
    find_tool(&tools, "builtin", "list_dir");
    assert_eq!(target(mod_context).revision, 1);
    assert_eq!(target(config_context).revision, 1);
    let old_mod = target(mod_context).clone();
    let mod_value = call_tool(&fixture.services, target(mod_context), json!({}))
        .await
        .expect("the mod tool must answer");
    assert_eq!(structured(&mod_value)["mark"], json!("v1"));
    let config_value = call_tool(&fixture.services, target(config_context), json!({}))
        .await
        .expect("the configured tool must answer");
    assert_eq!(structured(&config_value)["mark"], json!("configured"));

    let second = write_mod(
        &fixture.dirs,
        "alpha",
        2,
        mod_manifest("alpha", 2, json!({"WIND_TEST_MARK": "v2"})),
        &[("prompts/one.md", "prompt two")],
    );
    let updated = fixture
        .services
        .update(&cx(), &second)
        .await
        .expect("update");
    assert_eq!(updated.affected_topics, vec![topic_id]);
    assert!(updated.sync_errors.is_empty());
    assert_eq!(
        fixture
            .installer
            .get("alpha")
            .await
            .expect("query")
            .expect("installed")
            .version,
        2
    );
    assert_eq!(
        prompts(&fixture.services, topic_id).await,
        vec!["prompt two".to_string()]
    );
    let tools = declarations(&fixture.services, topic_id).await;
    let fresh_mod = find_tool(&tools, "mods", "context");
    assert_eq!(target(fresh_mod).revision, 2);
    assert!(target(fresh_mod).generation > old_mod.generation);
    assert!(
        call_tool(&fixture.services, &old_mod, json!({}))
            .await
            .is_err(),
        "an old target must not reach the new revision"
    );
    let fresh_value = call_tool(&fixture.services, target(fresh_mod), json!({}))
        .await
        .expect("the new revision must answer");
    assert_eq!(structured(&fresh_value)["mark"], json!("v2"));

    let disabled = fixture
        .services
        .disable(&cx(), topic_id, "alpha")
        .await
        .expect("disable");
    assert_eq!(disabled.affected_topics, vec![topic_id]);
    assert!(prompts(&fixture.services, topic_id).await.is_empty());
    let tools = declarations(&fixture.services, topic_id).await;
    assert!(
        tools.iter().all(|tool| target(tool).source != "mods"),
        "a disabled mod must not be listed"
    );
    assert!(
        call_tool(&fixture.services, target(fresh_mod), json!({}))
            .await
            .is_err(),
        "a disabled binding must reject new calls"
    );

    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("a disabled mod must be re-enableable");
    let deleted = fixture
        .services
        .delete(&cx(), "alpha")
        .await
        .expect("delete");
    assert_eq!(deleted.affected_topics, vec![topic_id]);
    assert!(
        fixture
            .installer
            .get("alpha")
            .await
            .expect("query")
            .is_none()
    );
    let tools = declarations(&fixture.services, topic_id).await;
    find_tool(&tools, "configured", "context");

    let reopened = fixture.reopen().await;
    assert!(
        reopened.services.status(topic_id).is_empty(),
        "a restart must not connect historical topics before a query"
    );
    let tools = declarations(&reopened.services, topic_id).await;
    let config_context = find_tool(&tools, "configured", "context");
    let value = call_tool(&reopened.services, target(config_context), json!({}))
        .await
        .expect("the configured tool must survive a restart");
    assert_eq!(structured(&value)["mark"], json!("configured"));
    assert!(
        tools.iter().all(|tool| target(tool).source != "mods"),
        "a deleted mod must stay deleted"
    );
    reopened.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn same_server_names_in_two_sources_route_independently() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "from-mod"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");
    fixture
        .services
        .create_mcp(&configured("tools", "from-config", &fixture.dirs.root()))
        .await
        .expect("create mcp");
    let configured_id = mcp_id(&fixture.services, "tools").await;
    fixture
        .services
        .enable_mcp(topic_id, configured_id)
        .await
        .expect("enable mcp");

    let tools = declarations(&fixture.services, topic_id).await;
    let from_mod = find_tool(&tools, "mods", "context");
    let from_config = find_tool(&tools, "configured", "context");
    assert_ne!(declaration_name(from_mod), declaration_name(from_config));
    assert_eq!(target(from_mod).tool, target(from_config).tool);

    let mod_value = call_tool(&fixture.services, target(from_mod), json!({}))
        .await
        .expect("the mod tool must answer");
    assert_eq!(structured(&mod_value)["mark"], json!("from-mod"));
    let config_value = call_tool(&fixture.services, target(from_config), json!({}))
        .await
        .expect("the configured tool must answer");
    assert_eq!(structured(&config_value)["mark"], json!("from-config"));

    let description = match &from_mod.declaration {
        Tools::Function(function) => function.description.clone().unwrap_or_default(),
    };
    assert!(description.contains("mods"));
    assert!(description.contains("context"));
    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_during_sync_never_returns_stale_tools() {
    let fixture = Fixture::new().await;
    let gate = fixture.dir.path().join("list-gate");
    let list_log = fixture.dir.path().join("list.log");
    let first = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest(
            "alpha",
            1,
            json!({
                "WIND_TEST_MARK": "v1",
                "WIND_TEST_LIST_GATE": gate.to_string_lossy(),
                "WIND_TEST_LIST_LOG": list_log.to_string_lossy(),
            }),
        ),
        &[],
    );
    let second = write_mod(
        &fixture.dirs,
        "alpha",
        2,
        mod_manifest("alpha", 2, json!({"WIND_TEST_MARK": "v2"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &first)
        .await
        .expect("install");
    let topic_id = fixture.topic().await;
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");

    let listing = tokio::spawn({
        let services = fixture.services.clone();
        async move { declarations(&services, topic_id).await }
    });
    wait_for_file(&list_log).await;

    let updating = tokio::spawn({
        let services = fixture.services.clone();
        let cx = cx();
        let second = second.clone();
        async move { services.update(&cx, &second).await }
    });
    let installer = fixture.installer.clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let current = installer
            .get("alpha")
            .await
            .expect("query")
            .expect("installed")
            .version;
        if current == 2 {
            break;
        }
        assert!(Instant::now() < deadline, "the update must commit");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fs::write(&gate, "open").expect("the gate must open");

    let tools = listing.await.expect("the listing task must finish");
    let context = find_tool(&tools, "mods", "context");
    assert_eq!(target(context).revision, 2);
    let value = call_tool(&fixture.services, target(context), json!({}))
        .await
        .expect("the current tool must answer");
    assert_eq!(structured(&value)["mark"], json!("v2"));

    let updated = updating
        .await
        .expect("the update task must finish")
        .expect("update");
    assert_eq!(updated.affected_topics, vec![topic_id]);
    assert!(updated.sync_errors.is_empty());
    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_failed_configured_server_does_not_roll_back_a_mod_update() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let broken = McpDefinition {
        name: "tools".to_string(),
        description: None,
        config: McpConfig::Stdio {
            command: fixture
                .dir
                .path()
                .join("missing-server")
                .to_string_lossy()
                .into_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: fixture.dirs.root().to_path_buf(),
        },
    };
    fixture
        .services
        .create_mcp(&broken)
        .await
        .expect("create mcp");
    let broken_id = mcp_id(&fixture.services, "tools").await;
    fixture
        .services
        .enable_mcp(topic_id, broken_id)
        .await
        .expect("enable mcp");

    let first = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "v1"})),
        &[("prompts/one.md", "prompt one")],
    );
    let second = write_mod(
        &fixture.dirs,
        "alpha",
        2,
        mod_manifest("alpha", 2, json!({"WIND_TEST_MARK": "v2"})),
        &[("prompts/one.md", "prompt two")],
    );
    fixture
        .services
        .install(&cx(), &first)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");

    let updated = fixture
        .services
        .update(&cx(), &second)
        .await
        .expect("the mod update must be published");
    assert_eq!(updated.affected_topics, vec![topic_id]);
    assert!(
        updated.sync_errors.is_empty(),
        "a binding failure must stay in its status"
    );
    assert_eq!(
        prompts(&fixture.services, topic_id).await,
        vec!["prompt two".to_string()]
    );
    let tools = declarations(&fixture.services, topic_id).await;
    let context = find_tool(&tools, "mods", "context");
    assert_eq!(target(context).revision, 2);
    let status = fixture.services.status(topic_id);
    assert!(
        status.iter().any(|binding| binding.source == "configured"
            && binding.state == wind_mcp::BindingState::Failed
            && binding.error.is_some()),
        "the configured failure must be queryable: {status:?}"
    );
    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn disabled_binding_rejects_new_calls() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "v1"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");
    let tools = declarations(&fixture.services, topic_id).await;
    let context = target(find_tool(&tools, "mods", "context")).clone();
    call_tool(&fixture.services, &context, json!({}))
        .await
        .expect("the tool must answer before the change");

    fixture
        .services
        .disable(&cx(), topic_id, "alpha")
        .await
        .expect("disable");
    let error = call_tool(&fixture.services, &context, json!({}))
        .await
        .expect_err("a disabled binding must reject new calls");
    assert!(
        matches!(error, CoreError::Denied(_)),
        "the rejection must name the disabled state: {error}"
    );
    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn catalog_rejects_duplicate_display_names_and_routes_valid_ones() {
    let key = serde_json::to_string(&["alpha", "tools"]).expect("key");
    let target = sample_target("mods", &key, "echo");
    let first = declared(target.clone());
    assert!(declaration_name(&first).starts_with("mcp_"));
    assert_eq!(declaration_name(&first).len(), 52);
    let duplicate = ToolCatalog::assemble(Vec::new(), vec![first.clone(), first.clone()]);
    assert!(
        matches!(duplicate, Err(CoreError::ToolNameConflict { .. })),
        "a duplicate display name must be rejected"
    );

    let other = declared(sample_target("configured", "1", "echo"));
    let catalog = ToolCatalog::assemble(Vec::new(), vec![first.clone(), other.clone()])
        .expect("distinct names must assemble");
    let recorder = Arc::new(RecordingMcp::default());
    let plan = execute_plan(
        vec![
            FunctionCall {
                item_id: None,
                call_id: "one".to_string(),
                name: declaration_name(&first),
                arguments: "{\"value\":1}".to_string(),
            },
            FunctionCall {
                item_id: None,
                call_id: "two".to_string(),
                name: declaration_name(&other),
                arguments: "not json".to_string(),
            },
        ],
        &catalog,
        &cx(),
        &*recorder,
        &NoMods,
    )
    .await
    .expect("the plan must execute");
    let calls = recorder.calls.lock().expect("call log").clone();
    assert_eq!(calls, vec![target]);
    assert!(
        plan.results[1].content.get("error").is_some(),
        "invalid arguments must not become an empty object: {:?}",
        plan.results[1].content
    );
}

#[tokio::test]
async fn script_requests_are_not_exposed_to_the_model() {
    let compiled = Fixture::with_compiler(Arc::new(Engine)).await;
    let topic_id = compiled.topic().await;
    let package = write_mod(
        &compiled.dirs,
        "alpha",
        1,
        json!({
            "name": "alpha",
            "version": 1,
            "requests": {"run": {"handler": "run"}},
            "mcpServers": {
                "tools": {"command": server_bin(), "env": {"WIND_TEST_MARK": "scripts"}}
            }
        }),
        &[],
    );
    compiled
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    compiled
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");
    let tools = ModService::agent_tools(&compiled.services, topic_id)
        .await
        .expect("agent tools");
    assert!(
        tools
            .iter()
            .all(|tool| !matches!(tool.origin, ToolOrigin::ModRequest { .. })),
        "script requests must not become tools"
    );
    let error = ModService::call(
        &compiled.services,
        &Scope::Global,
        "alpha",
        1,
        "run",
        json!({}),
    )
    .await
    .expect_err("a mod request must fail explicitly");
    assert!(matches!(error, CoreError::Denied(_)));
    let mcp_tools = declarations(&compiled.services, topic_id).await;
    find_tool(&mcp_tools, "mods", "context");
    compiled.services.shutdown().await.expect("shutdown");

    let plain = Fixture::new().await;
    let package = write_mod(
        &plain.dirs,
        "alpha",
        1,
        json!({
            "name": "alpha",
            "version": 1,
            "requests": {"run": {"handler": "run"}}
        }),
        &[],
    );
    let error = plain
        .services
        .install(&cx(), &package)
        .await
        .expect_err("a scripted mod must require an engine");
    assert!(
        error.to_string().contains("script"),
        "the error must explain the missing engine: {error}"
    );
    plain.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cancelled_management_still_publishes_and_releases_the_gate() {
    let fixture = Fixture::new().await;
    let package = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "v1"})),
        &[],
    );
    let services = fixture.services.clone();
    let timed_out = tokio::time::timeout(Duration::from_millis(0), async {
        services.install(&cx(), &package).await
    })
    .await;
    assert!(timed_out.is_err(), "the caller must give up waiting");

    let installer = fixture.installer.clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if installer.get("alpha").await.expect("query").is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the detached publish must finish after cancellation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let topic_id = fixture.topic().await;
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("the write gate must be free after the publish");
    fixture.services.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cancelled_tool_call_stops_waiting() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let package = write_mod(
        &fixture.dirs,
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "v1"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");
    let tools = declarations(&fixture.services, topic_id).await;
    let wait = target(find_tool(&tools, "mods", "wait")).clone();

    let call_cx = cx();
    let cancel = call_cx.cancel().clone();
    let calling = tokio::spawn({
        let services = fixture.services.clone();
        async move {
            LlmMcpService::call(
                &services,
                &call_cx,
                "wait",
                &wait,
                json!({"millis": 30_000}),
            )
            .await
        }
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
    let context = target(find_tool(&tools, "mods", "context")).clone();
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
        "alpha",
        1,
        mod_manifest("alpha", 1, json!({"WIND_TEST_MARK": "v1"})),
        &[],
    );
    fixture
        .services
        .install(&cx(), &package)
        .await
        .expect("install");
    fixture
        .services
        .enable(&cx(), topic_id, "alpha")
        .await
        .expect("enable");
    let tools = declarations(&fixture.services, topic_id).await;
    let context = target(find_tool(&tools, "mods", "context")).clone();

    fixture
        .services
        .release_topic(topic_id)
        .await
        .expect("release");
    assert!(
        fixture.services.status(topic_id).is_empty(),
        "releasing a topic must remove its bindings"
    );
    assert!(
        call_tool(&fixture.services, &context, json!({}))
            .await
            .is_err()
    );
    let tools = declarations(&fixture.services, topic_id).await;
    find_tool(&tools, "mods", "context");

    fixture.services.shutdown().await.expect("shutdown");
    let error = LlmMcpService::declarations(&fixture.services, &cx(), topic_id)
        .await
        .expect_err("a closed adapter must reject listings");
    assert!(error.to_string().contains("closed"), "{error}");
    assert!(
        call_tool(&fixture.services, &context, json!({}))
            .await
            .is_err()
    );
    let topic = fixture.topic().await;
    let error = fixture
        .services
        .enable(&cx(), topic, "alpha")
        .await
        .expect_err("a closed adapter must reject management");
    assert!(error.to_string().contains("closed"), "{error}");
}
