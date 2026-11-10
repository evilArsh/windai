//! 把 `.windai_test/mods` 下的 mod 挂到话题上，按 `TASK` 驱动一轮对话
//!
//! ```bash
//! RUST_LOG=debug,sqlx=warn cargo test -p wind-core --test mods -- --nocapture
//! ```
//!
//! mod 包放在 `~/.windai_test/mods/<name>/<version>/`，数据库每次重建；
//! 任务写在 `TASK`，改完直接重跑；`{workspace}` 会替换成话题工作区。
//! 全部事件按 debug 打印

#[path = "common/lib.rs"]
mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;
use wind_core::llm::McpService as LlmMcpService;
use wind_core::llm::tools::ToolOrigin;
use wind_core::resources::ResourceServices;
use wind_event::{DispatchCx, Origin, Scope, Source};

/// 用户任务，改完直接重跑
const TASK: &str = "先列出你当前可用的全部工具，标出其中哪些来自 mod（工具名带 mod 段）。\n\n\
     挑一个 mod 提供的工具实际调用一次，把返回结果原样贴出来。\n\n\
     最后用一句话总结：这次调用里哪些能力由 mod 提供，哪些是内建的。\n\n\
     需要引用工作区路径时使用 {workspace}。";

/// 等待 mod 服务握手的上限
const SERVER_DEADLINE: Duration = Duration::from_secs(60);

#[tokio::test]
async fn the_mounted_mods_answer_the_configured_task() {
    common::init_log();
    let fixture = common::Fixture::shared_mods().await;
    let model = common::seed_provider(&fixture).await;
    let root = common::seed_topic(&fixture, "mods", model).await;

    let mut bus = common::Bus::open(&fixture).await;
    let servers = mount(&fixture, &bus.services, root.topic).await;
    wait_servers(&bus.services, root.topic, &servers).await;

    let pending =
        common::seed_pending(&fixture.storage, root.instance, &task(&root.workspace)).await;
    let handle = common::start_agent(&bus, &fixture.storage, &root, pending).await;

    let mut phase = None;
    let mut sealed = false;
    while !sealed {
        let event = common::next_event(&mut bus.events, log::Level::Debug).await;
        if let Some(current) = common::terminal_phase(&event, root.instance) {
            phase = Some(current);
        }
        sealed = common::is_sealed(&event, root.instance);
    }

    handle.shutdown();
    handle.wait_stopped().await;
    bus.close().await;

    let reply = common::assistant_reply(&fixture.storage, root.instance).await;
    log::info!("reply: {reply}");
    assert_eq!(phase.as_deref(), Some("done"), "这一轮必须跑完");
    assert!(!reply.trim().is_empty(), "模型必须给出回答");
}

/// 安装并启用 `.windai_test/mods` 下的全部包，返回话题承载的 mod 服务名
async fn mount(fixture: &common::Fixture, services: &ResourceServices, topic: i64) -> Vec<String> {
    let cx = cx();
    let mut failures = Vec::new();
    let mut declared = Vec::new();
    for package in mod_packages(&fixture.dirs.mods_root()) {
        match declared_servers(&package) {
            Ok(servers) => declared.push(servers),
            Err(failure) => {
                failures.push(failure);
                continue;
            }
        }
        if let Err(error) = services.install(&cx, &package).await {
            failures.push(format!("{} 安装失败: {error}", package.display()));
        }
    }
    for module in fixture.storage.list_mods().await.expect("读已装 mod") {
        if let Err(error) = services.enable(&cx, topic, &module.name).await {
            failures.push(format!("{} 启用失败: {error}", module.name));
        }
    }
    for failure in &failures {
        log::error!("mod 未挂上: {failure}");
    }
    let mut mounted: Vec<String> = fixture
        .storage
        .enabled_mods(topic)
        .await
        .expect("读启用集合")
        .into_iter()
        .map(|module| module.name)
        .collect();
    mounted.sort();
    log::info!("mounted mods: {mounted:?}");
    assert!(
        !mounted.is_empty(),
        "话题上没有挂上任何 mod，把包放到 {}: {failures:#?}",
        fixture.dirs.mods_root().display()
    );
    declared
        .into_iter()
        .filter(|(name, _)| mounted.contains(name))
        .flat_map(|(_, servers)| servers)
        .collect()
}

/// 等声明的 mod 服务完成握手，工具进入目录
async fn wait_servers(services: &ResourceServices, topic: i64, expected: &[String]) {
    let deadline = Instant::now() + SERVER_DEADLINE;
    loop {
        let tools = LlmMcpService::declarations(services, &cx(), topic)
            .await
            .expect("列举工具");
        let ready: Vec<&str> = tools
            .iter()
            .filter_map(|tool| match &tool.origin {
                ToolOrigin::Mcp { target } => Some(target.server.as_str()),
                _ => None,
            })
            .collect();
        let missing: Vec<&String> = expected
            .iter()
            .filter(|server| !ready.contains(&server.as_str()))
            .collect();
        if missing.is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "mod 服务没有就绪: {missing:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Global,
        Source::core("mods-test"),
        Duration::from_secs(60),
    )
}

/// `<mods_root>/<name>/<version>` 下的全部包
fn mod_packages(mods_root: &Path) -> Vec<PathBuf> {
    let mut packages = Vec::new();
    let Ok(modules) = std::fs::read_dir(mods_root) else {
        return packages;
    };
    for module in modules.filter_map(|entry| entry.ok()) {
        let Ok(versions) = std::fs::read_dir(module.path()) else {
            continue;
        };
        for version in versions.filter_map(|entry| entry.ok()) {
            let package = version.path();
            if package.join("mod.json").is_file() {
                packages.push(package);
            }
        }
    }
    packages.sort();
    packages
}

/// 包声明的 mod 名与 mod 服务名
fn declared_servers(package: &Path) -> Result<(String, Vec<String>), String> {
    let path = package.join("mod.json");
    let raw = std::fs::read_to_string(&path)
        .map_err(|error| format!("{} 不可读: {error}", path.display()))?;
    let manifest: Value = serde_json::from_str(&raw)
        .map_err(|error| format!("{} 不是合法 JSON: {error}", path.display()))?;
    let name = manifest["name"]
        .as_str()
        .ok_or_else(|| format!("{} 缺少 name", path.display()))?
        .to_string();
    let servers = manifest["mcpServers"]
        .as_object()
        .map(|servers| servers.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default()
        .into_iter()
        .map(|local| format!("mod_{name}_{local}"))
        .collect();
    Ok((name, servers))
}

/// 任务文本，`{workspace}` 替换成话题工作区
fn task(workspace: &Path) -> String {
    TASK.replace("{workspace}", &workspace.to_string_lossy())
}
