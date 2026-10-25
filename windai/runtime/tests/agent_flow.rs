//! 真实 agent 任务：提交、工具调用、最终答复与落库
//!
//! 需要仓库根 `.env` 里的真实提供商配置，以及可执行的 `uvx`、`npx`
//!
//! 运行：
//!
//! ```text
//! rm -rf ~/.windai_test
//! cargo test -p wind-runtime --test agent_flow -- --ignored --nocapture
//! ```

#[path = "support/mod.rs"]
mod support;

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use support::Env;
use wind_core::agent::domain::{AgentFailure, AgentOutcome, pending_call_ids};
use wind_core::agent::hub::SubmitOutcome;
use wind_core::agent::tree::phase::Phase;
use wind_core::feed::log::LogRecord;
use wind_core::models::{
    AllowPolicy, BlockKind, BlockRow, CredentialUpdate, Message, ModelCreate, ModelUpdate,
    ProviderCreate, Role, TopicCreate,
};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::envelope::{Origin, Scope, Source};
use wind_mod::dirs::AppDirs;
use wind_runtime::Assembly;
use wind_script::assembly::ScriptCompiler;
use wind_wire::message::Content;

/// 一轮运行的上限
const TIMEOUT: Duration = Duration::from_secs(300);

/// 轮询快照的间隔
const POLL: Duration = Duration::from_millis(100);

/// 回查事件日志的条数上限
const RUN_LIMIT: i64 = 10_000;

const MOD: &str = "mod_demo";

const TOPIC_NAME: &str = "agent-flow";

#[tokio::test]
#[ignore = "needs a real provider and external MCP commands"]
async fn one_real_agent_task() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".to_string()),
        );
    }
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let env = Env::load();
    env.print();

    let root = dirs::home_dir().expect("home dir").join(".windai_test");
    fs::create_dir_all(&root)?;
    let dirs = AppDirs::under(root);
    let assembly = Assembly::open(dirs.clone(), None, None, Some(Arc::new(ScriptCompiler))).await?;
    let storage = assembly.storage();

    // 安装与启用事件也要落在本次范围内
    let before = storage.event_log_max_seq().await?;

    let provider_id = seed_provider(storage, &env).await?;
    seed_credential(storage, &env, provider_id).await?;
    let model_id = seed_model(storage, &env, provider_id).await?;
    let topic_id = seed_topic(storage, model_id).await?;
    seed_skills(&dirs)?;
    let package = seed_mod(&dirs)?;

    let cx = topic_cx(topic_id);
    install_and_enable(&assembly, &cx, &package, topic_id).await?;
    assembly.hub().flush().await?;

    let started = Instant::now();
    let outcome = assembly
        .submit(topic_id, vec![Content::new_text("现在几点了".into())])
        .await?;
    assert_eq!(outcome, SubmitOutcome::Launched);

    let tree = assembly.tree_of(topic_id).await?;
    let deadline = Instant::now() + TIMEOUT;
    let root = loop {
        let snapshot = tree.hub.snapshot().await?;
        let root = snapshot
            .into_iter()
            .find(|node| node.parent.is_none())
            .expect("root node");
        if root.phase.is_terminal() || matches!(&root.phase, Phase::Suspended(_)) {
            break root;
        }
        if Instant::now() >= deadline {
            panic!(
                "the run did not settle within {TIMEOUT:?}; last phase {:?}",
                root.phase
            );
        }
        tokio::time::sleep(POLL).await;
    };

    // 失败路径也要先把缓冲的事件落盘，再导出
    assembly.hub().flush().await?;
    let until = storage.event_log_max_seq().await?;
    let logs = storage
        .event_log_range(Some(topic_id), before, RUN_LIMIT, until)
        .await?;

    let instances = storage.agent_list_by_topic(topic_id).await?;
    let mut turns = Vec::new();
    for instance in &instances {
        turns.extend(storage.turn_list_by_instance(instance.id).await?);
    }
    let turn_ids: Vec<i64> = turns.iter().map(|turn| turn.id).collect();
    let messages = storage.message_list_by_turns(&turn_ids).await?;
    let message_ids: Vec<i64> = messages.iter().map(|message| message.id).collect();
    let blocks = storage.block_list_by_messages(&message_ids).await?;

    let (message_id, failure) = match root.phase {
        Phase::Done(AgentOutcome { message_id }) => (Some(message_id), None),
        Phase::Suspended(wait) => {
            let calls = wait
                .payload
                .as_ref()
                .map(pending_call_ids)
                .unwrap_or_default();
            (
                None,
                Some(format!(
                    "the root is suspended on {} ({}), pending calls: {calls:?}",
                    wait.handle, wait.kind
                )),
            )
        }
        Phase::Failed(AgentFailure { message_id, error }) => (
            message_id,
            Some(format!(
                "the root failed at message {message_id:?}: {error}"
            )),
        ),
        Phase::Cancelled(origin) => (None, Some(format!("the root was cancelled: {origin:?}"))),
        Phase::Aborted => (None, Some("the root was aborted".to_string())),
        other => (None, Some(format!("unexpected phase {other:?}"))),
    };

    let (text, assistant) = match message_id {
        Some(id) => {
            let assistant = messages
                .iter()
                .find(|message| message.id == id)
                .expect("final message row");
            let text = blocks
                .iter()
                .filter(|block| block.message_id == id && block.kind == BlockKind::Text)
                .map(|block| block.content.clone())
                .collect::<Vec<_>>()
                .join("");
            (text, Some(assistant))
        }
        None => (String::new(), None),
    };

    let tools: Vec<&str> = logs
        .iter()
        .filter(|record| record.name == "core/mcp.tool.call")
        .filter_map(|record| record.payload.get("tool").and_then(|value| value.as_str()))
        .collect();

    println!("[answer] {text}");
    println!("[mcp tools] {tools:?}");
    println!(
        "[summary] messages={} blocks={} events={} elapsed={:?}",
        messages.len(),
        blocks.len(),
        logs.len(),
        started.elapsed()
    );

    println!("[instances]\n{}", serde_json::to_string_pretty(&instances)?);
    println!("[turns]\n{}", serde_json::to_string_pretty(&turns)?);
    println!("[messages]\n{}", serde_json::to_string_pretty(&messages)?);
    println!("[blocks]\n{}", serde_json::to_string_pretty(&blocks)?);
    for record in &logs {
        println!(
            "[event] {} {}",
            record.seq.0,
            serde_json::json!({ "name": record.name, "payload": record.payload })
        );
    }

    export(
        &dirs,
        &env,
        topic_id,
        &text,
        &tools,
        &messages,
        &blocks,
        &logs,
        started,
        failure.as_deref(),
    )?;

    if let Some(failure) = failure {
        panic!("{failure}");
    }
    let assistant = assistant.expect("final message row");

    assert!(
        messages.iter().any(|message| message.role == Role::User),
        "no user message in this run"
    );
    assert_eq!(assistant.role, Role::Assistant, "final message role");
    assert!(
        text.trim_start().starts_with("[answer]"),
        "final answer does not start with [answer]: {text}"
    );
    assert!(
        blocks
            .iter()
            .any(|block| block.kind == BlockKind::ToolResult),
        "no tool result in this run"
    );

    let call = blocks
        .iter()
        .find(|block| {
            block.kind == BlockKind::ToolCall
                && matches!(
                    block
                        .data
                        .as_ref()
                        .and_then(|data| data.get("name"))
                        .and_then(|value| value.as_str()),
                    Some("m_mod_demo__now") | Some("time__get_current_time")
                )
        })
        .expect("the answer did not come from a time tool");
    assert!(
        blocks
            .iter()
            .any(|block| block.kind == BlockKind::ToolResult
                && block.call_id.as_deref() == call.call_id.as_deref()),
        "the time tool call has no result"
    );

    let names: Vec<&str> = logs.iter().map(|record| record.name.as_str()).collect();
    assert!(
        names.contains(&"core/llm.request"),
        "no model request in the event log"
    );
    if names.contains(&"core/mcp.tool.call") {
        assert!(
            names.contains(&"core/mcp.tool.result"),
            "an MCP call has no result"
        );
    }
    let started_servers: HashSet<&str> = logs
        .iter()
        .filter(|record| record.name == "core/mcp.start")
        .filter_map(|record| {
            record
                .payload
                .get("server_name")
                .and_then(|value| value.as_str())
        })
        .collect();
    for name in ["time", "server-everything", "fs", "skills"] {
        assert!(
            started_servers.contains(name),
            "core/mcp.start is missing {name}"
        );
    }

    assembly.shutdown().await?;
    Ok(())
}

async fn seed_provider(storage: &Storage, env: &Env) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(match storage.provider_get(1).await? {
        None => {
            assert!(
                storage.provider_list().await?.is_empty(),
                "the database already holds a provider that is not this fixture"
            );
            storage
                .provider_create(ProviderCreate {
                    name: TOPIC_NAME.into(),
                    base_url: env.base_url.clone(),
                    ..Default::default()
                })
                .await?
        }
        Some(provider) => {
            assert_eq!(
                provider.base_url, env.base_url,
                "provider 1 does not match .env; remove ~/.windai_test and rerun"
            );
            provider.id
        }
    })
}

async fn seed_credential(
    storage: &Storage,
    env: &Env,
    provider_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let credentials = storage.credential_list_by_provider(provider_id).await?;
    let keep = credentials
        .iter()
        .find(|credential| credential.active && credential.key == env.key)
        .map(|credential| credential.id);
    for credential in credentials
        .iter()
        .filter(|credential| credential.active && Some(credential.id) != keep)
    {
        storage
            .credential_update(
                credential.id,
                CredentialUpdate {
                    active: Some(false),
                },
            )
            .await?;
    }
    if keep.is_none() {
        storage.credential_create(provider_id, &env.key).await?;
    }
    Ok(())
}

async fn seed_model(
    storage: &Storage,
    env: &Env,
    provider_id: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let stream = serde_json::json!({ "stream": true });
    let models = storage.model_list_by_provider(provider_id).await?;
    Ok(match models.iter().find(|model| model.name == env.model) {
        Some(model) => {
            assert_eq!(
                model.provider_id, provider_id,
                "the model belongs to another provider; remove ~/.windai_test and rerun"
            );
            storage
                .model_update(
                    model.id,
                    ModelUpdate {
                        adapter: Some(env.adapter),
                        endpoint: env.endpoint.clone(),
                        config: Some(stream),
                        active: Some(true),
                        ..Default::default()
                    },
                )
                .await?;
            model.id
        }
        None => {
            storage
                .model_create(ModelCreate {
                    provider_id,
                    name: env.model.clone(),
                    alias: None,
                    adapter: env.adapter,
                    modalities: None,
                    icon: None,
                    endpoint: env.endpoint.clone(),
                    config: Some(stream),
                })
                .await?
        }
    })
}

async fn seed_topic(storage: &Storage, model_id: i64) -> Result<i64, Box<dyn std::error::Error>> {
    // 每次运行新建话题，持久目录重跑不冲突
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let label = format!("{TOPIC_NAME}-{stamp}");
    let topic_id = storage
        .topic_create(TopicCreate {
            parent_id: None,
            label,
            model_id: Some(model_id),
            tool_approval_policy: AllowPolicy {
                allow: vec!["*".into()],
            },
            icon: None,
        })
        .await?;
    Ok(topic_id)
}

fn seed_skills(dirs: &AppDirs) -> Result<(), Box<dyn std::error::Error>> {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/skills/answer-prefix/SKILL.md");
    let target = dirs.public_skills().join("answer-prefix/SKILL.md");
    copy_file(&source, &target)
}

fn seed_mod(dirs: &AppDirs) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mod_demo");
    let package = dirs.mod_version(MOD, 1);
    fs::create_dir_all(package.join("scripts"))?;
    fs::create_dir_all(package.join("skills/mod-answer"))?;

    let raw = fs::read_to_string(fixtures.join("mod.json"))?;
    let mut manifest: serde_json::Value = serde_json::from_str(&raw)?;
    let public_skills = dirs.public_skills().to_string_lossy().into_owned();
    let mod_skills = package.join("skills").to_string_lossy().into_owned();
    replace_placeholders(
        &mut manifest,
        &[
            ("{{SKILLS_DIR}}", &public_skills),
            ("{{MOD_SKILLS_DIR}}", &mod_skills),
        ],
    );
    fs::write(
        package.join("mod.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    copy_file(
        &fixtures.join("scripts/index.js"),
        &package.join("scripts/index.js"),
    )?;
    copy_file(
        &fixtures.join("skills/mod-answer/SKILL.md"),
        &package.join("skills/mod-answer/SKILL.md"),
    )?;
    Ok(package)
}

async fn install_and_enable(
    assembly: &Assembly,
    cx: &DispatchCx,
    package: &Path,
    topic_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let installer = assembly.installer();
    match assembly.storage().mod_get_by_name(MOD).await? {
        None => {
            installer.install(cx, package).await?;
        }
        Some(module) => {
            let expected = fs::read_to_string(package.join("mod.json"))?;
            let version = assembly
                .storage()
                .mod_get_version(module.id, 1)
                .await?
                .expect("mod_demo has no version 1 row");
            if version.manifest != expected {
                return Err(format!(
                    "the installed {MOD} manifest differs from the fixture; remove ~/.windai_test or raise its version"
                )
                .into());
            }
        }
    }
    require_command("uvx")?;
    require_command("npx")?;
    let enabled = tokio::time::timeout(
        Duration::from_secs(120),
        installer.enable(cx, topic_id, MOD),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "enabling the mod timed out"))?;
    enabled?;
    Ok(())
}

fn require_command(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ok = std::process::Command::new(name)
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false);
    if !ok {
        return Err(
            format!("`{name}` is not executable; install it before running this test").into(),
        );
    }
    Ok(())
}

fn copy_file(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, target)?;
    Ok(())
}

fn replace_placeholders(value: &mut serde_json::Value, pairs: &[(&str, &str)]) {
    match value {
        serde_json::Value::String(text) => {
            for (placeholder, replacement) in pairs {
                if text.contains(placeholder) {
                    *text = text.replace(placeholder, replacement);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                replace_placeholders(item, pairs);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values_mut() {
                replace_placeholders(item, pairs);
            }
        }
        _ => {}
    }
}

fn export(
    dirs: &AppDirs,
    env: &Env,
    topic_id: i64,
    text: &str,
    tools: &[&str],
    messages: &[Message],
    blocks: &[BlockRow],
    logs: &[LogRecord],
    started: Instant,
    failure: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let run_dir = dirs.root().join("runs").join(topic_id.to_string());
    fs::create_dir_all(&run_dir)?;

    let status = failure
        .map(|failure| format!("failure: {failure}\n"))
        .unwrap_or_default();
    let summary = format!(
        "topic: {topic_id}\nmodel: {}\nadapter: {}\nmessages: {}\nblocks: {}\nevents: {}\nelapsed: {:?}\n{status}\n[answer]\n{text}\n\n[mcp tools]\n{tools:?}\n",
        env.model,
        env.adapter,
        messages.len(),
        blocks.len(),
        logs.len(),
        started.elapsed()
    );
    fs::write(run_dir.join("summary.txt"), summary)?;
    fs::write(
        run_dir.join("messages.json"),
        serde_json::to_string_pretty(messages)?,
    )?;
    fs::write(
        run_dir.join("blocks.json"),
        serde_json::to_string_pretty(blocks)?,
    )?;

    let events: Vec<String> = logs
        .iter()
        .map(|record| {
            serde_json::json!({
                "seq": record.seq.0,
                "time": record.time,
                "name": record.name,
                "payload": record.payload,
            })
            .to_string()
        })
        .collect();
    fs::write(run_dir.join("events.jsonl"), events.join("\n"))?;
    Ok(())
}

fn topic_cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Topic { topic_id },
        Source::core("test"),
        Duration::from_secs(120),
    )
}
