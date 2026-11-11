//! 主实例派生一个 sync 子实例去列出话题工作区，结果回填给主实例
//!
//! ```bash
//! cargo test -p wind-core --test subagent -- --nocapture
//! ```

#[path = "common/lib.rs"]
mod common;

use wind_core::agent::SPAWN_AGENT_NAME;
use wind_core::agent::events::CORE_AGENT_SPAWN;
use wind_wire::AgentRole;

/// 工作区里的标记文件，子实例列出文件名后必须回流到主实例
const MARKER: &str = "note.txt";

#[ignore = "need .env"]
#[tokio::test]
async fn a_child_instance_lists_the_topic_workspace_for_its_parent() {
    common::init_log();
    let fixture = common::Fixture::open("subagent").await;
    let model = common::seed_provider(&fixture).await;
    let root = common::seed_topic(&fixture, "subagent", model).await;
    tokio::fs::write(root.workspace.join(MARKER), "hello")
        .await
        .expect("放一个标记文件");
    let prompt = format!(
        "调用 wind_spawn_agent 派生一个 mode 为 sync 的子 agent，\
         任务：用 list_dir 列出 {} 的目录和文件名。子 agent 返回后，把它交回的清单原样转述给我。",
        root.workspace.display()
    );
    let pending = common::seed_pending(&fixture.storage, root.instance, &prompt).await;

    let mut bus = common::Bus::open(&fixture).await;
    let handle = common::start_agent(&bus, &fixture.storage, &root, pending).await;

    let mut phase = None;
    let mut child = None;
    let mut sealed = false;
    while !sealed {
        let event = common::next_event(&mut bus.events, log::Level::Info).await;
        if let Some(current) = common::terminal_phase(&event, root.instance) {
            phase = Some(current);
        }
        sealed = common::is_sealed(&event, root.instance);
        if event.envelope.name == CORE_AGENT_SPAWN.name
            && let Some(payload) = event.envelope.payload.as_ref()
            && payload.get("parent_id").and_then(serde_json::Value::as_i64) == Some(root.instance)
        {
            child = payload
                .get("instance_id")
                .and_then(serde_json::Value::as_i64);
        }
    }

    handle.shutdown();
    handle.wait_stopped().await;
    bus.close().await;

    let child = child.expect("主实例必须派生子实例");
    let child_row = fixture
        .storage
        .get_instance_by_id(child)
        .await
        .expect("读子实例")
        .expect("子实例必须存在");
    assert_eq!(
        child_row.parent_id,
        Some(root.instance),
        "子实例的父实例是主实例"
    );
    assert_eq!(child_row.role, AgentRole::Child, "子实例的角色");

    let calls = common::tool_calls(&fixture.storage, root.instance).await;
    assert!(
        calls.iter().any(|name| name == SPAWN_AGENT_NAME),
        "主实例必须调用派生工具: {calls:?}"
    );

    let results = common::tool_results(&fixture.storage, root.instance).await;
    assert!(
        results.iter().any(|result| result.contains(MARKER)),
        "子实例的清单必须回流到主实例: {results:?}"
    );

    let reply = common::assistant_reply(&fixture.storage, root.instance).await;
    log::info!("parent reply: {reply}");
    assert_eq!(phase.as_deref(), Some("done"), "主实例必须跑完");
    assert!(!reply.trim().is_empty(), "主实例必须给出回答");
}
