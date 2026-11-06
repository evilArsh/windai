//! 主实例派生一个 sync 子实例去列出 home 目录，结果回填给主实例
//!
//! ```bash
//! cargo test -p wind-core --test subagent -- --nocapture
//! ```

#[path = "common/lib.rs"]
mod common;

use std::sync::Arc;
use wind_core::agent::SPAWN_AGENT_NAME;
use wind_core::agent::events::CORE_AGENT_SPAWN;
use wind_wire::AgentRole;

#[tokio::test]
async fn a_child_instance_lists_the_home_directory_for_its_parent() {
    common::init_log();
    let storage = common::open_storage("subagent").await;
    let model = common::seed_provider(&storage).await;
    let seed = common::seed_root(
        &storage,
        "subagent",
        model,
        "调用 wind_spawn_agent 派生一个 mode 为 sync 的子 agent，\
         任务：列出 ~ 的目录和文件名。子 agent 返回后，把它交回的清单原样转述给我。",
    )
    .await;
    let root = seed.1;

    let mut bus = common::Bus::open(&storage).await;
    let handle = common::start_agent(&storage, Arc::clone(&bus.dispatcher), seed).await;

    let mut phase = None;
    let mut child = None;
    let mut sealed = false;
    while !sealed {
        let event = common::next_event(&mut bus.events).await;
        if let Some(current) = common::terminal_phase(&event, root) {
            phase = Some(current);
        }
        sealed = common::is_sealed(&event, root);
        if event.envelope.name == CORE_AGENT_SPAWN.name
            && let Some(payload) = event.envelope.payload.as_ref()
            && payload.get("parent_id").and_then(serde_json::Value::as_i64) == Some(root)
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
    let child_row = storage
        .get_instance_by_id(child)
        .await
        .expect("读子实例")
        .expect("子实例必须存在");
    assert_eq!(child_row.parent_id, Some(root), "子实例的父实例是主实例");
    assert_eq!(child_row.role, AgentRole::Child, "子实例的角色");

    let calls = common::tool_calls(&storage, root).await;
    assert!(
        calls.iter().any(|name| name == SPAWN_AGENT_NAME),
        "主实例必须调用派生工具: {calls:?}"
    );

    let entries = common::home_entries();
    let results = common::tool_results(&storage, root).await;
    assert!(
        results
            .iter()
            .any(|result| entries.iter().any(|entry| result.contains(entry.as_str()))),
        "子实例的清单必须回流到主实例: {results:?}"
    );

    let reply = common::assistant_reply(&storage, root).await;
    log::info!("parent reply: {reply}");
    assert_eq!(phase.as_deref(), Some("done"), "主实例必须跑完");
    assert!(!reply.trim().is_empty(), "主实例必须给出回答");
}
