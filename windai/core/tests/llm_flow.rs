//! AgentHandle、LlmRunner 与事件总线的一次单轮往返
//!
//! ```bash
//! cargo test -p wind-core --test llm_flow -- --nocapture
//! ```
//!
//! 数据放在 `~/.windai_test`，提供商参数来自仓库根目录的 .env；
//! 事件的消费端是 feed 订阅，逐条写进日志

#[path = "common/lib.rs"]
mod common;

use std::sync::Arc;

#[tokio::test]
async fn an_agent_turn_against_the_real_provider_prints_its_events() {
    common::init_log();
    let storage = common::open_storage("llm_flow").await;
    let model = common::seed_provider(&storage).await;
    let seed = common::seed_root(&storage, "llm flow", model, "用一句话回答：2+2 等于几").await;
    let instance = seed.1;

    let mut bus = common::Bus::open(&storage).await;
    let handle = common::start_agent(&storage, Arc::clone(&bus.dispatcher), seed).await;

    let mut phase = None;
    while phase.is_none() {
        let event = common::next_event(&mut bus.events).await;
        phase = common::terminal_phase(&event, instance);
    }

    handle.shutdown();
    handle.wait_stopped().await;
    bus.close().await;

    let reply = common::assistant_reply(&storage, instance).await;
    log::info!("assistant: {reply}");
    assert_eq!(phase.as_deref(), Some("done"), "节点必须跑完");
    assert!(!reply.trim().is_empty(), "模型必须给出回答");
}
