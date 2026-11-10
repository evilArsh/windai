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

#[tokio::test]
async fn an_agent_turn_against_the_real_provider_prints_its_events() {
    common::init_log();
    let fixture = common::Fixture::open("llm_flow").await;
    let model = common::seed_provider(&fixture).await;
    let root = common::seed_topic(&fixture, "llm flow", model).await;
    let instance = root.instance;
    let pending =
        common::seed_pending(&fixture.storage, instance, "用一句话回答：2+2 等于几").await;

    let mut bus = common::Bus::open(&fixture).await;
    let handle = common::start_agent(&bus, &fixture.storage, &root, pending).await;

    let mut phase = None;
    let mut sealed = false;
    while !sealed {
        let event = common::next_event(&mut bus.events, log::Level::Info).await;
        if let Some(current) = common::terminal_phase(&event, instance) {
            phase = Some(current);
        }
        sealed = common::is_sealed(&event, instance);
    }

    handle.shutdown();
    handle.wait_stopped().await;
    bus.close().await;

    let reply = common::assistant_reply(&fixture.storage, instance).await;
    log::info!("assistant: {reply}");
    assert_eq!(phase.as_deref(), Some("done"), "节点必须跑完");
    assert!(!reply.trim().is_empty(), "模型必须给出回答");
}
