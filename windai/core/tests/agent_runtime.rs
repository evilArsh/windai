//! Topic 运行时与任务管理的行为测试

#[path = "./common/lib.rs"]
mod common;

use std::time::Duration;
use tokio::time::{sleep, timeout};
use wind_ai::message::Content;
use wind_core::agent::event::TopicEvent;

/// 主实例进入终态后，终态事件必须在关流之前送达订阅者
#[tokio::test]
async fn terminal_event_is_delivered_before_stream_closes() {
    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "stream-close").await;

    let handle = core.fetch_topic(topic.id);
    let mut rx = handle.subscribe().await.expect("subscribe topic events");

    // base_url 指向 example.invalid，模型请求必然失败并归约为 Failed
    handle
        .create_task(vec![Content::new_text("ping".into())])
        .await
        .expect("create task");

    let terminal = timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::Error { .. }) | Ok(TopicEvent::MessageFinished { .. }) => break,
                Ok(_) => continue,
                Err(err) => panic!("event stream closed before the terminal event: {err}"),
            }
        }
    })
    .await;

    assert!(
        terminal.is_ok(),
        "terminal event must be broadcast before the stream is closed"
    );

    handle.shutdown().await.expect("shutdown runtime");
}

/// 已停止的运行时句柄不能被继续复用：`fetch_topic` 必须换成新的运行时，
/// 否则该 topic 会在进程生命周期内永久不可用
#[tokio::test]
async fn fetch_topic_replaces_stopped_runtime() {
    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "restart-runtime").await;

    let handle = core.fetch_topic(topic.id);
    handle.shutdown().await.expect("shutdown runtime");

    let stopped = timeout(Duration::from_secs(10), async {
        while !handle.is_stopped() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(stopped.is_ok(), "runtime should stop after shutdown");

    let restarted = core.fetch_topic(topic.id);
    assert!(!restarted.is_stopped(), "stopped runtime must be replaced");

    restarted
        .shutdown()
        .await
        .expect("shutdown restarted runtime");
}
