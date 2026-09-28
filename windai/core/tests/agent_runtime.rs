//! Topic 运行时与任务管理的行为测试

#[path = "./common/lib.rs"]
mod common;

use std::time::Duration;
use tokio::time::{sleep, timeout};
use wind_ai::message::{Content, Role};
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
        .expect("create task")
        .expect("task accepted");

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

/// 主实例进入终态后，用户输入与错误信息都以 MessageContent 落库
///
/// 用户输入是该消息的第一块；失败时错误内容作为新的一块追加到同一条消息
#[tokio::test]
async fn terminal_state_content_is_persisted_as_message_contents() {
    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "persist-content").await;

    let handle = core.fetch_topic(topic.id);
    let mut rx = handle.subscribe().await.expect("subscribe topic events");
    handle
        .create_task(vec![Content::new_text("ping".into())])
        .await
        .expect("create task")
        .expect("task accepted");

    let error = timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::Error { error, .. }) => break error,
                Ok(TopicEvent::MessageFinished { .. }) => {
                    panic!("base_url 指向 example.invalid，模型请求必然失败")
                }
                Ok(_) => continue,
                Err(err) => panic!("event stream closed before the terminal event: {err}"),
            }
        }
    })
    .await
    .expect("terminal event");

    let instance = core
        .storage()
        .agent()
        .get_main_instance(topic.id)
        .await
        .expect("get main instance")
        .expect("main instance exists");
    let messages = core
        .storage()
        .message()
        .list_by_instance(instance.id)
        .await
        .expect("list messages");
    assert_eq!(
        messages.len(),
        2,
        "一轮对话应产生 user + assistant 两条消息"
    );

    let user_blocks = core
        .storage()
        .message()
        .list_contents(messages[0].id)
        .await
        .expect("user blocks");
    assert_eq!(user_blocks.len(), 1, "用户输入应恰好落一块");
    match user_blocks[0].data.content.first() {
        Some(Content::Text { data }) => assert_eq!(data, "ping"),
        other => panic!("用户块应是文本内容: {other:?}"),
    }
    assert_eq!(user_blocks[0].data.role, Role::User);

    // base_url 指向 example.invalid，模型请求必然失败，错误内容追加为一块
    let assistant_blocks = core
        .storage()
        .message()
        .list_contents(messages[1].id)
        .await
        .expect("assistant blocks");
    assert_eq!(assistant_blocks.len(), 1, "失败任务应留下一条错误内容");
    match assistant_blocks[0].data.content.first() {
        Some(Content::Text { data }) => {
            assert!(!data.is_empty(), "错误块的内容不应为空");
            assert_eq!(data, &error);
        }
        other => panic!("错误块应是文本内容: {other:?}"),
    }
    assert_eq!(assistant_blocks[0].data.role, Role::Assistant);

    handle.shutdown().await.expect("shutdown runtime");
}
