//! Agent 全链路行为测试：用本地假 SSE 服务驱动真实 runtime 流程（无 `.env`、无网络）
//!
//! 覆盖单测看不到的两条主线：审批恢复后的块追加，以及流式中取消。
//! 假服务只发 `data:` 帧，不发 `[DONE]`（后者会被 `parse_stream_chunk` 当 JSON 解析失败）

#[path = "./common/lib.rs"]
mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::tool::FunctionCall;
use wind_core::WindCore;
use wind_core::agent::event::TopicEvent;
use wind_core::agent::helper::get_or_create_main_instance;
use wind_core::models::{
    CreateCredentials, CreateMessage, CreateMessageContent, CreateModel, CreateProvider,
    CreateTopic, ModelConfig, ModelType, ToolApprovalPolicy,
};

// ---------------------------------------------------------------------------
// 假 SSE 服务
// ---------------------------------------------------------------------------

/// 第 N 个 HTTP 连接回放第 N 段脚本（每段 = 若干 `data:` 帧），随后关流
async fn fake_server(scripts: Vec<Vec<String>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        for script in scripts {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let mut buf = [0u8; 8192];
            let _ = sock.read(&mut buf).await;
            let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                        Cache-Control: no-cache\r\nConnection: close\r\n\r\n";
            let _ = sock.write_all(head.as_bytes()).await;
            for frame in &script {
                let _ = sock
                    .write_all(format!("data: {frame}\n\n").as_bytes())
                    .await;
                let _ = sock.flush().await;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            let _ = sock.flush().await;
            tokio::time::sleep(Duration::from_millis(80)).await;
            drop(sock);
        }
    });
    format!("http://{addr}")
}

fn chunk(delta: serde_json::Value) -> String {
    serde_json::json!({
        "id": "c1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "m",
        "choices": [{"index": 0, "delta": delta, "finish_reason": null}],
    })
    .to_string()
}

fn text_delta(text: &str) -> String {
    chunk(serde_json::json!({"role": "assistant", "content": text}))
}

/// `agent_list_agents` 的工具调用分片（内建 Agent 工具，不需要 MCP server）
fn tool_delta() -> String {
    chunk(serde_json::json!({
        "tool_calls": [{
            "index": 0,
            "id": "call-1",
            "type": "function",
            "function": {"name": "agent_list_agents", "arguments": "{}"}
        }]
    }))
}

/// `agent_spawn_agent` 的工具调用分片
fn spawn_delta(agent_key: &str, task: &str) -> String {
    chunk(serde_json::json!({
        "tool_calls": [{
            "index": 0,
            "id": "call-spawn",
            "type": "function",
            "function": {
                "name": "agent_spawn_agent",
                "arguments": serde_json::json!({
                    "agent_key": agent_key,
                    "mode": "sync",
                    "task": task,
                })
                .to_string(),
            }
        }]
    }))
}

fn stop_delta() -> String {
    serde_json::json!({
        "id": "c1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "m",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// 夹具
// ---------------------------------------------------------------------------

/// 建一个指向假服务、带审批策略的 topic
async fn seeded_topic(
    core: &WindCore,
    base_url: &str,
    label: &str,
    policy: ToolApprovalPolicy,
) -> i64 {
    let provider = core
        .storage()
        .provider()
        .create(CreateProvider {
            name: format!("p-{label}"),
            description: None,
            base_url: base_url.to_string(),
            doc: None,
            alias: None,
        })
        .await
        .expect("create provider");
    core.storage()
        .provider()
        .create_credentials(CreateCredentials {
            provider_id: provider.id,
            key: "k".into(),
        })
        .await
        .expect("create credentials");
    let model = core
        .storage()
        .model()
        .create(CreateModel {
            name: format!("m-{label}"),
            provider_id: provider.id,
            alias: None,
            adapter: wind_ai::model::AdapterType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: None,
            config: Some(ModelConfig {
                stream: Some(true),
                reasoning: None,
            }),
        })
        .await
        .expect("create model");
    core.storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            label: format!("t-{label}"),
            icon: None,
            model_id: Some(model.id),
            agent_id: None,
            tool_approval_policy: Some(policy),
        })
        .await
        .expect("create topic")
        .id
}

/// 主实例最后一条消息的全部内容块
async fn last_message_blocks(core: &WindCore, topic_id: i64) -> Vec<AiMessage> {
    let instance = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("get main instance");
    blocks_of_message(core, instance.id).await
}

async fn blocks_of_message(core: &WindCore, instance_id: i64) -> Vec<AiMessage> {
    let messages = core
        .storage()
        .message()
        .list_by_instance(instance_id)
        .await
        .expect("list messages");
    let last = messages.last().expect("assistant message");
    core.storage()
        .message_content()
        .list_by_message(last.id)
        .await
        .expect("list blocks")
        .into_iter()
        .map(|c| c.data)
        .collect()
}

/// 把内容块渲染成纯文本，便于断言
fn text_of(message: &AiMessage) -> String {
    message
        .content
        .iter()
        .map(|c| match c {
            Content::Text { data } => data.clone(),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join("")
}

/// 等到终态事件（完成 / 错误 / 取消）
async fn wait_terminal(rx: &mut tokio::sync::broadcast::Receiver<TopicEvent>) -> String {
    timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::MessageFinished { .. }) => break "finished".to_string(),
                Ok(TopicEvent::Error { error, .. }) => break format!("error: {error}"),
                Ok(TopicEvent::TaskStatusChanged { status, .. }) => {
                    if format!("{status:?}").contains("Cancelled") {
                        break "cancelled".to_string();
                    }
                    continue;
                }
                Ok(_) => continue,
                Err(err) => break format!("closed: {err}"),
            }
        }
    })
    .await
    .expect("terminal event")
}

/// 往主实例写一条消息 + 一块内容
async fn add_block(
    core: &WindCore,
    instance_id: i64,
    from_id: Option<i64>,
    data: AiMessage,
) -> i64 {
    let message = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id,
            model_id: 1,
            instance_id,
            is_boundary: false,
            is_excluded: false,
        })
        .await
        .expect("create message");
    core.storage()
        .message_content()
        .create(CreateMessageContent {
            message_id: message.id,
            data,
        })
        .await
        .expect("create block");
    message.id
}

// ---------------------------------------------------------------------------
// 审批恢复：块按 id 顺序追加
// ---------------------------------------------------------------------------

/// 审批恢复后新块必须追加在已有块之后，工具请求块不能被工具结果覆盖
#[tokio::test]
async fn approval_resume_appends_blocks_in_id_order() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![text_delta("let me check"), tool_delta(), stop_delta()],
        vec![text_delta("all done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "resume", ToolApprovalPolicy::Manual).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("hi".into())])
        .await
        .expect("send task")
        .expect("task accepted");

    let approval_id = timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::ApprovalRequired { requests, .. }) => break requests[0].id,
                Ok(_) => continue,
                Err(err) => panic!("stream closed before approval: {err}"),
            }
        }
    })
    .await
    .expect("approval required");

    // 审批前的块：模型文本与工具调用同属一块
    let blocks = last_message_blocks(&core, topic_id).await;
    assert_eq!(blocks.len(), 1, "审批前应只有一块内容");
    assert!(blocks[0].tool_calls.is_some(), "该块应带工具调用");

    let instance = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("get main instance");
    handle
        .approve(instance.id, vec![approval_id], vec![])
        .await
        .expect("approve");

    let mut rx = handle.subscribe().await.expect("re-subscribe");
    let terminal = wait_terminal(&mut rx).await;
    assert_eq!(terminal, "finished", "恢复后应正常完成");

    let blocks = last_message_blocks(&core, topic_id).await;
    assert_eq!(
        blocks.len(),
        3,
        "恢复后应共三块：工具请求 / 工具结果 / 最终回答"
    );
    assert!(
        blocks[0].tool_calls.is_some(),
        "工具请求块必须仍在最前，未被工具结果覆盖"
    );
    assert_eq!(blocks[1].role, Role::Tool, "第二块应是工具结果");
    assert_eq!(blocks[2].role, Role::Assistant);
    assert_eq!(text_of(&blocks[2]), "all done");

    handle.shutdown().await.expect("shutdown runtime");
}

// ---------------------------------------------------------------------------
// 取消：必须留下错误内容块
// ---------------------------------------------------------------------------

/// 流式中取消后，消息末尾必须留下一条取消错误块，且只有一份
#[tokio::test]
async fn cancel_mid_stream_persists_cancel_error_block() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![
        text_delta("hello"),
        text_delta(" world"),
        text_delta("!!!"),
    ]])
    .await;
    let topic_id = seeded_topic(&core, &base, "cancel", ToolApprovalPolicy::AllowAll).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("hi".into())])
        .await
        .expect("send task")
        .expect("task accepted");

    // 等到助手消息的第一个分片到达后再取消：
    // 用户内容块也是一条 Message，但它在 Effect::Start 之前就下发，不能当作模型已开始输出
    timeout(Duration::from_secs(10), async {
        let mut assistant_id = None;
        loop {
            match rx.recv().await {
                // 用户消息的 from_id 为 None，助手消息才有
                Ok(TopicEvent::MessageCreated { data, .. }) if data.from_id.is_some() => {
                    assistant_id = Some(data.id);
                }
                Ok(TopicEvent::Message { message_id, .. }) if assistant_id == Some(message_id) => {
                    break;
                }
                Ok(_) => continue,
                Err(err) => panic!("stream closed before first delta: {err}"),
            }
        }
    })
    .await
    .expect("first delta");

    let instance = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("get main instance");
    handle.cancel_task(instance.id).await.expect("cancel task");

    let terminal = wait_terminal(&mut rx).await;
    assert_eq!(terminal, "cancelled", "取消后应进入 Cancelled 终态");

    let blocks = last_message_blocks(&core, topic_id).await;
    let cancel_blocks = blocks
        .iter()
        .filter(|b| text_of(b).contains("cancelled"))
        .count();
    assert_eq!(
        cancel_blocks,
        1,
        "取消必须留下且只留一条错误内容块，实际块: {:?}",
        blocks.iter().map(text_of).collect::<Vec<_>>()
    );
    // 任务进入终态后 FSM 丢弃内容块，取消错误块因而必然是该消息的末尾
    let last = blocks.last().expect("取消后必须留下错误内容块");
    assert!(
        text_of(last).contains("cancelled"),
        "取消错误块必须是消息末尾，实际末尾块: {}",
        text_of(last)
    );

    handle.shutdown().await.expect("shutdown runtime");
}

// ---------------------------------------------------------------------------
// 工具调用判定范围
// ---------------------------------------------------------------------------

fn stale_tool_request_block() -> AiMessage {
    let mut message = AiMessage::new_tool_request(
        vec![FunctionCall {
            id: "stale-call-1".to_string(),
            name: "agent_list_agents".to_string(),
            arguments: "{}".to_string(),
        }],
        None,
    );
    message.content = vec![Content::new_text("let me look".to_string())];
    message
}

/// 历史里遗留的未执行工具调用不得被新任务重新执行
///
/// 上一轮请求了工具但没拿到结果（例如等待审批时被取消），
/// 新任务必须重新请求模型，而不是从历史里捡起旧调用直接执行
#[tokio::test]
async fn new_task_does_not_replay_stale_tool_call() {
    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "stale-call").await;
    let instance = get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("get main instance");

    let user = add_block(
        &core,
        instance.id,
        None,
        AiMessage::new_simple(Role::User, vec![Content::new_text("old".into())], None),
    )
    .await;
    add_block(&core, instance.id, Some(user), stale_tool_request_block()).await;

    let handle = core.fetch_topic(topic.id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("new question".into())])
        .await
        .expect("send task")
        .expect("task accepted");
    let terminal = wait_terminal(&mut rx).await;

    // base_url 指向 example.invalid，模型请求必然失败
    assert!(
        terminal.starts_with("error:"),
        "新任务应真正请求模型并失败，实际终态: {terminal}"
    );
    let blocks = blocks_of_message(&core, instance.id).await;
    let roles = blocks.iter().map(|b| b.role).collect::<Vec<_>>();
    assert!(
        !roles.contains(&Role::Tool),
        "历史遗留的工具调用被重新执行，模型未被请求：{roles:?}"
    );
}

/// 错误信息必须是可读文本，不是被 JSON 包起来的 Content
#[tokio::test]
async fn error_event_text_is_not_json_wrapped() {
    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "error-text").await;

    let handle = core.fetch_topic(topic.id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("ping".into())])
        .await
        .expect("send task")
        .expect("task accepted");
    let terminal = wait_terminal(&mut rx).await;

    let error = terminal
        .strip_prefix("error: ")
        .unwrap_or_else(|| panic!("应收到错误终态，实际: {terminal}"));
    assert!(!error.is_empty(), "错误信息不应为空");
    assert!(
        !error.starts_with("{\"type\""),
        "错误信息不应是 Content 的 JSON 包装：{error}"
    );

    let blocks = last_message_blocks(&core, topic.id).await;
    assert_eq!(blocks.len(), 1, "失败任务应留下一条错误内容块");
    assert!(
        !text_of(&blocks[0]).starts_with("{\"type\""),
        "落库的错误内容不应是 JSON 包装：{}",
        text_of(&blocks[0])
    );
}

// ---------------------------------------------------------------------------
// 空白内容块
// ---------------------------------------------------------------------------

/// 模型返回空响应时不落库空白块
#[tokio::test]
async fn empty_response_does_not_persist_blank_block() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![
        chunk(serde_json::json!({"role": "assistant"})),
        stop_delta(),
    ]])
    .await;
    let topic_id = seeded_topic(&core, &base, "empty", ToolApprovalPolicy::AllowAll).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("hi".into())])
        .await
        .expect("send task")
        .expect("task accepted");
    wait_terminal(&mut rx).await;

    let blocks = last_message_blocks(&core, topic_id).await;
    assert!(
        blocks.is_empty(),
        "空白块不应落库，实际: {:?}",
        blocks.iter().map(text_of).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// SSE index 语义
// ---------------------------------------------------------------------------

/// SSE 的 index 是本次运行内的块序号：同一块的分片共用一个 index，块推进时 +1；
/// 审批恢复后新一轮从 0 重新开始，但数据库里的块顺序不受影响
#[tokio::test]
async fn sse_index_is_per_run_block_sequence() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![text_delta("let me check"), tool_delta(), stop_delta()],
        vec![text_delta("all done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "sse-index", ToolApprovalPolicy::Manual).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("hi".into())])
        .await
        .expect("send task")
        .expect("task accepted");

    let mut first_run = Vec::new();
    let approval_id = timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::Message { index, .. }) => first_run.push(index),
                Ok(TopicEvent::ApprovalRequired { requests, .. }) => break requests[0].id,
                Ok(_) => continue,
                Err(err) => panic!("stream closed before approval: {err}"),
            }
        }
    })
    .await
    .expect("approval required");
    assert!(
        !first_run.is_empty(),
        "审批前应至少收到一块内容的分片，否则下面的断言恒真"
    );
    assert!(
        first_run.iter().all(|index| *index == 0),
        "同一块的分片必须共用一个 index：{first_run:?}"
    );

    let instance = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("get main instance");
    handle
        .approve(instance.id, vec![approval_id], vec![])
        .await
        .expect("approve");

    let mut rx = handle.subscribe().await.expect("re-subscribe");
    let mut second_run = Vec::new();
    timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::Message { index, .. }) => second_run.push(index),
                Ok(TopicEvent::MessageFinished { .. }) => break,
                Ok(TopicEvent::Error { error, .. }) => panic!("unexpected error: {error}"),
                Ok(_) => continue,
                Err(err) => panic!("stream closed before terminal event: {err}"),
            }
        }
    })
    .await
    .expect("terminal event");
    // 同一块可能由多帧拼成（结束帧的 delta 为空，也走一次 Message），
    // 因此只取块序号的变化来断言块序列
    let mut block_sequence: Vec<i64> = Vec::new();
    for index in second_run.iter().copied() {
        if block_sequence.last() != Some(&index) {
            block_sequence.push(index);
        }
    }
    assert_eq!(
        block_sequence,
        vec![0, 1],
        "恢复后新一轮从 0 开始：工具结果块 0、最终回答块 1"
    );

    // 数据库顺序不受 index 影响：工具请求 / 工具结果 / 最终回答
    let blocks = last_message_blocks(&core, topic_id).await;
    assert_eq!(blocks.len(), 3);
    assert!(blocks[0].tool_calls.is_some());
    assert_eq!(blocks[1].role, Role::Tool);
    assert_eq!(text_of(&blocks[2]), "all done");

    handle.shutdown().await.expect("shutdown runtime");
}

// ---------------------------------------------------------------------------
// 子实例审批：不关流，新一轮运行在同一条 message 上从 index 0 再起
// ---------------------------------------------------------------------------

/// 子实例进入审批不关闭事件流，审批后同一次订阅内继续收到事件，
/// 且新一轮运行在同一条 message 上从 `index` 0 重新开始
#[tokio::test]
async fn child_approval_keeps_stream_open_and_restarts_index() {
    let core = common::init_test_core().await;
    // 先建能力定义拿到 key，再用它构造模型返回的 spawn 工具调用
    let definition = common::seed_definition(&core, "child-approval").await;
    let base = fake_server(vec![
        // 1 父实例首轮：spawn 命中 AllowList，自动执行且阻塞到子实例结束
        vec![
            text_delta("spawning"),
            spawn_delta(&definition.key, "child-task"),
            stop_delta(),
        ],
        // 2 子实例首轮：正文 + 需要人工审批的工具调用
        vec![text_delta("child thinking"), tool_delta(), stop_delta()],
        // 3 子实例恢复：工具已执行，模型收尾
        vec![text_delta("child done"), stop_delta()],
        // 4 父实例拿到子实例结果后收尾
        vec![text_delta("parent done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(
        &core,
        &base,
        "child-approval",
        ToolApprovalPolicy::AllowList(vec!["agent_spawn_agent".into()]),
    )
    .await;
    core.storage()
        .agent()
        .create_topic_agent_map(wind_core::models::CreateTopicAgentMap {
            topic_id,
            agent_id: definition.id,
        })
        .await
        .expect("create topic agent map");

    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("get main instance");
    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("请派一个子任务".into())])
        .await
        .expect("send task")
        .expect("task accepted");

    // 父实例此刻在等子实例，子实例的审批不得关闭事件流
    let (child_instance_id, child_message_id, approval_id) =
        timeout(Duration::from_secs(10), async {
            loop {
                match rx.recv().await {
                    Ok(TopicEvent::ApprovalRequired {
                        instance_id,
                        message_id,
                        requests,
                    }) if instance_id != main.id => {
                        break (instance_id, message_id, requests[0].id);
                    }
                    Ok(_) => continue,
                    Err(err) => panic!("stream closed before child approval: {err}"),
                }
            }
        })
        .await
        .expect("child approval required");

    let child = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("list instances")
        .into_iter()
        .find(|instance| instance.id != main.id)
        .expect("child instance");
    assert_eq!(
        child.id, child_instance_id,
        "审批请求应来自 spawn 出来的子实例"
    );

    handle
        .approve(child_instance_id, vec![approval_id], vec![])
        .await
        .expect("approve child");

    // 同一次订阅内继续收事件：子实例审批若关流，这里会拿到 Closed 而非后续事件
    let mut child_run = Vec::new();
    timeout(Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Ok(TopicEvent::Message {
                    message_id, index, ..
                }) if message_id == child_message_id => child_run.push(index),
                Ok(TopicEvent::MessageFinished { instance_id, .. }) if instance_id == main.id => {
                    break;
                }
                Ok(TopicEvent::Error { error, .. }) => panic!("unexpected error: {error}"),
                Ok(_) => continue,
                Err(err) => panic!("stream closed after child approval: {err}"),
            }
        }
    })
    .await
    .expect("parent terminal event");

    assert_eq!(
        child_run.first().copied(),
        Some(0),
        "审批后子实例新一轮运行必须从 index 0 再起：{child_run:?}"
    );
    // 同一块可能由多帧拼成，因此只取块序号的变化来断言块序列
    let mut block_sequence: Vec<i64> = Vec::new();
    for index in child_run.iter().copied() {
        if block_sequence.last() != Some(&index) {
            block_sequence.push(index);
        }
    }
    assert_eq!(
        block_sequence,
        vec![0, 1],
        "新一轮运行内块序号从 0 起递增：工具结果块 0、最终回答块 1"
    );

    // 数据库顺序不受 index 影响：工具请求 / 工具结果 / 最终回答
    let blocks = blocks_of_message(&core, child_instance_id).await;
    assert_eq!(blocks.len(), 3, "子实例恢复后应共三块内容");
    assert!(blocks[0].tool_calls.is_some(), "首块应带工具调用");
    assert_eq!(blocks[1].role, Role::Tool, "第二块应是工具结果");
    assert_eq!(text_of(&blocks[2]), "child done");

    handle.shutdown().await.expect("shutdown runtime");
}

// ---------------------------------------------------------------------------
// 用户输入内容块下发
// ---------------------------------------------------------------------------

/// 用户输入的内容块必须随 create_task 经 SSE 下发（前端不必再走 HTTP 拉）
#[tokio::test]
async fn user_input_content_is_pushed_over_sse() {
    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "user-block").await;

    let handle = core.fetch_topic(topic.id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("你好".into())])
        .await
        .expect("send task")
        .expect("task accepted");

    let pushed = timeout(Duration::from_secs(10), async {
        let mut user_message_id = None;
        loop {
            match rx.recv().await {
                Ok(TopicEvent::MessageCreated { data, .. }) if data.from_id.is_none() => {
                    user_message_id = Some(data.id);
                }
                Ok(TopicEvent::Message {
                    message_id,
                    index,
                    data,
                    ..
                }) if user_message_id == Some(message_id) => {
                    break (index, text_of(&data));
                }
                Ok(_) => continue,
                Err(err) => panic!("stream closed: {err}"),
            }
        }
    })
    .await
    .expect("user content block event");

    assert_eq!(pushed, (0, "你好".to_string()));
    handle.shutdown().await.expect("shutdown runtime");
}

/// 子实例首轮的用户内容块同样下发（带子实例 id）
#[tokio::test]
async fn child_user_input_content_is_pushed_over_sse() {
    let core = common::init_test_core().await;
    // 先建能力定义拿到 key，再用它构造模型返回的工具调用
    let definition = common::seed_definition(&core, "child-agent").await;
    let base = fake_server(vec![
        vec![
            text_delta("spawning"),
            spawn_delta(&definition.key, "child-task"),
            stop_delta(),
        ],
        vec![text_delta("child done"), stop_delta()],
        vec![text_delta("parent done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "child-block", ToolApprovalPolicy::AllowAll).await;
    core.storage()
        .agent()
        .create_topic_agent_map(wind_core::models::CreateTopicAgentMap {
            topic_id,
            agent_id: definition.id,
        })
        .await
        .expect("create topic agent map");

    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("get main instance");
    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("请派一个子任务".into())])
        .await
        .expect("send task")
        .expect("task accepted");

    let pushed = timeout(Duration::from_secs(10), async {
        let mut child_message_id = None;
        loop {
            match rx.recv().await {
                Ok(TopicEvent::MessageCreated {
                    instance_id, data, ..
                }) if instance_id != main.id && data.from_id.is_none() => {
                    child_message_id = Some(data.id);
                }
                Ok(TopicEvent::Message {
                    message_id, data, ..
                }) if child_message_id == Some(message_id) => break text_of(&data),
                Ok(_) => continue,
                Err(err) => panic!("stream closed: {err}"),
            }
        }
    })
    .await
    .expect("child content block event");

    assert_eq!(pushed, "child-task");
    handle.shutdown().await.expect("shutdown runtime");
}
