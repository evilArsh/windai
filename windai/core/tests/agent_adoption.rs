//! Agent 接入后的端到端行为：用本地假 SSE 服务驱动真实 hub + AgentBehavior（无 `.env`、无网络）

#[path = "./common/lib.rs"]
mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::tool::FunctionCall;
use wind_core::WindCore;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome};
use wind_core::agent::domain::{AgentDomain, AgentProgress, AgentSpec};
use wind_core::agent::event::TopicEvent;
use wind_core::agent::runtime::AgentBehavior;
use wind_core::agent::store::AgentStore;
use wind_core::models::{
    AgentDefinition, AgentMode, AgentStatus, ApprovalRecord, CreateCredentials, CreateMessage,
    CreateMessageContent, CreateModel, CreateProvider, CreateTopic, CreateTopicAgentMap, Message,
    ModelConfig, ModelType, ToolApprovalPolicy, ToolApprovalStatus, UpdateInstance,
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

/// 一条 `call-1` 的 `agent_list_agents` 待审批调用
fn tool_call() -> FunctionCall {
    FunctionCall {
        id: "call-1".to_string(),
        name: "agent_list_agents".to_string(),
        arguments: "{}".to_string(),
    }
}

// ---------------------------------------------------------------------------
// 夹具
// ---------------------------------------------------------------------------

/// 建一个指向假服务、审批策略为 AllowAll 的 topic，返回它的 id
async fn seeded_topic(core: &WindCore, base_url: &str, label: &str) -> i64 {
    seeded_topic_with_policy(core, base_url, label, ToolApprovalPolicy::AllowAll).await
}

/// 建一个指向假服务、带指定审批策略的 topic
async fn seeded_topic_with_policy(
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

/// 读到 `MessageFinished` 或 `Error` 为止，返回沿途收到的事件
async fn drain_until_terminal(
    rx: &mut tokio::sync::broadcast::Receiver<TopicEvent>,
) -> Vec<TopicEvent> {
    drain_until(rx, |e| {
        matches!(
            e,
            TopicEvent::MessageFinished { .. } | TopicEvent::Error { .. }
        )
    })
    .await
}

/// 读到 `done` 返回 true 为止，返回沿途收到的事件
async fn drain_until<F>(
    rx: &mut tokio::sync::broadcast::Receiver<TopicEvent>,
    mut done: F,
) -> Vec<TopicEvent>
where
    F: FnMut(&TopicEvent) -> bool,
{
    let mut seen = Vec::new();
    timeout(Duration::from_secs(60), async {
        while let Ok(event) = rx.recv().await {
            let finished = done(&event);
            seen.push(event);
            if finished {
                break;
            }
        }
    })
    .await
    .expect("终态事件必须在超时前到达");
    seen
}

/// 读到事件流关闭为止，返回沿途收到的事件
///
/// 关流是适配层对 `TreeEvent::Sealed` 的处理，不会有一条同名的事件
async fn drain_to_close(rx: &mut tokio::sync::broadcast::Receiver<TopicEvent>) -> Vec<TopicEvent> {
    let mut seen = Vec::new();
    timeout(Duration::from_secs(60), async {
        while let Ok(event) = rx.recv().await {
            seen.push(event);
        }
    })
    .await
    .expect("事件流必须在超时前关闭");
    seen
}

/// 一条消息里所有文本块拼起来
fn text_of(message: &wind_ai::message::Message) -> String {
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

// ---------------------------------------------------------------------------
// 单轮对话
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_single_round_persists_one_block_and_finishes() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "single-round").await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("hi".into())])
        .await
        .expect("mailbox")
        .expect("business");

    let seen = drain_until_terminal(&mut rx).await;
    assert!(
        seen.iter()
            .any(|e| matches!(e, TopicEvent::MessageFinished { .. })),
        "一轮结束必须发 MessageFinished: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|e| matches!(e, TopicEvent::InstanceCreated { .. })),
        "新任务必须宣告 InstanceCreated"
    );

    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    let blocks = core
        .storage()
        .message()
        .list_by_instance(instance.id)
        .await
        .expect("messages");
    let assistant = blocks.last().expect("assistant message");
    let contents = core
        .storage()
        .message()
        .list_contents(assistant.id)
        .await
        .expect("contents");
    assert_eq!(contents.len(), 1, "一轮只有一块");
    assert_eq!(text_of(&contents[0].data), "hello");
}

// ---------------------------------------------------------------------------
// 工具轮：审批挂起与拒绝
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_manual_policy_suspends_and_the_stream_stays_open() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![tool_delta(), stop_delta()],
        vec![text_delta("done"), stop_delta()],
    ])
    .await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "manual", ToolApprovalPolicy::Manual).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".into())])
        .await
        .expect("mailbox")
        .expect("business");

    let seen = drain_until(&mut rx, |e| {
        matches!(e, TopicEvent::ApprovalRequired { .. })
    })
    .await;
    let (instance_id, message_id) = seen
        .iter()
        .find_map(|e| match e {
            TopicEvent::ApprovalRequired {
                instance_id,
                message_id,
                ..
            } => Some((*instance_id, *message_id)),
            _ => None,
        })
        .expect("审批请求");

    let requests = core
        .storage()
        .approval()
        .list_pending_by_message(message_id)
        .await
        .expect("pending");
    assert_eq!(requests.len(), 1, "manual 策略下这一条必须挂起");

    // 挂起不关流：整棵树安静才关，所以恢复后的事件仍到达同一个接收端
    handle
        .approve(instance_id, vec![requests[0].id], Vec::new())
        .await
        .expect("approve");

    let seen = drain_until_terminal(&mut rx).await;
    assert!(
        seen.iter()
            .any(|e| matches!(e, TopicEvent::MessageFinished { .. })),
        "恢复后必须跑到 MessageFinished: {seen:?}"
    );
    let contents = core
        .storage()
        .message()
        .list_contents(message_id)
        .await
        .expect("contents");
    let texts: Vec<String> = contents.iter().map(|c| text_of(&c.data)).collect();
    assert!(
        texts.iter().any(|t| t == "done"),
        "恢复后新块要按 id 追加在已落库的块之后: {texts:?}"
    );
}

#[tokio::test]
async fn a_denied_tool_reports_a_denial_without_json_wrapping() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![tool_delta(), stop_delta()],
        vec![text_delta("done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic_with_policy(&core, &base, "deny", ToolApprovalPolicy::Manual).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".into())])
        .await
        .expect("mailbox")
        .expect("business");

    let seen = drain_until(&mut rx, |e| {
        matches!(e, TopicEvent::ApprovalRequired { .. })
    })
    .await;
    let (instance_id, message_id) = seen
        .iter()
        .find_map(|e| match e {
            TopicEvent::ApprovalRequired {
                instance_id,
                message_id,
                ..
            } => Some((*instance_id, *message_id)),
            _ => None,
        })
        .expect("审批请求");
    let requests = core
        .storage()
        .approval()
        .list_pending_by_message(message_id)
        .await
        .expect("pending");

    // 挂起不关流：整棵树安静才关，所以恢复后的事件仍到达同一个接收端
    handle
        .approve(instance_id, Vec::new(), vec![requests[0].id])
        .await
        .expect("deny");

    drain_until_terminal(&mut rx).await;
    let contents = core
        .storage()
        .message()
        .list_contents(message_id)
        .await
        .expect("contents");
    assert!(
        contents
            .iter()
            .any(|c| tool_outputs(&c.data).contains("tool call denied")),
        "拒绝必须落成一条工具结果块: {:?}",
        contents
            .iter()
            .map(|c| text_of(&c.data))
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// 直驱 AgentBehavior：不经 hub，直接驱动 `run`
// ---------------------------------------------------------------------------

/// 不经过 hub，直接驱动 AgentBehavior：一次 run 的判定、落库产物与下发的进度
#[tokio::test]
async fn a_direct_run_completes_and_persists_one_block() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "direct").await;
    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    let spec = wind_core::agent::helper::start_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic_id,
        instance.clone(),
        &[Content::new_text("hi".into())],
    )
    .await
    .expect("start round");

    let node = instance.id;
    let behavior = AgentBehavior::new(core.storage().clone(), core.registry().clone(), topic_id);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let emit = move |chunk| {
        if let Ok(mut seen) = sink.lock() {
            seen.push(chunk);
        }
    };
    let ctx = Ctx::<AgentDomain>::new(node, 1, CancellationToken::new(), &emit);
    let outcome = behavior.run(ctx, Input::Fresh(spec)).await;

    let assistant_id = match outcome {
        Outcome::Completed(out) => out.message_id,
        other => panic!("单轮无工具必须以 Completed 收尾: {other:?}"),
    };
    assert_eq!(
        assistant_id,
        assistant_of(&core, node).await,
        "判定带的必须是本轮新建的 assistant 消息"
    );

    let contents = core
        .storage()
        .message()
        .list_contents(assistant_id)
        .await
        .expect("contents");
    assert_eq!(contents.len(), 1, "一轮只该落一块");
    assert_eq!(text_of(&contents[0].data), "hello");

    let seen = seen.lock().expect("lock");
    assert!(
        seen.iter().any(|c| matches!(
            c,
            AgentProgress::MessageCreated(m) if m.id == assistant_id
        )),
        "announce 必须宣告本轮写入的 assistant 消息"
    );
    assert!(
        seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { message_id, .. } if *message_id == assistant_id
        )),
        "内容分片必须带正确的归属消息"
    );
}

/// manual 策略下的直驱：一次 run 交还挂起判定，并把调用落成待审批行
#[tokio::test]
async fn a_direct_run_suspends_on_a_manual_tool_call() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![tool_delta(), stop_delta()]]).await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "direct-manual", ToolApprovalPolicy::Manual).await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    let pending = match outcome {
        Outcome::Suspend(pending) => pending,
        other => panic!("manual 策略下必须挂起等审批: {other:?}"),
    };
    assert_eq!(pending.instance_id, spec.instance_id, "挂起要带本节点");
    assert_eq!(
        pending.message_id,
        assistant_of(&core, spec.instance_id).await,
        "挂起要带本轮消息"
    );

    let requests = core
        .storage()
        .approval()
        .list_pending_by_message(pending.message_id)
        .await
        .expect("pending");
    assert_eq!(requests.len(), 1, "挂起必须落一条待审批行");
    assert_eq!(requests[0].tool_call_id, "call-1");
    assert!(
        !seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { data, .. } if tool_outputs(data).contains("tool call denied")
        )),
        "还没审批就不该有工具结果: {seen:?}"
    );
}

/// 已拒绝的直驱：拒绝结果整块下发（先广播）再落库，随后续跑到 Completed
#[tokio::test]
async fn a_direct_run_reports_a_denied_tool_as_a_whole_block() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![tool_delta(), stop_delta()],
        vec![text_delta("done"), stop_delta()],
    ])
    .await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "direct-deny", ToolApprovalPolicy::Manual).await;
    let spec = start_direct_round_with_assistant(&core, topic_id).await;
    let assistant_id = spec.message_id.expect("assistant message");

    // 预置一条已拒绝的审批行，模拟用户拒绝之后的恢复
    let requests = wind_core::agent::helper::save_approval_state(
        core.storage(),
        topic_id,
        spec.instance_id,
        assistant_id,
        vec![tool_call()],
    )
    .await
    .expect("seed approval");
    core.storage()
        .approval()
        .batch_set_status(vec![ApprovalRecord {
            id: requests[0].id,
            status: ToolApprovalStatus::Denied,
        }])
        .await
        .expect("deny");

    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    match outcome {
        Outcome::Completed(out) => assert_eq!(out.message_id, assistant_id),
        other => panic!("拒绝后应续跑收尾为 Completed: {other:?}"),
    }

    let contents = core
        .storage()
        .message()
        .list_contents(assistant_id)
        .await
        .expect("contents");
    assert!(
        contents
            .iter()
            .any(|c| tool_outputs(&c.data).contains("tool call denied")),
        "拒绝必须落成一条工具结果块: {:?}",
        contents
            .iter()
            .map(|c| text_of(&c.data))
            .collect::<Vec<_>>()
    );
    assert!(
        seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { data, .. } if tool_outputs(data).contains("tool call denied")
        )),
        "整块工具结果必须先下发给订阅方: {seen:?}"
    );
}

/// 开新一轮时由运行时建立本轮的 assistant 消息，`from_id` 指向它消费的用户消息
#[tokio::test]
async fn a_round_creates_its_own_assistant_message() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "round-opens").await;
    let spec = start_direct_round(&core, topic_id).await;
    assert!(
        spec.message_id.is_none(),
        "开新一轮的 spec 不带 assistant 消息"
    );

    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let messages = core
        .storage()
        .message()
        .list_by_instance(spec.instance_id)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        2,
        "一轮对话只该有 user + assistant 两条消息: {messages:?}"
    );
    assert!(messages[0].from_id.is_none(), "第一条是用户消息");
    assert_eq!(
        messages[1].from_id,
        Some(messages[0].id),
        "新 assistant 必须挂在它消费的那条用户消息上"
    );
    assert_eq!(messages[1].id, assistant_id);
}

/// 轮首的 assistant 锚在实例记下的那条输入上，整批还没被消费的输入一并宣告
#[tokio::test]
async fn a_round_anchors_on_the_bound_input_and_announces_the_backlog() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "anchor-bound").await;
    let spec = start_direct_round(&core, topic_id).await;
    let first = bound_messages(&core, spec.instance_id)
        .await
        .0
        .expect("本轮输入必须记进 user_id");
    let queued = queue_direct_input(&core, topic_id, "queued").await;
    assert_eq!(
        queued, first,
        "已经有没被消费的输入时，新到的输入不得顶替 user_id"
    );

    let (_, seen) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = assistant_of(&core, spec.instance_id).await;
    let anchored = core
        .storage()
        .message()
        .get(assistant_id)
        .await
        .expect("get assistant")
        .expect("assistant exists")
        .from_id;
    assert_eq!(
        anchored,
        Some(first),
        "轮首的 assistant 必须锚在记下的那条输入上"
    );

    let announced: Vec<i64> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::MessageCreated(message) if message.from_id.is_none() => Some(message.id),
            _ => None,
        })
        .collect();
    let backlog: Vec<i64> = core
        .storage()
        .message()
        .list_by_instance(spec.instance_id)
        .await
        .expect("messages")
        .iter()
        .filter(|message| message.from_id.is_none())
        .map(|message| message.id)
        .collect();
    assert_eq!(announced, backlog, "本轮消费的整批输入都要按 id 顺序宣告");
}

/// 子任务重入续写同一条 assistant 消息，不新开一条
#[tokio::test]
async fn a_children_round_continues_the_existing_assistant_message() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![text_delta("first"), stop_delta()],
        vec![text_delta("second"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "children-continue").await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let (outcome, _) = drive_input(
        &core,
        topic_id,
        spec.instance_id,
        Input::Children(Vec::new()),
    )
    .await;
    assert_eq!(
        completed_id(outcome),
        assistant_id,
        "重入必须续写原来那条 assistant 消息"
    );

    let messages = core
        .storage()
        .message()
        .list_by_instance(spec.instance_id)
        .await
        .expect("messages");
    assert_eq!(messages.len(), 2, "重入不得新开消息: {messages:?}");
    let texts: Vec<String> = core
        .storage()
        .message()
        .list_contents(assistant_id)
        .await
        .expect("contents")
        .iter()
        .map(|content| text_of(&content.data))
        .collect();
    assert_eq!(
        texts,
        vec!["first".to_string(), "second".to_string()],
        "两轮的块都按 id 追加在同一条消息上"
    );
}

/// 一轮把实例上全部没被消费的用户消息都宣告出去，且排在 assistant 之前
#[tokio::test]
async fn a_round_announces_the_whole_unconsumed_backlog() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "backlog").await;
    let model_id = core
        .storage()
        .topic()
        .get_topic(topic_id)
        .await
        .expect("topic")
        .expect("topic row")
        .model_id
        .expect("topic model");

    let spec = start_direct_round(&core, topic_id).await;
    // 第二条用户消息排在第一轮启动消息之后，还没有被任何一轮消费
    let queued = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id,
            is_boundary: false,
            is_excluded: false,
            instance_id: spec.instance_id,
        })
        .await
        .expect("queued user");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: queued.id,
            data: AiMessage::new_simple(Role::User, vec![Content::new_text("queued".into())], None),
        })
        .await
        .expect("queued block");
    let user_ids: Vec<i64> = core
        .storage()
        .message()
        .list_by_instance(spec.instance_id)
        .await
        .expect("messages")
        .iter()
        .map(|message| message.id)
        .collect();
    assert_eq!(user_ids.len(), 2, "实例上有两条待消费的用户消息");

    let (_, seen) = drive(&core, topic_id, spec.clone()).await;
    let announced: Vec<(String, i64)> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::MessageCreated(message) => {
                Some((kind_of(message).to_string(), message.id))
            }
            _ => None,
        })
        .collect();
    let users: Vec<i64> = announced
        .iter()
        .filter(|(kind, _)| kind == "user")
        .map(|(_, id)| *id)
        .collect();
    assert_eq!(users, user_ids, "两条用户消息都要按 id 顺序宣告");
    assert_eq!(
        announced.last(),
        Some(&(
            "assistant".to_string(),
            assistant_of(&core, spec.instance_id).await
        )),
        "assistant 的宣告排在全部用户消息之后"
    );

    let announced_blocks: Vec<i64> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::Block { message_id, .. } => Some(*message_id),
            _ => None,
        })
        .collect();
    assert!(
        announced_blocks.contains(&queued.id),
        "待消费输入的正文同样要下发: {announced_blocks:?}"
    );
}

/// 停泊期间写入的输入必须被重入的那一轮宣告出去，不能对客户端不可见
#[tokio::test]
async fn a_children_round_announces_input_written_while_the_node_was_parked() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![text_delta("first"), stop_delta()],
        vec![text_delta("second"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "parked-input").await;
    let model_id = core
        .storage()
        .topic()
        .get_topic(topic_id)
        .await
        .expect("topic")
        .expect("topic row")
        .model_id
        .expect("topic model");
    let spec = start_direct_round(&core, topic_id).await;
    let own_input = user_message_of(&core, spec.instance_id).await;
    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    // 节点停泊（等子任务）期间投递进来的输入：它排在上一轮的 assistant 之后
    let delivered = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id,
            is_boundary: false,
            is_excluded: false,
            instance_id: spec.instance_id,
        })
        .await
        .expect("delivered user");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: delivered.id,
            data: AiMessage::new_simple(
                Role::User,
                vec![Content::new_text("delivered".into())],
                None,
            ),
        })
        .await
        .expect("delivered block");

    let (_, seen) = drive_input(
        &core,
        topic_id,
        spec.instance_id,
        Input::Children(Vec::new()),
    )
    .await;
    let users: Vec<i64> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::MessageCreated(message) if message.from_id.is_none() => Some(message.id),
            _ => None,
        })
        .collect();
    assert!(
        users.contains(&delivered.id),
        "停泊期间写入的输入必须被宣告，实际宣告了 {users:?}"
    );
    assert!(
        !users.contains(&own_input),
        "本轮开轮时已经宣告过的那条输入不得重发，实际宣告了 {users:?}"
    );
    assert_eq!(
        seen.iter()
            .filter_map(|chunk| match chunk {
                AgentProgress::MessageCreated(message) => Some(message.id),
                _ => None,
            })
            .last(),
        Some(assistant_id),
        "assistant 的宣告仍排在全部用户消息之后"
    );
}

/// 重入轮把停在库里的输入一并消费掉：指针随之回退，重启不会再把它跑一遍
#[tokio::test]
async fn a_reentrant_round_consumes_the_parked_input_for_good() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![text_delta("first"), stop_delta()],
        vec![text_delta("second"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "parked-consumed").await;
    let spec = start_direct_round(&core, topic_id).await;
    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    // 停泊期间投递进来的输入经生产路径落库，实例空闲，它会顶替 user_id
    let queued = queue_direct_input(&core, topic_id, "queued").await;
    let bound = bound_messages(&core, spec.instance_id).await;
    assert!(
        bound.0 > Some(assistant_id),
        "新到的输入必须成为待消费的输入: {bound:?}"
    );

    // 重入本轮：它把那条输入读进上下文一并作答，指针回到本轮锚定的形状
    let (outcome, _) = drive_input(
        &core,
        topic_id,
        spec.instance_id,
        Input::Children(Vec::new()),
    )
    .await;
    assert_eq!(
        completed_id(outcome),
        assistant_id,
        "重入续写同一条 assistant"
    );
    let bound = bound_messages(&core, spec.instance_id).await;
    assert!(
        bound.0 < Some(assistant_id),
        "输入被消费后 user_id 必须回退，否则重启会把它再跑一遍: {bound:?}"
    );
    assert_eq!(bound.1, Some(assistant_id), "assistant_id 保持本轮那条");

    // 恢复不得把它当成还没消费的输入
    set_instance_status(&core, spec.instance_id, AgentStatus::Finished).await;
    let snaps = AgentStore::new(core.storage().clone())
        .load_snapshots(topic_id)
        .await
        .expect("snapshots");
    let node = snaps
        .iter()
        .find(|snap| snap.id == spec.instance_id)
        .expect("node");
    assert!(
        node.pending.is_none(),
        "已经被重入轮消费掉的输入（{queued}）不得派生: {node:?}"
    );
}

/// 取消正在进行中的一轮：它锚定的那条输入已经在被作答，不算未消费输入
#[tokio::test]
async fn discarding_inputs_keeps_the_input_the_round_is_answering() {
    let core = common::init_test_core().await;
    let (_model, topic) = common::seed_chat_fixture(&core, "discard-under-way").await;
    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    let spec = wind_core::agent::helper::start_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        instance,
        &[Content::new_text("go".to_string())],
    )
    .await
    .expect("start round");
    let assistant = wind_core::agent::helper::open_assistant(core.storage(), spec.instance_id)
        .await
        .expect("open assistant");

    wind_core::agent::helper::discard_unconsumed_input(core.storage(), spec.instance_id)
        .await
        .expect("discard");

    let stored = core
        .storage()
        .message()
        .list_by_instance(spec.instance_id)
        .await
        .expect("messages");
    assert!(
        stored.iter().all(|message| !message.is_excluded),
        "本轮正在作答的输入与它的助手消息都不算未消费输入: {stored:?}"
    );
    let (user_id, assistant_id) = bound_messages(&core, spec.instance_id).await;
    assert_eq!(
        (user_id, assistant_id),
        (
            Some(user_message_of(&core, spec.instance_id).await),
            Some(assistant.id)
        ),
        "指针必须停在本轮上"
    );
}

/// 开新一轮时本轮 assistant 只宣告一次，且它的块不重复下发
#[tokio::test]
async fn a_round_announces_its_assistant_exactly_once() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "announce-once").await;
    let spec = start_direct_round(&core, topic_id).await;
    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let created: Vec<i64> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::MessageCreated(message) => Some(message.id),
            _ => None,
        })
        .collect();
    assert_eq!(
        created.iter().filter(|id| **id == assistant_id).count(),
        1,
        "本轮 assistant 只能宣告一次，实际宣告了 {created:?}"
    );

    // 流式层对每个分片都发一条 Block（含结束帧那个空分片），只比对非空的
    let own: Vec<String> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::Block {
                message_id, data, ..
            } if *message_id == assistant_id => Some(text_of(data)),
            _ => None,
        })
        .filter(|text| !text.is_empty())
        .collect();
    assert_eq!(
        own,
        vec!["hello".to_string()],
        "本轮 assistant 的内容只该下发一次: {own:?}"
    );
}

/// 续轮不得把本轮 assistant 已落库的块当成本轮内容重放
#[tokio::test]
async fn a_children_round_does_not_replay_the_assistant_blocks() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![text_delta("first"), stop_delta()],
        vec![text_delta("second"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "no-replay").await;
    let spec = start_direct_round(&core, topic_id).await;
    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let (_, seen) = drive_input(
        &core,
        topic_id,
        spec.instance_id,
        Input::Children(Vec::new()),
    )
    .await;
    let blocks: Vec<(i64, i64, String)> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::Block {
                message_id,
                index,
                data,
            } => Some((*message_id, *index, text_of(data))),
            _ => None,
        })
        .collect();
    assert!(
        !blocks
            .iter()
            .any(|(id, _, text)| *id == assistant_id && text == "first"),
        "续轮不得重放上一轮已落库的块: {blocks:?}"
    );
    // 流式层对每个分片都发一条 Block（含结束帧那个空分片），只比对非空的
    let own: Vec<(i64, String)> = blocks
        .iter()
        .filter(|(id, _, text)| *id == assistant_id && !text.is_empty())
        .map(|(_, index, text)| (*index, text.clone()))
        .collect();
    assert_eq!(
        own,
        vec![(0, "second".to_string())],
        "续轮只该下发本轮新跑的那一块，且 index 从 0 起: {blocks:?}"
    );
}

/// fork 出的子实例只宣告它自己那条任务消息，不重放克隆来的父历史
#[tokio::test]
async fn a_fork_child_announces_only_its_own_task() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "fork-announce").await;
    let base = fake_server(vec![
        vec![text_delta("hello"), stop_delta()],
        vec![text_delta("child done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "fork-announce").await;
    attach_child_agent(&core, topic_id, &child).await;
    let spec = start_direct_round(&core, topic_id).await;
    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    completed_id(outcome);

    let parent = wind_core::agent::helper::get_instance_by_id(core.storage(), spec.instance_id)
        .await
        .expect("parent");
    let child_spec = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic_id,
        &parent,
        &spawn_action(&child.key, AgentMode::Fork, "work"),
    )
    .await
    .expect("fork spawn");

    let (_, seen) = drive(&core, topic_id, child_spec.clone()).await;
    let users: Vec<i64> = seen
        .iter()
        .filter_map(|chunk| match chunk {
            AgentProgress::MessageCreated(message) if message.from_id.is_none() => Some(message.id),
            _ => None,
        })
        .collect();
    let task = core
        .storage()
        .message()
        .list_by_instance(child_spec.instance_id)
        .await
        .expect("messages")
        .into_iter()
        .rev()
        .find(|message| message.from_id.is_none())
        .expect("task message")
        .id;
    assert_eq!(
        users,
        vec![task],
        "fork 子实例只该宣告自己的任务消息，克隆来的父历史不得重放"
    );
}

/// 轮首就建本轮 assistant：加载阶段失败（凭证被删）也要把错误落成它的内容块
#[tokio::test]
async fn a_round_that_fails_to_load_still_persists_the_error_on_its_own_message() {
    let core = common::init_test_core().await;
    let base = "http://127.0.0.1:1".to_string();
    let topic_id = seeded_topic(&core, &base, "no-cred").await;
    let topic = core
        .storage()
        .topic()
        .get_topic(topic_id)
        .await
        .expect("topic")
        .expect("topic row");
    let model_id = topic.model_id.expect("topic model");
    let model = core
        .storage()
        .model()
        .get(model_id)
        .await
        .expect("model")
        .expect("model row");
    let credential = core
        .storage()
        .provider()
        .get_provider_credentials(model.provider_id)
        .await
        .expect("credentials")
        .into_iter()
        .next()
        .expect("seeded topic has a credential");
    core.storage()
        .provider()
        .delete_credentials(credential.id)
        .await
        .expect("delete credentials");

    // 落库这一步不查凭证：输入先落库，失败留给这一轮
    let spec = start_direct_round(&core, topic_id).await;
    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    let failure = match outcome {
        Outcome::Failed(failure) => failure,
        other => panic!("拿不到凭证必须交还失败判定: {other:?}"),
    };
    let message_id = failure
        .message_id
        .expect("失败必须带轮首建好的 assistant 消息");

    let blocks = core
        .storage()
        .message()
        .list_contents(message_id)
        .await
        .expect("contents");
    assert_eq!(blocks.len(), 1, "失败必须落成恰好一块内容块: {blocks:?}");
    assert!(
        text_of(&blocks[0].data).contains("credentials"),
        "错误文本要能被人看懂: {}",
        text_of(&blocks[0].data)
    );

    let messages = core
        .storage()
        .message()
        .list_by_instance(spec.instance_id)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        2,
        "用户消息 + 轮首建好的 assistant: {messages:?}"
    );
    assert!(
        messages[1].from_id.is_some(),
        "末条消息必须是 assistant，否则恢复时会被判成待消费输入重跑"
    );
}

/// 起一轮直驱对话，返回开新一轮的 spec（不带 assistant 消息）
async fn start_direct_round(core: &WindCore, topic_id: i64) -> AgentSpec {
    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    wind_core::agent::helper::start_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic_id,
        instance,
        &[Content::new_text("go".into())],
    )
    .await
    .expect("start round")
}

/// 经生产路径再投一条输入，返回它记进的 `user_id`
async fn queue_direct_input(core: &WindCore, topic_id: i64, text: &str) -> i64 {
    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    let instance_id = instance.id;
    wind_core::agent::helper::start_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic_id,
        instance,
        &[Content::new_text(text.to_string())],
    )
    .await
    .expect("start round");
    bound_messages(core, instance_id)
        .await
        .0
        .expect("本轮输入必须记进 user_id")
}

/// 实例当前的两个消息指针 `(user_id, assistant_id)`
async fn bound_messages(core: &WindCore, instance_id: i64) -> (Option<i64>, Option<i64>) {
    let instance = wind_core::agent::helper::get_instance_by_id(core.storage(), instance_id)
        .await
        .expect("instance");
    (instance.user_id, instance.assistant_id)
}

/// 实例上第一条用户消息，即本轮要作答的那条
async fn user_message_of(core: &WindCore, instance_id: i64) -> i64 {
    core.storage()
        .message()
        .list_by_instance(instance_id)
        .await
        .expect("messages")
        .into_iter()
        .find(|message| message.from_id.is_none())
        .expect("user message")
        .id
}

/// 起一轮并把本轮的 assistant 消息预先建好：需要在驱动前挂审批行的用例用它
async fn start_direct_round_with_assistant(core: &WindCore, topic_id: i64) -> AgentSpec {
    let spec = start_direct_round(core, topic_id).await;
    let assistant = wind_core::agent::helper::open_assistant(core.storage(), spec.instance_id)
        .await
        .expect("open assistant");
    AgentSpec {
        instance_id: spec.instance_id,
        message_id: Some(assistant.id),
    }
}

/// 实例末条 assistant 消息的 id
async fn assistant_of(core: &WindCore, instance_id: i64) -> i64 {
    core.storage()
        .message()
        .list_by_instance(instance_id)
        .await
        .expect("messages")
        .iter()
        .rev()
        .find(|m| m.from_id.is_some())
        .expect("assistant message")
        .id
}

/// 断言一轮以 `Completed` 收尾，返回它交出的消息 id
fn completed_id(outcome: Outcome<AgentDomain>) -> i64 {
    match outcome {
        Outcome::Completed(out) => out.message_id,
        other => panic!("本轮必须以 Completed 收尾: {other:?}"),
    }
}

/// 直接驱动一次 `AgentBehavior::run`，返回判定与沿途下发的进度
async fn drive(
    core: &WindCore,
    topic_id: i64,
    spec: AgentSpec,
) -> (Outcome<AgentDomain>, Vec<AgentProgress>) {
    let node = spec.instance_id;
    drive_input(core, topic_id, node, Input::Fresh(spec)).await
}

/// 以指定输入直接驱动一次 `AgentBehavior::run`
async fn drive_input(
    core: &WindCore,
    topic_id: i64,
    node: i64,
    input: Input<AgentDomain>,
) -> (Outcome<AgentDomain>, Vec<AgentProgress>) {
    let behavior = AgentBehavior::new(core.storage().clone(), core.registry().clone(), topic_id);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let emit = move |chunk: AgentProgress| {
        if let Ok(mut seen) = sink.lock() {
            seen.push(chunk);
        }
    };
    let ctx = Ctx::<AgentDomain>::new(node, 1, CancellationToken::new(), &emit);
    let outcome = behavior.run(ctx, input).await;
    let seen = seen.lock().expect("lock").clone();
    (outcome, seen)
}

/// 一条消息是用户消息还是助手消息
fn kind_of(message: &Message) -> &'static str {
    if message.from_id.is_none() {
        "user"
    } else {
        "assistant"
    }
}

// ---------------------------------------------------------------------------
// 派生：直驱 AgentBehavior
// ---------------------------------------------------------------------------

/// `agent_spawn_agent` 的工具调用分片
fn spawn_delta_mode(agent_key: &str, task: &str, mode: &str) -> String {
    chunk(serde_json::json!({
        "tool_calls": [{
            "index": 0,
            "id": "call-spawn",
            "type": "function",
            "function": {
                "name": "agent_spawn_agent",
                "arguments": serde_json::json!({
                    "agent_key": agent_key,
                    "mode": mode,
                    "task": task,
                })
                .to_string(),
            }
        }]
    }))
}

/// 一条 sync 的 `agent_spawn_agent` 工具调用分片
fn spawn_delta(agent_key: &str, task: &str) -> String {
    spawn_delta_mode(agent_key, task, "sync")
}

/// 一条 `call-spawn` 的参数畸形派生调用分片
fn spawn_delta_bad_arguments() -> String {
    chunk(serde_json::json!({
        "tool_calls": [{
            "index": 0,
            "id": "call-spawn",
            "type": "function",
            "function": {"name": "agent_spawn_agent", "arguments": "{not json"}
        }]
    }))
}

/// 同一条响应里一条 `call-spawn` 派生 + 一条 `call-list` 的工具调用分片
fn spawn_and_list_delta(agent_key: &str) -> String {
    chunk(serde_json::json!({
        "tool_calls": [
            {
                "index": 0,
                "id": "call-spawn",
                "type": "function",
                "function": {
                    "name": "agent_spawn_agent",
                    "arguments": serde_json::json!({
                        "agent_key": agent_key,
                        "mode": "sync",
                        "task": "work",
                    })
                    .to_string(),
                }
            },
            {
                "index": 1,
                "id": "call-list",
                "type": "function",
                "function": {"name": "agent_list_agents", "arguments": "{}"}
            }
        ]
    }))
}

/// 一条派生调用的 `FunctionCall`（call id 由调用方指定）
fn spawn_call(call_id: &str, agent_key: &str, mode: &str, task: &str) -> FunctionCall {
    FunctionCall {
        id: call_id.to_string(),
        name: "agent_spawn_agent".to_string(),
        arguments: serde_json::json!({
            "agent_key": agent_key,
            "mode": mode,
            "task": task,
        })
        .to_string(),
    }
}

/// 给实例补一轮已完成的 user/assistant 对话
async fn seed_completed_turn(
    core: &WindCore,
    model_id: i64,
    instance_id: i64,
    user_text: &str,
    reply: &str,
) {
    let user = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("user message");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: user.id,
            data: AiMessage::new_simple(
                Role::User,
                vec![Content::new_text(user_text.into())],
                None,
            ),
        })
        .await
        .expect("user block");
    let assistant = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: Some(user.id),
            model_id,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("assistant message");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: assistant.id,
            data: AiMessage::new_simple(
                Role::Assistant,
                vec![Content::new_text(reply.into())],
                None,
            ),
        })
        .await
        .expect("assistant block");
}

/// 给实例补一条只带工具调用的 assistant 消息，返回它的 id
async fn seed_tool_request(
    core: &WindCore,
    model_id: i64,
    instance_id: i64,
    calls: Vec<FunctionCall>,
) -> i64 {
    let user = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("user message");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: user.id,
            data: AiMessage::new_simple(Role::User, vec![Content::new_text("go".into())], None),
        })
        .await
        .expect("user block");
    let assistant = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: Some(user.id),
            model_id,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("assistant message");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: assistant.id,
            data: AiMessage::new_tool_request(calls, None),
        })
        .await
        .expect("tool request block");
    assistant.id
}

/// 给实例的某条消息补一块 assistant 文本
async fn finish_with_text(core: &WindCore, message_id: i64, text: &str) {
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id,
            data: AiMessage::new_simple(
                Role::Assistant,
                vec![Content::new_text(text.into())],
                None,
            ),
        })
        .await
        .expect("finish block");
}

/// 直接改实例状态列，模拟落库落后于内存树的过期状态
async fn set_instance_status(core: &WindCore, instance_id: i64, status: AgentStatus) {
    core.storage()
        .agent()
        .update_instance(
            instance_id,
            UpdateInstance {
                status: Some(status),
                ..Default::default()
            },
        )
        .await
        .expect("update instance");
}

/// 一条 `call-spawn` 的派生动作
fn spawn_action(
    agent_key: &str,
    mode: AgentMode,
    task: &str,
) -> wind_core::agent::tool::SpawnAgentAction {
    wind_core::agent::tool::SpawnAgentAction {
        call_id: "call-spawn".to_string(),
        data: wind_core::agent::tool::SpawnAgentRequest {
            agent_key: agent_key.to_string(),
            mode,
            task: task.to_string(),
        },
    }
}

/// 把一个子 agent 定义挂到 topic 的能力映射上
async fn attach_child_agent(core: &WindCore, topic_id: i64, definition: &AgentDefinition) {
    core.storage()
        .agent()
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id,
            agent_id: definition.id,
        })
        .await
        .expect("attach agent");
}

/// 一个实例末条 assistant 消息的全部内容块拼起来的文本
async fn assistant_text(core: &WindCore, instance_id: i64) -> String {
    let messages = core
        .storage()
        .message()
        .list_by_instance(instance_id)
        .await
        .expect("messages");
    let Some(assistant) = messages.iter().rev().find(|m| m.from_id.is_some()) else {
        return String::new();
    };
    core.storage()
        .message()
        .list_contents(assistant.id)
        .await
        .expect("contents")
        .iter()
        .map(|content| text_of(&content.data))
        .collect::<Vec<_>>()
        .join("")
}

/// 一个实例全部消息块拼起来的文本
async fn blocks_text(core: &WindCore, instance_id: i64) -> String {
    let messages = core
        .storage()
        .message()
        .list_by_instance(instance_id)
        .await
        .expect("messages");
    let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    core.storage()
        .message()
        .list_contents_by_messages(&ids)
        .await
        .expect("contents")
        .into_iter()
        .flat_map(|c| c.data.content)
        .filter_map(|content| match content {
            wind_ai::message::Content::Text { data } => Some(data),
            other => Some(other.to_string()),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// `agent_list_agents` 的结果作为工具结果块回到模型，本轮继续
#[tokio::test]
async fn a_direct_round_answers_list_agents_with_a_tool_result() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "listed").await;
    let base = fake_server(vec![
        vec![tool_delta(), stop_delta()],
        vec![text_delta("done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "direct-list").await;
    attach_child_agent(&core, topic_id, &child).await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let contents = core
        .storage()
        .message()
        .list_contents(assistant_id)
        .await
        .expect("contents");
    let outputs: String = contents.iter().map(|c| tool_outputs(&c.data)).collect();
    assert!(
        outputs.contains(&child.key),
        "能力清单必须作为工具结果回到上下文: {outputs}"
    );
    assert!(
        seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { data, .. } if tool_outputs(data).contains(&child.key)
        )),
        "工具结果整块必须先下发给订阅方: {seen:?}"
    );
}

/// 直驱一轮只开一条 chat 连接，sync 派生交还引擎并留下子实例
#[tokio::test]
async fn a_direct_round_hands_a_sync_spawn_to_the_engine() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "sync-child").await;
    let base = fake_server(vec![vec![spawn_delta(&child.key, "work"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "direct-sync").await;
    attach_child_agent(&core, topic_id, &child).await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    match outcome {
        Outcome::Spawn { spawn } => {
            assert_eq!(spawn.len(), 1, "一个 spawn 调用交还一个请求");
            assert!(!spawn[0].detached, "sync 子任务要进父节点的等待屏障");
            assert_eq!(
                spawn[0].id, spawn[0].spec.instance_id,
                "请求 id 就是子实例 id"
            );
            assert!(
                spawn[0].spec.message_id.is_none(),
                "子实例开新一轮，由运行时自己建本轮的 assistant 消息"
            );
            assert_ne!(spawn[0].spec.instance_id, spec.instance_id);

            let child = wind_core::agent::helper::get_instance_by_id(
                core.storage(),
                spawn[0].spec.instance_id,
            )
            .await
            .expect("child");
            assert_eq!(child.parent_id, Some(spec.instance_id));
            assert_eq!(child.mode, Some(AgentMode::Sync));
        }
        other => panic!("sync 派生必须交还 Outcome::Spawn: {other:?}"),
    }

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 2, "派生必须真的建出一个子实例");
    assert!(
        !seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { data, .. } if tool_output_ids(data).iter().any(|id| id == "call-spawn")
        )),
        "派生成功的调用不在这里回报，留给子任务结束后的补报: {seen:?}"
    );
}

/// background 派生不阻塞父节点：请求 detached，模式落库
#[tokio::test]
async fn a_direct_round_hands_a_background_spawn_to_the_engine_detached() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-child").await;
    let base = fake_server(vec![vec![
        spawn_delta_mode(&child.key, "work", "background"),
        stop_delta(),
    ]])
    .await;
    let topic_id = seeded_topic(&core, &base, "direct-bg").await;
    attach_child_agent(&core, topic_id, &child).await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    match outcome {
        Outcome::Spawn { spawn } => {
            assert_eq!(spawn.len(), 1);
            assert!(spawn[0].detached, "background 不进入父节点的等待屏障");
            let child = wind_core::agent::helper::get_instance_by_id(
                core.storage(),
                spawn[0].spec.instance_id,
            )
            .await
            .expect("child");
            assert_eq!(
                child.mode,
                Some(AgentMode::Background),
                "background 模式必须落库，恢复时靠它重建 detached"
            );
        }
        other => panic!("background 派生必须交还 Outcome::Spawn: {other:?}"),
    }
}

/// 派生失败只交回一条错误工具结果，本轮继续，不建实例
#[tokio::test]
async fn a_direct_round_turns_a_failed_spawn_into_a_tool_error() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![spawn_delta("no-such-agent", "work"), stop_delta()],
        vec![text_delta("recovered"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "direct-bad-spawn").await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let contents = core
        .storage()
        .message()
        .list_contents(assistant_id)
        .await
        .expect("contents");
    let outputs: String = contents.iter().map(|c| tool_outputs(&c.data)).collect();
    assert!(
        outputs.contains("no-such-agent"),
        "失败原因必须作为该调用的工具结果回到上下文: {outputs}"
    );
    assert!(
        seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { data, .. } if tool_outputs(data).contains("no-such-agent")
        )),
        "错误工具结果必须先下发给订阅方: {seen:?}"
    );

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 1, "被拒的派生不得留下实例");
}

/// fork 派生把父实例的消息历史复制进子实例
#[tokio::test]
async fn a_fork_spawn_copies_the_parent_history() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "fork-child").await;
    let base = fake_server(vec![vec![text_delta("hello"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "fork").await;
    attach_child_agent(&core, topic_id, &child).await;
    let spec = start_direct_round(&core, topic_id).await;
    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    assert!(
        matches!(outcome, Outcome::Completed(_)),
        "先让父实例跑完一轮，才有历史可 fork"
    );

    let parent = wind_core::agent::helper::get_instance_by_id(core.storage(), spec.instance_id)
        .await
        .expect("parent");
    let action = spawn_action(&child.key, AgentMode::Fork, "work");
    let child = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic_id,
        &parent,
        &action,
    )
    .await
    .expect("fork spawn");

    let text = blocks_text(&core, child.instance_id).await;
    assert!(
        text.contains("hello"),
        "fork 必须把父实例的历史复制进子实例: {text}"
    );
    assert!(
        text.contains("work"),
        "子实例必须带上自己的任务输入: {text}"
    );
}

/// 深度上限取自**派生的那一方**的定义
///
/// 父定义用默认的 `max_spawn_depth = 1`，子定义显式设成 99：
/// 只有「从父读」会被拒，「从子读」或「从常量读」都会放行 —— 这条用例才钉得住来源
#[tokio::test]
async fn a_spawn_past_the_depth_ceiling_is_refused() {
    use wind_core::models::{AgentDefinitionData, CreateAgentDefinition};

    let core = common::init_test_core().await;
    let (_, topic) = common::seed_chat_fixture(&core, "depth").await;

    let parent_def = common::seed_definition(&core, "depth-parent").await;
    let mut data = AgentDefinitionData::default();
    data.permission_policy.max_spawn_depth = 99;
    let child_def = core
        .storage()
        .agent()
        .create_definition(CreateAgentDefinition {
            name: "depth-child".into(),
            description: "depth-child".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data,
        })
        .await
        .expect("child definition");

    let root = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    let at_depth_one = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        root.id,
        parent_def.id,
        AgentMode::Sync,
    )
    .await
    .expect("depth-one instance");

    let action = wind_core::agent::tool::SpawnAgentAction {
        call_id: "c1".into(),
        data: wind_core::agent::tool::SpawnAgentRequest {
            agent_key: child_def.key.clone(),
            mode: AgentMode::Sync,
            task: "too deep".into(),
        },
    };

    let err = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        &at_depth_one,
        &action,
    )
    .await
    .expect_err("越界必须被拒");
    assert!(
        err.to_string().contains("depth"),
        "错误要说清是深度问题: {err}"
    );

    let allowed = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        &root,
        &action,
    )
    .await;
    assert!(allowed.is_ok(), "无绑定定义的父节点不设上限: {allowed:?}");

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic.id)
        .await
        .expect("instances");
    assert_eq!(
        instances.len(),
        3,
        "被拒的那一次不得留下实例（根 + 深度 1 + 放行的那一个）"
    );
}

/// 一条消息里所有工具调用结果拼起来
fn tool_outputs(message: &wind_ai::message::Message) -> String {
    message
        .content
        .iter()
        .filter_map(|c| match c {
            Content::FunctionCall { data } => Some(data.content.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// 一条消息里所有工具调用结果对应的 call id
fn tool_output_ids(message: &wind_ai::message::Message) -> Vec<String> {
    message
        .content
        .iter()
        .filter_map(|c| match c {
            Content::FunctionCall { data } => Some(data.id.clone()),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// hub 层端到端：真实门面 + 真实 hub + 真实 Behavior
// ---------------------------------------------------------------------------

/// 按请求体分派的假 SSE 服务
///
/// 每条连接独立成一个任务，`route` 按请求体决定这条连接回放哪一段脚本
/// —— 父节点与子节点并发时，连接到达顺序不保证
async fn routed_server<F>(route: F) -> String
where
    F: Fn(&str) -> Vec<String> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let route = Arc::new(route);
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let route = route.clone();
            tokio::spawn(async move {
                let body = read_request(&mut sock).await;
                let script = route(&body);
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
            });
        }
    });
    format!("http://{addr}")
}

/// 读完一个 HTTP 请求，返回它的 body
async fn read_request(sock: &mut tokio::net::TcpStream) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        let read = sock.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            return String::new();
        }
        buf.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buf.len() < head_end + length {
        let read = sock.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8_lossy(&buf[head_end..]).to_string()
}

/// 取消根实例级联整棵子树，且每个节点的终态事件都在流关闭之前到达
#[tokio::test]
async fn cancelling_the_root_stops_the_whole_subtree_before_the_stream_closes() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "cascade-child").await;
    // 父请求派生一个同步子任务；子任务持续产出，取消时它一定还在跑
    let mut slow_child: Vec<String> = (0..50).map(|_| text_delta("x")).collect();
    slow_child.push(stop_delta());
    let base = fake_server(vec![
        vec![spawn_delta(&child.key, "work"), stop_delta()],
        slow_child,
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "cascade").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    // 等到子实例被宣告出来：父的 InstanceCreated 在前，子的在后
    let mut created = 0usize;
    drain_until(&mut rx, |e| {
        if matches!(e, TopicEvent::InstanceCreated { .. }) {
            created += 1;
        }
        created == 2
    })
    .await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    let root = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root")
        .id;

    handle.cancel_task(root).await.expect("cancel");

    // 读到流关闭为止：Sealed 在适配层被翻译成「关流」，不是一条 TopicEvent
    let seen = drain_to_close(&mut rx).await;
    let cancelled: std::collections::BTreeSet<i64> = seen
        .iter()
        .filter_map(|e| match e {
            TopicEvent::TaskStatusChanged {
                instance_id,
                status,
                ..
            } if *status == AgentStatus::Cancelled => Some(*instance_id),
            _ => None,
        })
        .collect();
    let expected: std::collections::BTreeSet<i64> = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances")
        .iter()
        .map(|i| i.id)
        .collect();
    assert_eq!(cancelled, expected, "每个节点都必须收到取消终态，一个不丢");
    assert!(
        !seen.iter().any(|e| matches!(
            e,
            TopicEvent::TaskStatusChanged {
                status: AgentStatus::Cancelling,
                ..
            }
        )),
        "Cancelling 是短暂中间相，整条取消链上都不许对外发: {seen:?}"
    );

    for instance in core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances")
    {
        assert_eq!(
            instance.status,
            AgentStatus::Cancelled,
            "实例 {} 取消后必须落库",
            instance.id
        );
    }
}

/// 主实例在跑（停在等待审批，确定性地不依赖时序）时，第二条任务必须入队而不是被拒
#[tokio::test]
async fn a_second_task_while_the_first_runs_is_queued_instead_of_rejected() {
    let core = common::init_test_core().await;
    // 逐次记下模型请求的 body：排队的那条输入必须出现在下一轮的上下文里
    let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded = bodies.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let base = routed_server(move |body: &str| {
        recorded.lock().expect("lock").push(body.to_string());
        match calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![tool_delta(), stop_delta()],
            _ => vec![text_delta("answered"), stop_delta()],
        }
    })
    .await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "queued", ToolApprovalPolicy::Manual).await;
    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");

    handle
        .create_task(vec![Content::new_text("one".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_until(&mut rx, |e| {
        matches!(e, TopicEvent::ApprovalRequired { .. })
    })
    .await;
    let (instance_id, message_id) = seen
        .iter()
        .find_map(|e| match e {
            TopicEvent::ApprovalRequired {
                instance_id,
                message_id,
                ..
            } => Some((*instance_id, *message_id)),
            _ => None,
        })
        .expect("审批请求");

    handle
        .create_task(vec![Content::new_text("two".to_string())])
        .await
        .expect("mailbox")
        .expect("主实例未终结时也必须受理：入队不是业务错误");

    // 入队即落库：多一条 user 消息，且没有空转的 assistant
    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    let queued = core
        .storage()
        .message()
        .list_by_instance(instance.id)
        .await
        .expect("messages");
    assert_eq!(queued.len(), 3, "两条输入 + 本轮 assistant: {queued:?}");
    assert!(
        queued[2].from_id.is_none(),
        "排队的是用户消息，不该先建助手消息: {queued:?}"
    );

    // 恢复到终态：排队的那条输入被下一轮模型请求带上
    let requests = core
        .storage()
        .approval()
        .list_pending_by_message(message_id)
        .await
        .expect("pending");
    handle
        .approve(
            instance_id,
            requests.iter().map(|r| r.id).collect::<Vec<i64>>(),
            Vec::new(),
        )
        .await
        .expect("approve");
    drain_to_close(&mut rx).await;

    let texts = bodies.lock().expect("lock").clone();
    assert_eq!(texts.len(), 2, "恢复后的下一轮要真的请求模型: {texts:?}");
    assert!(
        texts[1].contains("two"),
        "排队的输入必须进入下一轮的上下文: {}",
        texts[1]
    );

    // 排队不额外建助手消息：恢复续写本轮那条，库里仍是两条输入 + 一条 assistant
    let after = core
        .storage()
        .message()
        .list_by_instance(instance.id)
        .await
        .expect("messages");
    let roles: Vec<bool> = after.iter().map(|m| m.from_id.is_some()).collect();
    assert_eq!(
        roles,
        vec![false, true, false],
        "排队输入不得留下空转的助手消息: {after:?}"
    );
}

/// 两个并发的 create_task：建节点与投放必须串行，两条输入都得进模型上下文
#[tokio::test]
async fn two_concurrent_tasks_both_reach_the_model() {
    let core = common::init_test_core().await;
    let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded = bodies.clone();
    let base = routed_server(move |body: &str| {
        recorded.lock().expect("lock").push(body.to_string());
        vec![text_delta("ok"), stop_delta()]
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "concurrent").await;
    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");

    let (first, second) = tokio::join!(
        handle.create_task(vec![Content::new_text("one".to_string())]),
        handle.create_task(vec![Content::new_text("two".to_string())]),
    );
    first.expect("mailbox").expect("第一条必须受理");
    second.expect("mailbox").expect("第二条必须受理");
    drain_to_close(&mut rx).await;

    let requests = bodies.lock().expect("lock").clone();
    assert!(
        requests
            .iter()
            .any(|body| body.contains("one") && body.contains("two")),
        "两条输入必须落进同一轮的上下文，各开一轮就是被拒或丢输入: {requests:?}"
    );
    let combined = requests.join("\n");
    assert!(combined.contains("one"), "第一条输入丢了: {combined}");
    assert!(
        combined.contains("two"),
        "并发的第二条输入没有对应节点，丢了"
    );
}

/// 取消的边界：未知 id 报错，已终态的 id 静默
#[tokio::test]
async fn cancelling_an_unknown_instance_errors_and_a_terminal_one_is_silent() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![vec![text_delta("ok"), stop_delta()]]).await;
    let topic_id = seeded_topic(&core, &base, "cancel-edges").await;
    let handle = core.fetch_topic(topic_id);

    let unknown = handle
        .cancel_task(999_999)
        .await
        .expect_err("树里不存在的实例必须报错");
    assert!(
        unknown.to_string().contains("Task not found"),
        "错误文本要能被人看懂: {unknown}"
    );

    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("hi".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_until_terminal(&mut rx).await;
    let instance_id = seen
        .iter()
        .find_map(|e| match e {
            TopicEvent::InstanceCreated { data } => Some(data.id),
            _ => None,
        })
        .expect("instance");

    handle
        .cancel_task(instance_id)
        .await
        .expect("已终态实例的取消必须成功返回");
    // 整棵树安静后适配层会关流，所以「没有事件」有两个来源：超时，或流已关闭
    match timeout(Duration::from_millis(200), rx.recv()).await {
        Ok(Ok(event)) => panic!("已终态实例的取消不得产生任何事件: {event:?}"),
        _ => {}
    }
}

/// 取消节点要连库里那批未消费的输入一起丢弃：留着会被下一轮当成待消费输入捡回来
#[tokio::test]
async fn cancelling_a_node_discards_its_queued_input_for_good() {
    let core = common::init_test_core().await;
    let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded = bodies.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let base = routed_server(move |body: &str| {
        recorded.lock().expect("lock").push(body.to_string());
        match calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![tool_delta(), stop_delta()],
            _ => vec![text_delta("answered"), stop_delta()],
        }
    })
    .await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "discard-queued", ToolApprovalPolicy::Manual).await;
    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");

    handle
        .create_task(vec![Content::new_text("one".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    drain_until(&mut rx, |e| {
        matches!(e, TopicEvent::ApprovalRequired { .. })
    })
    .await;

    // 节点停在等待审批，这条输入确定性地只排队、不被消费
    handle
        .create_task(vec![Content::new_text("two".to_string())])
        .await
        .expect("mailbox")
        .expect("主实例未终结时也必须受理：入队不是业务错误");

    let instance = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    let before: Vec<bool> = core
        .storage()
        .message()
        .list_by_instance(instance.id)
        .await
        .expect("messages")
        .iter()
        .map(|message| message.is_excluded)
        .collect();
    assert_eq!(
        before,
        vec![false, false, false],
        "取消之前「本轮输入 + 助手消息 + 排队输入」都不许被排除"
    );

    handle.cancel_task(instance.id).await.expect("cancel");
    drain_to_close(&mut rx).await;

    // 逐条：(是否助手消息, 是否被排除) —— 本轮自己的输入与助手消息都不许动
    let stored = core
        .storage()
        .message()
        .list_by_instance(instance.id)
        .await
        .expect("messages");
    let rows: Vec<(bool, bool)> = stored
        .iter()
        .map(|message| (message.from_id.is_some(), message.is_excluded))
        .collect();
    assert_eq!(
        rows,
        vec![(false, false), (true, false), (false, true)],
        "取消只许丢弃排队的那条输入: {stored:?}"
    );

    // 重新开一轮：模型只能看到新输入，看不到被丢弃的那条
    let mut rx = handle.subscribe().await.expect("resubscribe");
    handle
        .create_task(vec![Content::new_text("three".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    drain_to_close(&mut rx).await;

    let texts = bodies.lock().expect("lock").clone();
    assert_eq!(texts.len(), 2, "取消之后必须真的再请求一次模型: {texts:?}");
    assert!(
        texts[1].contains("three"),
        "新输入必须进入上下文: {}",
        texts[1]
    );
    assert!(
        !texts[1].contains("two"),
        "被丢弃的输入不得进入上下文: {}",
        texts[1]
    );
}

/// 同步子任务的产出必须作为工具结果回到父上下文
#[tokio::test]
async fn a_sync_spawn_returns_the_child_text_to_the_parent() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "sync-return").await;
    // 一段脚本 = 一条连接：父 spawn → 子答 → 父收下结果再答
    let base = fake_server(vec![
        vec![spawn_delta(&child.key, "work"), stop_delta()],
        vec![text_delta("child done"), stop_delta()],
        vec![text_delta("parent done"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "sync-return").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_to_close(&mut rx).await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 2, "父 + 一个同步子实例");
    let parent = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");

    let text = blocks_text(&core, parent.id).await;
    assert!(
        text.contains("child done"),
        "同步子任务的产出必须作为工具结果回到父上下文: {text}"
    );
    assert!(
        text.contains("parent done"),
        "父节点必须收下结果后继续这一轮: {text}"
    );
    assert!(
        seen.iter()
            .any(|e| matches!(e, TopicEvent::InstanceCreated { .. })),
        "父与子都要宣告 InstanceCreated"
    );
}

/// 后台派生的父节点不等子任务；子任务的产出作为新一轮输入回到父节点
#[tokio::test]
async fn a_background_spawn_does_not_block_the_parent() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-return").await;

    // 子任务持续产出，父的次轮一定在它跑完之前发生
    let mut child_script: Vec<String> = (0..30).map(|_| text_delta("child working")).collect();
    child_script.push(text_delta("child done"));
    child_script.push(stop_delta());
    let key = child.key.clone();
    // 按请求体分派：父收下后台产出的第三轮 / 父次轮 / 父首轮 / 子
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-return").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_to_close(&mut rx).await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 2, "父 + 一个后台子实例");
    let parent = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let background = instances
        .iter()
        .find(|i| i.parent_id.is_some())
        .expect("child");
    assert_eq!(
        background.mode,
        Some(AgentMode::Background),
        "background 模式必须落库"
    );

    let text = blocks_text(&core, parent.id).await;
    assert!(
        text.contains("background task started"),
        "父节点必须先拿到「已启动」再跑自己的下一轮: {text}"
    );
    assert!(
        text.contains("[后台任务") && text.contains("child done"),
        "后台子任务的产出必须作为新一轮输入回到父节点: {text}"
    );
    assert!(
        text.contains("parent again"),
        "父节点必须在流关闭之前把投递的那一轮跑完: {text}"
    );
    let finished = seen
        .iter()
        .filter(|e| {
            matches!(
                e,
                TopicEvent::MessageFinished { instance_id, .. } if *instance_id == parent.id
            )
        })
        .count();
    assert_eq!(
        finished, 2,
        "父节点必须跑完「拿到已启动」与「收下后台产出」两轮"
    );
}

/// 后台子任务的产出作为新一轮输入回到父节点，并在同一条订阅上宣告出来
#[tokio::test]
async fn a_background_result_comes_back_as_a_new_turn() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-turn").await;

    let mut child_script: Vec<String> = (0..30).map(|_| text_delta("child working")).collect();
    child_script.push(text_delta("child done"));
    child_script.push(stop_delta());
    let key = child.key.clone();
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-turn").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_to_close(&mut rx).await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    let root = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let messages = core
        .storage()
        .message()
        .list_by_instance(root.id)
        .await
        .expect("messages");
    let delivered = messages
        .iter()
        .rev()
        .find(|m| m.from_id.is_none() && !m.is_excluded)
        .expect("投递的输入必须落成父节点的一条 user 消息");
    assert_ne!(
        delivered.id, messages[0].id,
        "投递的输入必须是新写的一条，不是首轮那条输入"
    );
    assert!(
        messages.last().is_some_and(|m| m.id != delivered.id),
        "投递的输入必须喂进后续的一轮，而不是留在末条没人消费"
    );
    let blocks = core
        .storage()
        .message()
        .list_contents(delivered.id)
        .await
        .expect("contents");
    let block_text = blocks
        .iter()
        .map(|content| text_of(&content.data))
        .collect::<Vec<_>>()
        .join("");
    assert!(
        block_text.contains("[后台任务") && block_text.contains("child done"),
        "投递的输入要带上抬头与子任务产出: {block_text}"
    );

    // 投递的那一轮：先宣告新的 user 消息，再下发它的内容块，最后才是 MessageFinished
    let created = seen
        .iter()
        .position(
            |e| matches!(e, TopicEvent::MessageCreated { data, .. } if data.id == delivered.id),
        )
        .expect("投递的输入必须被宣告");
    let block = seen
        .iter()
        .position(
            |e| matches!(e, TopicEvent::Message { message_id, .. } if *message_id == delivered.id),
        )
        .expect("投递的输入的内容块必须下发");
    let finished = seen
        .iter()
        .rposition(|e| matches!(e, TopicEvent::MessageFinished { instance_id, .. } if *instance_id == root.id))
        .expect("投递的那一轮必须在流关闭之前跑完");
    assert!(
        created < block && block < finished,
        "投递的输入、它的内容块与这一轮的收尾必须按序到达: {created} {block} {finished}"
    );
}

/// 后台子任务失败时，错误文本同样作为新一轮输入回到父节点
#[tokio::test]
async fn a_background_failure_comes_back_too() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-failure").await;

    let key = child.key.clone();
    // 子任务的服务回一段非 JSON：解析失败即这一轮失败
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            vec!["not json".to_string()]
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-failure").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    drain_to_close(&mut rx).await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    let root = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let background = instances
        .iter()
        .find(|i| i.parent_id.is_some())
        .expect("child");
    assert_eq!(
        background.status,
        AgentStatus::Failed,
        "子任务必须真的失败，否则这条用例测的不是失败路径"
    );

    // 子任务自己落库的错误文本，就是它要交付给父节点的产出
    let failure = assistant_text(&core, background.id).await;
    assert!(!failure.is_empty(), "失败轮必须把错误文本落成块");
    let text = blocks_text(&core, root.id).await;
    assert!(
        text.contains("[后台任务") && text.contains(&failure),
        "失败的产出必须作为新一轮输入回到父节点: {text}"
    );
    assert!(
        text.contains("parent again"),
        "父节点必须在流关闭之前把投递的那一轮跑完: {text}"
    );
}

/// 后台产出投递的那一轮必须在流关闭之前到达，流不在子任务终结处就关掉
#[tokio::test]
async fn an_undelivered_background_result_keeps_the_stream_open() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-open").await;

    let mut child_script: Vec<String> = (0..30).map(|_| text_delta("child working")).collect();
    child_script.push(text_delta("child done"));
    child_script.push(stop_delta());
    let key = child.key.clone();
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-open").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_to_close(&mut rx).await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    let root = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let background = instances
        .iter()
        .find(|i| i.parent_id.is_some())
        .expect("child");

    let terminated = seen
        .iter()
        .position(|e| {
            matches!(
                e,
                TopicEvent::TaskStatusChanged { instance_id, status, .. }
                    if *instance_id == background.id && *status == AgentStatus::Finished
            )
        })
        .expect("子任务的终结必须在流关闭之前到达");
    let delivered = seen
        .iter()
        .rposition(|e| matches!(e, TopicEvent::MessageFinished { instance_id, .. } if *instance_id == root.id))
        .expect("投递的那一轮必须在流关闭之前跑完");
    assert!(
        terminated < delivered,
        "流必须活到投递之后，不能在子任务终结的那一刻就关掉: {terminated} {delivered}"
    );
}

/// 取消一个还有后台子任务在跑的父节点：丢弃投递，但义务照样释放，树仍能收敛
#[tokio::test]
async fn cancelling_a_parent_with_a_running_background_child_does_not_revive_it() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-cancel").await;

    // 父次轮与子任务都持续产出到远超用例时长，取消一定落在两者都还在跑的时候
    let mut parent_script: Vec<String> = (0..3000).map(|_| text_delta("parent working")).collect();
    parent_script.push(stop_delta());
    let mut child_script: Vec<String> = (0..3000).map(|_| text_delta("child working")).collect();
    child_script.push(stop_delta());
    let key = child.key.clone();
    // 父被后台产出复活时走第一条：真复活了，父上下文里就会出现它的输出
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("revived"), stop_delta()]
        } else if body.contains("call-spawn") {
            parent_script.clone()
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-cancel").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    // 等到子实例被宣告出来：父与子都还在跑
    let mut created = 0usize;
    drain_until(&mut rx, |e| {
        if matches!(e, TopicEvent::InstanceCreated { .. }) {
            created += 1;
        }
        created == 2
    })
    .await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    let root = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let background = instances
        .iter()
        .find(|i| i.parent_id.is_some())
        .expect("child");
    assert_eq!(
        background.mode,
        Some(AgentMode::Background),
        "这条用例测的是投递义务的丢弃路径"
    );

    handle.cancel_task(root.id).await.expect("cancel");
    // 丢弃路径不释放义务就会挂在这里：树永不安静，流永不关
    drain_to_close(&mut rx).await;

    let text = blocks_text(&core, root.id).await;
    assert!(
        !text.contains("[后台任务") && !text.contains("revived"),
        "已取消的父节点不得被后台产出复活: {text}"
    );
    let stored = core
        .storage()
        .agent()
        .get_instance(root.id)
        .await
        .expect("get")
        .expect("instance");
    assert_eq!(
        stored.status,
        AgentStatus::Cancelled,
        "父节点取消后必须停在取消终态"
    );
}

/// 等主实例答完（`Finished`）且后台子实例已被宣告，返回两者
async fn wait_for_a_finished_root_with_a_running_child(
    core: &WindCore,
    topic_id: i64,
) -> (
    wind_core::models::AgentInstance,
    wind_core::models::AgentInstance,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let instances = core
            .storage()
            .agent()
            .list_instances_by_topic(topic_id)
            .await
            .expect("instances");
        let root = instances.iter().find(|i| i.parent_id.is_none());
        let background = instances.iter().find(|i| i.parent_id.is_some());
        if let (Some(root), Some(background)) = (root, background)
            && root.status == AgentStatus::Finished
        {
            return (root.clone(), background.clone());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "主实例必须在超时前答完，且后台子实例已被宣告"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// 实例当前落库的状态
async fn stored_status(core: &WindCore, instance_id: i64) -> AgentStatus {
    core.storage()
        .agent()
        .get_instance(instance_id)
        .await
        .expect("get instance")
        .expect("instance")
        .status
}

/// 取消一个已经答完的父节点：级联取消它的后台子节点，不得把父节点顶回 Running
#[tokio::test]
async fn cancelling_a_finished_parent_does_not_reopen_it() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-cancel-done").await;

    // 子任务远超用例时长，取消一定落在它还在跑的时候
    let mut child_script: Vec<String> = (0..3000).map(|_| text_delta("child working")).collect();
    child_script.push(stop_delta());
    let key = child.key.clone();
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-cancel-done").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    let (root, background) = wait_for_a_finished_root_with_a_running_child(&core, topic_id).await;
    handle.cancel_task(root.id).await.expect("cancel");
    // 流关闭即义务被释放、树收敛，含被丢弃投递的那一条
    let seen = drain_to_close(&mut rx).await;

    let text = blocks_text(&core, root.id).await;
    assert!(
        !text.contains("[后台任务") && !text.contains("parent again"),
        "已答完的父节点不得被自己的取消唤醒: {text}"
    );
    let finished = seen
        .iter()
        .filter(|e| {
            matches!(
                e,
                TopicEvent::MessageFinished { instance_id, .. } if *instance_id == root.id
            )
        })
        .count();
    assert_eq!(finished, 1, "父节点不该再跑一轮");
    assert_eq!(
        stored_status(&core, root.id).await,
        AgentStatus::Finished,
        "父节点必须停在答完的那一刻"
    );
    assert_eq!(
        stored_status(&core, background.id).await,
        AgentStatus::Cancelled,
        "后台子节点必须真的被级联取消，否则测的不是这条判据"
    );
}

/// 外部取消后台子节点、父节点同时也在被取消：不得把父节点顶回 Running
#[tokio::test]
async fn cancelling_a_child_externally_does_not_revive_its_cancelling_parent() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-cancel-both").await;

    // 父次轮与子任务都远超用例时长，取消落在两者都还在跑的时候
    let mut parent_script: Vec<String> = (0..3000).map(|_| text_delta("parent working")).collect();
    parent_script.push(stop_delta());
    let mut child_script: Vec<String> = (0..3000).map(|_| text_delta("child working")).collect();
    child_script.push(stop_delta());
    let key = child.key.clone();
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            parent_script.clone()
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-cancel-both").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    // 等到两个实例都在跑（父节点还没有答完）
    let mut running = 0usize;
    drain_until(&mut rx, |e| {
        if matches!(e, TopicEvent::InstanceCreated { .. }) {
            running += 1;
        }
        running == 2
    })
    .await;
    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    let root = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let background = instances
        .iter()
        .find(|i| i.parent_id.is_some())
        .expect("child");

    // 先取消子节点（来源是外部），再取消父节点
    handle
        .cancel_task(background.id)
        .await
        .expect("cancel child");
    handle.cancel_task(root.id).await.expect("cancel parent");
    let seen = drain_to_close(&mut rx).await;

    let text = blocks_text(&core, root.id).await;
    assert!(
        !text.contains("[后台任务") && !text.contains("parent again"),
        "取消中的父节点不得被外部取消的子节点唤醒: {text}"
    );
    let finished = seen
        .iter()
        .filter(|e| {
            matches!(
                e,
                TopicEvent::MessageFinished { instance_id, .. } if *instance_id == root.id
            )
        })
        .count();
    assert_eq!(finished, 0, "父节点一轮都没答完，不该有 MessageFinished");
    assert_eq!(
        stored_status(&core, background.id).await,
        AgentStatus::Cancelled,
        "后台子节点必须真的被取消"
    );
}

/// 直接取消后台子节点：来源是外部，父节点照常被告知
#[tokio::test]
async fn cancelling_a_background_child_directly_still_tells_the_parent() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "bg-cancel-child").await;

    let mut child_script: Vec<String> = (0..3000).map(|_| text_delta("child working")).collect();
    child_script.push(stop_delta());
    let key = child.key.clone();
    let base = routed_server(move |body: &str| {
        if body.contains("后台任务") {
            vec![text_delta("parent again"), stop_delta()]
        } else if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("agent_spawn_agent") {
            vec![spawn_delta_mode(&key, "work", "background"), stop_delta()]
        } else {
            child_script.clone()
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "bg-cancel-child").await;
    attach_child_agent(&core, topic_id, &child).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    let (root, background) = wait_for_a_finished_root_with_a_running_child(&core, topic_id).await;
    handle.cancel_task(background.id).await.expect("cancel");
    drain_to_close(&mut rx).await;

    assert_eq!(
        stored_status(&core, background.id).await,
        AgentStatus::Cancelled,
        "后台子节点必须真的被取消"
    );
    let text = blocks_text(&core, root.id).await;
    assert!(
        text.contains("[后台任务") && text.contains("任务已取消"),
        "父节点必须知道它的后台任务被取消了: {text}"
    );
    assert!(
        text.contains("parent again"),
        "父节点必须为这条消息跑一轮: {text}"
    );
}

/// 补报只认一次：父已回报过的派生调用不再进补报队列
#[tokio::test]
async fn report_spawns_pairs_a_background_child_with_its_call_once() {
    let core = common::init_test_core().await;
    let definition = common::seed_definition(&core, "report-child").await;
    let (model, topic) = common::seed_chat_fixture(&core, "report").await;

    let parent = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    let user = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: model.id,
            is_boundary: false,
            is_excluded: false,
            instance_id: parent.id,
        })
        .await
        .expect("user message");
    let assistant = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: Some(user.id),
            model_id: model.id,
            is_boundary: false,
            is_excluded: false,
            instance_id: parent.id,
        })
        .await
        .expect("assistant message");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: user.id,
            data: AiMessage::new_simple(Role::User, vec![Content::new_text("go".into())], None),
        })
        .await
        .expect("user block");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: assistant.id,
            data: AiMessage::new_tool_request(
                vec![FunctionCall {
                    id: "call-spawn".into(),
                    name: "agent_spawn_agent".into(),
                    arguments: serde_json::json!({
                        "agent_key": definition.key,
                        "mode": "background",
                        "task": "work",
                    })
                    .to_string(),
                }],
                None,
            ),
        })
        .await
        .expect("tool request block");

    let child = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        parent.id,
        definition.id,
        AgentMode::Background,
    )
    .await
    .expect("child instance");

    let instance = wind_core::agent::helper::get_instance_by_id(core.storage(), parent.id)
        .await
        .expect("instance");
    let report = wind_core::agent::helper::report_spawns(core.storage(), &instance)
        .await
        .expect("report")
        .expect("第一次必须补报");
    let text = tool_outputs(&report);
    assert!(
        text.contains("background task started"),
        "还在跑的后台子任务只回报「已启动」: {text}"
    );
    assert!(
        text.contains(&child.id.to_string()),
        "补报要带上子实例 id: {text}"
    );

    // 模拟父已回报：把这块落库
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: assistant.id,
            data: report,
        })
        .await
        .expect("report block");

    let second = wind_core::agent::helper::report_spawns(core.storage(), &instance)
        .await
        .expect("report");
    assert!(second.is_none(), "已回报的调用不得再补报一次");
}

/// 失败的一轮必须把错误文本落成一块内容块，事件里是同一段文本
#[tokio::test]
async fn a_failed_round_persists_the_error_text_as_a_block() {
    let core = common::init_test_core().await;
    // 指向一个不可达的端口：模型请求必然失败
    let base = "http://127.0.0.1:1".to_string();
    let topic_id = seeded_topic(&core, &base, "fail-block").await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    let seen = drain_until_terminal(&mut rx).await;
    let (message_id, error) = seen
        .iter()
        .find_map(|e| match e {
            TopicEvent::Error {
                message_id, error, ..
            } => Some((message_id.expect("错误事件要带 message_id"), error.clone())),
            _ => None,
        })
        .expect("失败必须发 Error 终态");

    let contents = core
        .storage()
        .message()
        .list_contents(message_id)
        .await
        .expect("contents");
    assert_eq!(
        contents.len(),
        1,
        "失败必须留下恰好一块内容块: {contents:?}"
    );
    let text = text_of(&contents[0].data);
    assert!(
        text.contains(&error),
        "落库的必须是同一段错误文本: {text:?} vs {error:?}"
    );
    assert!(
        !text.starts_with("{\"type\""),
        "错误文本不得是 Content 的 JSON 包装: {text}"
    );
}

// ---------------------------------------------------------------------------
// 不可信输入：模型写出来的参数
// ---------------------------------------------------------------------------

/// 一条参数畸形的派生调用只回报自己的错误结果，本轮继续，不建实例
#[tokio::test]
async fn a_direct_round_turns_a_malformed_spawn_call_into_a_tool_error() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![spawn_delta_bad_arguments(), stop_delta()],
        vec![text_delta("recovered"), stop_delta()],
    ])
    .await;
    let topic_id = seeded_topic(&core, &base, "direct-malformed-spawn").await;
    let spec = start_direct_round(&core, topic_id).await;

    let (outcome, seen) = drive(&core, topic_id, spec.clone()).await;
    let assistant_id = completed_id(outcome);

    let contents = core
        .storage()
        .message()
        .list_contents(assistant_id)
        .await
        .expect("contents");
    let outputs: Vec<String> = contents
        .iter()
        .flat_map(|c| c.data.content.clone())
        .filter_map(|content| match content {
            Content::FunctionCall { data } => Some(format!("{} {}", data.id, data.content)),
            _ => None,
        })
        .collect();
    assert!(
        outputs
            .iter()
            .any(|out| out.starts_with("call-spawn ") && out.contains("error")),
        "畸形参数必须挂在它自己的 call id 上回报: {outputs:?}"
    );
    assert!(
        seen.iter().any(|c| matches!(
            c,
            AgentProgress::Block { data, .. } if tool_output_ids(data).contains(&"call-spawn".to_string())
        )),
        "错误工具结果整块必须先下发给订阅方: {seen:?}"
    );

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 1, "解析失败的派生不得留下实例");
}

/// 补报遇到参数畸形的调用：只回报它自己的错误结果，不让整轮失败
#[tokio::test]
async fn report_spawns_reports_a_malformed_call_instead_of_failing() {
    let core = common::init_test_core().await;
    let (model, topic) = common::seed_chat_fixture(&core, "report-malformed").await;
    let parent = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    seed_tool_request(
        &core,
        model.id,
        parent.id,
        vec![FunctionCall {
            id: "call-spawn".into(),
            name: "agent_spawn_agent".into(),
            arguments: "{not json".into(),
        }],
    )
    .await;

    let instance = wind_core::agent::helper::get_instance_by_id(core.storage(), parent.id)
        .await
        .expect("instance");
    let report = wind_core::agent::helper::report_spawns(core.storage(), &instance)
        .await
        .expect("畸形参数不得让补报失败")
        .expect("必须回报一条错误结果");
    assert_eq!(
        tool_output_ids(&report),
        vec!["call-spawn".to_string()],
        "错误结果要挂在它自己的 call id 上"
    );
    assert!(
        tool_outputs(&report).contains("error"),
        "回报的必须是解析失败文本: {}",
        tool_outputs(&report)
    );
}

// ---------------------------------------------------------------------------
// 补报的配对与幂等
// ---------------------------------------------------------------------------

/// 子实例行存在（状态列还停在非终态）时也必须回报，
/// 否则下一轮会把同一个 spawn 再发一次、建出第二棵子树
#[tokio::test]
async fn report_spawns_reports_a_child_whose_persisted_status_lags() {
    let core = common::init_test_core().await;
    let definition = common::seed_definition(&core, "stale-child").await;
    let (model, topic) = common::seed_chat_fixture(&core, "stale-status").await;
    let parent = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    let assistant = seed_tool_request(
        &core,
        model.id,
        parent.id,
        vec![spawn_call("call-spawn", &definition.key, "sync", "work")],
    )
    .await;

    // 子实例已经建出来，但状态列停在非终态：这正是有界重试放弃后的后果
    let child = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        parent.id,
        definition.id,
        AgentMode::Sync,
    )
    .await
    .expect("child instance");
    set_instance_status(&core, child.id, AgentStatus::Running).await;

    let instance = wind_core::agent::helper::get_instance_by_id(core.storage(), parent.id)
        .await
        .expect("instance");
    let report = wind_core::agent::helper::report_spawns(core.storage(), &instance)
        .await
        .expect("report")
        .expect("有子实例行就必须回报");
    assert!(
        tool_outputs(&report).contains(&child.id.to_string()),
        "回报要带上子实例 id: {}",
        tool_outputs(&report)
    );

    // 落库之后不再补报：同一个 spawn 不会被发第二次
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: assistant,
            data: report,
        })
        .await
        .expect("persist report");
    let second = wind_core::agent::helper::report_spawns(core.storage(), &instance)
        .await
        .expect("report");
    assert!(second.is_none(), "已回报的调用不得再补报一次");
}

/// 两个同 key 的 fork 子实例按各自的任务文本配对，而不是按 key 取第一个未用的
#[tokio::test]
async fn report_spawns_pairs_fork_children_by_their_task() {
    let core = common::init_test_core().await;
    let definition = common::seed_definition(&core, "fork-pair").await;
    let (model, topic) = common::seed_chat_fixture(&core, "fork-pair").await;
    let parent = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    // 父先有一轮已完成的对话，fork 才复制得到东西，子实例的首条 user 消息才是父的
    seed_completed_turn(&core, model.id, parent.id, "parent history", "parent reply").await;
    seed_tool_request(
        &core,
        model.id,
        parent.id,
        vec![
            spawn_call("call-alpha", &definition.key, "fork", "alpha"),
            spawn_call("call-beta", &definition.key, "fork", "beta"),
        ],
    )
    .await;

    let parent_instance = wind_core::agent::helper::get_instance_by_id(core.storage(), parent.id)
        .await
        .expect("parent instance");
    // 建库顺序与调用顺序故意相反：按库序认领会把两条调用配错
    let beta = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        &parent_instance,
        &spawn_action(&definition.key, AgentMode::Fork, "beta"),
    )
    .await
    .expect("beta child");
    let alpha = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        &parent_instance,
        &spawn_action(&definition.key, AgentMode::Fork, "alpha"),
    )
    .await
    .expect("alpha child");
    // 子实例的 assistant 消息由它自己那一轮建立，这里替它建好再补产出
    let beta_assistant = wind_core::agent::helper::open_assistant(core.storage(), beta.instance_id)
        .await
        .expect("beta assistant");
    let alpha_assistant =
        wind_core::agent::helper::open_assistant(core.storage(), alpha.instance_id)
            .await
            .expect("alpha assistant");
    finish_with_text(&core, beta_assistant.id, "beta done").await;
    finish_with_text(&core, alpha_assistant.id, "alpha done").await;
    set_instance_status(&core, beta.instance_id, AgentStatus::Finished).await;
    set_instance_status(&core, alpha.instance_id, AgentStatus::Finished).await;

    let report = wind_core::agent::helper::report_spawns(core.storage(), &parent_instance)
        .await
        .expect("report")
        .expect("必须回报");
    let outputs: Vec<(String, String)> = report
        .content
        .iter()
        .filter_map(|content| match content {
            Content::FunctionCall { data } => Some((data.id.clone(), data.content.to_string())),
            _ => None,
        })
        .collect();
    let output_of = |call_id: &str| {
        outputs
            .iter()
            .find(|(id, _)| id == call_id)
            .map(|(_, content)| content.clone())
            .unwrap_or_else(|| panic!("{call_id} 必须有回报: {outputs:?}"))
    };
    assert!(
        output_of("call-alpha").contains("alpha done"),
        "call-alpha 必须配到任务为 alpha 的子实例: {outputs:?}"
    );
    assert!(
        output_of("call-beta").contains("beta done"),
        "call-beta 必须配到任务为 beta 的子实例: {outputs:?}"
    );
}

/// 后台产出先以一条普通 user 消息投递进父实例时，
/// 同批派发的 sync 子任务的结果仍必须回到父上下文
#[tokio::test]
async fn a_sync_result_survives_a_background_delivery_landing_first() {
    let core = common::init_test_core().await;
    let definition = common::seed_definition(&core, "delivery-order").await;
    let (model, topic) = common::seed_chat_fixture(&core, "delivery-order").await;
    let parent = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main");
    seed_tool_request(
        &core,
        model.id,
        parent.id,
        vec![
            spawn_call("call-sync", &definition.key, "sync", "sync work"),
            spawn_call("call-bg", &definition.key, "background", "bg work"),
        ],
    )
    .await;

    let parent_instance = wind_core::agent::helper::get_instance_by_id(core.storage(), parent.id)
        .await
        .expect("parent instance");
    let sync_child = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        &parent_instance,
        &spawn_action(&definition.key, AgentMode::Sync, "sync work"),
    )
    .await
    .expect("sync child");
    let bg_child = wind_core::agent::helper::spawn_child_round(
        core.storage(),
        std::path::Path::new("/tmp"),
        topic.id,
        &parent_instance,
        &spawn_action(&definition.key, AgentMode::Background, "bg work"),
    )
    .await
    .expect("bg child");
    let sync_assistant =
        wind_core::agent::helper::open_assistant(core.storage(), sync_child.instance_id)
            .await
            .expect("sync assistant");
    finish_with_text(&core, sync_assistant.id, "sync done").await;
    set_instance_status(&core, sync_child.instance_id, AgentStatus::Finished).await;

    // 后台产出以一条普通 user 消息投递进父实例，落在两个派生调用的后面
    let delivery = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: model.id,
            is_boundary: false,
            is_excluded: false,
            instance_id: parent.id,
        })
        .await
        .expect("delivery message");
    core.storage()
        .message()
        .create_content(CreateMessageContent {
            message_id: delivery.id,
            data: AiMessage::new_simple(
                Role::User,
                vec![Content::new_text("[后台任务 bg 已完成]".into())],
                None,
            ),
        })
        .await
        .expect("delivery block");

    let report = wind_core::agent::helper::report_spawns(core.storage(), &parent_instance)
        .await
        .expect("report")
        .expect("后台投递不得顶掉还没回报的派生调用");
    let outputs: Vec<(String, String)> = report
        .content
        .iter()
        .filter_map(|content| match content {
            Content::FunctionCall { data } => Some((data.id.clone(), data.content.to_string())),
            _ => None,
        })
        .collect();
    let output_of = |call_id: &str| {
        outputs
            .iter()
            .find(|(id, _)| id == call_id)
            .map(|(_, content)| content.clone())
            .unwrap_or_else(|| panic!("{call_id} 必须有回报: {outputs:?}"))
    };
    assert!(
        output_of("call-sync").contains("sync done"),
        "sync 子任务的结果必须回报给父模型: {outputs:?}"
    );
    assert!(
        output_of("call-bg").contains(&bg_child.instance_id.to_string()),
        "还在跑的后台子任务只回报「已启动」并带上子实例 id: {outputs:?}"
    );
}

// ---------------------------------------------------------------------------
// 同一批里的派生与审批
// ---------------------------------------------------------------------------

/// 同一批里既有派生又有待审批的调用时，审批行必须先落库
#[tokio::test]
async fn a_spawn_and_a_waiting_call_in_one_batch_persist_the_approval() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "mixed-child").await;
    let base = fake_server(vec![vec![spawn_and_list_delta(&child.key), stop_delta()]]).await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "mixed-batch", ToolApprovalPolicy::Manual).await;
    attach_child_agent(&core, topic_id, &child).await;
    let spec = start_direct_round_with_assistant(&core, topic_id).await;
    let assistant_id = spec.message_id.expect("assistant message");

    // 预置一条已批准的审批行：它进 exec_agent，同一批里未处理的 list 调用进 waiting
    let approved = wind_core::agent::helper::save_approval_state(
        core.storage(),
        topic_id,
        spec.instance_id,
        assistant_id,
        vec![spawn_call("call-spawn", &child.key, "sync", "work")],
    )
    .await
    .expect("seed approval");
    core.storage()
        .approval()
        .batch_set_status(vec![ApprovalRecord {
            id: approved[0].id,
            status: ToolApprovalStatus::Approved,
        }])
        .await
        .expect("approve");

    let (outcome, _) = drive(&core, topic_id, spec.clone()).await;
    assert!(
        matches!(outcome, Outcome::Spawn { .. }),
        "已批准的派生调用要交还引擎: {outcome:?}"
    );

    let pending = core
        .storage()
        .approval()
        .list_pending_by_message(assistant_id)
        .await
        .expect("pending");
    assert_eq!(
        pending.len(),
        1,
        "同批里的待审批调用必须落库，否则审批提示会被整棵子树挡住: {pending:?}"
    );
    assert_eq!(pending[0].tool_call_id, "call-list");

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 2, "父 + 一个派生出来的子实例");
}

// ---------------------------------------------------------------------------
// fork：走完整 hub
// ---------------------------------------------------------------------------

/// fork 派生走完整的 hub：子实例继承父的历史，产出回到父上下文
#[tokio::test]
async fn a_fork_spawn_through_the_hub_copies_the_parent_history() {
    let core = common::init_test_core().await;
    let child = common::seed_definition(&core, "fork-hub").await;
    let key = child.key.clone();
    let base = routed_server(move |body: &str| {
        if body.contains("call-spawn") {
            vec![text_delta("parent done"), stop_delta()]
        } else if body.contains("work") {
            vec![text_delta("child done"), stop_delta()]
        } else {
            vec![spawn_delta_mode(&key, "work", "fork"), stop_delta()]
        }
    })
    .await;
    let topic_id = seeded_topic(&core, &base, "fork-hub").await;
    attach_child_agent(&core, topic_id, &child).await;

    // 父实例先有一轮已完成的对话，fork 才有历史可复制
    let model_id = core
        .storage()
        .topic()
        .get_topic(topic_id)
        .await
        .expect("topic")
        .expect("topic row")
        .model_id
        .expect("topic model");
    let main = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("main");
    seed_completed_turn(&core, model_id, main.id, "hi", "hello").await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");
    let seen = drain_to_close(&mut rx).await;

    let instances = core
        .storage()
        .agent()
        .list_instances_by_topic(topic_id)
        .await
        .expect("instances");
    assert_eq!(instances.len(), 2, "父 + 一个 fork 子实例");
    let parent = instances
        .iter()
        .find(|i| i.parent_id.is_none())
        .expect("root");
    let fork = instances
        .iter()
        .find(|i| i.parent_id.is_some())
        .expect("child");
    assert_eq!(
        fork.mode,
        Some(AgentMode::Fork),
        "fork 模式必须落库，恢复时靠它重建"
    );

    let child_text = blocks_text(&core, fork.id).await;
    assert!(
        child_text.contains("hello"),
        "fork 必须把父实例的已完成对话复制进子实例: {child_text}"
    );
    assert!(
        child_text.contains("work"),
        "子实例必须带上自己的任务输入: {child_text}"
    );
    assert!(
        child_text.contains("child done"),
        "子实例必须自己跑一轮: {child_text}"
    );

    let parent_text = blocks_text(&core, parent.id).await;
    assert!(
        parent_text.contains("child done"),
        "同步子任务的产出必须回到父上下文: {parent_text}"
    );
    assert!(
        parent_text.contains("parent done"),
        "父节点必须收下结果后继续这一轮: {parent_text}"
    );
    assert!(
        seen.iter().any(|e| matches!(
            e,
            TopicEvent::MessageFinished { instance_id, .. } if *instance_id == fork.id
        )),
        "子实例要宣告自己的完成"
    );
}

// ---------------------------------------------------------------------------
// 过期应答
// ---------------------------------------------------------------------------

/// 节点跑完之后到达的审批应答必须被拒，且不再写审批行
#[tokio::test]
async fn a_stale_approval_after_the_node_finished_is_rejected() {
    let core = common::init_test_core().await;
    let base = fake_server(vec![
        vec![tool_delta(), stop_delta()],
        vec![text_delta("done"), stop_delta()],
    ])
    .await;
    let topic_id =
        seeded_topic_with_policy(&core, &base, "stale-approve", ToolApprovalPolicy::Manual).await;

    let handle = core.fetch_topic(topic_id);
    let mut rx = handle.subscribe().await.expect("subscribe");
    handle
        .create_task(vec![Content::new_text("go".to_string())])
        .await
        .expect("mailbox")
        .expect("business");

    let seen = drain_until(&mut rx, |e| {
        matches!(e, TopicEvent::ApprovalRequired { .. })
    })
    .await;
    let (instance_id, message_id) = seen
        .iter()
        .find_map(|e| match e {
            TopicEvent::ApprovalRequired {
                instance_id,
                message_id,
                ..
            } => Some((*instance_id, *message_id)),
            _ => None,
        })
        .expect("审批请求");
    let requests = core
        .storage()
        .approval()
        .list_pending_by_message(message_id)
        .await
        .expect("pending");

    handle
        .approve(instance_id, vec![requests[0].id], Vec::new())
        .await
        .expect("第一次必须受理");
    drain_until_terminal(&mut rx).await;

    // 节点已经跑完，这条应答过期了
    let err = handle
        .approve(instance_id, vec![requests[0].id], Vec::new())
        .await
        .expect_err("过期应答必须被拒");
    assert!(
        err.to_string().contains("not waiting for approval"),
        "拒绝原因要能被人看懂: {err}"
    );
    let rows = core
        .storage()
        .approval()
        .list_by_message(message_id)
        .await
        .expect("approval rows");
    assert_eq!(rows.len(), 1, "被拒的应答不得再写审批行: {rows:?}");
}
