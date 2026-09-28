//! `message_contents` 表的行为测试
//!
//! 消息正文从 `messages.content` 拆到独立表后，这里覆盖写入、排序、
//! 级联删除与上下文获取这几条契约

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use wind_ai::{
    message::{Content, Message as AiMessage, Role},
    tool::FunctionCall,
};
use wind_core::{
    WindCore,
    agent::helper::get_message_contexts,
    models::{
        AgentDefinitionData, ContextPolicy, CreateAgentDefinition, CreateInstance, CreateMessage,
        CreateMessageContent, CreateTopic, MessageContent,
    },
};

/// 单连接内存池，保证 schema 初始化与后续查询命中同一个库
async fn pool() -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .expect("parse sqlite url")
        .shared_cache(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect sqlite")
}

async fn setup() -> WindCore {
    WindCore::init_with_pool(pool().await)
        .await
        .expect("init core")
}

/// 建一个 topic 及其主实例，返回实例 id
async fn create_instance(core: &WindCore, label: &str) -> i64 {
    let topic = core
        .storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            label: label.into(),
            icon: None,
            model_id: None,
            agent_id: None,
            tool_approval_policy: None,
        })
        .await
        .expect("create topic");
    core.storage()
        .agent()
        .create_instance(CreateInstance::new_main(topic.id, None))
        .await
        .expect("create instance")
        .id
}

async fn create_message(core: &WindCore, instance_id: i64) -> i64 {
    core.storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: 1,
            instance_id,
            is_boundary: false,
            is_excluded: false,
        })
        .await
        .expect("create message")
        .id
}

fn text_message(role: Role, text: &str, input_tokens: i32, output_tokens: i32) -> AiMessage {
    AiMessage {
        role,
        content: vec![Content::new_text(text.into())],
        reasoning_content: None,
        created_at: 7,
        input_tokens,
        output_tokens,
        tool_calls: None,
    }
}

/// 取内容块中的纯文本
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

fn content(message_id: i64, data: AiMessage) -> CreateMessageContent {
    CreateMessageContent { message_id, data }
}

/// 一次运行内追加的块按 id 升序读出，AiMessage 各字段原样往返
#[tokio::test]
async fn contents_round_trip_ordered_by_id() {
    let core = setup().await;
    let instance = create_instance(&core, "round-trip").await;
    let message_id = create_message(&core, instance).await;
    let contents = core.storage().message_content();

    let first = contents
        .create(content(message_id, text_message(Role::User, "你好", 5, 0)))
        .await
        .expect("create first");
    let second = contents
        .create(content(
            message_id,
            AiMessage {
                tool_calls: Some(vec![FunctionCall {
                    id: "call-1".into(),
                    name: "server0m0tool".into(),
                    arguments: "{\"a\":1}".into(),
                }]),
                reasoning_content: Some("思考中".into()),
                ..text_message(Role::Assistant, "调用工具", 0, 20)
            },
        ))
        .await
        .expect("create second");

    let rows = contents
        .list_by_message(message_id)
        .await
        .expect("list by message");
    assert_eq!(
        rows.iter().map(|c| c.id).collect::<Vec<i64>>(),
        vec![first.id, second.id],
        "块顺序等于插入顺序"
    );
    assert_eq!(rows[0].message_id, message_id);
    assert_eq!(text_of(&rows[0].data), "你好");
    assert_eq!(rows[0].data.input_tokens, 5);
    assert_eq!(rows[0].data.created_at, 7, "created_at 应原样往返");
    assert_eq!(rows[1].data.reasoning_content.as_deref(), Some("思考中"));
    assert_eq!(rows[1].data.output_tokens, 20);
    let calls = rows[1].data.tool_calls.as_ref().expect("tool_calls 应往返");
    assert_eq!(calls[0].id, "call-1");
}

/// 同一消息的多个块各自成行，后写的不会覆盖先写的
#[tokio::test]
async fn appending_creates_distinct_rows() {
    let core = setup().await;
    let instance = create_instance(&core, "append").await;
    let message_id = create_message(&core, instance).await;
    let contents = core.storage().message_content();

    for text in ["第一块", "第二块"] {
        contents
            .create(content(
                message_id,
                text_message(Role::Assistant, text, 0, 3),
            ))
            .await
            .expect("create");
    }

    let rows = contents
        .list_by_message(message_id)
        .await
        .expect("list by message");
    assert_eq!(rows.len(), 2, "两次写入应产生两行");
    assert!(rows[0].id < rows[1].id, "id 应递增");
    assert_eq!(
        rows.iter()
            .map(|c| text_of(&c.data))
            .collect::<Vec<String>>(),
        vec!["第一块", "第二块"]
    );
}

/// 批量查询按 message_id 升序、组内 id 升序返回
#[tokio::test]
async fn list_by_messages_orders_by_message_then_id() {
    let core = setup().await;
    let instance = create_instance(&core, "batch").await;
    let first = create_message(&core, instance).await;
    let second = create_message(&core, instance).await;
    let contents = core.storage().message_content();

    // 每条消息的块按写入先后落库，读出顺序应与写入顺序一致
    for (message_id, text) in [(second, "b0"), (first, "a0"), (second, "b1"), (first, "a1")] {
        contents
            .create(content(
                message_id,
                text_message(Role::Assistant, text, 0, 0),
            ))
            .await
            .expect("create");
    }
    // 未被查询的消息不应出现在结果里
    let untouched = create_message(&core, instance).await;
    contents
        .create(content(
            untouched,
            text_message(Role::Assistant, "c0", 0, 0),
        ))
        .await
        .expect("create");

    let rows = contents
        .list_by_messages(&[first, second])
        .await
        .expect("list by messages");
    let keys = rows
        .iter()
        .map(|c| (c.message_id, c.id))
        .collect::<Vec<_>>();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "应按 (message_id, id) 升序返回");

    let actual: Vec<(i64, String)> = rows
        .iter()
        .map(|c| (c.message_id, text_of(&c.data)))
        .collect();
    assert_eq!(
        actual,
        vec![
            (first, "a0".to_string()),
            (first, "a1".to_string()),
            (second, "b0".to_string()),
            (second, "b1".to_string()),
        ]
    );

    // 空 id 列表不查库
    assert!(
        contents
            .list_by_messages(&[])
            .await
            .expect("empty ids")
            .is_empty()
    );
}

/// 删除单条消息时其内容一并删除
#[tokio::test]
async fn deleting_message_removes_its_contents() {
    let core = setup().await;
    let instance = create_instance(&core, "delete-message").await;
    let message_id = create_message(&core, instance).await;
    let contents = core.storage().message_content();
    contents
        .create(content(
            message_id,
            text_message(Role::Assistant, "x", 0, 0),
        ))
        .await
        .expect("create");

    core.storage()
        .message()
        .delete(message_id)
        .await
        .expect("delete message");

    assert!(
        contents
            .list_by_message(message_id)
            .await
            .expect("list")
            .is_empty()
    );
}

/// 删除实例时级联删除其消息与消息内容
#[tokio::test]
async fn deleting_instance_cascades_contents() {
    let core = setup().await;
    let instance = create_instance(&core, "delete-instance").await;
    let kept_instance = create_instance(&core, "delete-instance-kept").await;
    let message_id = create_message(&core, instance).await;
    let kept_message = create_message(&core, kept_instance).await;
    let contents = core.storage().message_content();

    for (message_id, text) in [(message_id, "gone"), (kept_message, "kept")] {
        contents
            .create(content(
                message_id,
                text_message(Role::Assistant, text, 0, 0),
            ))
            .await
            .expect("create");
    }

    core.storage()
        .agent()
        .delete_instances(&[instance])
        .await
        .expect("delete instances");

    assert!(
        contents
            .list_by_message(message_id)
            .await
            .expect("list removed")
            .is_empty()
    );
    let kept: Vec<MessageContent> = contents
        .list_by_message(kept_message)
        .await
        .expect("list kept");
    assert_eq!(kept.len(), 1, "其它实例的内容不应被级联删除");
}

// ---------------------------------------------------------------------------
// 上下文获取
// ---------------------------------------------------------------------------

/// 建一条消息并写入若干块内容，返回消息 id
async fn create_message_with_blocks(
    core: &WindCore,
    instance_id: i64,
    from_id: Option<i64>,
    blocks: &[(Role, &str)],
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
    for (role, text) in blocks.iter() {
        core.storage()
            .message_content()
            .create(content(message.id, text_message(*role, text, 0, 0)))
            .await
            .expect("create block");
    }
    message.id
}

/// 播种一个带 `max_context` 的 Agent 定义
async fn create_agent(
    core: &WindCore,
    max_context: Option<i32>,
) -> wind_core::models::AgentDefinition {
    core.storage()
        .agent()
        .create_definition(CreateAgentDefinition {
            name: "ctx-agent".into(),
            description: "for context test".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData {
                context_policy: ContextPolicy { max_context },
                ..Default::default()
            },
        })
        .await
        .expect("create definition")
}

/// 历史上下文按「消息顺序 → 块插入顺序」展平，且同一条消息的每一块都进入上下文
#[tokio::test]
async fn contexts_flatten_every_block_in_message_order() {
    let core = setup().await;
    let instance = create_instance(&core, "flatten").await;

    // 助手消息有两块：工具调用结果与最终回答，两块都要进入上下文
    create_message_with_blocks(&core, instance, None, &[(Role::User, "q1")]).await;
    create_message_with_blocks(
        &core,
        instance,
        Some(1),
        &[(Role::Assistant, "a1-1"), (Role::Tool, "a1-2")],
    )
    .await;

    let contexts = get_message_contexts(&core.storage().clone(), instance, None)
        .await
        .expect("load contexts");
    let texts = contexts.iter().map(text_of).collect::<Vec<String>>();
    assert_eq!(texts, vec!["q1", "a1-1", "a1-2"]);
    assert_eq!(contexts[2].role, Role::Tool);
}

/// max_context 按 Message 条数截断，截断后从第一条用户消息开始
#[tokio::test]
async fn max_context_truncates_by_message_count() {
    let core = setup().await;
    let instance = create_instance(&core, "max-context").await;

    let m1 = create_message_with_blocks(&core, instance, None, &[(Role::User, "q1")]).await;
    create_message_with_blocks(&core, instance, Some(m1), &[(Role::Assistant, "a1")]).await;
    let m3 = create_message_with_blocks(&core, instance, None, &[(Role::User, "q2")]).await;
    create_message_with_blocks(&core, instance, Some(m3), &[(Role::Assistant, "a2")]).await;

    let storage = core.storage().clone();
    let texts =
        |ctx: Vec<wind_ai::message::Message>| ctx.iter().map(text_of).collect::<Vec<String>>();

    // 不限制：全部消息
    let all = get_message_contexts(&storage, instance, None)
        .await
        .expect("no limit");
    assert_eq!(texts(all), vec!["q1", "a1", "q2", "a2"]);

    // 保留最后 4 条消息 = 全部
    let agent = create_agent(&core, Some(4)).await;
    let ctx = get_message_contexts(&storage, instance, Some(&agent))
        .await
        .expect("limit 4");
    assert_eq!(texts(ctx), vec!["q1", "a1", "q2", "a2"]);

    // 保留最后 2 条消息 = 第二对问答
    let agent = create_agent(&core, Some(2)).await;
    let ctx = get_message_contexts(&storage, instance, Some(&agent))
        .await
        .expect("limit 2");
    assert_eq!(texts(ctx), vec!["q2", "a2"]);

    // 保留最后 3 条消息时窗口以助手消息开头，需要前移到第一条用户消息
    let agent = create_agent(&core, Some(3)).await;
    let ctx = get_message_contexts(&storage, instance, Some(&agent))
        .await
        .expect("limit 3");
    assert_eq!(texts(ctx), vec!["q2", "a2"]);
}

/// 被排除的消息既不进入上下文，其内容块也不进入
#[tokio::test]
async fn excluded_message_keeps_its_blocks_out_of_context() {
    let core = setup().await;
    let instance = create_instance(&core, "excluded").await;

    let user = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: 1,
            instance_id: instance,
            is_boundary: false,
            is_excluded: true,
        })
        .await
        .expect("create excluded message");
    core.storage()
        .message_content()
        .create(content(user.id, text_message(Role::User, "dropped", 0, 0)))
        .await
        .expect("create block");
    create_message_with_blocks(&core, instance, None, &[(Role::User, "kept")]).await;

    let contexts = get_message_contexts(&core.storage().clone(), instance, None)
        .await
        .expect("load contexts");
    assert_eq!(
        contexts.iter().map(text_of).collect::<Vec<String>>(),
        vec!["kept"]
    );
}

/// 库里还是 stage1 的 message_contents（带 "index" 列）时，写入必须报错而不是静默错序
///
/// 读会成功（读侧不再选 index 列），只有写入会大声失败 —— 这是无迁移机制的既定代价：
/// 删列后必须删掉本地库文件，这条测试固定住它
#[tokio::test]
async fn stale_schema_write_fails_loudly() {
    let pool = pool().await;
    sqlx::raw_sql(
        r#"CREATE TABLE message_contents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id BIGINT NOT NULL,
            "index" BIGINT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '[]',
            reasoning_content TEXT,
            tool_calls TEXT,
            input_tokens BIGINT NOT NULL DEFAULT 0,
            output_tokens BIGINT NOT NULL DEFAULT 0,
            created_at BIGINT NOT NULL DEFAULT 0,
            updated_at BIGINT
        );
        CREATE UNIQUE INDEX idx_message_contents_unique ON message_contents(message_id, "index");"#,
    )
    .execute(&pool)
    .await
    .expect("create stale table");
    let core = WindCore::init_with_pool(pool).await.expect("init core");
    let instance = create_instance(&core, "stale-schema").await;
    let message_id = create_message(&core, instance).await;

    let err = core
        .storage()
        .message_content()
        .create(content(
            message_id,
            text_message(Role::Assistant, "x", 0, 0),
        ))
        .await
        .expect_err("老 schema 下写入应报错，而不是静默写出一行错序数据");
    let text = err.to_string();
    assert!(
        text.contains("message_contents.index"),
        "报错应点名 index 列，实际：{text}"
    );
}
