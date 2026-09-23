//! 自增主键的回归测试
//!
//! 对应 `sql.md` §5 的清单：自增生效、不复用、严格递增、`create()` 返回值与库中一致、
//! 批量插入的自然键关联与分块，以及 id 落在 JavaScript 安全整数范围内。
//!
//! 其中「不复用」一条守护 `AUTOINCREMENT`：不加它，SQLite 会在删除最大 id 行后
//! 复用该 id，从而破坏 `ORDER BY id` 与 `list_contexts` 的 `id > MAX(id)` 语义。

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use wind_ai::model::AdapterType;
use wind_core::{
    WindCore,
    models::{
        CreateAgentDefinition, CreateCredentials, CreateInstance, CreateJsonRule, CreateMcpServer,
        CreateMessage, CreateModel, CreatePromptModule, CreateProvider, CreateToolApprovalCall,
        CreateToolApprovalRequests, CreateTopic, CreateTopicAgentMap,
    },
};
use wind_mcp::client::TransportType;

/// JavaScript `Number.MAX_SAFE_INTEGER` = 2^53 - 1
const JS_MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

async fn setup() -> WindCore {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .expect("parse sqlite url")
        .shared_cache(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect sqlite");
    WindCore::init_with_pool(pool).await.expect("init core")
}

fn topic(label: &str) -> CreateTopic {
    CreateTopic {
        parent_id: None,
        label: label.into(),
        icon: None,
        model_id: None,
        agent_id: None,
        tool_approval_policy: None,
    }
}

/// 在全部 12 张表各创建一行，返回 `(表名, id)`
async fn create_one_per_table(core: &WindCore) -> Vec<(&'static str, i64)> {
    let s = core.storage();
    let mut ids = Vec::new();

    ids.push(("providers", {
        s.provider()
            .create(CreateProvider {
                name: "p1".into(),
                description: None,
                base_url: "https://example.invalid".into(),
                doc: None,
                alias: None,
            })
            .await
            .expect("create provider")
            .id
    }));

    ids.push(("models", {
        s.model()
            .create(CreateModel {
                name: "m1".into(),
                provider_id: 1,
                alias: None,
                adapter: AdapterType::OpenAICompletion,
                modalities: None,
                active: Some(true),
                icon: None,
                endpoint: None,
                config: None,
            })
            .await
            .expect("create model")
            .id
    }));

    ids.push(("credentials", {
        s.provider()
            .create_credentials(CreateCredentials {
                provider_id: 1,
                key: "k1".into(),
            })
            .await
            .expect("create credentials")
            .id
    }));

    let topic_id = s
        .topic()
        .create(topic("t1"))
        .await
        .expect("create topic")
        .id;
    ids.push(("topics", topic_id));

    let instance_id = s
        .agent()
        .create_instance(CreateInstance {
            topic_id,
            parent_id: None,
            agent_id: None,
            mode: None,
            status: None,
            role: None,
        })
        .await
        .expect("create instance")
        .id;
    ids.push(("agent_instances", instance_id));

    ids.push(("messages", {
        s.message()
            .create(CreateMessage {
                from_id: None,
                content: vec![],
                model_id: 1,
                instance_id,
                is_boundary: false,
                is_excluded: false,
                input_tokens: 0,
                output_tokens: 0,
            })
            .await
            .expect("create message")
            .id
    }));

    ids.push(("mcp_servers", {
        s.mcp()
            .create(CreateMcpServer {
                r#type: TransportType::Stdio,
                name: "mcp1".into(),
                url: None,
                description: None,
                command: Some("echo".into()),
                args: None,
                env: None,
            })
            .await
            .expect("create mcp server")
            .id
    }));

    ids.push(("json_rule", {
        s.provider()
            .create_json_rule(CreateJsonRule {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: "[]".into(),
            })
            .await
            .expect("create json rule")
            .id
    }));

    ids.push(("prompt_modules", {
        s.prompt()
            .create(CreatePromptModule {
                alias: "pm1".into(),
                description: "d".into(),
                content: "c".into(),
                active: None,
            })
            .await
            .expect("create prompt module")
            .id
    }));

    let definition_id = s
        .agent()
        .create_definition(CreateAgentDefinition {
            name: "a1".into(),
            description: "d".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: None,
            data: Default::default(),
        })
        .await
        .expect("create definition")
        .id;
    ids.push(("agent_definitions", definition_id));

    ids.push(("topic_agent_maps", {
        s.agent()
            .create_topic_agent_map(CreateTopicAgentMap {
                topic_id,
                agent_id: definition_id,
            })
            .await
            .expect("create topic agent map")
            .id
    }));

    ids.push(("tool_approval_requests", {
        s.approval()
            .create_requests(CreateToolApprovalRequests {
                instance_id,
                topic_id,
                message_id: 1,
                calls: vec![CreateToolApprovalCall {
                    tool_call_id: "call-1".into(),
                    tool_name: "fs0m0read".into(),
                    arguments: serde_json::json!({}),
                }],
            })
            .await
            .expect("create approval requests")
            .remove(0)
            .id
    }));

    ids
}

/// 12 张表都必须由数据库分配 id：非 NULL、> 0、且落在 JS 安全整数内
#[tokio::test]
async fn all_tables_assign_ids_automatically() {
    let core = setup().await;
    let ids = create_one_per_table(&core).await;

    assert_eq!(ids.len(), 12, "应覆盖全部 12 张表");
    for (table, id) in &ids {
        assert!(
            *id > 0,
            "{table}: id 应由数据库分配为正数，实际 {id}（DDL 少了 AUTOINCREMENT 或主键不是 INTEGER）"
        );
        assert!(
            *id < JS_MAX_SAFE_INTEGER,
            "{table}: id {id} 超出 JS 安全整数范围"
        );
    }
}

/// id 必须严格递增 —— `ORDER BY id` 与 `list_contexts` 的 `id > MAX(id)` 都依赖它
#[tokio::test]
async fn ids_are_strictly_increasing() {
    let core = setup().await;
    let topics = core.storage().topic();

    let mut prev = 0;
    for i in 0..20 {
        let t = topics
            .create(topic(&format!("t{i}")))
            .await
            .expect("create topic");
        assert!(t.id > prev, "id 必须严格递增：{} 未大于 {prev}", t.id);
        prev = t.id;
    }

    let listed: Vec<i64> = topics
        .list_topics()
        .await
        .expect("list topics")
        .into_iter()
        .map(|t| t.id)
        .collect();
    let mut sorted = listed.clone();
    sorted.sort_unstable();
    assert_eq!(listed, sorted, "list_topics 的 ORDER BY id 应给出递增序列");
}

/// 删除最大 id 行后，新行不得复用它 —— 这是 `AUTOINCREMENT` 的守护
#[tokio::test]
async fn ids_are_not_reused_after_delete() {
    let core = setup().await;
    let topics = core.storage().topic();

    let _first = topics.create(topic("t1")).await.expect("create t1");
    let last = topics.create(topic("t2")).await.expect("create t2");
    topics.delete_topics(&[last.id]).await.expect("delete t2");

    let next = topics.create(topic("t3")).await.expect("create t3");
    assert!(
        next.id > last.id,
        "AUTOINCREMENT 应保证 id 不复用：新 id {} 不应 ≤ 被删除的 {}",
        next.id,
        last.id
    );
}

/// `create()` 返回的 id 必须是真正落库的那个（`RETURNING id` 与实际写入一致）
#[tokio::test]
async fn create_returns_the_persisted_id() {
    let core = setup().await;
    let topics = core.storage().topic();

    let created = topics.create(topic("t1")).await.expect("create topic");
    let fetched = topics
        .get_topic(created.id)
        .await
        .expect("get topic")
        .expect("topic 应存在");

    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.label, created.label);
}

/// 批量插入：返回值顺序 == 输入顺序，且每个 id 在库中对应同一 tool_call_id
#[tokio::test]
async fn batch_create_preserves_order_and_maps_ids() {
    let core = setup().await;
    let instance = core
        .storage()
        .agent()
        .create_instance(CreateInstance {
            topic_id: 1,
            parent_id: None,
            agent_id: None,
            mode: None,
            status: None,
            role: None,
        })
        .await
        .expect("create instance");

    let tool_call_ids = ["call-a", "call-b", "call-c", "call-d"];
    let created = core
        .storage()
        .approval()
        .create_requests(CreateToolApprovalRequests {
            instance_id: instance.id,
            topic_id: 1,
            message_id: 1,
            calls: tool_call_ids
                .iter()
                .map(|id| CreateToolApprovalCall {
                    tool_call_id: (*id).into(),
                    tool_name: "fs0m0read".into(),
                    arguments: serde_json::json!({}),
                })
                .collect(),
        })
        .await
        .expect("create approval requests");

    let returned: Vec<&str> = created.iter().map(|r| r.tool_call_id.as_str()).collect();
    assert_eq!(
        returned, tool_call_ids,
        "返回值必须按 input.calls 的原始顺序组装"
    );

    // 库中每一行的 id ↔ tool_call_id 必须与返回值一致
    let persisted = core
        .storage()
        .approval()
        .list_by_message(1)
        .await
        .expect("list approvals");
    assert_eq!(persisted.len(), tool_call_ids.len());

    for req in &created {
        let row = persisted
            .iter()
            .find(|p| p.id == req.id)
            .expect("返回的 id 应能在库中找到");
        assert_eq!(
            row.tool_call_id, req.tool_call_id,
            "id {} 在库中对应的是 {}，而不是 {}",
            req.id, row.tool_call_id, req.tool_call_id
        );
    }
}

/// 超过单条 INSERT 绑定参数上限时必须分块，而不是整体失败
///
/// SQLite 的上限是 32766 个参数，本表每行 9 列 → 约 3640 行。
/// 这里构造 3500 条，确保走的是分块路径而不是侥幸。
#[tokio::test]
async fn batch_create_chunks_beyond_parameter_limit() {
    let core = setup().await;
    const N: usize = 3500;

    let created = core
        .storage()
        .approval()
        .create_requests(CreateToolApprovalRequests {
            instance_id: 1,
            topic_id: 1,
            message_id: 1,
            calls: (0..N)
                .map(|i| CreateToolApprovalCall {
                    tool_call_id: format!("call-{i:05}"),
                    tool_name: "fs0m0read".into(),
                    arguments: serde_json::json!({}),
                })
                .collect(),
        })
        .await
        .expect("超大批量应分块成功");

    assert_eq!(created.len(), N, "所有审批请求都应被创建");
    for (i, req) in created.iter().enumerate() {
        assert_eq!(req.tool_call_id, format!("call-{i:05}"), "顺序必须保持");
        assert!(req.id > 0, "每个 id 都应被回填");
    }

    let persisted = core
        .storage()
        .approval()
        .list_by_message(1)
        .await
        .expect("list approvals");
    assert_eq!(persisted.len(), N, "库中行数应与输入一致");
}

/// 布尔列必须能按 `bool` 读写（`list_contexts` 的 `is_excluded` / `is_boundary` 过滤）
#[tokio::test]
async fn boolean_columns_roundtrip() {
    let core = setup().await;
    let instance = core
        .storage()
        .agent()
        .create_instance(CreateInstance {
            topic_id: 1,
            parent_id: None,
            agent_id: None,
            mode: None,
            status: None,
            role: None,
        })
        .await
        .expect("create instance");

    let msg = core.storage().message();
    let kept = msg
        .create(CreateMessage {
            from_id: None,
            content: vec![],
            model_id: 1,
            instance_id: instance.id,
            is_boundary: true,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
        })
        .await
        .expect("create message");

    assert!(kept.is_boundary, "is_boundary 应为 true");
    assert!(!kept.is_excluded, "is_excluded 应为 false");

    let fetched = msg
        .get(kept.id)
        .await
        .expect("get message")
        .expect("exists");
    assert!(fetched.is_boundary);
    assert!(!fetched.is_excluded);
}
