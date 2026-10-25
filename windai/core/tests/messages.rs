//! 验证消息块事件目录，以及写入与投影对块的处理。

use wind_core::conversation::directory;
use wind_core::conversation::events;
use wind_core::models::TurnCreate;
use wind_event::def::{Mount, Shape};
use wind_stream::id::{BlockId, MessageId};
use wind_stream::payload::{BlockCommit, BlockCommitted, BlockDelta, BlockOpened};

#[test]
fn the_message_family_declares_five_events() {
    assert_eq!(
        directory().len(),
        5,
        "message.seal plus three block events, plus input.accepted"
    );
}

#[test]
fn the_block_commit_intercept_is_gone() {
    assert!(directory().get("core/block.commit").is_none());
    assert!(directory().get("core/block.committed").is_some());
}

#[test]
fn no_message_event_is_an_intercept() {
    for def in [
        events::CORE_MESSAGE_SEAL.meta(),
        events::CORE_BLOCK_OPEN.meta(),
        events::CORE_BLOCK_DELTA.meta(),
        events::CORE_BLOCK_COMMITTED.meta(),
        events::CORE_INPUT_ACCEPTED.meta(),
    ] {
        assert_eq!(
            def.shape,
            Shape::Observe,
            "{} must be observe-only",
            def.name
        );
    }
}

#[test]
fn the_draft_is_the_only_event_kept_out_of_the_log() {
    const { assert!(!events::CORE_BLOCK_DELTA.log, "a draft is not a fact") };
    for def in [
        events::CORE_MESSAGE_SEAL.meta(),
        events::CORE_BLOCK_OPEN.meta(),
        events::CORE_BLOCK_COMMITTED.meta(),
    ] {
        assert!(def.log, "{} must enter the log", def.name);
    }
}

#[test]
fn every_message_event_hangs_off_the_projection_layer() {
    for def in [
        events::CORE_MESSAGE_SEAL.meta(),
        events::CORE_BLOCK_OPEN.meta(),
        events::CORE_BLOCK_DELTA.meta(),
        events::CORE_BLOCK_COMMITTED.meta(),
    ] {
        assert_eq!(
            def.mount,
            Mount::TranslationLayer,
            "{} hangs off the projection exit",
            def.name
        );
    }
}

#[test]
fn the_event_names_use_the_full_form_of_every_segment() {
    // 事件名逐段写全，不使用缩写
    assert_eq!(events::CORE_BLOCK_COMMITTED.name, "core/block.committed");
    assert_eq!(events::CORE_MESSAGE_SEAL.name, "core/message.seal");
}

#[test]
fn a_seal_payload_carries_the_usage_the_block_layer_cannot_express() {
    let seal = events::MessageSeal {
        message_id: MessageId(11),
        finish_reason: "stop".into(),
        usage: events::Usage {
            input_tokens: 120,
            output_tokens: 45,
        },
    };
    let value = serde_json::to_value(&seal).expect("serialize");
    assert_eq!(value["message_id"], serde_json::json!(11));
    assert_eq!(value["finish_reason"], serde_json::json!("stop"));
    assert_eq!(value["usage"]["input_tokens"], serde_json::json!(120));
}

// 消息块一族按名字给出四个条目

#[test]
fn every_name_in_the_directory_carries_its_shape_log_flag_and_mount() {
    let expected = [
        ("core/message.seal", Shape::Observe, true),
        ("core/block.open", Shape::Observe, true),
        ("core/block.delta", Shape::Observe, false),
        ("core/block.committed", Shape::Observe, true),
    ];
    let dir = directory();
    for (name, shape, log) in expected {
        let meta = dir.get(name).expect("look up four events by name");
        assert_eq!(meta.shape, shape, "{name} shape");
        assert_eq!(meta.log, log, "whether {name} enters the log");
        assert_eq!(meta.mount, Mount::TranslationLayer, "{name} mount point");
    }
    assert_eq!(
        expected.iter().filter(|(_, _, log)| *log).count(),
        3,
        "only one of the four stays out of the log"
    );
}

#[test]
fn every_directory_entry_generates_an_object_schema() {
    let dir = directory();
    for name in dir.names() {
        let schema = (dir.get(name).expect("look up catalog entry").schema)();
        assert_eq!(schema["type"], serde_json::json!("object"), "{name} schema");
    }
}

#[test]
fn the_block_schemas_keep_the_block_id_a_string() {
    let dir = directory();
    for name in [
        "core/block.open",
        "core/block.delta",
        "core/block.committed",
    ] {
        let schema = (dir.get(name).expect("find three block events").schema)();
        assert_eq!(
            schema["properties"]["block_id"]["type"],
            serde_json::json!("string"),
            "{name} block id is a string in the serialized result"
        );
    }
}

/// 一条 schema 用例，含取 schema 的函数、载荷样例与必须声明的 `required` 集合
type SchemaCase = (
    fn() -> serde_json::Value,
    serde_json::Value,
    &'static [&'static str],
);

/// 三条块事件的 schema 与对应的载荷一一对应
///
/// 只检查 `type` 与 `block_id` 时，把常量接到别的 schema 函数上仍会通过。
/// 因此逐个核对 `required` 集合与属性键集合，接错即失败
#[test]
fn each_block_schema_matches_the_payload_it_describes() {
    let opened = BlockOpened {
        block_id: BlockId(1),
        of_message: MessageId(2),
        role: Role::Assistant,
        kind: BlockKind::ToolCall,
        call_id: Some("c1".into()),
    };
    let delta = BlockDelta {
        block_id: BlockId(1),
        revision: 3,
        fragment: "ah".into(),
    };
    let commit = BlockCommit {
        block_id: BlockId(1),
        kind: BlockKind::Text,
        call_id: Some("c1".into()),
        content: "hello".into(),
        data: Some(serde_json::json!({ "x": 1 })),
    };
    let committed = BlockCommitted {
        block: commit.clone(),
        revision: 3,
    };

    let cases: [SchemaCase; 3] = [
        (
            events::CORE_BLOCK_OPEN.schema,
            serde_json::to_value(&opened).expect("serialize"),
            &["block_id", "of_message", "role", "kind"],
        ),
        (
            events::CORE_BLOCK_DELTA.schema,
            serde_json::to_value(&delta).expect("serialize"),
            &["block_id", "revision", "fragment"],
        ),
        (
            events::CORE_BLOCK_COMMITTED.schema,
            serde_json::to_value(&committed).expect("serialize"),
            &["block_id", "kind", "revision"],
        ),
    ];

    for (schema, sample, expected_required) in cases {
        let value = schema();
        let mut required: Vec<String> = value["required"]
            .as_array()
            .expect("read required from schema")
            .iter()
            .map(|item| item.as_str().expect("read required entries").to_string())
            .collect();
        let mut expected: Vec<String> = expected_required
            .iter()
            .map(|item| item.to_string())
            .collect();
        required.sort();
        expected.sort();
        assert_eq!(required, expected, "required fields of the schema");

        let mut properties: Vec<String> = value["properties"]
            .as_object()
            .expect("read properties from schema")
            .keys()
            .cloned()
            .collect();
        let mut sample_keys: Vec<String> = sample
            .as_object()
            .expect("read sample payload object")
            .keys()
            .cloned()
            .collect();
        properties.sort();
        sample_keys.sort();
        assert_eq!(
            properties, sample_keys,
            "schema property keys must correspond one-to-one with payload keys"
        );
    }
}

#[test]
fn the_seal_schema_carries_the_usage_the_payload_carries() {
    let schema = (events::CORE_MESSAGE_SEAL.schema)();
    assert_eq!(
        schema["properties"]["message_id"]["type"],
        serde_json::json!("integer")
    );
    assert_eq!(
        schema["properties"]["usage"]["$ref"],
        serde_json::json!("#/$defs/Usage")
    );
    for field in ["input_tokens", "output_tokens"] {
        assert_eq!(
            schema["$defs"]["Usage"]["properties"][field]["type"],
            serde_json::json!("integer"),
            "{field} in usage"
        );
    }
}

// 入向投影

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use wind_core::conversation::writer::BlockWriter;
use wind_core::db::Db;
use wind_core::feed::hub::FeedHub;
use wind_core::models::MessageCreate;
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::def::EventMeta;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::{EnabledSet, Subscription};
use wind_stream::block::BlockKind;
use wind_wire::Role;
use wind_wire::message::{Content, Message};
use wind_wire::tool::FunctionCall;

/// 一个不判定也不抛错的 host
struct NoopHost;

#[async_trait::async_trait]
impl ModHost for NoopHost {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        Ok(None)
    }
}

/// 放行全部作用域的启用集
struct AllowAll;

impl ModEnablement for AllowAll {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 记录派发给内建观察者的完整信封，供断言核对
///
/// 克隆共享同一份记录
#[derive(Default, Clone)]
struct Recorder {
    /// 按收到顺序记下的信封
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Recorder {
    /// 记下的事件名，按序
    fn names(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("lock the record")
            .iter()
            .filter_map(|event| event["name"].as_str().map(str::to_string))
            .collect()
    }

    /// 某个事件名记下的载荷，按序
    fn payloads(&self, name: &str) -> Vec<serde_json::Value> {
        self.seen
            .lock()
            .expect("lock the record")
            .iter()
            .filter(|event| event["name"] == serde_json::json!(name))
            .map(|event| event["payload"].clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for Recorder {
    async fn on_event(&self, _meta: &EventMeta, event: &serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the record")
            .push(event.clone());
    }
}

/// 一次并入的派发上下文
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("conversation"),
        Duration::from_millis(50),
    )
}

/// 临时库、帧设施、派发器与记录观察者
async fn rig(
    host: Arc<dyn ModHost>,
) -> (TempDir, Arc<FeedHub>, Arc<Dispatcher>, Storage, Recorder) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let hub = FeedHub::open(storage.clone(), None)
        .await
        .expect("open hub");
    let bus = Arc::new(Dispatcher::new(
        directory(),
        Arc::new(AllowAll),
        host,
        Limits::default(),
    ));
    let recorder = Recorder::default();
    bus.attach_observer(Arc::new(recorder.clone()));
    (dir, hub, bus, storage, recorder)
}

/// 构造一个带临时库与记录观察者的写者
async fn writer() -> (TempDir, Arc<FeedHub>, BlockWriter, Recorder) {
    let (dir, hub, bus, storage, recorder) = rig(Arc::new(NoopHost)).await;
    let writer = BlockWriter::new(
        Arc::clone(&hub),
        bus,
        storage,
        ProjectionInvalidations::default(),
    );
    (dir, hub, writer, recorder)
}

/// 一段只有正文的助手增量
fn text(content: &str) -> Message {
    Message {
        role: Role::Assistant,
        content: vec![Content::Text {
            data: content.into(),
        }],
        ..Message::default()
    }
}

#[tokio::test]
async fn absorbing_two_fragments_emits_one_open_and_one_delta() {
    let (_dir, _hub, writer, rec) = writer().await;
    let cx = cx();

    let first = writer.absorb(&cx, 1, MessageId(2), &text("hel")).await;
    assert_eq!(first.len(), 1, "the first absorb opened one block");
    writer.absorb(&cx, 1, MessageId(2), &text("lo")).await;

    assert_eq!(
        rec.names(),
        vec!["core/block.open", "core/block.delta", "core/block.delta"]
    );
}

#[tokio::test]
async fn the_delta_carries_the_revision_that_orders_it() {
    let (_dir, _hub, writer, rec) = writer().await;
    let cx = cx();
    writer.absorb(&cx, 1, MessageId(2), &text("hel")).await;
    writer.absorb(&cx, 1, MessageId(2), &text("lo")).await;

    let deltas = rec.payloads("core/block.delta");
    assert_eq!(deltas[0]["revision"], serde_json::json!(1));
    assert_eq!(deltas[0]["fragment"], serde_json::json!("hel"));
    assert_eq!(deltas[1]["revision"], serde_json::json!(2));
    assert_eq!(deltas[1]["fragment"], serde_json::json!("lo"));
}

#[tokio::test]
async fn the_writer_never_decides_which_block_a_fragment_belongs_to() {
    // 同一段文本分两次到达仍然并入同一个块，路由由累加器决定
    // 调用方没有做过任何判断
    let (_dir, hub, writer, _rec) = writer().await;
    let cx = cx();
    let ids = writer.absorb(&cx, 1, MessageId(2), &text("first")).await;
    writer.absorb(&cx, 1, MessageId(2), &text("second")).await;
    let blocks = hub.snapshot_blocks(&Scope::Topic { topic_id: 1 });
    assert_eq!(blocks.len(), 1);
    assert_eq!(ids[0], blocks[0].id);
}

#[tokio::test]
async fn a_tool_call_absorbs_its_arguments_fragment_by_fragment() {
    let (_dir, _hub, writer, rec) = writer().await;
    let cx = cx();
    let mut partial = Message {
        role: Role::Assistant,
        ..Message::default()
    };
    partial.tool_calls = Some(vec![FunctionCall {
        id: "abc".into(),
        name: "get_weather".into(),
        arguments: "{\"city\":".into(),
    }]);
    writer.absorb(&cx, 1, MessageId(2), &partial).await;
    partial.tool_calls = Some(vec![FunctionCall {
        id: "abc".into(),
        name: "get_weather".into(),
        arguments: "\"Beijing\"}".into(),
    }]);
    writer.absorb(&cx, 1, MessageId(2), &partial).await;

    let deltas = rec.payloads("core/block.delta");
    let joined: String = deltas
        .iter()
        .map(|delta| delta["fragment"].as_str().expect("read fragment string"))
        .collect();
    assert_eq!(
        joined, "{\"city\":\"Beijing\"}",
        "arguments are concatenated fragment by fragment, not replaced"
    );
}

// 提交与定稿

/// 块提交的写入侧，另给出存储以便核对落库结果
async fn writer_with_storage() -> (TempDir, Arc<FeedHub>, BlockWriter, Storage, Recorder) {
    let (dir, hub, bus, storage, recorder) = rig(Arc::new(NoopHost)).await;
    let writer = BlockWriter::new(
        Arc::clone(&hub),
        bus,
        storage.clone(),
        ProjectionInvalidations::default(),
    );
    (dir, hub, writer, storage, recorder)
}

/// 构造一个带临时库与记录观察者、且存储中已有一条消息行的写者
async fn writer_with_message() -> (TempDir, BlockWriter, i64, Recorder) {
    let (dir, hub, bus, storage, recorder) = rig(Arc::new(NoopHost)).await;
    let message_id = storage
        .message_create(MessageCreate {
            turn_id: 1,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    let writer = BlockWriter::new(hub, bus, storage, ProjectionInvalidations::default());
    (dir, writer, message_id, recorder)
}

#[tokio::test]
async fn committing_a_block_writes_the_row_and_announces_it() {
    let (_dir, hub, writer, storage, rec) = writer_with_storage().await;
    let cx = cx();
    let ids = writer.absorb(&cx, 1, MessageId(2), &text("hello")).await;
    let committed = writer.commit(&cx, 1, ids[0]).await.expect("commit");

    let block = committed.expect("return persisted block");
    assert!(block.finished);
    assert_eq!(
        rec.names().last().map(String::as_str),
        Some("core/block.committed")
    );
    assert!(
        hub.snapshot_blocks(&Scope::Topic { topic_id: 1 })
            .is_empty(),
        "after commit the block leaves the accumulator, so neither history queries nor snapshots can read it"
    );

    let rows = storage
        .block_list_by_message(2)
        .await
        .expect("read the committed block");
    assert_eq!(rows.len(), 1, "commit writes one row");
    assert_eq!(rows[0].content, "hello");
    assert_eq!(rows[0].kind, BlockKind::Text);
    assert_eq!(rows[0].seq, 0, "rank of the block within its message");
    assert_eq!(rows[0].revision, 1);
    assert!(rows[0].finished);
}

#[tokio::test]
async fn the_second_block_of_a_message_lands_on_the_next_rank() {
    let (_dir, _hub, writer, storage, _rec) = writer_with_storage().await;
    let cx = cx();
    let mut partial = text("answer");
    partial.tool_calls = Some(vec![FunctionCall {
        id: "c1".into(),
        name: "get_time".into(),
        arguments: "{}".into(),
    }]);
    let ids = writer.absorb(&cx, 1, MessageId(2), &partial).await;
    assert_eq!(
        ids.len(),
        2,
        "one block for the text and one for the tool call"
    );
    for id in &ids {
        writer.commit(&cx, 1, *id).await.expect("commit");
    }

    let rows = storage
        .block_list_by_message(2)
        .await
        .expect("read the committed block");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].seq, 0);
    assert_eq!(
        rows[1].seq, 1,
        "the second block in the same message continues the previous block's rank"
    );
    assert_eq!(rows[0].kind, BlockKind::Text);
    assert_eq!(rows[1].kind, BlockKind::ToolCall);
}

#[tokio::test]
async fn committing_one_message_out_of_order_loses_nothing() {
    // 名次取已提交的最大名次加一，与提交顺序无关。先提交标识较大的块时，
    // 后提交的块接着往下排。两块都写入存储，内容都不丢失
    let (_dir, hub, writer, storage, _rec) = writer_with_storage().await;
    let cx = cx();
    let mut partial = text("answer");
    partial.tool_calls = Some(vec![FunctionCall {
        id: "c1".into(),
        name: "get_time".into(),
        arguments: "{}".into(),
    }]);
    let ids = writer.absorb(&cx, 1, MessageId(2), &partial).await;
    assert_eq!(ids.len(), 2);
    assert!(ids[0] < ids[1], "absorb order is id order");

    writer
        .commit(&cx, 1, ids[1])
        .await
        .expect("commit")
        .expect("not denied");
    let second = writer
        .commit(&cx, 1, ids[0])
        .await
        .expect("submit out of order")
        .expect("not denied");
    assert!(
        second.finished,
        "a block committed out of order is a fact all the same"
    );

    let rows = storage
        .block_list_by_message(2)
        .await
        .expect("read the committed block");
    assert_eq!(
        rows.len(),
        2,
        "both blocks are written to the database, out-of-order commit loses content"
    );
    assert_eq!(
        rows[0].id, ids[1].0 as i64,
        "the first committed sits at rank 0"
    );
    assert_eq!(rows[0].seq, 0);
    assert_eq!(
        rows[1].id, ids[0].0 as i64,
        "the later committed continues at rank 1"
    );
    assert_eq!(rows[1].seq, 1);
    assert_eq!(
        rows[0].kind,
        BlockKind::ToolCall,
        "the first committed tool call landed in storage"
    );
    assert_eq!(
        rows[1].content, "answer",
        "the later committed text content is intact"
    );
    assert!(
        hub.snapshot_blocks(&Scope::Topic { topic_id: 1 })
            .is_empty(),
        "both blocks are committed, nothing is left in the accumulator"
    );

    let again = writer
        .absorb(&cx, 1, MessageId(2), &text("continuation"))
        .await;
    assert_eq!(again.len(), 1, "later fragments open a new block as usual");
    assert_ne!(again[0], ids[0]);
    assert_ne!(again[0], ids[1]);
}

#[tokio::test]
async fn a_failed_persist_leaves_the_block_open() {
    let (_dir, hub, writer, storage, _rec) = writer_with_storage().await;
    // 先用掉块标识 1，hub 分配的下一个块落库时与它撞主键
    storage
        .block_insert(BlockRow {
            id: 1,
            message_id: 1,
            seq: 0,
            kind: BlockKind::Text,
            call_id: None,
            revision: 1,
            content: "seed".into(),
            data: None,
            finished: true,
        })
        .await
        .expect("write the row that collides");

    let cx = cx();
    let ids = writer.absorb(&cx, 1, MessageId(2), &text("hello")).await;
    assert_eq!(ids[0], BlockId(1), "the hub allocates the next id");

    let outcome = writer.commit(&cx, 1, ids[0]).await;
    assert!(outcome.is_err(), "a failed persist surfaces as an error");
    assert!(
        hub.snapshot_blocks(&Scope::Topic { topic_id: 1 })
            .iter()
            .any(|block| block.id == ids[0]),
        "a block that never reached storage stays open so the caller can retry"
    );
}

#[tokio::test]
async fn a_failed_cache_invalidation_leaves_the_commit_a_fact() {
    // 写入存储之后，作废投影缓存需要查询消息属于哪一轮。该查询是一次无关的读，
    // 失败不应把已写入的事实变成错误，也不应丢掉事实的宣告
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db.clone());
    let hub = FeedHub::open(storage.clone(), None)
        .await
        .expect("open hub");
    let bus = Arc::new(Dispatcher::new(
        directory(),
        Arc::new(AllowAll),
        Arc::new(NoopHost),
        Limits::default(),
    ));
    let recorder = Recorder::default();
    bus.attach_observer(Arc::new(recorder.clone()));
    // 在作废出口登记一份缓存，写路径才会去查询消息属于哪一轮
    let invalidations = ProjectionInvalidations::default();
    let _projection = ContextProjection::new(storage.clone(), 1, invalidations.clone());
    let writer = BlockWriter::new(Arc::clone(&hub), bus, storage.clone(), invalidations);

    // 破坏作废流程要查询的表。查询从此报错，而块的插入不受影响
    sqlx::query("DROP TABLE message")
        .execute(db.pool())
        .await
        .expect("drop table");

    let cx = cx();
    let ids = writer.absorb(&cx, 1, MessageId(2), &text("hello")).await;
    let committed = writer.commit(&cx, 1, ids[0]).await;

    assert!(
        committed.is_ok(),
        "a failed cache invalidation must not turn an already written fact into an error"
    );
    assert!(
        committed.expect("commit").is_some(),
        "the block is still returned"
    );
    assert_eq!(
        recorder.names().last().map(String::as_str),
        Some("core/block.committed"),
        "the fact is still announced"
    );
    let rows = storage
        .block_list_by_message(2)
        .await
        .expect("read the committed block");
    assert_eq!(rows.len(), 1, "the row really was written");
    assert_eq!(rows[0].content, "hello");
}

#[tokio::test]
async fn the_committed_payload_carries_the_settled_content() {
    let (_dir, _hub, writer, rec) = writer().await;
    let cx = cx();
    let ids = writer
        .absorb(&cx, 1, MessageId(2), &text("original text"))
        .await;
    writer
        .commit(&cx, 1, ids[0])
        .await
        .expect("commit")
        .expect("not denied");

    let committed = rec.payloads("core/block.committed");
    assert_eq!(committed[0]["content"], serde_json::json!("original text"));
    assert_eq!(committed[0]["revision"], serde_json::json!(1));
    assert_eq!(
        committed[0]["block_id"],
        serde_json::json!(ids[0].0.to_string())
    );
}

#[tokio::test]
async fn sealing_a_round_carries_the_usage_the_blocks_cannot_express() {
    let (_dir, writer, message_id, rec) = writer_with_message().await;
    let cx = cx();
    writer
        .seal(
            &cx,
            MessageId(message_id),
            "stop".into(),
            events::Usage {
                input_tokens: 120,
                output_tokens: 45,
            },
        )
        .await
        .expect("finalize block");

    let seals = rec.payloads("core/message.seal");
    assert_eq!(
        seals.len(),
        1,
        "a seal emits one message at a time, not one per fragment"
    );
    assert_eq!(seals[0]["finish_reason"], serde_json::json!("stop"));
    assert_eq!(seals[0]["usage"]["output_tokens"], serde_json::json!(45));
    assert_eq!(seals[0]["message_id"], serde_json::json!(message_id));
}

#[tokio::test]
async fn sealing_an_unknown_message_reports_rather_than_dropping_the_usage() {
    let (_dir, writer, _message_id, _rec) = writer_with_message().await;
    let cx = cx();
    let outcome = writer
        .seal(
            &cx,
            MessageId(9999),
            "stop".into(),
            events::Usage {
                input_tokens: 1,
                output_tokens: 1,
            },
        )
        .await;
    assert!(
        outcome.is_err(),
        "sealing a message that does not exist must report an error"
    );
}

// 出向投影

use wind_core::models::BlockRow;
use wind_stream::id::TurnId;

/// 建一个实例、一轮，以及该轮下块数为 `blocks_per_message` 的消息
///
/// 块数为 0 时不建消息。空消息会与测试补入该轮的消息并列，
/// 两者都占用轮内序号 0，投影结果的次序因此不确定
async fn seeded(blocks_per_message: usize) -> (TempDir, Storage, i64) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);

    let turn_id = storage
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: None,
        })
        .await
        .expect("build turn");

    if blocks_per_message == 0 {
        return (dir, storage, turn_id);
    }

    let message_id = storage
        .message_create(MessageCreate {
            turn_id,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        })
        .await
        .expect("build message");
    for seq in 0..blocks_per_message {
        storage
            .block_insert(BlockRow {
                id: seq as i64 + 1,
                message_id,
                seq: seq as i64,
                kind: BlockKind::Text,
                call_id: None,
                revision: 1,
                content: format!("block {seq}"),
                data: None,
                finished: true,
            })
            .await
            .expect("write block");
    }
    (dir, storage, turn_id)
}

#[tokio::test]
async fn a_statement_inside_a_transaction_reports_into_the_same_counter() {
    let (_dir, storage, _turn) = seeded(0).await;
    let before = storage.statement_count();
    storage
        .with_tx(|tx| async move {
            tx.turn_create(TurnCreate {
                instance_id: 2,
                topic_id: 1,
                from_id: None,
                is_boundary: false,
                created_at: None,
            })
            .await?;
            Ok(())
        })
        .await
        .expect("create turn in transaction");

    assert_eq!(
        storage.statement_count() - before,
        1,
        "statements inside a transaction scope count into the same outer counter"
    );
}

// 按轮追加的投影缓存

use wind_core::conversation::assembler::{ContextProjection, ProjectionInvalidations};

/// 构造三轮，每轮一条用户消息与一条助手消息，各带一块文本，返回三个轮标识
async fn three_turns(storage: &Storage) -> Vec<i64> {
    let mut ids = Vec::new();
    for seq in 0..3 {
        let turn_id = storage
            .turn_create(TurnCreate {
                instance_id: 1,
                topic_id: 1,
                from_id: None,
                is_boundary: false,
                created_at: None,
            })
            .await
            .expect("build turn");
        let user_id = storage
            .message_create(MessageCreate {
                turn_id,
                role: Role::User,
                model_id: None,
                finish_reason: None,
                input_tokens: 0,
                output_tokens: 0,
                created_at: None,
            })
            .await
            .expect("build user message");
        storage
            .block_insert(BlockRow {
                id: seq * 2 + 1,
                message_id: user_id,
                seq: 0,
                kind: BlockKind::Text,
                call_id: None,
                revision: 1,
                content: format!("ask {seq}"),
                data: None,
                finished: true,
            })
            .await
            .expect("write user block");
        let message_id = storage
            .message_create(MessageCreate {
                turn_id,
                role: Role::Assistant,
                model_id: None,
                finish_reason: None,
                input_tokens: 0,
                output_tokens: 0,
                created_at: None,
            })
            .await
            .expect("build message");
        storage
            .block_insert(BlockRow {
                id: seq * 2 + 2,
                message_id,
                seq: 0,
                kind: BlockKind::Text,
                call_id: None,
                revision: 1,
                content: format!("round {seq}"),
                data: None,
                finished: true,
            })
            .await
            .expect("write block");
        ids.push(turn_id);
    }
    ids
}

#[tokio::test]
async fn appending_a_block_rebuilds_only_its_own_turn() {
    let (_dir, storage, _turn) = seeded(0).await;
    let turns = three_turns(&storage).await;
    let projection = ContextProjection::new(storage.clone(), 1, ProjectionInvalidations::default());

    projection.context(None).await.expect("assemble");
    assert_eq!(projection.rebuilt_turns(), 3);

    // 在最后一轮追加一块，再组装一次
    let message_id = storage
        .message_list_by_turns(&turns[2..])
        .await
        .expect("query")
        .last()
        .expect("find the assistant message")
        .id;
    storage
        .block_insert(BlockRow {
            id: 100,
            message_id,
            seq: 1,
            kind: BlockKind::Text,
            call_id: None,
            revision: 1,
            content: "tail".into(),
            data: None,
            finished: true,
        })
        .await
        .expect("write block");
    projection.invalidate(TurnId(turns[2]));

    let context = projection.context(None).await.expect("assemble");
    assert_eq!(
        projection.rebuilt_turns(),
        4,
        "only the last round is recomputed, the first two are reused"
    );
    assert_eq!(context.len(), 6);
    assert_eq!(
        context[5].content.len(),
        2,
        "the new block entered its own round"
    );
}

#[tokio::test]
async fn a_projection_without_invalidation_reuses_every_turn() {
    let (_dir, storage, _turn) = seeded(0).await;
    three_turns(&storage).await;
    let projection = ContextProjection::new(storage, 1, ProjectionInvalidations::default());

    projection.context(None).await.expect("assemble");
    projection.context(None).await.expect("assemble");
    assert_eq!(
        projection.rebuilt_turns(),
        3,
        "without invalidation nothing is recomputed"
    );
}

#[tokio::test]
async fn a_turn_range_narrows_the_context_without_rebuilding() {
    let (_dir, storage, _turn) = seeded(0).await;
    let turns = three_turns(&storage).await;
    let projection = ContextProjection::new(storage, 1, ProjectionInvalidations::default());

    let context = projection
        .context(Some(TurnId(turns[0])))
        .await
        .expect("assemble");
    assert_eq!(context.len(), 2);
    assert_eq!(
        projection.rebuilt_turns(),
        1,
        "with only one round, only one round is projected"
    );
}

#[tokio::test]
async fn a_commit_invalidates_only_the_turn_it_lands_in() {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    // 先构造内容再打开 hub。累加器的块标识种子取自存储中已有的最大块标识
    let turns = three_turns(&storage).await;
    let hub = FeedHub::open(storage.clone(), None)
        .await
        .expect("open hub");
    let bus = Arc::new(Dispatcher::new(
        directory(),
        Arc::new(AllowAll),
        Arc::new(NoopHost),
        Limits::default(),
    ));
    let invalidations = ProjectionInvalidations::default();
    let writer = BlockWriter::new(
        Arc::clone(&hub),
        bus,
        storage.clone(),
        invalidations.clone(),
    );
    let projection = ContextProjection::new(storage.clone(), 1, invalidations);

    let before = storage.statement_count();
    projection.context(None).await.expect("assemble");
    let full = storage.statement_count() - before;
    assert_eq!(
        projection.rebuilt_turns(),
        3,
        "the first assembly must compute all three rounds"
    );
    assert_eq!(
        full, 3,
        "an unhit round is fetched in one pass: turns, messages and blocks each once"
    );

    // 向最后一轮的消息并入并提交一块。写者提交时作废该块所属的 turn
    let message_id = storage
        .message_list_by_turns(&turns[2..])
        .await
        .expect("query")
        .last()
        .expect("find the assistant message")
        .id;
    let cx = cx();
    let blocks = writer
        .absorb(&cx, 1, MessageId(message_id), &text("tail"))
        .await;
    writer
        .commit(&cx, 1, blocks[0])
        .await
        .expect("commit")
        .expect("not denied");

    let before = storage.statement_count();
    let context = projection.context(None).await.expect("assemble");
    let appended = storage.statement_count() - before;

    assert_eq!(
        projection.rebuilt_turns(),
        4,
        "a commit invalidates only its own round"
    );
    assert_eq!(
        appended, 3,
        "appending one block still takes only three statements: turns, this round's messages, this round's blocks; \
         the two cached rounds are no longer read, and the statement count does not vary with how many there are"
    );
    assert_eq!(context.len(), 6);
    assert_eq!(
        context[5].content.len(),
        2,
        "the committed block entered its own round"
    );
}

#[tokio::test]
async fn sealing_a_round_invalidates_its_turn_too() {
    // 定稿写回该轮的用量与结束原因，缓存中对应的 turn 必须一同作废
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let turn_id = storage
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: None,
        })
        .await
        .expect("build turn");
    let user_id = storage
        .message_create(MessageCreate {
            turn_id,
            role: Role::User,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        })
        .await
        .expect("build user message");
    storage
        .block_insert(BlockRow {
            id: 1,
            message_id: user_id,
            seq: 0,
            kind: BlockKind::Text,
            call_id: None,
            revision: 1,
            content: "ask".into(),
            data: None,
            finished: true,
        })
        .await
        .expect("write user block");
    let message_id = storage
        .message_create(MessageCreate {
            turn_id,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        })
        .await
        .expect("build message");
    storage
        .block_insert(BlockRow {
            id: 2,
            message_id,
            seq: 0,
            kind: BlockKind::Text,
            call_id: None,
            revision: 1,
            content: "round 0".into(),
            data: None,
            finished: true,
        })
        .await
        .expect("write block");
    let hub = FeedHub::open(storage.clone(), None)
        .await
        .expect("open hub");
    let bus = Arc::new(Dispatcher::new(
        directory(),
        Arc::new(AllowAll),
        Arc::new(NoopHost),
        Limits::default(),
    ));
    let invalidations = ProjectionInvalidations::default();
    let writer = BlockWriter::new(hub, bus, storage.clone(), invalidations.clone());
    let projection = ContextProjection::new(storage.clone(), 1, invalidations);

    let context = projection.context(None).await.expect("assemble");
    assert_eq!(projection.rebuilt_turns(), 1);
    assert_eq!(context[1].output_tokens, 0, "not sealed yet");

    writer
        .seal(
            &cx(),
            MessageId(message_id),
            "stop".into(),
            events::Usage {
                input_tokens: 120,
                output_tokens: 45,
            },
        )
        .await
        .expect("finalize block");

    let context = projection.context(None).await.expect("assemble");
    assert_eq!(
        projection.rebuilt_turns(),
        2,
        "a seal also invalidates its own round"
    );
    assert_eq!(
        context[1].output_tokens, 45,
        "after recomputation the usage written back by the seal is present"
    );
    assert_eq!(context[1].input_tokens, 120);
}
