//! 验证事件日志的写入与回补。

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
use wind_core::db::Db;
use wind_core::feed::frame::Frame;
use wind_core::feed::hub::FeedHub;
use wind_core::feed::log::LogRecord;
use wind_core::feed::subscribe::{Subscribe, Subscriber};
use wind_core::storage::{Storage, StorageTx};
use wind_event::def::{EventDef, Mount, Shape};
use wind_event::envelope::{EventSeq, Origin, Scope, Source};
use wind_event::observer::BuiltinObserver;

async fn fixture() -> (TempDir, Storage, Arc<FeedHub>) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let hub = FeedHub::open(storage.clone(), None)
        .await
        .expect("open hub");
    (dir, storage, hub)
}

async fn publish(hub: &FeedHub, name: &'static str, log: bool, scope: serde_json::Value, seq: u64) {
    let def: EventDef<serde_json::Value> =
        EventDef::new(name, Shape::Observe, log, Mount::Subsystem, || {
            serde_json::json!({})
        });
    let envelope = serde_json::json!({
        "seq": seq,
        "time": 1_760_000_000_000i64,
        "name": name,
        "scope": scope,
        "origin": "agent",
        "source": { "kind": "core", "subsystem": "test" },
        "payload": { "n": seq },
    });
    hub.on_event(&def.meta(), &envelope).await;
}

#[tokio::test]
async fn a_logged_event_becomes_a_pending_record() {
    let (_dir, _storage, hub) = fixture().await;
    publish(
        &hub,
        "core/message.seal",
        true,
        serde_json::json!({ "kind": "topic", "topic_id": 1 }),
        1,
    )
    .await;

    let pending = hub.take_pending();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].name, "core/message.seal");
    assert_eq!(pending[0].seq, EventSeq(1));
    assert_eq!(pending[0].payload, serde_json::json!({ "n": 1 }));
}

#[tokio::test]
async fn the_draft_event_is_delivered_but_never_logged() {
    let (_dir, _storage, hub) = fixture().await;
    publish(
        &hub,
        "core/block.delta",
        false,
        serde_json::json!({ "kind": "topic", "topic_id": 1 }),
        1,
    )
    .await;
    assert!(
        hub.take_pending().is_empty(),
        "a draft is not a fact and is never logged, the only exception among all events"
    );
}

#[tokio::test]
async fn a_drained_record_lands_in_the_table() {
    let (_dir, storage, hub) = fixture().await;
    publish(
        &hub,
        "core/message.seal",
        true,
        serde_json::json!({ "kind": "topic", "topic_id": 3 }),
        1,
    )
    .await;
    publish(
        &hub,
        "core/block.open",
        true,
        serde_json::json!({ "kind": "global" }),
        2,
    )
    .await;

    let pending = hub.take_pending();
    storage
        .event_log_append(&pending)
        .await
        .expect("write to the database");

    assert_eq!(storage.event_log_max_seq().await.expect("query"), 2);
    let topic = storage
        .event_log_range(Some(3), 0, 100, 1000)
        .await
        .expect("query");
    assert_eq!(
        topic.len(),
        1,
        "filtered by scope: a topic connection does not receive global records"
    );
    assert_eq!(topic[0].name, "core/message.seal");
}

#[tokio::test]
async fn deleting_a_topic_takes_its_log_with_it() {
    let (_dir, storage, hub) = fixture().await;
    publish(
        &hub,
        "core/message.seal",
        true,
        serde_json::json!({ "kind": "topic", "topic_id": 3 }),
        1,
    )
    .await;
    publish(
        &hub,
        "core/block.open",
        true,
        serde_json::json!({ "kind": "global" }),
        2,
    )
    .await;
    let pending = hub.take_pending();
    storage
        .event_log_append(&pending)
        .await
        .expect("write to the database");

    storage.event_log_delete_topic(3).await.expect("delete");
    assert_eq!(
        storage.event_log_max_seq().await.expect("query"),
        2,
        "deleting clears records only and does not reclaim sequence numbers"
    );
    assert!(
        storage
            .event_log_range(Some(3), 0, 100, 1000)
            .await
            .expect("query")
            .is_empty()
    );
    assert_eq!(
        storage
            .event_log_range(None, 0, 100, 1000)
            .await
            .expect("query")
            .len(),
        1,
        "global records are not deleted along with any topic"
    );
}

#[tokio::test]
async fn a_record_survives_the_round_trip_through_its_columns() {
    let (_dir, storage, hub) = fixture().await;
    let def: EventDef<serde_json::Value> = EventDef::new(
        "core/block.committed",
        Shape::Observe,
        true,
        Mount::Subsystem,
        || serde_json::json!({}),
    );
    let envelope = serde_json::json!({
        "seq": 9,
        "time": 1_760_000_000_000i64,
        "name": "core/block.committed",
        "scope": { "kind": "topic", "topic_id": 4 },
        "origin": "ui",
        "source": { "kind": "mod", "name": "panel", "version": 2 },
        "cause": 8,
        "payload": { "block_id": "7", "revision": 3 },
    });
    hub.on_event(&def.meta(), &envelope).await;
    let pending = hub.take_pending();
    storage
        .event_log_append(&pending)
        .await
        .expect("write to the database");

    let back = &storage
        .event_log_range(Some(4), 8, 1, 1000)
        .await
        .expect("query")[0];
    assert_eq!(back.seq, EventSeq(9));
    assert_eq!(back.shape, Shape::Observe);
    assert_eq!(back.origin, Origin::Ui);
    assert_eq!(back.cause, Some(EventSeq(8)));
    assert_eq!(
        back.payload,
        serde_json::json!({ "block_id": "7", "revision": 3 })
    );
}

//
// 历史回补
//

/// 话题作用域的信封表示
fn topic_scope(topic_id: i64) -> serde_json::Value {
    serde_json::json!({ "kind": "topic", "topic_id": topic_id })
}

/// 订阅 1 号话题，从给定序号之后回补
async fn open_backfill(hub: &FeedHub, since: u64) -> Subscriber {
    hub.subscribe(Subscribe {
        scope: Scope::Topic { topic_id: 1 },
        since_seq: Some(EventSeq(since)),
        until_seq: None,
        blocks: false,
    })
    .await
    .expect("subscribe")
    .subscriber
}

/// 取下一帧，等待超过上限即判失败
///
/// 等待上限把缺失的帧转成断言失败，避免测试一直等待
async fn next_frame(subscriber: &mut Subscriber) -> Frame {
    timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("a frame arrives")
        .expect("a frame arrives")
}

/// 取下一帧并要求为事件帧，返回事件序号
async fn next_seq(subscriber: &mut Subscriber) -> u64 {
    match next_frame(subscriber).await {
        Frame::Event { envelope } => envelope["seq"].as_u64().expect("read integer sequence"),
        Frame::Snapshot { .. } => panic!("expected event frame, got snapshot"),
    }
}

/// 在短窗口内取一帧，用于断言此刻不该有帧
async fn frame_within(subscriber: &mut Subscriber) -> Option<Frame> {
    timeout(Duration::from_millis(200), subscriber.next())
        .await
        .ok()
        .flatten()
}

/// 回补帧与实时帧形状相同。客户端不需要区分二者
#[tokio::test]
async fn backfilled_frames_are_event_frames_like_live_ones() {
    let (_dir, storage, hub) = fixture().await;
    for seq in 1..=3 {
        publish(&hub, "core/message.seal", true, topic_scope(1), seq).await;
    }
    storage
        .event_log_append(&hub.take_pending())
        .await
        .expect("write to the database");

    let mut subscriber = open_backfill(&hub, 0).await;
    assert_eq!(next_seq(&mut subscriber).await, 1);
    assert_eq!(next_seq(&mut subscriber).await, 2);
    assert_eq!(next_seq(&mut subscriber).await, 3);
}

/// 注册时已在库里的记录先全部送出。注册后到达的实时帧随后送出
#[tokio::test]
async fn backfill_and_live_frames_do_not_interleave() {
    let (_dir, storage, hub) = fixture().await;
    publish(&hub, "core/message.seal", true, topic_scope(1), 1).await;
    storage
        .event_log_append(&hub.take_pending())
        .await
        .expect("write to the database");

    let mut subscriber = open_backfill(&hub, 0).await;

    // 注册之后、取首帧之前到达的实时帧
    publish(&hub, "core/block.open", true, topic_scope(1), 2).await;

    assert_eq!(
        next_seq(&mut subscriber).await,
        1,
        "backfill must come before live frames"
    );
    assert_eq!(next_seq(&mut subscriber).await, 2);
}

/// 单次回补有上限，页尾的序号即下一个游标
#[tokio::test]
async fn one_backfill_page_stops_at_a_thousand() {
    let (_dir, storage, hub) = fixture().await;
    for seq in 1..=1100 {
        publish(&hub, "core/message.seal", true, topic_scope(1), seq).await;
    }
    storage
        .event_log_append(&hub.take_pending())
        .await
        .expect("write to the database");

    let mut subscriber = open_backfill(&hub, 0).await;
    let mut seqs = Vec::new();
    for _ in 0..1000 {
        seqs.push(next_seq(&mut subscriber).await);
    }
    assert_eq!(seqs.first(), Some(&1));
    assert_eq!(
        seqs.last(),
        Some(&1000),
        "the seq at the end of the page forms the next cursor"
    );
    assert!(
        seqs.windows(2).all(|pair| pair[0] < pair[1]),
        "backfill is in ascending seq order"
    );

    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "a hundred records remain in the store; they belong to the next page"
    );
}

/// 拦截事件不进入前端流，回补同样不得包含
#[tokio::test]
async fn an_intercept_record_is_not_backfilled() {
    let (_dir, storage, hub) = fixture().await;
    let def: EventDef<serde_json::Value> = EventDef::new(
        "core/test.intercept",
        Shape::Intercept,
        true,
        Mount::Subsystem,
        || serde_json::json!({}),
    );
    let envelope = serde_json::json!({
        "seq": 1,
        "time": 1_760_000_000_000i64,
        "name": "core/test.intercept",
        "scope": topic_scope(1),
        "origin": "agent",
        "source": { "kind": "core", "subsystem": "conversation" },
        "payload": {},
    });
    hub.on_event(&def.meta(), &envelope).await;
    storage
        .event_log_append(&hub.take_pending())
        .await
        .expect("write to the database");

    let mut subscriber = open_backfill(&hub, 0).await;

    publish(&hub, "core/message.seal", true, topic_scope(1), 2).await;
    assert_eq!(
        next_seq(&mut subscriber).await,
        2,
        "control-plane events do not enter the frontend stream and must not appear in backfill either"
    );
}

/// 序号可以只被消耗而不留记录。回补不得因此等待一个不会出现的序号
#[tokio::test]
async fn a_sequence_gap_does_not_stall_the_backfill() {
    let (_dir, storage, hub) = fixture().await;
    for seq in [1, 2, 4] {
        publish(&hub, "core/message.seal", true, topic_scope(1), seq).await;
    }
    storage
        .event_log_append(&hub.take_pending())
        .await
        .expect("write to the database");

    let mut subscriber = open_backfill(&hub, 0).await;
    assert_eq!(next_seq(&mut subscriber).await, 1);
    assert_eq!(next_seq(&mut subscriber).await, 2);
    assert_eq!(
        next_seq(&mut subscriber).await,
        4,
        "gaps are not filled; only records present in the store are delivered"
    );

    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "a gap is not a record waiting to be backfilled"
    );
}

/// 重启后的序号源接着存储中的最大序号发放。记录条数与序号空洞都不能作为种子
#[tokio::test]
async fn the_highest_logged_sequence_seeds_a_restart() {
    let (_dir, storage, hub) = fixture().await;
    for seq in [1, 2, 5] {
        publish(&hub, "core/message.seal", true, topic_scope(1), seq).await;
    }
    storage
        .event_log_append(&hub.take_pending())
        .await
        .expect("write to the database");

    assert_eq!(
        storage.event_log_max_seq().await.expect("query"),
        5,
        "the seed is the maximum seq, not the record count 3, nor 4 after filling gaps"
    );
}

//
// 写入与回补并发
//

/// 占住日志表的写锁，直到返回的事务被回滚
///
/// 写入与回补的并发窗口从外部不可见，只能通过阻塞一次数据库写入来固定该窗口
async fn hold_write_lock(storage: &Storage) -> StorageTx {
    let tx = storage.begin().await.expect("begin transaction");
    let occupier = LogRecord {
        seq: EventSeq(9999),
        time: 1_760_000_000_000,
        name: "core/message.seal".into(),
        shape: Shape::Observe,
        scope: Scope::Topic { topic_id: 1 },
        origin: Origin::Agent,
        source: Source::core("test"),
        cause: None,
        payload: serde_json::json!({}),
    };
    tx.storage()
        .event_log_append(&[occupier])
        .await
        .expect("write");
    tx
}

/// 一批仍在写入时，并发的 `flush` 必须等待该批完成，不得绕过
///
/// 绕过时该调用会看到空的待写队列并立即查询存储。此时前一批尚未提交，回补会丢掉一批序号在边界之内的记录
#[tokio::test]
async fn a_concurrent_flush_waits_for_the_write_in_flight() {
    let (_dir, storage, hub) = fixture().await;
    let held = hold_write_lock(&storage).await;

    for seq in 1..=3 {
        publish(&hub, "core/message.seal", true, topic_scope(1), seq).await;
    }
    let writing = tokio::spawn({
        let hub = Arc::clone(&hub);
        async move { hub.flush().await }
    });
    // 让先发起的调用取走待写批次，并停在被写锁挡住的插入上
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        timeout(Duration::from_millis(300), hub.flush())
            .await
            .is_err(),
        "while a write is in flight, a concurrent call must wait for it"
    );

    held.rollback().await.expect("roll back");
    writing
        .await
        .expect("finish write task")
        .expect("write to the database");
    assert_eq!(
        storage
            .event_log_range(Some(1), 0, 100, 1000)
            .await
            .expect("query")
            .len(),
        3,
        "the batch that waited landed in the store without losing a single record"
    );
}

/// 写失败的批次放回队首，排在该批之后产生的记录之前
#[tokio::test]
async fn a_failed_batch_goes_back_before_newer_records() {
    let (_dir, storage, hub) = fixture().await;
    publish(&hub, "core/message.seal", true, topic_scope(1), 2).await;
    hub.flush().await.expect("write to the database");

    let held = hold_write_lock(&storage).await;
    // 该记录与已写入的 2 号序号相撞，整批注定写入失败
    publish(&hub, "core/message.seal", true, topic_scope(1), 2).await;
    let writing = tokio::spawn({
        let hub = Arc::clone(&hub);
        async move { hub.flush().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 该批仍在写入时再产生一条记录，这条记录比失败的批次更晚
    publish(&hub, "core/message.seal", true, topic_scope(1), 3).await;

    held.rollback().await.expect("roll back");
    assert!(
        writing.await.expect("finish write task").is_err(),
        "a batch that collides with the primary key must fail"
    );

    let pending = hub.take_pending();
    assert_eq!(
        pending.iter().map(|record| record.seq).collect::<Vec<_>>(),
        vec![EventSeq(2), EventSeq(3)],
        "the failed batch returns to the head of the queue, with new records after it"
    );
}

//
// 日志写失败
//

/// 写失败时整批留在待写队列里，下一次调用还能重试
#[tokio::test]
async fn a_failed_flush_keeps_the_batch_for_the_next_attempt() {
    let (_dir, _storage, hub) = fixture().await;
    publish(&hub, "core/message.seal", true, topic_scope(1), 1).await;
    hub.flush().await.expect("write to the database");

    // 再产生一条同序号的记录，整批插入会撞上 1 号主键
    publish(&hub, "core/message.seal", true, topic_scope(1), 1).await;
    assert!(
        hub.flush().await.is_err(),
        "a failed batch insert must report an error"
    );

    let pending = hub.take_pending();
    assert_eq!(pending.len(), 1, "a failed batch must not be dropped");
    assert_eq!(pending[0].seq, EventSeq(1));
}

/// 保留下来的批次在下一次调用中写入存储
#[tokio::test]
async fn a_restored_batch_lands_on_the_next_flush() {
    let (_dir, storage, hub) = fixture().await;
    publish(&hub, "core/message.seal", true, topic_scope(1), 1).await;
    hub.flush().await.expect("write to the database");
    publish(&hub, "core/message.seal", true, topic_scope(1), 1).await;
    assert!(
        hub.flush().await.is_err(),
        "a batch that collides with the primary key must fail"
    );

    // 删除冲突的记录行后重试，保留下来的记录应当写入
    storage.event_log_delete_topic(1).await.expect("delete");
    hub.flush().await.expect("flush after retry");

    let back = storage
        .event_log_range(Some(1), 0, 10, 100)
        .await
        .expect("query");
    assert_eq!(back.len(), 1, "the retained batch landed in the store");
    assert_eq!(back[0].seq, EventSeq(1));
    assert_eq!(back[0].payload, serde_json::json!({ "n": 1 }));
}
