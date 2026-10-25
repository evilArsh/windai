//! 验证前端事件流的序列化形状与订阅行为。

use std::iter::from_fn;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
use wind_core::db::Db;
use wind_core::feed::frame::{Baseline, Frame};
use wind_core::feed::hub::{BaselineProvider, FeedHub};
use wind_core::feed::queue::{Channel, FrameQueue, Overflow, channel_of};
use wind_core::feed::subscribe::{Subscribe, Subscriber};
use wind_core::storage::Storage;
use wind_event::def::{EventDef, Mount, Shape};
use wind_event::envelope::{EventSeq, Scope};
use wind_event::observer::BuiltinObserver;
use wind_stream::accumulator::BlockChange;
use wind_stream::block::{Block, BlockKind};
use wind_stream::id::{BlockId, MessageId};
use wind_stream::payload::{BlockCommit, BlockCommitted, BlockDelta};
use wind_wire::Role;
use wind_wire::message::{Content, Message};

fn block(id: u64, content: &str) -> Block {
    Block {
        id: BlockId(id),
        of_message: MessageId(2),
        role: Role::Assistant,
        kind: BlockKind::Text,
        call_id: None,
        revision: 3,
        content: content.into(),
        data: None,
        finished: false,
    }
}

#[test]
fn a_block_baseline_is_a_snapshot_frame() {
    let frame = Frame::Snapshot {
        baseline: Baseline::Blocks {
            blocks: vec![block(7, "hello")],
        },
    };
    let value = serde_json::to_value(&frame).expect("serialize");
    assert_eq!(value["kind"], serde_json::json!("snapshot"));
    assert_eq!(value["baseline"]["baseline"], serde_json::json!("blocks"));
    assert_eq!(
        value["baseline"]["blocks"][0]["id"],
        serde_json::json!("7"),
        "block ids exceed the JavaScript safe integer range and must be strings"
    );
    assert_eq!(
        value["baseline"]["blocks"][0]["revision"],
        serde_json::json!(3)
    );
}

#[test]
fn a_state_baseline_carries_a_key_and_a_value() {
    let frame = Frame::Snapshot {
        baseline: Baseline::Stateful {
            key: "items".into(),
            value: serde_json::json!([1, 2]),
        },
    };
    let value = serde_json::to_value(&frame).expect("serialize");
    assert_eq!(value["baseline"]["baseline"], serde_json::json!("stateful"));
    assert_eq!(value["baseline"]["key"], serde_json::json!("items"));
    assert_eq!(value["baseline"]["value"], serde_json::json!([1, 2]));
}

#[test]
fn an_event_frame_carries_the_whole_envelope() {
    let frame = Frame::Event {
        envelope: serde_json::json!({
            "seq": 12,
            "time": 1760000000000i64,
            "name": "core/block.committed",
            "scope": { "kind": "topic", "topic_id": 1 },
            "origin": "agent",
            "source": { "kind": "core", "subsystem": "conversation" },
            "payload": { "block_id": "7" },
        }),
    };
    let value = serde_json::to_value(&frame).expect("serialize");
    assert_eq!(value["kind"], serde_json::json!("event"));
    assert_eq!(
        value["envelope"]["name"],
        serde_json::json!("core/block.committed")
    );
    assert_eq!(value["envelope"]["seq"], serde_json::json!(12));
}

#[test]
fn both_frame_kinds_round_trip() {
    for frame in [
        Frame::Snapshot {
            baseline: Baseline::Blocks { blocks: vec![] },
        },
        Frame::Event {
            envelope: serde_json::json!({ "seq": 1 }),
        },
    ] {
        let text = serde_json::to_string(&frame).expect("serialize");
        let back: Frame = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, frame);
    }
}

#[test]
fn a_frame_of_an_unknown_kind_is_rejected() {
    let parsed = serde_json::from_str::<Frame>(r#"{"kind":"heartbeat"}"#);
    assert!(
        parsed.is_err(),
        "unknown frame kinds must be rejected, not silently downgraded"
    );
}

// 有界帧队列

fn event(seq: u64) -> Frame {
    Frame::Event {
        envelope: serde_json::json!({ "seq": seq }),
    }
}

fn seq_of(frame: &Frame) -> u64 {
    match frame {
        Frame::Event { envelope } => envelope["seq"].as_u64().expect("read sequence"),
        Frame::Snapshot { .. } => panic!("expected event frame in queue"),
    }
}

#[test]
fn a_queue_within_its_bound_keeps_everything_in_order() {
    let mut queue = FrameQueue::new(4);
    for seq in 1..=4 {
        assert_eq!(queue.push(event(seq), Channel::Journal), None);
    }
    assert_eq!(queue.len(), 4);
    let drained: Vec<u64> = from_fn(|| queue.pop()).map(|f| seq_of(&f)).collect();
    assert_eq!(drained, vec![1, 2, 3, 4]);
}

#[test]
fn a_fragment_overflow_drops_the_oldest() {
    let mut queue = FrameQueue::new(3);
    for seq in 1..=3 {
        queue.push(event(seq), Channel::Fragment);
    }
    assert_eq!(
        queue.push(event(4), Channel::Fragment),
        Some(Overflow::DropOldest)
    );
    let drained: Vec<u64> = from_fn(|| queue.pop()).map(|f| seq_of(&f)).collect();
    assert_eq!(
        drained,
        vec![2, 3, 4],
        "fragments are idempotent and the final value replaces wholesale; dropping the oldest is safe"
    );
}

#[test]
fn a_stateful_overflow_drops_the_oldest() {
    let mut queue = FrameQueue::new(3);
    for seq in 1..=3 {
        queue.push(event(seq), Channel::Stateful);
    }
    assert_eq!(
        queue.push(event(4), Channel::Stateful),
        Some(Overflow::DropOldest),
        "state keys are not in the event frame; merging by key is left to the side that holds the key"
    );
    let drained: Vec<u64> = from_fn(|| queue.pop()).map(|f| seq_of(&f)).collect();
    assert_eq!(
        drained,
        vec![2, 3, 4],
        "stateful frames drop the oldest as well"
    );
}

#[test]
fn a_journal_overflow_asks_for_a_disconnect() {
    let mut queue = FrameQueue::new(2);
    queue.push(event(1), Channel::Journal);
    queue.push(event(2), Channel::Journal);
    assert_eq!(
        queue.push(event(3), Channel::Journal),
        Some(Overflow::Disconnect),
        "the remaining events must not be dropped; prefer disconnecting so the client reconnects"
    );
    assert_eq!(
        queue.len(),
        2,
        "asking for a disconnect does not rewrite the queue: choosing frames is left to the unsubscribe step"
    );
}

#[test]
fn a_popped_queue_accepts_new_frames_again() {
    let mut queue = FrameQueue::new(2);
    queue.push(event(1), Channel::Journal);
    queue.push(event(2), Channel::Journal);
    assert!(queue.pop().is_some());
    assert_eq!(queue.push(event(3), Channel::Journal), None);
    assert_eq!(queue.len(), 2);
}

#[test]
fn an_empty_queue_pops_nothing() {
    let mut queue = FrameQueue::new(2);
    assert!(queue.is_empty());
    assert!(queue.pop().is_none());
}

// 握手与发布

/// 假的基线提供者。不论作用域，固定返回一个键
struct FakeStates {
    /// 状态键
    key: String,
    /// 该键的当前值
    value: serde_json::Value,
}

impl BaselineProvider for FakeStates {
    fn stateful(&self, _scope: &Scope) -> Vec<(String, serde_json::Value)> {
        vec![(self.key.clone(), self.value.clone())]
    }
}

/// 构造一个带临时库的空 hub
pub(crate) async fn hub() -> (TempDir, Arc<FeedHub>) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let hub = FeedHub::open(storage, None).await.expect("open hub");
    (dir, hub)
}

/// 构造一份事件定义，形态与是否写日志由调用方给出
fn def_of(name: &'static str, shape: Shape, log: bool) -> EventDef<serde_json::Value> {
    EventDef::new(name, shape, log, Mount::Subsystem, || serde_json::json!({}))
}

/// 构造一份完整信封，字段与派发器给出的形状一致
fn envelope_of(
    name: &'static str,
    scope: Scope,
    seq: u64,
    payload: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "seq": seq,
        "time": 1_760_000_000_000i64,
        "name": name,
        "scope": scope,
        "origin": "agent",
        "source": { "kind": "core", "subsystem": "test" },
        "payload": payload,
    })
}

/// 构造一条观察类事件，直接交给观察者，绕开派发器
pub(crate) async fn publish(
    hub: &FeedHub,
    name: &'static str,
    scope: Scope,
    seq: u64,
    payload: serde_json::Value,
) {
    let def = def_of(name, Shape::Observe, true);
    hub.on_event(&def.meta(), &envelope_of(name, scope, seq, payload))
        .await;
}

/// 一段只有正文的助手增量
fn text_block(content: &str) -> Message {
    Message {
        role: Role::Assistant,
        content: vec![Content::Text {
            data: content.into(),
        }],
        ..Message::default()
    }
}

/// 一次只要实时帧的订阅请求
fn listening(scope: Scope) -> Subscribe {
    Subscribe {
        scope,
        since_seq: None,
        until_seq: None,
        blocks: false,
    }
}

#[tokio::test]
async fn a_subscriber_gets_the_block_baseline_before_any_delta() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };

    // 先让一个块进入未提交状态
    hub.absorb(1, MessageId(2), &text_block("hello"));
    let opening = hub
        .subscribe(Subscribe {
            scope: scope.clone(),
            since_seq: None,
            until_seq: None,
            blocks: true,
        })
        .await
        .expect("subscribe");

    let mut subscriber = opening.subscriber;
    let first = subscriber
        .next()
        .await
        .expect("receive snapshot frame first");
    assert!(
        matches!(first, Frame::Snapshot { .. }),
        "the first frame is a snapshot, not an event"
    );

    publish(&hub, "core/block.delta", scope, 1, serde_json::json!({})).await;
    let second = subscriber.next().await.expect("receive event frame next");
    assert!(matches!(second, Frame::Event { .. }));
}

#[tokio::test]
async fn a_subscriber_of_one_topic_does_not_see_another_topic() {
    let (_dir, hub) = hub().await;
    let opening = hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: None,
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    publish(
        &hub,
        "core/block.delta",
        Scope::Topic { topic_id: 2 },
        1,
        serde_json::json!({}),
    )
    .await;
    publish(
        &hub,
        "core/block.delta",
        Scope::Topic { topic_id: 1 },
        2,
        serde_json::json!({}),
    )
    .await;

    let frame = subscriber.next().await.expect("receive event on topic");
    match frame {
        Frame::Event { envelope } => assert_eq!(envelope["seq"], serde_json::json!(2)),
        Frame::Snapshot { .. } => panic!("expected an event frame"),
    }
}

#[tokio::test]
async fn a_parked_subscriber_of_another_topic_is_woken_by_its_own_event() {
    // 唤醒信号是共享的，因此必须唤醒每一个正在等待的订阅方。若只唤醒最先等待的订阅方，
    // 信号会到达一个队列为空的订阅方，真正有帧的订阅方会一直等待下去
    let (_dir, hub) = hub().await;
    let mut idle = hub
        .subscribe(listening(Scope::Topic { topic_id: 1 }))
        .await
        .expect("subscribe")
        .subscriber;
    let mut waiting = hub
        .subscribe(listening(Scope::Topic { topic_id: 2 }))
        .await
        .expect("subscribe")
        .subscriber;

    // 让无关的订阅方先进入等待。按先进先出唤醒时，被唤醒的正是该订阅方
    let parked = tokio::spawn(async move { idle.next().await });
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let publisher = tokio::spawn({
        let hub = Arc::clone(&hub);
        async move {
            tokio::task::yield_now().await;
            publish(
                &hub,
                "core/block.delta",
                Scope::Topic { topic_id: 2 },
                1,
                serde_json::json!({}),
            )
            .await;
        }
    });

    let frame = timeout(Duration::from_secs(2), waiting.next())
        .await
        .expect("wake on own topic event")
        .expect("find frame in queue");
    match frame {
        Frame::Event { envelope } => assert_eq!(envelope["seq"], serde_json::json!(1)),
        Frame::Snapshot { .. } => panic!("expected an event frame"),
    }
    publisher.await.expect("run publisher");
    parked.abort();
}

#[tokio::test]
async fn the_pending_snapshot_comes_before_frames_that_have_already_queued() {
    // 回补帧与实时帧的先后由先清首帧再取队列保证，与两者的到达顺序无关
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    hub.absorb(1, MessageId(2), &text_block("first"));
    let mut subscriber = hub
        .subscribe(Subscribe {
            scope: scope.clone(),
            since_seq: None,
            until_seq: None,
            blocks: true,
        })
        .await
        .expect("subscribe")
        .subscriber;

    // 先发布，让实时帧已进入队列，再取首帧
    publish(&hub, "core/block.delta", scope, 1, serde_json::json!({})).await;

    let first = subscriber
        .next()
        .await
        .expect("receive snapshot frame first");
    assert!(
        matches!(first, Frame::Snapshot { .. }),
        "frames are already queued; the first frame must still be a snapshot"
    );
    let second = subscriber.next().await.expect("receive queued frame next");
    assert!(matches!(second, Frame::Event { .. }));
}

#[tokio::test]
async fn a_subscriber_without_a_start_point_gets_only_what_follows_the_handshake() {
    // 省略起始位置时不回补，握手之前产生的事件不得混进这个订阅方的队列
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    publish(
        &hub,
        "core/block.delta",
        scope.clone(),
        1,
        serde_json::json!({}),
    )
    .await;

    let opening = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    publish(&hub, "core/block.delta", scope, 2, serde_json::json!({})).await;
    match subscriber
        .next()
        .await
        .expect("receive event after handshake")
    {
        Frame::Event { envelope } => assert_eq!(
            envelope["seq"],
            serde_json::json!(2),
            "events from before the handshake must not be mixed in"
        ),
        Frame::Snapshot { .. } => panic!("expected an event frame"),
    }
}

#[tokio::test]
async fn a_block_baseline_is_taken_at_the_handshake_not_at_open() {
    // 基线是注册时刻的累加器状态。后并入的块对先建立的订阅不可见，
    // 对后建立的订阅必须可见
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };

    let before = hub
        .subscribe(Subscribe {
            scope: scope.clone(),
            since_seq: None,
            until_seq: None,
            blocks: true,
        })
        .await
        .expect("subscribe");
    hub.absorb(1, MessageId(2), &text_block("first"));

    let mut early = before.subscriber;
    match early.next().await.expect("receive block snapshot") {
        Frame::Snapshot {
            baseline: Baseline::Blocks { blocks },
        } => assert!(
            blocks.is_empty(),
            "there is no in-flight block at handshake time"
        ),
        other => panic!("expected block snapshot, got {other:?}"),
    }

    let after = hub
        .subscribe(Subscribe {
            scope,
            since_seq: None,
            until_seq: None,
            blocks: true,
        })
        .await
        .expect("subscribe");
    let mut late = after.subscriber;
    match late.next().await.expect("receive block snapshot") {
        Frame::Snapshot {
            baseline: Baseline::Blocks { blocks },
        } => assert_eq!(
            blocks.len(),
            1,
            "the in-flight block must appear in the snapshot at handshake time"
        ),
        other => panic!("expected block snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn a_blocked_subscriber_is_woken_by_the_next_frame() {
    let (_dir, hub) = hub().await;
    let mut subscriber = hub
        .subscribe(listening(Scope::Topic { topic_id: 1 }))
        .await
        .expect("subscribe")
        .subscriber;

    let publisher = tokio::spawn({
        let hub = Arc::clone(&hub);
        async move {
            tokio::task::yield_now().await;
            publish(
                &hub,
                "core/block.delta",
                Scope::Topic { topic_id: 1 },
                5,
                serde_json::json!({}),
            )
            .await;
        }
    });

    let frame = subscriber.next().await.expect("receive frame after wake");
    assert!(matches!(frame, Frame::Event { .. }));
    publisher.await.expect("run publisher");
}

#[tokio::test]
async fn an_intercept_event_never_reaches_the_frame_stream() {
    // 拦截类事件不送前端。写日志与送前端由两个标志分别控制，
    // `core/test.intercept` 这类拦截事件不得进入帧流
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    let mut subscriber = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe")
        .subscriber;

    let def = def_of("core/test.intercept", Shape::Intercept, true);
    hub.on_event(
        &def.meta(),
        &envelope_of(
            "core/test.intercept",
            scope.clone(),
            1,
            serde_json::json!({}),
        ),
    )
    .await;

    publish(
        &hub,
        "core/block.committed",
        scope,
        2,
        serde_json::json!({}),
    )
    .await;
    match subscriber.next().await.expect("receive observe event") {
        Frame::Event { envelope } => assert_eq!(
            envelope["seq"],
            serde_json::json!(2),
            "intercept events must not appear before it"
        ),
        Frame::Snapshot { .. } => panic!("expected an event frame"),
    }
}

#[tokio::test]
async fn a_draft_event_reaches_the_frame_stream_even_though_it_is_never_logged() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    let mut subscriber = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe")
        .subscriber;

    let def = def_of("core/block.delta", Shape::Observe, false);
    hub.on_event(
        &def.meta(),
        &envelope_of("core/block.delta", scope.clone(), 1, serde_json::json!({})),
    )
    .await;
    publish(
        &hub,
        "core/block.committed",
        scope,
        2,
        serde_json::json!({}),
    )
    .await;

    match subscriber.next().await.expect("receive draft fragment") {
        Frame::Event { envelope } => assert_eq!(
            envelope["seq"],
            serde_json::json!(1),
            "drafts are not logged but must be delivered to the frontend, and must precede the next observe event"
        ),
        Frame::Snapshot { .. } => panic!("expected an event frame"),
    }
}

#[test]
fn only_the_draft_event_is_droppable() {
    assert_eq!(
        channel_of("core/block.delta"),
        Channel::Fragment,
        "drafts are idempotent and the final value replaces wholesale; dropping the oldest is safe"
    );
    assert_eq!(
        channel_of("core/block.committed"),
        Channel::Journal,
        "not a single committed block may be dropped"
    );
}

#[tokio::test]
async fn the_high_water_never_goes_backwards_across_unsubscribes() {
    let (_dir, hub) = hub().await;
    assert_eq!(hub.high_water(), EventSeq(0));
    let opening = hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: None,
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    // 订阅号用于退订。退订之后该订阅方不再收帧
    hub.unsubscribe(opening.id);
    publish(
        &hub,
        "core/block.delta",
        Scope::Topic { topic_id: 1 },
        7,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        hub.high_water(),
        EventSeq(7),
        "unsubscribing does not reclaim the high water mark"
    );
    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "no further frames enter this queue after unsubscribing"
    );
}

#[tokio::test]
async fn a_stateful_baseline_comes_from_the_provider_inside_the_same_lock() {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let hub = FeedHub::open(
        Storage::new(db),
        Some(Arc::new(FakeStates {
            key: "items".into(),
            value: serde_json::json!([1, 2]),
        })),
    )
    .await
    .expect("open hub");

    let opening = hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: None,
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    let frame = subscriber.next().await.expect("receive state baseline");
    match frame {
        Frame::Snapshot {
            baseline: Baseline::Stateful { key, value },
        } => {
            assert_eq!(key, "items");
            assert_eq!(value, serde_json::json!([1, 2]));
        }
        other => panic!("expected state baseline, got {other:?}"),
    }
}

#[tokio::test]
async fn absorbing_a_second_message_does_not_disturb_the_first() {
    let (_dir, hub) = hub().await;
    hub.absorb(1, MessageId(2), &text_block("first"));
    hub.absorb(1, MessageId(3), &text_block("second"));
    let blocks = hub.snapshot_blocks(&Scope::Topic { topic_id: 1 });
    assert_eq!(
        blocks.len(),
        2,
        "the two messages each have their own block and do not interfere"
    );
}

#[tokio::test]
async fn a_block_baseline_of_one_topic_excludes_another_topics_blocks() {
    let (_dir, hub) = hub().await;
    hub.absorb(1, MessageId(2), &text_block("first"));
    hub.absorb(2, MessageId(3), &text_block("second"));

    let one = hub.snapshot_blocks(&Scope::Topic { topic_id: 1 });
    assert_eq!(
        one.len(),
        1,
        "snapshots are per topic; in-flight blocks of another topic do not come in"
    );
    assert_eq!(one[0].of_message, MessageId(2));
    assert!(
        hub.snapshot_blocks(&Scope::Global).is_empty(),
        "the global scope has no blocks"
    );
}

#[tokio::test]
async fn two_topics_never_hand_out_the_same_block_id() {
    // 块标识全局唯一，因此累加器只有一个。为每个话题各建一个累加器时，
    // 两个话题会从同一个种子编号，标识立即相撞
    let (_dir, hub) = hub().await;
    let first = hub.absorb(1, MessageId(2), &text_block("first"));
    let second = hub.absorb(2, MessageId(3), &text_block("second"));
    let id_of = |changes: &[BlockChange]| match changes.first().expect("get block change") {
        BlockChange::Opened(block) => block.id,
        other => panic!("expected block open, got {other:?}"),
    };
    assert_ne!(
        id_of(&first),
        id_of(&second),
        "block ids across topics must not repeat"
    );
}

// 握手临界区的验收

/// 取下一帧，等待超过上限即判失败
///
/// 等待上限把缺失的帧转成断言失败，避免测试一直等待
async fn next_frame(subscriber: &mut Subscriber) -> Frame {
    timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("a frame arrives")
        .expect("a frame arrives")
}

/// 在短窗口内取一帧，用于断言此刻不该有帧
async fn frame_within(subscriber: &mut Subscriber) -> Option<Frame> {
    timeout(Duration::from_millis(200), subscriber.next())
        .await
        .ok()
        .flatten()
}

/// 与握手并发产生的事件既不缺也不重
///
/// 取水位与注册在同一次加锁里完成，因此并发到达的事件只有两种去向。序号不大于水位时由回补给出，
/// 大于水位时进入实时队列。两种去向都不缺帧，也不会让同一事件出现两次
#[tokio::test]
async fn no_event_can_fall_between_the_baseline_and_the_registration() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    publish(
        &hub,
        "core/message.seal",
        scope.clone(),
        1,
        serde_json::json!({}),
    )
    .await;

    // 与建立订阅争用同一次加锁的事件
    let publisher = tokio::spawn({
        let hub = Arc::clone(&hub);
        let scope = scope.clone();
        async move {
            publish(&hub, "core/message.seal", scope, 2, serde_json::json!({})).await;
        }
    });
    let opening = hub
        .subscribe(Subscribe {
            scope: scope.clone(),
            since_seq: Some(EventSeq(0)),
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe");
    publisher.await.expect("run publisher");

    let mut subscriber = opening.subscriber;
    let seen = vec![
        seq_of(&next_frame(&mut subscriber).await),
        seq_of(&next_frame(&mut subscriber).await),
    ];
    assert_eq!(
        seen,
        vec![1, 2],
        "both the one below the high water mark and the one above it must arrive"
    );
    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "backfill and live must not deliver the same event twice"
    );
}

/// 注册可能发生在两条乱序通知之间，回补与实时因此不保证序号递增
///
/// 序号在观察者被 await 之前就已分配。并发派发下序号 10 可能早于 9 到达观察者。
/// 订阅若在该窗口内注册，先到达的事件由回补给出，后到达的事件由实时队列给出，客户端会先看到序号大的帧，
/// 再看到序号小的帧。水位规则正是为这种情形而设，即丢弃序号不大于已应用序号的帧
#[tokio::test]
async fn a_subscription_registering_between_out_of_order_notifications_gets_both() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    // 序号 10 先到达观察者
    publish(
        &hub,
        "core/message.seal",
        scope.clone(),
        10,
        serde_json::json!({}),
    )
    .await;

    let opening = hub
        .subscribe(Subscribe {
            scope: scope.clone(),
            since_seq: Some(EventSeq(0)),
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    // 序号 9 在注册之后才到达观察者
    publish(&hub, "core/message.seal", scope, 9, serde_json::json!({})).await;

    let backfilled = next_frame(&mut subscriber).await;
    let live = next_frame(&mut subscriber).await;
    assert_eq!(
        seq_of(&backfilled),
        10,
        "backfill delivers the one that arrived before registration"
    );
    assert_eq!(
        seq_of(&live),
        9,
        "live delivers the one that arrived after registration"
    );
    assert!(
        seq_of(&live) < seq_of(&backfilled),
        "the seq of a live frame may be smaller than that of a backfilled frame already delivered"
    );

    // 客户端按水位应用。先应用序号 10，再遇到序号 9 时整帧丢弃
    let mut watermark = 0;
    let mut applied = |frame: &Frame| {
        let seq = seq_of(frame);
        if seq <= watermark {
            false
        } else {
            watermark = seq;
            true
        }
    };
    assert!(applied(&backfilled), "the backfilled one is applied");
    assert!(
        !applied(&live),
        "the high water mark is already 10; the live frame with seq 9 must be dropped"
    );
}

/// 回补帧与实时帧的序号可比较，回补帧在前
#[tokio::test]
async fn a_backfilled_frame_and_a_live_frame_are_orderable_by_seq() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    publish(
        &hub,
        "core/message.seal",
        scope.clone(),
        5,
        serde_json::json!({}),
    )
    .await;

    let opening = hub
        .subscribe(Subscribe {
            scope: scope.clone(),
            since_seq: Some(EventSeq(0)),
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    let backfilled = next_frame(&mut subscriber).await;
    publish(&hub, "core/message.seal", scope, 9, serde_json::json!({})).await;
    let live = next_frame(&mut subscriber).await;

    assert_eq!(
        seq_of(&backfilled),
        5,
        "backfill delivers the one from before the handshake"
    );
    assert_eq!(
        seq_of(&live),
        9,
        "live delivers the one from after the handshake"
    );
    assert!(
        seq_of(&backfilled) < seq_of(&live),
        "the seqs of the two frames are comparable, with backfill before live"
    );
}

// 分片可丢、重复被挡的验收

/// 一次增长变化的分片载荷，形状与分片帧的序列化结果一致
fn delta_payload(id: BlockId, revision: u64, fragment: &str) -> serde_json::Value {
    serde_json::to_value(BlockDelta {
        block_id: id,
        revision,
        fragment: fragment.to_string(),
    })
    .expect("serialize payload")
}

/// 一个已提交块的载荷，形状与提交帧的序列化结果一致
fn committed_payload(block: &Block) -> serde_json::Value {
    serde_json::to_value(BlockCommitted {
        block: BlockCommit {
            block_id: block.id,
            kind: block.kind,
            call_id: block.call_id.clone(),
            content: block.content.clone(),
            data: block.data.clone(),
        },
        revision: block.revision,
    })
    .expect("serialize payload")
}

/// 客户端侧持有的块状态
///
/// 合并规则中与本例相关的有两条：分片按修订号并入，事实整体替换。
struct ClientBlock {
    /// 块标识
    id: BlockId,
    /// 已并入的内容
    content: String,
    /// 已持有的修订号
    revision: u64,
}

impl ClientBlock {
    /// 从零起持有一个块
    fn empty(id: BlockId) -> Self {
        Self {
            id,
            content: String::new(),
            revision: 0,
        }
    }

    /// 取一条块事件帧的载荷
    fn payload(frame: &Frame) -> serde_json::Value {
        match frame {
            Frame::Event { envelope } => envelope["payload"].clone(),
            Frame::Snapshot { .. } => panic!("expected event frame, got snapshot"),
        }
    }

    /// 并入一片，返回本次是否被采纳
    ///
    /// 帧的修订号不大于已持有的值即整帧忽略
    fn apply_delta(&mut self, frame: &Frame) -> bool {
        let payload = Self::payload(frame);
        assert_eq!(
            payload["block_id"],
            serde_json::json!(self.id.0.to_string()),
            "a fragment must belong to the held block"
        );
        let revision = payload["revision"]
            .as_u64()
            .expect("read revision from fragment frame");
        if revision <= self.revision {
            return false;
        }
        self.content
            .push_str(payload["fragment"].as_str().expect("read fragment text"));
        self.revision = revision;
        true
    }

    /// 事实到达时整体替换内容与修订号
    fn apply_committed(&mut self, frame: &Frame) {
        let payload = Self::payload(frame);
        self.content = payload["content"]
            .as_str()
            .expect("read block content from fact frame")
            .to_string();
        self.revision = payload["revision"]
            .as_u64()
            .expect("read revision from fact frame");
    }
}

/// 依次处理两片分片与一次事实，返回客户端在事实之前与之后持有的内容
///
/// `give_fragments` 为假时两片分片都不送给订阅方，即全被丢掉
async fn merge_a_round(give_fragments: bool) -> (String, String) {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    let mut subscriber = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe")
        .subscriber;

    let mut opened = None;
    let mut deltas = Vec::new();
    for (seq, fragment) in ["he", "llo"].into_iter().enumerate() {
        for change in hub.absorb(1, MessageId(2), &text_block(fragment)) {
            match change {
                BlockChange::Opened(block) => opened = Some(block.id),
                BlockChange::Grew {
                    id,
                    revision,
                    fragment,
                } => deltas.push((seq as u64 + 1, delta_payload(id, revision, &fragment))),
            }
        }
    }
    let block_id = opened.expect("open block with first fragment");
    let mut client = ClientBlock::empty(block_id);

    if give_fragments {
        for (seq, payload) in &deltas {
            publish(
                &hub,
                "core/block.delta",
                scope.clone(),
                *seq,
                payload.clone(),
            )
            .await;
        }
        for _ in &deltas {
            let frame = next_frame(&mut subscriber).await;
            assert!(
                client.apply_delta(&frame),
                "every fragment sent out must be accepted"
            );
        }
    }
    let draft = client.content.clone();

    // 事实的内容取自拦截链给出的最终值，与拼出来的草稿不同
    let block = hub
        .commit(block_id, "hello world".into(), None)
        .expect("find block in accumulator");
    publish(
        &hub,
        "core/block.committed",
        scope,
        deltas.len() as u64 + 1,
        committed_payload(&block),
    )
    .await;
    client.apply_committed(&next_frame(&mut subscriber).await);
    (draft, client.content)
}

/// 丢掉分片不改变最终一致
///
/// 分片经队列送出，事实经事件送出。丢掉全部分片的客户端在事实之前内容为空，
/// 事实到达时整体替换，最终持有的内容与一片不丢时完全相同
#[tokio::test]
async fn a_dropped_fragment_leaves_the_committed_value_unchanged() {
    let (draft_without, after_without) = merge_a_round(false).await;
    let (draft_with, after_with) = merge_a_round(true).await;

    assert_eq!(
        draft_without, "",
        "a client that received no fragments can only be empty before the fact"
    );
    assert_eq!(
        draft_with, "hello",
        "when all fragments arrive, the value before the fact is the assembled draft"
    );
    assert_eq!(
        after_without, after_with,
        "dropping all fragments does not change eventual consistency"
    );
    assert_eq!(
        after_with, "hello world",
        "the final value comes from the fact, not the assembled draft"
    );
}

/// 重发一片由修订号挡下
///
/// 服务端给出的修订号是增长之后的值，重发的分片携带旧值，因此第二次收到
/// 时整帧忽略，内容不会翻倍
#[tokio::test]
async fn a_redelivered_fragment_is_rejected_by_its_revision() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    let mut subscriber = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe")
        .subscriber;

    // 两片分片的修订号来自累加器，不由本例构造
    let mut opened = None;
    for (seq, fragment) in ["he", "llo"].into_iter().enumerate() {
        for change in hub.absorb(1, MessageId(2), &text_block(fragment)) {
            match change {
                BlockChange::Opened(block) => opened = Some(block.id),
                BlockChange::Grew {
                    id,
                    revision,
                    fragment,
                } => {
                    publish(
                        &hub,
                        "core/block.delta",
                        scope.clone(),
                        seq as u64 + 1,
                        delta_payload(id, revision, &fragment),
                    )
                    .await
                }
            }
        }
    }
    let mut client = ClientBlock::empty(opened.expect("open block with first fragment"));

    let first = next_frame(&mut subscriber).await;
    let second = next_frame(&mut subscriber).await;
    assert!(
        client.apply_delta(&first),
        "the first fragment must be accepted"
    );
    assert!(
        client.apply_delta(&second),
        "the second fragment has a larger revision"
    );
    assert_eq!(client.content, "hello");

    assert!(
        !client.apply_delta(&first),
        "resending the same fragment does not increase the revision and must be blocked"
    );
    assert_eq!(
        client.content, "hello",
        "content is unchanged after being blocked"
    );
}

/// 服务端从不发回退的修订号
///
/// 客户端依赖修订号挡下重复分片与过期快照。前提是每并入一段修订号严格加一，且快照中
/// 的修订号不低于任何已发出的分片
#[tokio::test]
async fn the_revisions_the_server_sends_never_go_backwards() {
    let (_dir, hub) = hub().await;
    let mut seen: Vec<u64> = Vec::new();
    for fragment in ["he", "llo", "!"] {
        for change in hub.absorb(1, MessageId(2), &text_block(fragment)) {
            if let BlockChange::Grew { revision, .. } = change {
                seen.push(revision);
            }
        }
    }
    assert_eq!(
        seen,
        vec![1, 2, 3],
        "each appended fragment strictly increases the revision by one"
    );
    assert!(
        hub.snapshot_blocks(&Scope::Topic { topic_id: 1 })[0].revision >= seen[2],
        "the snapshot is not older than any emitted fragment"
    );
}

// 契约中帧种类的验收

/// 契约列出的每一种帧都能序列化往返
///
/// 契约中只有快照帧与事件帧两类。快照的基线又分块基线与状态基线两种。回补帧不是
/// 第三种帧，它就是事件帧。契约中另有一个心跳示例，该示例明确不是本协议的一帧
#[test]
fn every_frame_kind_the_contract_names_round_trips() {
    for frame in [
        Frame::Snapshot {
            baseline: Baseline::Blocks {
                blocks: vec![block(7, "hello")],
            },
        },
        Frame::Snapshot {
            baseline: Baseline::Stateful {
                key: "items".into(),
                value: serde_json::json!([1, 2]),
            },
        },
        Frame::Event {
            envelope: serde_json::json!({ "seq": 41, "name": "core/block.open" }),
        },
    ] {
        let value = serde_json::to_value(&frame).expect("serialize");
        let kind = value["kind"].as_str().expect("read frame kind");
        assert!(
            kind == "snapshot" || kind == "event",
            "the outermost discriminant has only the two values the contract gives, got {kind}"
        );
        let back: Frame = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back, frame);
    }
}

// 宿主自建事件的派发

/// mod 产生的事件名是运行期数据，因此使用 `publish` 而不是派发器
#[tokio::test]
async fn a_host_built_event_reaches_a_subscriber() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    let opening = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    let envelope = envelope_of("m_demo/notify", scope, 9, serde_json::json!({ "n": 1 }));
    hub.publish("m_demo/notify", Channel::Journal, true, envelope.clone());

    let frame = next_frame(&mut subscriber).await;
    assert_eq!(frame, Frame::Event { envelope });
}

#[tokio::test]
async fn a_host_built_event_lands_in_the_pending_log() {
    let (_dir, hub) = hub().await;
    hub.publish(
        "m_demo/notify",
        Channel::Journal,
        true,
        envelope_of("m_demo/notify", Scope::Global, 3, serde_json::json!({})),
    );
    assert_eq!(hub.take_pending().len(), 1);
}

/// 派发类别由调用方给出，因此调用方可以声明一帧是终值
///
/// 状态类的帧溢出时丢最旧，队列满之后订阅方仍在册
#[tokio::test]
async fn a_host_built_event_of_the_stateful_channel_is_merged_in_the_queue() {
    let (_dir, hub) = hub().await;
    let scope = Scope::Topic { topic_id: 1 };
    let mut subscriber = hub
        .subscribe(listening(scope.clone()))
        .await
        .expect("subscribe")
        .subscriber;

    // 队列上限为 256。先把队列填满，再多派发一帧
    for seq in 1..=257 {
        hub.publish(
            "m_demo/notify",
            Channel::Stateful,
            true,
            envelope_of("m_demo/notify", scope.clone(), seq, serde_json::json!({})),
        );
    }

    let frame = next_frame(&mut subscriber).await;
    assert_eq!(
        seq_of(&frame),
        2,
        "stateful overflow drops the oldest; the remaining queue starts from the second frame"
    );
}
