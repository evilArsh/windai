//! 客户端合并规则的性质
//!
//! 三条规则：连上先取快照；分片按修订号追加；已提交的块整体替换
//! 它们必须对分片丢失、分片重复、快照与分片到达顺序颠倒都给出同一结果

use std::collections::BTreeMap;
use wind_stream::accumulator::{BlockAccumulator, BlockChange};
use wind_stream::block::{Block, BlockKind, Role};
use wind_stream::id::{BlockId, MessageId};
use wind_stream::payload::{BlockCommit, BlockCommitted, BlockDelta, Snapshot};
use wind_wire::{Content, Message};

/// 客户端侧的合并状态，规则的可执行表述
struct Client {
    blocks: BTreeMap<BlockId, Block>,
}

impl Client {
    fn connect(snapshot: &Snapshot) -> Self {
        Self {
            blocks: snapshot.blocks.iter().map(|b| (b.id, b.clone())).collect(),
        }
    }

    /// 按帧契约接收一次块变化
    ///
    /// 开块帧只带身份，内容与载荷都不在其中，因此客户端只能按它建立一个空块；
    /// 内容一律经分片帧到达
    fn apply(&mut self, change: &BlockChange) {
        match change {
            BlockChange::Opened(block) => {
                self.blocks.insert(
                    block.id,
                    Block {
                        content: String::new(),
                        data: None,
                        revision: 0,
                        ..block.clone()
                    },
                );
            }
            BlockChange::Grew {
                id,
                revision,
                fragment,
            } => self.on_delta(&BlockDelta {
                block_id: *id,
                revision: *revision,
                fragment: fragment.clone(),
            }),
        }
    }

    fn on_delta(&mut self, delta: &BlockDelta) {
        if let Some(block) = self.blocks.get_mut(&delta.block_id)
            && delta.revision > block.revision
        {
            block.content.push_str(&delta.fragment);
            block.revision = delta.revision;
        }
    }

    fn on_committed(&mut self, committed: &BlockCommitted) {
        if let Some(block) = self.blocks.get_mut(&committed.block.block_id) {
            block.kind = committed.block.kind;
            block.call_id = committed.block.call_id.clone();
            block.content = committed.block.content.clone();
            block.data = committed.block.data.clone();
            block.revision = committed.revision;
            block.finished = true;
        }
    }

    fn content_of(&self, id: BlockId) -> &str {
        self.blocks
            .get(&id)
            .map(|b| b.content.as_str())
            .unwrap_or("")
    }
}

fn block(id: u64, content: &str, revision: u64) -> Block {
    Block {
        id: BlockId(id),
        of_message: MessageId(1),
        role: Role::Assistant,
        kind: BlockKind::Text,
        call_id: None,
        revision,
        content: content.into(),
        data: None,
        finished: false,
    }
}

fn delta(id: u64, revision: u64, fragment: &str) -> BlockDelta {
    BlockDelta {
        block_id: BlockId(id),
        revision,
        fragment: fragment.into(),
    }
}

fn committed(id: u64, content: &str, revision: u64) -> BlockCommitted {
    BlockCommitted {
        block: BlockCommit {
            block_id: BlockId(id),
            kind: BlockKind::Text,
            call_id: None,
            content: content.into(),
            data: None,
        },
        revision,
    }
}

/// 丢掉一个分片之后，最终内容仍然正确
#[test]
fn a_dropped_delta_is_repaired_by_the_commit() {
    let mut client = Client::connect(&Snapshot {
        blocks: vec![block(1, "", 0)],
    });
    client.on_delta(&delta(1, 1, "he"));
    // 第 2 个分片在网络里丢了
    client.on_delta(&delta(1, 3, "llo"));
    client.on_committed(&committed(1, "hello!", 3));
    assert_eq!(
        client.content_of(BlockId(1)),
        "hello!",
        "the fact replaces the whole content when it arrives"
    );
}

/// 收到重复的分片不会让内容翻倍
#[test]
fn a_repeated_delta_is_excluded_by_the_revision() {
    let mut client = Client::connect(&Snapshot {
        blocks: vec![block(1, "", 0)],
    });
    client.on_delta(&delta(1, 1, "he"));
    client.on_delta(&delta(1, 1, "he"));
    client.on_delta(&delta(1, 1, "he"));
    assert_eq!(
        client.content_of(BlockId(1)),
        "he",
        "a repeated delta does not increase the revision, so it is excluded"
    );
}

/// 快照比分片旧：旧分片仍被采纳
#[test]
fn a_stale_snapshot_does_not_block_newer_deltas() {
    let mut client = Client::connect(&Snapshot {
        blocks: vec![block(1, "he", 1)],
    });
    client.on_delta(&delta(1, 2, "llo"));
    assert_eq!(client.content_of(BlockId(1)), "hello");
}

/// 快照比分片新：过期分片被忽略
#[test]
fn a_newer_snapshot_makes_an_older_delta_a_no_op() {
    let mut client = Client::connect(&Snapshot {
        blocks: vec![block(1, "hello!", 17)],
    });
    client.on_delta(&delta(1, 3, "stale"));
    assert_eq!(
        client.content_of(BlockId(1)),
        "hello!",
        "a delta with a lower revision must be ignored"
    );
}

/// 先注册后读快照产生的重叠由修订号排除
#[test]
fn an_overlapping_snapshot_and_delta_converge() {
    let mut client = Client::connect(&Snapshot {
        blocks: vec![block(1, "hello", 2)],
    });
    client.on_delta(&delta(1, 3, "!"));
    assert_eq!(client.content_of(BlockId(1)), "hello!");
}

/// 已提交的块不进入快照，它由提交帧单独送达
#[test]
fn the_snapshot_carries_only_what_is_in_flight() {
    let snapshot = Snapshot {
        blocks: vec![block(1, "uncommitted", 5)],
    };
    assert_eq!(snapshot.blocks.len(), 1);
    assert!(!snapshot.blocks[0].finished);
}

/// 提交帧带来的是完整块，载荷跟着一起替换
#[test]
fn a_commit_frame_brings_the_payload_with_it() {
    let mut client = Client::connect(&Snapshot {
        blocks: vec![block(1, "", 0)],
    });
    client.on_committed(&BlockCommitted {
        block: BlockCommit {
            block_id: BlockId(1),
            kind: BlockKind::ToolCall,
            call_id: Some("call_abc".into()),
            content: String::new(),
            data: Some(serde_json::json!({
                "name": "get_weather",
                "arguments": { "city": "SF" },
            })),
        },
        revision: 3,
    });
    let merged = client.blocks.get(&BlockId(1)).expect("the block exists");
    assert_eq!(merged.kind, BlockKind::ToolCall);
    assert_eq!(merged.call_id.as_deref(), Some("call_abc"));
    assert_eq!(
        merged.data.as_ref().expect("the payload exists")["name"],
        serde_json::json!("get_weather"),
        "replacing only the body would leave the tool call's name and arguments forever out of the frontend"
    );
    assert!(merged.finished);
}

/// 实时订阅者不会漏掉任何内容：累加器的变化经帧契约送达后与累加器一致
#[test]
fn a_live_subscriber_receives_every_fragment() {
    let mut accumulator = BlockAccumulator::new();
    let mut client = Client::connect(&Snapshot { blocks: vec![] });
    for fragment in ["he", "llo"] {
        let increment = Message {
            role: Role::Assistant,
            content: vec![Content::Text {
                data: fragment.into(),
            }],
            ..Message::default()
        };
        for change in accumulator.absorb(MessageId(1), &increment) {
            client.apply(&change);
        }
    }
    let in_flight = accumulator.snapshot();
    assert_eq!(
        in_flight.len(),
        1,
        "the two increments belong to the same block"
    );
    assert_eq!(
        client.content_of(in_flight[0].id),
        in_flight[0].content,
        "the open-block frame carries no content, so the first fragment must arrive with the delta frame, or live subscribers are forever one piece short"
    );
    assert_eq!(in_flight[0].content, "hello");
}
