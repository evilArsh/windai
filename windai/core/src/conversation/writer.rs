use super::assembler::ProjectionInvalidations;
use super::events::{
    CORE_BLOCK_COMMITTED, CORE_BLOCK_DELTA, CORE_BLOCK_OPEN, CORE_MESSAGE_SEAL, MessageSeal, Usage,
};
use crate::error::{CoreError, Result};
use crate::feed::hub::FeedHub;
use crate::models::BlockCreate;
use crate::storage::Storage;
use serde::Serialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use wind_event::context::DispatchCx;
use wind_event::def::EventDef;
use wind_event::dispatch::Dispatcher;
use wind_stream::accumulator::BlockChange;
use wind_stream::block::Block;
use wind_stream::id::{BlockId, MessageId, TurnId};
use wind_stream::payload::{BlockCommit, BlockCommitted, BlockDelta, BlockOpened};
use wind_wire::message::Message;

pub struct BlockWriter {
    hub: Arc<FeedHub>,
    bus: Arc<Dispatcher>,
    storage: Storage,
    invalidations: ProjectionInvalidations,
}

impl BlockWriter {
    pub fn new(
        hub: Arc<FeedHub>,
        bus: Arc<Dispatcher>,
        storage: Storage,
        invalidations: ProjectionInvalidations,
    ) -> Self {
        Self {
            hub,
            bus,
            storage,
            invalidations,
        }
    }

    pub(crate) fn invalidations(&self) -> ProjectionInvalidations {
        self.invalidations.clone()
    }

    pub(crate) fn allocate_block_id(&self) -> BlockId {
        self.hub.allocate_block_id()
    }

    pub async fn absorb(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        of_message: MessageId,
        partial: &Message,
    ) -> Vec<BlockId> {
        let changes = self.hub.absorb(topic_id, of_message, partial);
        let mut touched = BTreeSet::new();
        for change in changes {
            match change {
                BlockChange::Opened(block) => {
                    touched.insert(block.id);
                    self.notify(
                        &CORE_BLOCK_OPEN,
                        cx,
                        BlockOpened {
                            block_id: block.id,
                            of_message: block.of_message,
                            role: block.role,
                            kind: block.kind,
                            call_id: block.call_id,
                        },
                    )
                    .await;
                }
                BlockChange::Grew {
                    id,
                    revision,
                    fragment,
                } => {
                    touched.insert(id);
                    self.notify(
                        &CORE_BLOCK_DELTA,
                        cx,
                        BlockDelta {
                            block_id: id,
                            revision,
                            fragment,
                        },
                    )
                    .await;
                }
            }
        }
        touched.into_iter().collect()
    }

    pub async fn commit(
        &self,
        cx: &DispatchCx,
        _topic_id: i64,
        id: BlockId,
    ) -> Result<Option<Block>> {
        let Some(block) = self.settle(id) else {
            return Ok(None);
        };
        self.persist(&self.storage, &block).await?;
        self.hub
            .commit(id, block.content.clone(), block.data.clone());
        self.announce(cx, &block).await;
        Ok(Some(block))
    }

    /// 定稿一份块内容的副本，累加器不变
    pub(crate) fn settle(&self, id: BlockId) -> Option<Block> {
        let candidate = self.hub.candidate(id)?;
        self.hub.settled(id, candidate.content, candidate.data)
    }

    /// 把已落库的块移出累加器
    pub(crate) fn commit_memory(&self, block: &Block) {
        self.hub
            .commit(block.id, block.content.clone(), block.data.clone());
    }

    /// 把块写进给定的存储
    pub(crate) async fn persist(&self, storage: &Storage, block: &Block) -> Result<()> {
        storage.block_append(block_create(block)?).await
    }

    /// 块落库后的对外可见动作，须在提交之后调用
    pub(crate) async fn announce(&self, cx: &DispatchCx, block: &Block) {
        self.invalidate_of(block.of_message).await;
        self.notify(
            &CORE_BLOCK_COMMITTED,
            cx,
            BlockCommitted {
                block: BlockCommit {
                    block_id: block.id,
                    kind: block.kind,
                    call_id: block.call_id.clone(),
                    content: block.content.clone(),
                    data: block.data.clone(),
                },
                revision: block.revision,
            },
        )
        .await;
    }

    pub(crate) async fn announce_atomic(&self, cx: &DispatchCx, blocks: &[Block]) {
        for block in blocks {
            self.invalidate_of(block.of_message).await;
        }
        let mut opened = Vec::with_capacity(blocks.len());
        for block in blocks {
            opened.push(BlockOpened {
                block_id: block.id,
                of_message: block.of_message,
                role: block.role,
                kind: block.kind,
                call_id: block.call_id.clone(),
            });
        }
        self.bus.notify_all(&CORE_BLOCK_OPEN, cx, opened).await;
        let mut committed = Vec::with_capacity(blocks.len());
        for block in blocks {
            committed.push(BlockCommitted {
                block: BlockCommit {
                    block_id: block.id,
                    kind: block.kind,
                    call_id: block.call_id.clone(),
                    content: block.content.clone(),
                    data: block.data.clone(),
                },
                revision: block.revision,
            });
        }
        self.bus
            .notify_all(&CORE_BLOCK_COMMITTED, cx, committed)
            .await;
    }

    async fn invalidate_of(&self, message_id: MessageId) {
        if !self.invalidations.watched() {
            return;
        }
        match self.storage.message_turn(message_id.0).await {
            Ok(Some(turn_id)) => self.invalidations.invalidate(TurnId(turn_id)),
            Ok(None) => {
                log::error!(
                    "[conversation] message {} resolves to no turn, dropping the projection cache",
                    message_id.0
                );
                self.invalidations.invalidate_all();
            }
            Err(error) => {
                log::error!(
                    "[conversation] invalidating the projection for message {} failed: {error}",
                    message_id.0
                );
                self.invalidations.invalidate_all();
            }
        }
    }

    pub async fn seal(
        &self,
        cx: &DispatchCx,
        message_id: MessageId,
        finish_reason: String,
        usage: Usage,
    ) -> Result<()> {
        self.storage
            .message_seal(
                message_id.0,
                &finish_reason,
                usage.input_tokens,
                usage.output_tokens,
            )
            .await?;
        self.invalidate_of(message_id).await;
        self.notify(
            &CORE_MESSAGE_SEAL,
            cx,
            MessageSeal {
                message_id,
                finish_reason,
                usage,
            },
        )
        .await;
        Ok(())
    }

    async fn notify<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        self.bus.notify(def, cx, payload).await;
    }
}

pub(crate) fn block_create(block: &Block) -> Result<BlockCreate> {
    Ok(BlockCreate {
        id: storage_id(block.id)?,
        message_id: block.of_message.0,
        kind: block.kind,
        call_id: block.call_id.clone(),
        revision: block.revision as i64,
        content: block.content.clone(),
        data: block.data.clone(),
        finished: block.finished,
    })
}

fn storage_id(id: BlockId) -> Result<i64> {
    i64::try_from(id.0).map_err(|_| CoreError::MalformedColumn {
        column: "block.id".into(),
        detail: "Block id exceeds i64 range".into(),
    })
}
