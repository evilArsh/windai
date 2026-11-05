use super::events::{
    BlockCommitted, BlockDelta, CORE_BLOCK_COMMITTED, CORE_BLOCK_DELTA, CORE_MESSAGE_INTERRUPTED,
    CORE_MESSAGE_SEAL, InterruptedReason, MessageInterrupted, MessageSeal,
};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;
use wind_event::{DispatchCx, Dispatcher, EventDef, GroupKey, Retention};
use wind_storage::Storage;
use wind_storage::error::StorageError;
use wind_wire::{Block, Usage};

pub struct MessageStream {
    message_id: i64,
    host: Arc<Dispatcher>,
    cancel: CancellationToken,
    cx: DispatchCx,
    terminal: AtomicBool,
}

impl MessageStream {
    pub fn new(
        message_id: i64,
        host: Arc<Dispatcher>,
        cancel: CancellationToken,
        cx: DispatchCx,
    ) -> Self {
        Self {
            message_id,
            host,
            cancel,
            cx,
            terminal: AtomicBool::new(false),
        }
    }

    pub fn delta(&self, block: &Block) {
        self.submit(
            &CORE_BLOCK_DELTA,
            BlockDelta {
                message_id: self.message_id,
                delta: block.clone(),
            },
            Retention::Group(GroupKey::Message(self.message_id)),
        );
    }

    pub fn committed(&self, block_id: i64, seq: i64, block: Option<Block>) {
        self.submit(
            &CORE_BLOCK_COMMITTED,
            BlockCommitted {
                message_id: self.message_id,
                seq,
                block_id,
                block,
            },
            Retention::Group(GroupKey::Message(self.message_id)),
        );
    }

    pub fn seal(&mut self, finish_reason: String, usage: Usage) {
        if self.terminal.load(Ordering::Relaxed) {
            return;
        }
        self.terminal.store(true, Ordering::Relaxed);
        self.submit(
            &CORE_MESSAGE_SEAL,
            MessageSeal {
                message_id: self.message_id,
                finish_reason,
                usage,
            },
            Retention::Close(GroupKey::Message(self.message_id)),
        );
    }

    pub fn interrupted(&mut self, reason: InterruptedReason) {
        if self.terminal.load(Ordering::Relaxed) {
            return;
        }
        self.terminal.store(true, Ordering::Relaxed);
        self.submit(
            &CORE_MESSAGE_INTERRUPTED,
            MessageInterrupted {
                message_id: self.message_id,
                reason,
            },
            Retention::Close(GroupKey::Message(self.message_id)),
        );
    }

    /// 转交给独立任务提交并发送终态；接收端丢弃不影响任务
    pub async fn complete(
        mut self,
        storage: &Storage,
        blocks: Vec<Block>,
        finish_reason: String,
        usage: Usage,
    ) -> Result<(), StorageError> {
        let message_id = self.message_id;
        match commit_message(storage, message_id, &blocks, &finish_reason, usage).await {
            Ok(block_ids) => {
                for (block, block_id) in blocks.iter().zip(block_ids) {
                    self.committed(block_id, block.seq, None);
                }
                self.seal(finish_reason.to_string(), usage);
                Ok(())
            }
            Err(error) => {
                self.keep_pending();
                Err(error)
            }
        }
    }

    fn submit<P>(&self, def: &EventDef<P>, payload: P, retention: Retention)
    where
        P: Serialize + Send + Sync,
    {
        if let Err(error) = self.host.notify_host(def, &self.cx, payload, retention) {
            log::error!(
                "[llm] message {} event {} was not submitted: {error}",
                self.message_id,
                def.name
            );
        }
    }

    fn keep_pending(&mut self) {
        self.terminal.store(true, Ordering::Relaxed);
    }
}

impl Drop for MessageStream {
    fn drop(&mut self) {
        if self.terminal.load(Ordering::Relaxed) {
            return;
        }
        let reason = if self.cancel.is_cancelled() {
            InterruptedReason::Cancelled
        } else {
            InterruptedReason::Aborted
        };
        self.interrupted(reason);
    }
}

/// 把已存在的领域块推给当前连接
pub fn notify_persisted_block(
    host: &Dispatcher,
    cx: &DispatchCx,
    message_id: i64,
    block_id: i64,
    block: Block,
) {
    let seq = block.seq;
    if let Err(error) = host.notify_host(
        &CORE_BLOCK_COMMITTED,
        cx,
        BlockCommitted {
            message_id,
            seq,
            block_id,
            block: Some(block),
        },
        Retention::Live,
    ) {
        log::error!(
            "[llm] message {message_id} persisted block {block_id} was not submitted: {error}"
        );
    }
}
async fn commit_message(
    storage: &Storage,
    message_id: i64,
    blocks: &[Block],
    finish_reason: &str,
    usage: Usage,
) -> Result<Vec<i64>, StorageError> {
    storage
        .with_tx(async |tx| {
            let block_ids = tx.create_blocks(message_id, blocks).await?;
            tx.seal_message(
                message_id,
                finish_reason,
                usage.input_tokens,
                usage.output_tokens,
            )
            .await?;
            Ok(block_ids)
        })
        .await
}
