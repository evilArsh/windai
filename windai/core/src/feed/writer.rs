use super::FeedOptions;
use super::hub::Command;
use super::stats::WriterState;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use wind_storage::Storage;
use wind_wire::event::{EventKey, FeedEvent};

const MAX_BATCH: usize = 256;

pub(crate) enum WriterCommand {
    Emit(Arc<FeedEvent>),
    Shutdown {
        reply: oneshot::Sender<WriterReport>,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WriterReport {
    pub(crate) unpersisted: usize,
}

#[derive(Clone)]
pub(crate) struct WriterHandle {
    tx: mpsc::UnboundedSender<WriterCommand>,
}

impl WriterHandle {
    pub(crate) fn new(tx: mpsc::UnboundedSender<WriterCommand>) -> Self {
        Self { tx }
    }

    /// 返回 false 表示 writer 已经不接收事件
    pub(crate) fn submit(&self, event: Arc<FeedEvent>) -> bool {
        self.tx.send(WriterCommand::Emit(event)).is_ok()
    }

    pub(crate) async fn shutdown(&self) -> Option<WriterReport> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx.send(WriterCommand::Shutdown { reply }).ok()?;
        reply_rx.await.ok()
    }
}

pub(crate) struct Writer {
    storage: Storage,
    rx: mpsc::UnboundedReceiver<WriterCommand>,
    commands_tx: mpsc::UnboundedSender<Command>,
    options: FeedOptions,
    unpersisted: usize,
}

impl Writer {
    pub(crate) fn new(
        storage: Storage,
        rx: mpsc::UnboundedReceiver<WriterCommand>,
        commands_tx: mpsc::UnboundedSender<Command>,
        options: FeedOptions,
    ) -> Self {
        Self {
            storage,
            rx,
            commands_tx,
            options,
            unpersisted: 0,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut shutdown = None;
        loop {
            let Some(command) = self.rx.recv().await else {
                return;
            };
            let mut batch = Vec::new();
            match command {
                WriterCommand::Emit(event) => {
                    self.unpersisted += 1;
                    batch.push(event);
                }
                WriterCommand::Shutdown { reply } => {
                    let _ = reply.send(WriterReport {
                        unpersisted: self.unpersisted,
                    });
                    return;
                }
            }
            while batch.len() < self.options.batch_size.min(MAX_BATCH) {
                match self.rx.try_recv() {
                    Ok(WriterCommand::Emit(event)) => {
                        self.unpersisted += 1;
                        batch.push(event);
                    }
                    Ok(WriterCommand::Shutdown { reply }) => {
                        shutdown = Some(reply);
                        break;
                    }
                    Err(_) => break,
                }
            }
            self.write(batch).await;
            if let Some(reply) = shutdown.take() {
                let _ = reply.send(WriterReport {
                    unpersisted: self.unpersisted,
                });
                return;
            }
        }
    }

    /// 写不进去就丢弃这一批，由 Hub 收摊
    async fn write(&mut self, events: Vec<Arc<FeedEvent>>) {
        let (values, keys): (Vec<FeedEvent>, Vec<EventKey>) = events
            .into_iter()
            .map(|event| ((*event).clone(), event.key))
            .unzip();
        self.unpersisted -= values.len();

        self.state(WriterState::Writing, None);
        let error = self.storage.append_feed_batch(&values).await.err();
        let _ = self.commands_tx.send(Command::Committed { keys });
        match error {
            None => self.state(WriterState::Idle, None),
            Some(error) => self.state(WriterState::Faulted, Some(error.to_string())),
        }
    }

    fn state(&self, state: WriterState, error: Option<String>) {
        let _ = self.commands_tx.send(Command::WriterState { state, error });
    }
}
