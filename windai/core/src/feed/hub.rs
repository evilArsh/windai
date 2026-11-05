use super::FeedOptions;
use super::buffer::Buffer;
use super::error::FeedError;
use super::queue::Subscriber;
use super::stats::{FeedStats, ShutdownReport, WriterState};
use super::writer::{Writer, WriterHandle};
use serde_json::Value;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_event::{EventOutlet, Observer, Pending, Retention, SubmitError};
use wind_storage::Storage;
use wind_wire::event::{EventKey, FeedEvent};

pub(crate) enum Command {
    Emit(Pending),
    Subscribe {
        reply: oneshot::Sender<Opening>,
    },
    Stats {
        reply: oneshot::Sender<FeedStats>,
    },
    Committed {
        keys: Vec<EventKey>,
    },
    WriterState {
        state: WriterState,
        error: Option<String>,
    },
    Shutdown {
        reply: oneshot::Sender<ShutdownReport>,
    },
}

pub struct Opening {
    pub replay_len: usize,
    pub cut: EventKey,
    pub subscription: Subscription,
}

pub struct Subscription {
    replay: std::vec::IntoIter<Arc<FeedEvent>>,
    rx: mpsc::Receiver<Arc<FeedEvent>>,
    cancel: CancellationToken,
}

impl Subscription {
    pub async fn recv(&mut self) -> Result<Option<Arc<FeedEvent>>, FeedError> {
        if self.cancel.is_cancelled() {
            return Ok(None);
        }
        if let Some(event) = self.replay.next() {
            return Ok(Some(event));
        }
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Ok(None),
            event = self.rx.recv() => Ok(event),
        }
    }
}

#[derive(Clone)]
pub struct FeedHandle {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl FeedHandle {
    pub async fn subscribe(&self) -> Result<Opening, FeedError> {
        let (reply, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Subscribe { reply })
            .map_err(|_| FeedError::Closed)?;
        reply_rx.await.map_err(|_| FeedError::Closed)
    }

    pub async fn stats(&self) -> Result<FeedStats, FeedError> {
        let (reply, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Stats { reply })
            .map_err(|_| FeedError::Closed)?;
        reply_rx.await.map_err(|_| FeedError::Closed)
    }

    pub async fn shutdown(&self) -> Result<ShutdownReport, FeedError> {
        let (reply, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Shutdown { reply })
            .map_err(|_| FeedError::Closed)?;
        reply_rx.await.map_err(|_| FeedError::Closed)
    }
}

impl EventOutlet for FeedHandle {
    fn submit(&self, pending: Pending) -> Result<(), SubmitError> {
        self.command_tx
            .send(Command::Emit(pending))
            .map_err(|_| SubmitError::Closed)
    }
}

pub struct FeedRuntime {
    command_rx: mpsc::UnboundedReceiver<Command>,
    hub: Hub,
    writer: Writer,
    observers: Vec<Weak<dyn Observer>>,
}

impl FeedRuntime {
    // TODO: 接入
    pub fn set_observers(&mut self, observers: Vec<Weak<dyn Observer>>) {
        self.observers = observers;
    }

    pub async fn run(mut self) -> Result<ShutdownReport, FeedError> {
        self.hub.observers = std::mem::take(&mut self.observers);
        let writer_task = tokio::spawn(self.writer.run());
        let mut shutdown = None;
        let mut failure = None;
        loop {
            let Some(command) = self.command_rx.recv().await else {
                break;
            };
            match command {
                Command::Shutdown { reply } => {
                    self.command_rx.close();
                    shutdown = Some(reply);
                    while let Ok(command) = self.command_rx.try_recv() {
                        if let Command::Shutdown { reply } = command {
                            shutdown = Some(reply);
                            continue;
                        }
                        if let Err(error) = self.hub.handle(command) {
                            failure = Some(error);
                            break;
                        }
                    }
                    break;
                }
                command => {
                    if let Err(error) = self.hub.handle(command) {
                        failure = Some(error);
                        break;
                    }
                }
            }
        }
        if let Some(error) = failure {
            writer_task.abort();
            return Err(error);
        }

        let mut timed_out = false;
        let writer_report = match tokio::time::timeout(
            self.hub.options.shutdown_timeout,
            self.hub.writer_handle.shutdown(),
        )
        .await
        {
            Ok(report) => report,
            Err(_) => {
                timed_out = true;
                writer_task.abort();
                None
            }
        };
        let journal_remaining = match writer_report {
            Some(report) => report.unpersisted,
            None => self.hub.buffer.pending_write(),
        };
        let report = self.hub.report(journal_remaining, timed_out);
        if let Some(reply) = shutdown {
            let _ = reply.send(report.clone());
        }
        Ok(report)
    }
}

pub async fn open(
    storage: Storage,
    options: FeedOptions,
) -> Result<(FeedHandle, FeedRuntime), FeedError> {
    if options.subscriber_capacity == 0 {
        return Err(FeedError::InvalidQuery(
            "subscriber capacity must be greater than zero".into(),
        ));
    }
    if options.batch_size == 0 {
        return Err(FeedError::InvalidQuery(
            "batch size must be greater than zero".into(),
        ));
    }
    let epoch = storage.allocate_feed_epoch().await?;
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();

    let writer_handle = WriterHandle::new(writer_tx);
    let writer = Writer::new(storage, writer_rx, command_tx.clone(), options.clone());

    let feed_handle = FeedHandle { command_tx };
    let feed = FeedRuntime {
        command_rx,
        hub: Hub::new(epoch, options, writer_handle),
        writer,
        observers: Vec::new(),
    };

    Ok((feed_handle, feed))
}

struct Hub {
    epoch: i64,
    last_seq: i64,
    buffer: Buffer,
    subscribers: Vec<Subscriber>,
    options: FeedOptions,
    observers: Vec<Weak<dyn Observer>>,
    writer_handle: WriterHandle,
    writer_state: WriterState,
    last_error: Option<String>,
}

impl Hub {
    fn new(epoch: i64, options: FeedOptions, writer_handle: WriterHandle) -> Self {
        Self {
            epoch,
            last_seq: 0,
            buffer: Buffer::default(),
            subscribers: Vec::new(),
            options,
            observers: Vec::new(),
            writer_handle,
            writer_state: WriterState::Idle,
            last_error: None,
        }
    }

    fn handle(&mut self, command: Command) -> Result<(), FeedError> {
        match command {
            Command::Emit(pending) => self.emit(pending),
            Command::Subscribe { reply } => {
                let _ = reply.send(self.subscribe());
                Ok(())
            }
            Command::Stats { reply } => {
                let _ = reply.send(self.stats());
                Ok(())
            }
            Command::Committed { keys } => {
                for event in self.buffer.release_keys(&keys) {
                    self.notify(&event);
                }
                Ok(())
            }
            Command::WriterState { state, error } => {
                self.writer_state = state;
                if let Some(error) = error {
                    self.last_error = Some(error);
                }
                Ok(())
            }
            Command::Shutdown { .. } => Ok(()),
        }
    }

    fn emit(&mut self, pending: Pending) -> Result<(), FeedError> {
        let seq = self
            .last_seq
            .checked_add(1)
            .ok_or(FeedError::SequenceExhausted)?;
        let key = EventKey::new(self.epoch, seq).ok_or(FeedError::SequenceExhausted)?;
        self.last_seq = seq;

        let event = Arc::new(FeedEvent {
            key,
            envelope: pending.envelope,
            target: pending.target,
        });
        let retention = pending.retention;
        if event.envelope.shape.visible_to_frontend() {
            if matches!(&retention, Retention::Journal | Retention::Group(_)) {
                self.buffer.insert(
                    Arc::clone(&event),
                    retention.clone(),
                    payload_bytes(&event.envelope.payload),
                );
            }
            self.deliver(&event);
            if !matches!(&retention, Retention::Journal) {
                self.notify(&event);
            }
        }
        if matches!(&retention, Retention::Journal)
            && !self.writer_handle.submit(Arc::clone(&event))
        {
            self.writer_state = WriterState::Stopped;
            self.last_error = Some("the journal writer is gone".into());
        }
        if let Retention::Close(group) = retention {
            self.buffer.release_group(&group);
        }
        Ok(())
    }

    fn subscribe(&mut self) -> Opening {
        let cancel = CancellationToken::new();
        let (subscriber, rx) = Subscriber::new(self.options.subscriber_capacity, cancel.clone());
        let replay = self.buffer.snapshot();
        self.subscribers.push(subscriber);
        Opening {
            replay_len: replay.len(),
            cut: EventKey {
                epoch: self.epoch,
                seq: self.last_seq,
            },
            subscription: Subscription {
                replay: replay.into_iter(),
                rx,
                cancel,
            },
        }
    }

    fn deliver(&mut self, event: &Arc<FeedEvent>) {
        self.subscribers
            .retain(|subscriber| subscriber.deliver(event));
    }

    fn notify(&mut self, event: &Arc<FeedEvent>) {
        self.observers.retain(|observer| {
            let Some(observer) = observer.upgrade() else {
                return false;
            };
            observer.on_event(event);
            true
        });
    }

    fn stats(&self) -> FeedStats {
        FeedStats {
            buffer_events: self.buffer.len(),
            buffer_bytes: self.buffer.bytes(),
            groups: self.buffer.groups_len(),
            pending_write: self.buffer.pending_write(),
            oldest_pending_ms: self.buffer.oldest_pending_ms(),
            connections: self.subscribers.len(),
            max_connection_backlog: self
                .subscribers
                .iter()
                .map(Subscriber::backlog)
                .max()
                .unwrap_or(0),
            writer: self.writer_state,
            last_error: self.last_error.clone(),
        }
    }

    fn report(&self, journal_remaining: usize, timed_out: bool) -> ShutdownReport {
        ShutdownReport {
            journal_remaining,
            buffer_remaining: self.buffer.len(),
            timed_out,
        }
    }
}

fn payload_bytes(payload: &Option<Value>) -> usize {
    payload
        .as_ref()
        .and_then(|value| serde_json::to_vec(value).ok())
        .map(|bytes| bytes.len())
        .unwrap_or(0)
}
