use crate::hub::Hub;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;
use wind_event::{Committed, EventOutlet, Observer, Pending};
use wind_storage::Storage;

const MAX_BATCH: usize = 256;

enum Work {
    Pending(Pending),
    Committed(Committed),
}

/// 事件出口：未落库事件直接进 Hub，落库事件排队等 writer
#[derive(Clone)]
pub struct WriteHandle {
    tx: mpsc::UnboundedSender<Work>,
    hub: Arc<Hub>,
}

impl EventOutlet for WriteHandle {
    fn submit(&self, pending: Pending) {
        if pending.log {
            if self.tx.send(Work::Pending(pending)).is_err() {
                log::error!("[feed] write queue is closed, event was not logged");
            }
        } else {
            self.hub.publish_unlogged(pending.envelope);
        }
    }

    fn submit_committed(&self, committed: Committed) {
        if self.tx.send(Work::Committed(committed)).is_err() {
            log::error!("[feed] write queue is closed, event was not broadcast");
        }
    }
}

pub struct Writer {
    rx: mpsc::UnboundedReceiver<Work>,
    storage: Storage,
    hub: Arc<Hub>,
    observers: Vec<Weak<dyn Observer>>,
}

/// 建立写入队列，返回出口与需要交给运行时启动的 writer
pub fn open(storage: Storage, hub: Arc<Hub>) -> (WriteHandle, Writer) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = WriteHandle {
        tx,
        hub: Arc::clone(&hub),
    };
    let writer = Writer {
        rx,
        storage,
        hub,
        observers: Vec::new(),
    };
    (handle, writer)
}

impl Writer {
    pub fn set_observers(&mut self, observers: Vec<Weak<dyn Observer>>) {
        self.observers = observers;
    }

    pub async fn run(mut self) {
        while let Some(first) = self.rx.recv().await {
            let mut batch = Vec::with_capacity(MAX_BATCH);
            batch.push(first);
            while batch.len() < MAX_BATCH {
                match self.rx.try_recv() {
                    Ok(work) => batch.push(work),
                    Err(_) => break,
                }
            }
            self.process(batch).await;
        }
    }

    async fn process(&mut self, batch: Vec<Work>) {
        let mut pending = Vec::new();
        let mut committed = Vec::new();
        for work in batch {
            match work {
                Work::Pending(pending_event) if pending_event.log => pending.push(pending_event),
                Work::Pending(pending_event) => self.hub.publish_unlogged(pending_event.envelope),
                Work::Committed(event) => committed.push(event),
            }
        }

        if !pending.is_empty() {
            let storage = self.storage.clone();
            let written = storage
                .with_tx(|tx| async move {
                    let mut rows = Vec::with_capacity(pending.len());
                    for pending in pending {
                        let log_id = tx.create_event_log(&pending.envelope).await?;
                        rows.push(Committed {
                            log_id,
                            envelope: pending.envelope,
                        });
                    }
                    Ok(rows)
                })
                .await;
            match written {
                Ok(mut rows) => committed.append(&mut rows),
                Err(error) => log::error!("[feed] cannot write event log: {error}"),
            }
        }

        committed.sort_unstable_by_key(|committed| committed.log_id);
        for committed in committed {
            self.hub.publish_committed(&committed);
            self.observers.retain(|observer| {
                let Some(observer) = observer.upgrade() else {
                    return false;
                };
                observer.on_committed(&committed);
                true
            });
        }
    }
}
