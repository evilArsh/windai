use tokio::sync::mpsc;
use wind_event::{EventOutlet, Pending, Scope};
use wind_wire::BlockKind;

pub enum Work {
    Event(Pending),
    BlockOpen {
        block_id: i64,
        message_id: i64,
        scope: Scope,
        kind: BlockKind,
        call_id: Option<String>,
    },
    BlockGrow {
        block_id: i64,
        message_id: i64,
        scope: Scope,
        revision: u64,
        fragment: String,
    },
    BlockCommit {
        block_id: i64,
        message_id: i64,
        scope: Scope,
    },
    MessageSeal {
        scope: Scope,
        message_id: i64,
        finish_reason: Option<String>,
    },
}

pub struct WriteHandle {
    tx: mpsc::Sender<Work>,
}

impl EventOutlet for WriteHandle {
    fn submit(&self, pending: Pending) {
        todo!()
    }
}
