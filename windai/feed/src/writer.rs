use tokio::sync::mpsc;
use wind_event::{Committed, EventOutlet, Pending, Scope};

pub enum Work {
    Pending(Pending),
    Committed(Committed),
}

pub struct WriteHandle {
    tx: mpsc::Sender<Work>,
}
impl WriteHandle {}

impl EventOutlet for WriteHandle {
    fn submit(&self, pending: Pending) {
        todo!()
    }

    fn submit_committed(&self, committed: Committed) {
        todo!()
    }
}
