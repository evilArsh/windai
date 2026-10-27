use crate::def::Shape;
use crate::envelope::Envelope;
use serde_json::Value;

pub struct Pending {
    pub shape: Shape,
    pub log: bool,
    pub envelope: Envelope<Value>,
}
pub struct Committed {
    pub pos: i64,
    pub shape: Shape,
    pub envelope: Envelope<Value>,
}
pub trait EventOutlet: Send + Sync {
    fn submit(&self, pending: Pending);
}
