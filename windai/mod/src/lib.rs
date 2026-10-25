use wind_event::envelope::Scope;

pub mod assembly;
pub mod capability;
pub mod catalogue;
pub mod compiler;
pub mod dirs;
pub mod enablement;
pub mod events;
pub mod feed;
pub mod installer;
pub mod manifest;
pub mod package;
pub mod prompt;
pub mod state;

/// 对外状态在存储里的种类
pub(crate) const STATE_KIND: &str = "state";

pub(crate) fn split_scope(scope: &Scope) -> (&'static str, i64) {
    match scope {
        Scope::Global => ("global", 0),
        Scope::Topic { topic_id } => ("topic", *topic_id),
    }
}
