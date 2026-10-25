use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Mount, Shape};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CoreInputDraft {
    pub topic_id: i64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CoreModNotify {
    pub level: String,
    pub text: String,
}

fn schema<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("the generated schema is valid JSON")
}

pub const CORE_INPUT_DRAFT: EventDef<CoreInputDraft> = EventDef::new(
    "core/input.draft",
    Shape::Observe,
    true,
    Mount::Subsystem,
    schema::<CoreInputDraft>,
);

pub const CORE_MOD_NOTIFY: EventDef<CoreModNotify> = EventDef::new(
    "core/mod.notify",
    Shape::Observe,
    true,
    Mount::Subsystem,
    schema::<CoreModNotify>,
);

pub fn directory() -> Directory {
    Directory::from_metas([CORE_INPUT_DRAFT.meta(), CORE_MOD_NOTIFY.meta()])
}
