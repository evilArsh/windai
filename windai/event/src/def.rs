use std::collections::HashMap;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Shape {
    Intercept,
    Observe,
}

pub const fn visible_to_frontend(shape: Shape) -> bool {
    matches!(shape, Shape::Observe)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Mount {
    UseCase,
    TranslationLayer,
    Subsystem,
}

pub struct EventDef<P> {
    pub name: &'static str,
    pub shape: Shape,
    pub log: bool,
    /// 挂载点，审计用，派发器不读
    pub mount: Mount,
    pub schema: fn() -> serde_json::Value,
    _payload: PhantomData<fn() -> P>,
}

impl<P> EventDef<P> {
    pub const fn new(
        name: &'static str,
        shape: Shape,
        log: bool,
        mount: Mount,
        schema: fn() -> serde_json::Value,
    ) -> Self {
        Self {
            name,
            shape,
            log,
            mount,
            schema,
            _payload: PhantomData,
        }
    }

    pub fn meta(&self) -> EventMeta {
        EventMeta {
            name: self.name,
            shape: self.shape,
            log: self.log,
            mount: self.mount,
            schema: self.schema,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventMeta {
    pub name: &'static str,
    pub shape: Shape,
    pub log: bool,
    /// 挂载点，审计用，派发器不读
    pub mount: Mount,
    pub schema: fn() -> serde_json::Value,
}

impl EventMeta {
    pub fn is_intercept(&self) -> bool {
        self.shape == Shape::Intercept
    }

    pub fn is_observe(&self) -> bool {
        self.shape == Shape::Observe
    }
}

#[derive(Debug, Clone, Default)]
pub struct Directory {
    by_name: HashMap<&'static str, EventMeta>,
}

impl Directory {
    pub fn from_metas(metas: impl IntoIterator<Item = EventMeta>) -> Self {
        Self {
            by_name: metas.into_iter().map(|m| (m.name, m)).collect(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&EventMeta> {
        self.by_name.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.by_name.keys().copied()
    }

    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    pub fn merge(self, other: Directory) -> Self {
        let mut by_name = other.by_name;
        for (name, meta) in self.by_name {
            by_name.insert(name, meta);
        }
        Self { by_name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> serde_json::Value {
        serde_json::json!({ "type": "object" })
    }

    const TOOL_CALL: EventDef<serde_json::Value> = EventDef::new(
        "core/mcp.tool.call",
        Shape::Intercept,
        true,
        Mount::Subsystem,
        schema,
    );

    const BLOCK_DELTA: EventDef<serde_json::Value> = EventDef::new(
        "core/block.delta",
        Shape::Observe,
        false,
        Mount::TranslationLayer,
        schema,
    );

    #[test]
    fn only_observe_events_reach_the_frontend() {
        assert!(!visible_to_frontend(Shape::Intercept));
        assert!(visible_to_frontend(Shape::Observe));
    }

    #[test]
    fn the_log_flag_is_per_event_not_derived_from_shape() {
        const { assert!(TOOL_CALL.log) };
        const { assert!(!BLOCK_DELTA.log, "drafts are not logged") };
    }

    #[test]
    fn meta_carries_every_field_through_erasure() {
        let meta = TOOL_CALL.meta();
        assert_eq!(meta.name, "core/mcp.tool.call");
        assert_eq!(meta.shape, Shape::Intercept);
        assert_eq!(meta.mount, Mount::Subsystem);
        assert!(meta.log);
        assert!(meta.is_intercept());
        assert!(!meta.is_observe());
    }

    #[test]
    fn a_directory_looks_up_by_name() {
        let dir = Directory::from_metas([TOOL_CALL.meta(), BLOCK_DELTA.meta()]);
        assert_eq!(dir.len(), 2);
        assert!(!dir.is_empty());
        assert_eq!(
            dir.get("core/mcp.tool.call")
                .expect("the entry exists")
                .shape,
            Shape::Intercept
        );
        assert!(dir.get("core/not.a.real.event").is_none());
    }

    #[test]
    fn the_directory_lists_its_names() {
        let dir = Directory::from_metas([TOOL_CALL.meta()]);
        let names: Vec<&str> = dir.names().collect();
        assert_eq!(names, vec!["core/mcp.tool.call"]);
    }

    #[test]
    fn an_empty_directory_reports_itself_empty() {
        let dir = Directory::from_metas([]);
        assert!(dir.is_empty());
        assert_eq!(dir.len(), 0);
    }

    #[test]
    fn merging_keeps_every_entry_and_prefers_the_left_one() {
        const REPLACED: EventDef<serde_json::Value> = EventDef::new(
            "core/block.delta",
            Shape::Intercept,
            true,
            Mount::UseCase,
            schema,
        );
        let left = Directory::from_metas([REPLACED.meta()]);
        let right = Directory::from_metas([TOOL_CALL.meta(), BLOCK_DELTA.meta()]);

        let merged = left.merge(right);
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged
                .get("core/block.delta")
                .expect("the entry exists")
                .mount,
            Mount::UseCase,
            "on duplicate names the caller wins"
        );
        assert!(merged.get("core/mcp.tool.call").is_some());
    }
}
