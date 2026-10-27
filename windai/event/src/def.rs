use serde_json::{Value, to_value};
use std::collections::HashMap;
use std::marker::PhantomData;

pub fn schema<T: schemars::JsonSchema>() -> Value {
    to_value(schemars::schema_for!(T)).expect("the generated schema is valid JSON")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Shape {
    Intercept,
    Observe,
}

impl Shape {
    pub const fn visible_to_frontend(&self, shape: Shape) -> bool {
        matches!(shape, Self::Observe)
    }
}

pub struct EventDef<P> {
    pub name: &'static str,
    pub shape: Shape,
    pub log: bool,
    pub schema: fn() -> serde_json::Value,
    _payload: PhantomData<fn() -> P>,
}

impl<P> EventDef<P> {
    pub const fn new(
        name: &'static str,
        shape: Shape,
        log: bool,
        schema: fn() -> serde_json::Value,
    ) -> Self {
        Self {
            name,
            shape,
            log,
            schema,
            _payload: PhantomData,
        }
    }

    pub fn meta(&self) -> EventMeta {
        EventMeta {
            name: self.name,
            shape: self.shape,
            log: self.log,
            schema: self.schema,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventMeta {
    pub name: &'static str,
    pub shape: Shape,
    pub log: bool,
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
