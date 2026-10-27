mod compile;
mod cond;
mod error;
mod path;

#[cfg(test)]
mod test;

pub use compile::RuleSet;
pub use error::{Error, Result};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default)]
pub struct EvalContext {
    vars: Map<String, Value>,
}

impl EvalContext {
    pub fn new() -> Self {
        Self { vars: Map::new() }
    }

    /// 键按字面量存入，因此含 `.` 的键用 `get` 取不到。
    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.vars.insert(key.to_string(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        let segs = path::segments(key);
        let (head, tail) = segs.split_first()?;
        let root = self.vars.get(head)?;
        if tail.is_empty() {
            return Some(root);
        }
        path::get(root, tail).ok().flatten()
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.vars.iter()
    }
}

impl From<Value> for EvalContext {
    fn from(value: Value) -> Self {
        let vars = match value {
            Value::Object(map) => map,
            _ => {
                let mut default_map = Map::new();
                default_map.insert("data".to_string(), value);
                default_map
            }
        };
        Self { vars }
    }
}
