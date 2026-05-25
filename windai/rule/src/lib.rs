mod compile;
mod cond;
mod error;
mod path;

#[cfg(test)]
mod test;

pub use compile::RuleSet;
pub use error::{Error, Result};
use serde_json::{Map, Value};

/// 规则求值的上下文，由调用方注入。
#[derive(Debug, Clone, Default)]
pub struct EvalContext {
    vars: Map<String, Value>,
}

impl EvalContext {
    pub fn new() -> Self {
        Self { vars: Map::new() }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.vars.insert(key.to_string(), value.into());
        self
    }

    /// 获取变量值。
    ///
    /// TODO: 获取嵌套路径值
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.vars.get(key)
        // let segs = path::segments(key);
        // path::get(&self.vars, &segs)
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
