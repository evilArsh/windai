mod compile;
mod cond;
mod error;
mod path;

pub use compile::RuleSet;
pub use error::{Error, Result};
use serde_json::{Map, Value};

/// 规则求值的上下文，由调用方注入。
///
/// 内部是一个平坦的键值对，在执行计算表达式时注入为 `ctx_<key>` 变量，
/// 在条件求值时可通过 `$ctx.<key>` 引用。
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

impl From<Map<String, Value>> for EvalContext {
    fn from(vars: Map<String, Value>) -> Self {
        Self { vars }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> EvalContext {
        EvalContext::new()
            .with("provider", "deepseek")
            .with("model", "deepseek-r1")
    }

    #[test]
    fn test_set_field() {
        let rules = r#"{
            "rules": [
                {"type": "set", "path": "stream", "value": true}
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();
        let mut body = serde_json::json!({"model": "gpt-4"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["stream"], true);
    }

    #[test]
    fn test_set_nested_field() {
        let rules = r#"{
            "rules": [
                {"type": "set", "path": "thinking.type", "value": "enabled"}
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();
        let mut body = serde_json::json!({"model": "deepseek"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");
    }

    #[test]
    fn test_remove_field() {
        let rules = r#"{
            "rules": [
                {"type": "remove", "path": "reasoning_effort"}
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();
        let mut body = serde_json::json!({"reasoning_effort": "medium", "model": "x"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert!(body.get("reasoning_effort").is_none());
        assert!(body.get("model").is_some());
    }

    #[test]
    fn test_map_value() {
        let rules = r#"{
            "rules": [
                {
                    "type": "map_value",
                    "path": "reasoning_effort",
                    "mappings": {
                        "medium": {"thinking": {"type": "enabled"}},
                        "high": {"thinking": {"type": "enabled"}}
                    },
                    "default": {"thinking": {"type": "disabled"}},
                    "remove_source": true
                }
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();

        let mut body = serde_json::json!({"reasoning_effort": "medium"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");
        assert!(body.get("reasoning_effort").is_none());

        let mut body = serde_json::json!({"reasoning_effort": "unknown"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["thinking"]["type"], "disabled");
    }

    #[test]
    fn test_when_eq() {
        let rules = r#"{
            "rules": [
                {
                    "type": "when",
                    "cond": {"eq": ["$ctx.provider", "deepseek"]},
                    "then": [
                        {"type": "set", "path": "thinking.type", "value": "enabled"}
                    ]
                }
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();

        let mut body = serde_json::json!({"model": "test"});
        rs.apply(&mut body, &test_ctx()).unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");

        let other_ctx = EvalContext::new().with("provider", "openai");
        let mut body = serde_json::json!({"model": "test"});
        rs.apply(&mut body, &other_ctx).unwrap();
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn test_when_else() {
        let rules = r#"{
            "rules": [
                {
                    "type": "when",
                    "cond": {"eq": ["$ctx.provider", "deepseek"]},
                    "then": [
                        {"type": "set", "path": "thinking.type", "value": "enabled"}
                    ],
                    "else": [
                        {"type": "remove", "path": "reasoning_effort"}
                    ]
                }
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();

        let mut body = serde_json::json!({"reasoning_effort": "medium"});
        rs.apply(&mut body, &test_ctx()).unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");

        let other_ctx = EvalContext::new().with("provider", "openai");
        let mut body = serde_json::json!({"reasoning_effort": "medium"});
        rs.apply(&mut body, &other_ctx).unwrap();
        assert!(body.get("reasoning_effort").is_none());
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn test_exists_condition() {
        let rules = r#"{
            "rules": [
                {
                    "type": "when",
                    "cond": {"exists": "reasoning_effort"},
                    "then": [
                        {"type": "set", "path": "thinking.type", "value": "enabled"}
                    ]
                }
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();

        let mut body = serde_json::json!({"reasoning_effort": "medium"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");

        let mut body = serde_json::json!({"model": "test"});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn test_and_or_not() {
        // and
        let rs = RuleSet::from_json(
            r#"{
            "rules": [{
                "type": "when",
                "cond": {"and": [
                    {"eq": ["$ctx.provider", "deepseek"]},
                    {"exists": "reasoning_effort"}
                ]},
                "then": [{"type": "set", "path": "x", "value": 1}]
            }]
        }"#,
        )
        .unwrap();

        let mut body = serde_json::json!({"reasoning_effort": "medium"});
        rs.apply(&mut body, &test_ctx()).unwrap();
        assert_eq!(body["x"], 1);

        let mut body = serde_json::json!({});
        rs.apply(&mut body, &test_ctx()).unwrap();
        assert!(body.get("x").is_none());

        // or
        let rs = RuleSet::from_json(
            r#"{
            "rules": [{
                "type": "when",
                "cond": {"or": [
                    {"eq": ["$ctx.provider", "deepseek"]},
                    {"eq": ["$ctx.provider", "openai"]}
                ]},
                "then": [{"type": "set", "path": "y", "value": 1}]
            }]
        }"#,
        )
        .unwrap();

        let mut body = serde_json::json!({});
        rs.apply(&mut body, &EvalContext::new().with("provider", "openai"))
            .unwrap();
        assert_eq!(body["y"], 1);

        // not
        let rs = RuleSet::from_json(
            r#"{
            "rules": [{
                "type": "when",
                "cond": {"not": {"exists": "reasoning_effort"}},
                "then": [{"type": "set", "path": "z", "value": "no_reasoning"}]
            }]
        }"#,
        )
        .unwrap();

        let mut body = serde_json::json!({});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["z"], "no_reasoning");
    }

    #[test]
    fn test_compute() {
        let rules = r#"{
            "rules": [
                {"type": "compute", "path": "max_tokens", "expr": "min($value, 4096)"}
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();

        // value = 8192 → min(8192, 4096) = 4096
        let mut body = serde_json::json!({"max_tokens": 8192});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["max_tokens"], 4096);

        // value = 1024 → min(1024, 4096) = 1024
        let mut body = serde_json::json!({"max_tokens": 1024});
        rs.apply(&mut body, &EvalContext::new()).unwrap();
        assert_eq!(body["max_tokens"], 1024);
    }

    #[test]
    fn test_compute_with_context() {
        let rules = r#"{
            "rules": [
                {"type": "compute", "path": "max_tokens", "expr": "min($value, 8192 - $ctx.input_tokens)"}
            ]
        }"#;
        let rs = RuleSet::from_json(rules).unwrap();

        let ctx = EvalContext::new().with("input_tokens", 7000);
        let mut body = serde_json::json!({"max_tokens": 4096});
        rs.apply(&mut body, &ctx).unwrap();
        // min(4096, 8192 - 7000) = min(4096, 1192) = 1192
        assert_eq!(body["max_tokens"], 1192);
    }

    #[test]
    fn test_deepseek_example() {
        let json = r#"{
            "rules": [
                {
                    "type": "when",
                    "cond": {"eq": ["$ctx.provider", "deepseek"]},
                    "then": [
                        {
                            "type": "map_value",
                            "path": "reasoning_effort",
                            "mappings": {
                                "medium": {"thinking": {"type": "enabled"}},
                                "high": {"thinking": {"type": "enabled"}}
                            },
                            "default": {"thinking": {"type": "disabled"}},
                            "remove_source": true
                        }
                    ],
                    "else": [
                        {"type": "remove", "path": "reasoning_effort"}
                    ]
                }
            ]
        }"#;
        let rs = RuleSet::from_json(json).unwrap();

        let mut body = serde_json::json!({"model": "deepseek-r1", "reasoning_effort": "medium", "messages": []});
        rs.apply(&mut body, &test_ctx()).unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn test_neq_condition() {
        let rs = RuleSet::from_json(
            r#"{
            "rules": [{
                "type": "when",
                "cond": {"neq": ["$ctx.provider", "deepseek"]},
                "then": [{"type": "remove", "path": "reasoning_effort"}]
            }]
        }"#,
        )
        .unwrap();

        let mut body = serde_json::json!({"reasoning_effort": "medium"});
        rs.apply(&mut body, &EvalContext::new().with("provider", "openai"))
            .unwrap();
        assert!(body.get("reasoning_effort").is_none());
    }
}
