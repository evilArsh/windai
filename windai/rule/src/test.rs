use super::*;
use crate::cond::{Arg, CompiledCond};
use serde_json::json;

fn ctx() -> EvalContext {
    EvalContext::new()
        .with("provider", "deepseek")
        .with("model", "deepseek-r1")
}

// ==================== EvalContext ====================

#[test]
fn ctx_default_empty() {
    assert!(EvalContext::default().get("any").is_none());
}

#[test]
fn ctx_with_chain() {
    let c = EvalContext::new()
        .with("a", 1)
        .with("b", "hi")
        .with("c", true);
    assert_eq!(c.get("a"), Some(&json!(1)));
    assert_eq!(c.get("b"), Some(&json!("hi")));
    assert_eq!(c.get("c"), Some(&json!(true)));
    assert!(c.get("d").is_none());
}

#[test]
fn ctx_from_value() {
    let c = EvalContext::from(json!({"x": 42}));
    assert_eq!(c.get("x"), Some(&json!(42)));
}

#[test]
fn ctx_from_non_object_value() {
    let c = EvalContext::from(json!("naked_value"));
    assert_eq!(c.get("data"), Some(&json!("naked_value")));
}

// ==================== path ====================

#[test]
fn path_segments_empty() {
    assert_eq!(path::segments(""), &[""]);
}

#[test]
fn path_segments_single_and_multi() {
    assert_eq!(path::segments("a"), &["a"]);
    assert_eq!(path::segments("a.b.c"), &["a", "b", "c"]);
}

#[test]
fn path_get_nested() {
    let v = json!({"a": {"b": {"c": 42}}});
    assert_eq!(
        path::get(&v, &path::segments("a.b.c")).unwrap(),
        Some(&json!(42))
    );
}

#[test]
fn path_get_missing_key() {
    let v = json!({"a": 1});
    assert!(!path::get(&v, &path::segments("b")).unwrap().is_some());
}

#[test]
fn path_get_non_object_mid() {
    let v = json!({"a": 1});
    assert!(path::get(&v, &path::segments("a.b")).is_err());
}

#[test]
fn path_walk_creates_intermediate() {
    let mut v = json!({});
    let dst = path::walk(&mut v, &path::segments("a.b.c")).unwrap();
    *dst = json!(42);
    assert_eq!(v["a"]["b"]["c"], 42);
}

#[test]
fn path_walk_non_object_mid() {
    let mut v = json!({"a": 1});
    assert!(path::walk(&mut v, &path::segments("a.b")).is_err());
}

#[test]
fn path_remove_leaf() {
    let mut v = json!({"a": {"b": 1, "c": 2}});
    path::remove(&mut v, &path::segments("a.b")).unwrap();
    assert!(v["a"].get("b").is_none());
    assert_eq!(v["a"]["c"], 2);
}

#[test]
fn path_remove_empty_segs() {
    assert!(path::remove(&mut json!({"a": 1}), &[]).is_err());
}

#[test]
fn path_remove_non_existent_silent() {
    // HashMap 的 remove 忽略不存在的键，静默成功
    let mut v = json!({"x": 1});
    assert!(path::remove(&mut v, &path::segments("missing")).is_ok());
}

#[test]
fn path_remove_through_non_object() {
    let mut v = json!({"a": 1});
    assert!(path::remove(&mut v, &path::segments("a.b")).is_err());
}

// ==================== 条件编译错误 ====================

#[test]
fn cond_compile_non_object() {
    assert!(CompiledCond::compile(&json!("bad")).is_err());
    assert!(CompiledCond::compile(&json!([])).is_err());
    assert!(CompiledCond::compile(&json!(null)).is_err());
}

#[test]
fn cond_compile_empty_obj() {
    assert!(CompiledCond::compile(&json!({})).is_err());
}

#[test]
fn cond_compile_multi_key() {
    assert!(CompiledCond::compile(&json!({"eq": [1, 2], "neq": [3, 4]})).is_err());
}

#[test]
fn cond_compile_unknown_op() {
    assert!(CompiledCond::compile(&json!({"gt": [1, 2]})).is_err());
}

#[test]
fn cond_compile_exists_non_string() {
    assert!(CompiledCond::compile(&json!({"exists": 123})).is_err());
    assert!(CompiledCond::compile(&json!({"exists": ["a"]})).is_err());
}

#[test]
fn cond_compile_eq_neq_non_array() {
    assert!(CompiledCond::compile(&json!({"eq": "bad"})).is_err());
    assert!(CompiledCond::compile(&json!({"neq": 1})).is_err());
}

#[test]
fn cond_compile_eq_neq_wrong_count() {
    assert!(CompiledCond::compile(&json!({"eq": ["a"]})).is_err());
    assert!(CompiledCond::compile(&json!({"eq": ["a", "b", "c"]})).is_err());
    assert!(CompiledCond::compile(&json!({"neq": []})).is_err());
}

#[test]
fn cond_compile_and_or_non_array() {
    assert!(CompiledCond::compile(&json!({"and": "bad"})).is_err());
    assert!(CompiledCond::compile(&json!({"or": 1})).is_err());
}

// ==================== 条件求值 ====================

#[test]
fn cond_eval_eq_types() {
    let c = CompiledCond::compile(&json!({"eq": [42, "42"]})).unwrap();
    assert!(!c.eval(&json!({}), &EvalContext::new()).unwrap());
}

#[test]
fn cond_eval_eq_bool_and_null() {
    let t = CompiledCond::compile(&json!({"eq": [true, true]})).unwrap();
    assert!(t.eval(&json!({}), &EvalContext::new()).unwrap());
    let n = CompiledCond::compile(&json!({"eq": [null, null]})).unwrap();
    assert!(n.eval(&json!({}), &EvalContext::new()).unwrap());
}

#[test]
fn cond_eval_eq_value_ref() {
    let c = CompiledCond::compile(&json!({"eq": ["$value", "hello"]})).unwrap();
    assert!(c.eval(&json!("hello"), &EvalContext::new()).unwrap());
    assert!(!c.eval(&json!("world"), &EvalContext::new()).unwrap());
}

#[test]
fn cond_eval_exists_nested() {
    let c = CompiledCond::compile(&json!({"exists": "a.b"})).unwrap();
    assert!(
        c.eval(&json!({"a": {"b": 1}}), &EvalContext::new())
            .unwrap()
    );
    assert!(!c.eval(&json!({"a": 1}), &EvalContext::new()).unwrap());
}

#[test]
fn cond_eval_and_or_empty() {
    assert!(
        CompiledCond::And(vec![])
            .eval(&json!({}), &EvalContext::new())
            .unwrap()
    );
    assert!(
        !CompiledCond::Or(vec![])
            .eval(&json!({}), &EvalContext::new())
            .unwrap()
    );
}

#[test]
fn cond_eval_double_not() {
    let inner = CompiledCond::Eq(Arg::Literal(json!(true)), Arg::Literal(json!(true)));
    let c = CompiledCond::Not(Box::new(CompiledCond::Not(Box::new(inner))));
    assert!(c.eval(&json!({}), &EvalContext::new()).unwrap());
}

#[test]
fn cond_eval_missing_ctx_var_is_null() {
    let c = CompiledCond::compile(&json!({"eq": ["$ctx.missing", null]})).unwrap();
    assert!(c.eval(&json!({}), &EvalContext::new()).unwrap());
}

#[test]
fn cond_eval_deeply_nested() {
    let c = CompiledCond::And(vec![
        CompiledCond::Or(vec![
            CompiledCond::Eq(Arg::Literal(json!(1)), Arg::Literal(json!(1))),
            CompiledCond::Not(Box::new(CompiledCond::Eq(
                Arg::Literal(json!(true)),
                Arg::Literal(json!(false)),
            ))),
        ]),
        CompiledCond::Exists(vec!["x".into()]),
    ]);
    assert!(c.eval(&json!({"x": 1}), &EvalContext::new()).unwrap());
    assert!(!c.eval(&json!({}), &EvalContext::new()).unwrap());
}

// ==================== RuleSet 解析 ====================

#[test]
fn ruleset_invalid_json() {
    assert!(RuleSet::from_json("not json").is_err());
}

#[test]
fn ruleset_missing_rules_key() {
    assert!(RuleSet::from_json(r#"{"ops": []}"#).is_err());
}

#[test]
fn ruleset_empty_rules() {
    let rs = RuleSet::from_json(r#"{"rules": []}"#).unwrap();
    let mut body = json!({"x": 1});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body, json!({"x": 1}));
}

// ==================== set ====================

#[test]
fn test_set_field() {
    let rs = RuleSet::from_json(r#"{"rules": [{"type": "set", "path": "stream", "value": true}]}"#)
        .unwrap();
    let mut body = json!({"model": "gpt-4"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["stream"], true);
}

#[test]
fn test_set_nested_field() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "set", "path": "thinking.type", "value": "enabled"}]}"#,
    )
    .unwrap();
    let mut body = json!({"model": "deepseek"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");
}

#[test]
fn set_overwrite() {
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "set", "path": "x", "value": 2}]}"#).unwrap();
    let mut body = json!({"x": 1});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 2);
}

#[test]
fn set_deep_create() {
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "set", "path": "a.b.c.d", "value": "deep"}]}"#)
            .unwrap();
    let mut body = json!({});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["a"]["b"]["c"]["d"], "deep");
}

#[test]
fn set_non_object_mid_error() {
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "set", "path": "x.y", "value": 1}]}"#).unwrap();
    let mut body = json!({"x": 42});
    assert!(rs.apply(&mut body, &EvalContext::new()).is_err());
}

#[test]
fn set_null_value() {
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "set", "path": "x", "value": null}]}"#).unwrap();
    let mut body = json!({"x": 42});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], Value::Null);
}

#[test]
fn set_on_array_root_errors() {
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "set", "path": "x", "value": 1}]}"#).unwrap();
    assert!(rs.apply(&mut json!([]), &EvalContext::new()).is_err());
}

// ==================== remove ====================

#[test]
fn test_remove_field() {
    let rs = RuleSet::from_json(r#"{"rules": [{"type": "remove", "path": "reasoning_effort"}]}"#)
        .unwrap();
    let mut body = json!({"reasoning_effort": "medium", "model": "x"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert!(body.get("reasoning_effort").is_none());
    assert!(body.get("model").is_some());
}

#[test]
fn remove_nested() {
    let rs = RuleSet::from_json(r#"{"rules": [{"type": "remove", "path": "a.b"}]}"#).unwrap();
    let mut body = json!({"a": {"b": 1, "c": 2}});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert!(body["a"].get("b").is_none());
    assert_eq!(body["a"]["c"], 2);
}

#[test]
fn remove_non_existent_silent() {
    let rs = RuleSet::from_json(r#"{"rules": [{"type": "remove", "path": "missing"}]}"#).unwrap();
    let mut body = json!({"x": 1});
    assert!(rs.apply(&mut body, &EvalContext::new()).is_ok());
}

// ==================== map_value ====================

#[test]
fn test_map_value() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [{
                "type": "map_value",
                "path": "reasoning_effort",
                "mappings": {
                    "medium": {"thinking": {"type": "enabled"}},
                    "high": {"thinking": {"type": "enabled"}}
                },
                "default": {"thinking": {"type": "disabled"}},
                "remove_source": true
            }]
        }"#,
    )
    .unwrap();

    let mut body = json!({"reasoning_effort": "medium"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");
    assert!(body.get("reasoning_effort").is_none());

    let mut body = json!({"reasoning_effort": "unknown"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["thinking"]["type"], "disabled");
}

#[test]
fn map_value_source_missing_error() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "map_value", "path": "missing", "mappings": {"a": {"x": 1}}}]}"#,
    )
    .unwrap();
    // 没有匹配到值，不会报错
    assert!(rs.apply(&mut json!({}), &EvalContext::new()).is_ok());
}

#[test]
fn map_value_source_missing_with_default() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "map_value", "path": "foo", "mappings": {"a": {"x": 1}}, "default": {"y": 2}}]}"#,
    )
    .unwrap();
    let mut body = json!({});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["y"], 2);
    assert!(body.get("x").is_none());
}

#[test]
fn map_value_no_match_no_default() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "map_value", "path": "x", "mappings": {"a": {"y": 1}}}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": "unknown"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    // remove_source 默认 false，源字段保留
    assert_eq!(body["x"], "unknown");
    assert!(body.get("y").is_none());
}

#[test]
fn map_value_keep_source() {
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "map_value", "path": "x", "mappings": {"a": {"y": 1}}, "remove_source": false}]}"#,
        )
        .unwrap();
    let mut body = json!({"x": "a"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], "a");
    assert_eq!(body["y"], 1);
}

#[test]
fn map_value_numeric_key() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "map_value", "path": "x", "mappings": {"42": {"y": "answer"}}}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": 42});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["y"], "answer");
}

#[test]
fn map_value_null_key() {
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "map_value", "path": "x", "mappings": {"null": {"y": "was_null"}}}]}"#,
        )
        .unwrap();
    let mut body = json!({"x": null});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["y"], "was_null");
}

#[test]
fn map_value_merge_deep() {
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "map_value", "path": "x", "mappings": {"a": {"deep": {"nested": 1, "extra": 2}}}}]}"#,
        )
        .unwrap();
    let mut body = json!({"x": "a", "deep": {"nested": 0, "other": 3}});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["deep"]["nested"], 1);
    assert_eq!(body["deep"]["extra"], 2);
    // 同级键保留
    assert_eq!(body["deep"]["other"], 3);
}

// ==================== compute ====================

#[test]
fn test_compute() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "max_tokens", "expr": "min($value, 4096)"}]}"#,
    )
    .unwrap();
    let mut body = json!({"max_tokens": 8192});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["max_tokens"], 4096);

    let mut body = json!({"max_tokens": 1024});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["max_tokens"], 1024);
}

#[test]
fn test_compute_with_context() {
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "compute", "path": "max_tokens", "expr": "min($value, 8192 - $ctx.input_tokens)"}]}"#,
        )
        .unwrap();
    let ctx = EvalContext::new().with("input_tokens", 7000);
    let mut body = json!({"max_tokens": 4096});
    rs.apply(&mut body, &ctx).unwrap();
    assert_eq!(body["max_tokens"], 1192);
}

#[test]
fn compute_path_missing_error() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "missing", "expr": "$value + 1"}]}"#,
    )
    .unwrap();
    assert!(rs.apply(&mut json!({}), &EvalContext::new()).is_err());
}

#[test]
fn compute_invalid_expr() {
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "compute", "path": "x", "expr": "$value +"}]}"#)
            .unwrap();
    let mut body = json!({"x": 1});
    assert!(rs.apply(&mut body, &EvalContext::new()).is_err());
}

#[test]
fn compute_string_concat() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "x", "expr": "$value + \"_suffix\""}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": "hello"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], "hello_suffix");
}

#[test]
fn compute_bool_negate() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "flag", "expr": "!$value"}]}"#,
    )
    .unwrap();
    let mut body = json!({"flag": true});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["flag"], false);
}

#[test]
fn compute_arithmetic() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "x", "expr": "($value + 10) * 2"}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": 5});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 30);
}

#[test]
fn compute_float() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "x", "expr": "$value * 1.5"}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": 4});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 6.0);
}

#[test]
fn compute_multiple_ctx_vars() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "r", "expr": "$ctx.a + $ctx.b * $value"}]}"#,
    )
    .unwrap();
    let ctx = EvalContext::new().with("a", 10).with("b", 3);
    let mut body = json!({"r": 2});
    rs.apply(&mut body, &ctx).unwrap();
    assert_eq!(body["r"], 16);
}

#[test]
fn compute_non_value_expr() {
    // 完全忽略 $value 的表达式
    let rs =
        RuleSet::from_json(r#"{"rules": [{"type": "compute", "path": "x", "expr": "1 + 2"}]}"#)
            .unwrap();
    let mut body = json!({"x": null});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 3);
}

#[test]
fn compute_str_len() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "x", "expr": "len($value)"}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": "hello"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 5);
}

// ==================== when ====================

#[test]
fn test_when_eq() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [{
                "type": "when",
                "cond": {"eq": ["$ctx.provider", "deepseek"]},
                "then": [{"type": "set", "path": "thinking.type", "value": "enabled"}]
            }]
        }"#,
    )
    .unwrap();

    let mut body = json!({"model": "test"});
    rs.apply(&mut body, &ctx()).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");

    let other = EvalContext::new().with("provider", "openai");
    let mut body = json!({"model": "test"});
    rs.apply(&mut body, &other).unwrap();
    assert!(body.get("thinking").is_none());
}

#[test]
fn test_when_else() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [{
                "type": "when",
                "cond": {"eq": ["$ctx.provider", "deepseek"]},
                "then": [{"type": "set", "path": "thinking.type", "value": "enabled"}],
                "else": [{"type": "remove", "path": "reasoning_effort"}]
            }]
        }"#,
    )
    .unwrap();

    let mut body = json!({"reasoning_effort": "medium"});
    rs.apply(&mut body, &ctx()).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");

    let other = EvalContext::new().with("provider", "openai");
    let mut body = json!({"reasoning_effort": "medium"});
    rs.apply(&mut body, &other).unwrap();
    assert!(body.get("reasoning_effort").is_none());
    assert!(body.get("thinking").is_none());
}

#[test]
fn test_exists_condition() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [{
                "type": "when",
                "cond": {"exists": "reasoning_effort"},
                "then": [{"type": "set", "path": "thinking.type", "value": "enabled"}]
            }]
        }"#,
    )
    .unwrap();

    let mut body = json!({"reasoning_effort": "medium"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");

    let mut body = json!({"model": "test"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert!(body.get("thinking").is_none());
}

#[test]
fn test_and_or_not() {
    // and
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "when", "cond": {"and": [{"eq": ["$ctx.provider", "deepseek"]}, {"exists": "reasoning_effort"}]}, "then": [{"type": "set", "path": "x", "value": 1}]}]}"#,
        )
        .unwrap();
    let mut body = json!({"reasoning_effort": "medium"});
    rs.apply(&mut body, &ctx()).unwrap();
    assert_eq!(body["x"], 1);

    let mut body = json!({});
    rs.apply(&mut body, &ctx()).unwrap();
    assert!(body.get("x").is_none());

    // or
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "when", "cond": {"or": [{"eq": ["$ctx.provider", "deepseek"]}, {"eq": ["$ctx.provider", "openai"]}]}, "then": [{"type": "set", "path": "y", "value": 1}]}]}"#,
        )
        .unwrap();
    let mut body = json!({});
    rs.apply(&mut body, &EvalContext::new().with("provider", "openai"))
        .unwrap();
    assert_eq!(body["y"], 1);

    // not
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "when", "cond": {"not": {"exists": "reasoning_effort"}}, "then": [{"type": "set", "path": "z", "value": "no_reasoning"}]}]}"#,
        )
        .unwrap();
    let mut body = json!({});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["z"], "no_reasoning");
}

#[test]
fn test_neq_condition() {
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "when", "cond": {"neq": ["$ctx.provider", "deepseek"]}, "then": [{"type": "remove", "path": "reasoning_effort"}]}]}"#,
        )
        .unwrap();
    let mut body = json!({"reasoning_effort": "medium"});
    rs.apply(&mut body, &EvalContext::new().with("provider", "openai"))
        .unwrap();
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn test_deepseek_example() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [{
                "type": "when",
                "cond": {"eq": ["$ctx.provider", "deepseek"]},
                "then": [{
                    "type": "map_value",
                    "path": "reasoning_effort",
                    "mappings": {
                        "medium": {"thinking": {"type": "enabled"}},
                        "high": {"thinking": {"type": "enabled"}}
                    },
                    "default": {"thinking": {"type": "disabled"}},
                    "remove_source": true
                }],
                "else": [{"type": "remove", "path": "reasoning_effort"}]
            }]
        }"#,
    )
    .unwrap();

    let mut body = json!({"model": "deepseek-r1", "reasoning_effort": "medium", "messages": []});
    rs.apply(&mut body, &ctx()).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn when_nested() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [{
                "type": "when",
                "cond": {"eq": ["$ctx.provider", "deepseek"]},
                "then": [{
                    "type": "when",
                    "cond": {"eq": ["$ctx.model", "deepseek-r1"]},
                    "then": [{"type": "set", "path": "x", "value": "r1"}],
                    "else": [{"type": "set", "path": "x", "value": "other"}]
                }]
            }]
        }"#,
    )
    .unwrap();

    let mut body = json!({});
    rs.apply(&mut body, &ctx()).unwrap();
    assert_eq!(body["x"], "r1");

    let ctx2 = EvalContext::new()
        .with("provider", "deepseek")
        .with("model", "v3");
    let mut body = json!({});
    rs.apply(&mut body, &ctx2).unwrap();
    assert_eq!(body["x"], "other");
}

#[test]
fn when_false_without_else_is_noop() {
    let rs = RuleSet::from_json(
            r#"{"rules": [{"type": "when", "cond": {"eq": [1, 2]}, "then": [{"type": "set", "path": "x", "value": 1}]}]}"#,
        )
        .unwrap();
    let mut body = json!({"y": 2});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body, json!({"y": 2}));
}

// ==================== 链式规则 ====================

#[test]
fn chained_rules_see_prior_changes() {
    let rs = RuleSet::from_json(
            r#"{
            "rules": [
                {"type": "when", "cond": {"exists": "a"}, "then": [{"type": "set", "path": "b", "value": 1}]},
                {"type": "when", "cond": {"exists": "b"}, "then": [{"type": "set", "path": "c", "value": 2}]}
            ]
        }"#,
        )
        .unwrap();
    let mut body = json!({"a": true});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["b"], 1);
    assert_eq!(body["c"], 2);
}

#[test]
fn set_then_compute() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [
                {"type": "set", "path": "x", "value": 10},
                {"type": "compute", "path": "x", "expr": "$value * $value"}
            ]
        }"#,
    )
    .unwrap();
    let mut body = json!({});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 100);
}

#[test]
fn set_then_remove_same_path() {
    let rs = RuleSet::from_json(
        r#"{
            "rules": [
                {"type": "set", "path": "x", "value": 1},
                {"type": "remove", "path": "x"}
            ]
        }"#,
    )
    .unwrap();
    let mut body = json!({});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert!(body.get("x").is_none());
}

#[test]
fn map_value_then_compute() {
    let rs = RuleSet::from_json(
            r#"{
            "rules": [
                {"type": "map_value", "path": "level", "mappings": {"high": {"count": 100}}, "default": {"count": 10}},
                {"type": "compute", "path": "count", "expr": "$value * 2"}
            ]
        }"#,
        )
        .unwrap();
    let mut body = json!({"level": "high"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["count"], 200);
}

// ==================== 杂项 ====================

#[test]
fn neq_with_value_ref_body() {
    let c = CompiledCond::compile(&json!({"neq": ["$value", "str"]})).unwrap();
    assert!(c.eval(&json!({"a": 1}), &EvalContext::new()).unwrap());
}

#[test]
fn compute_math_max() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "compute", "path": "x", "expr": "max(0, $value)"}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": -5});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], 0);
}

#[test]
fn multi_rule_all_types() {
    let rs = RuleSet::from_json(
            r#"{
            "rules": [
                {"type": "set", "path": "stream", "value": true},
                {"type": "map_value", "path": "mode", "mappings": {"fast": {"speed": "high"}}, "default": {"speed": "low"}, "remove_source": true},
                {"type": "compute", "path": "max_tokens", "expr": "min($value, 8192)"}
            ]
        }"#,
        )
        .unwrap();
    let mut body = json!({"mode": "fast", "max_tokens": 16384});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["stream"], true);
    assert!(body.get("mode").is_none());
    assert_eq!(body["speed"], "high");
    assert_eq!(body["max_tokens"], 8192);
}

// ==================== 嵌套上下文路径 ====================

#[test]
fn a_context_variable_can_be_a_nested_path() {
    // 规则里写 $ctx.provider.name，上下文按路径取
    let ctx = EvalContext::new().with("provider", json!({ "name": "deepseek" }));
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"when",
             "cond":{"eq":["$ctx.provider.name","deepseek"]},
             "then":[{"type":"set","path":"hit","value":true}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({});
    rules.apply(&mut body, &ctx).expect("evaluate");
    assert_eq!(body["hit"], json!(true));
}

#[test]
fn a_context_path_that_does_not_resolve_is_not_an_error() {
    // 条件取不到值时按不成立处理，不是让整条链路失败
    let ctx = EvalContext::new();
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"when",
             "cond":{"eq":["$ctx.provider.name","deepseek"]},
             "then":[{"type":"set","path":"hit","value":true}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({});
    rules.apply(&mut body, &ctx).expect("evaluate");
    assert!(
        body.get("hit").is_none(),
        "then must not run when the condition does not hold"
    );
}

// ==================== for_each ====================

/// 只删 assistant 元素的字段，其它元素与字段原样保留
const STRIP_ASSISTANT_REASONING: &str = r#"{
    "rules": [{
        "type": "for_each",
        "path": "messages",
        "then": [{
            "type": "when",
            "cond": { "eq": ["$value.role", "assistant"] },
            "then": [{ "type": "remove", "path": "reasoning_content" }]
        }]
    }]
}"#;

#[test]
fn for_each_applies_to_every_element() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"messages",
             "then":[{"type":"remove","path":"reasoning_content"}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({
        "messages": [
            { "role": "assistant", "content": "a", "reasoning_content": "r1" },
            { "role": "assistant", "content": "b", "reasoning_content": "r2" },
        ]
    });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["messages"][0].get("reasoning_content"), None);
    assert_eq!(body["messages"][1].get("reasoning_content"), None);
    assert_eq!(body["messages"][0]["content"], json!("a"));
    assert_eq!(body["messages"][1]["content"], json!("b"));
}

#[test]
fn for_each_runs_each_element_as_its_root() {
    // 子规则写入的是当前元素，不是整个请求体
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"messages",
             "then":[{"type":"set","path":"meta.seen","value":true}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "messages": [{ "role": "user" }, { "role": "user" }] });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["messages"][0]["meta"]["seen"], json!(true));
    assert_eq!(body["messages"][1]["meta"]["seen"], json!(true));
    assert!(
        body.get("meta").is_none(),
        "写入必须落在元素里，不能逃到根上"
    );
}

#[test]
fn for_each_conditions_read_the_element() {
    let rules = RuleSet::from_json(STRIP_ASSISTANT_REASONING).expect("compile");
    let mut body = json!({
        "messages": [
            { "role": "assistant", "reasoning_content": "r1" },
            { "role": "user", "reasoning_content": "keep" },
            { "role": "tool", "reasoning_content": "keep" },
        ]
    });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["messages"][0].get("reasoning_content"), None);
    assert_eq!(body["messages"][1]["reasoning_content"], json!("keep"));
    assert_eq!(body["messages"][2]["reasoning_content"], json!("keep"));
}

#[test]
fn for_each_skips_elements_that_are_not_objects() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"messages",
             "then":[{"type":"remove","path":"reasoning_content"}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({
        "messages": [
            "naked",
            null,
            { "role": "assistant", "reasoning_content": "r" },
            42
        ]
    });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["messages"][0], json!("naked"));
    assert_eq!(body["messages"][1], json!(null));
    assert_eq!(body["messages"][2].get("reasoning_content"), None);
    assert_eq!(body["messages"][3], json!(42));
}

#[test]
fn for_each_of_an_absent_path_is_a_no_op() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"messages",
             "then":[{"type":"remove","path":"reasoning_content"}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "model": "m" });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body, json!({ "model": "m" }));
}

#[test]
fn for_each_refuses_a_value_that_is_not_an_array() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"messages",
             "then":[{"type":"remove","path":"reasoning_content"}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "messages": { "role": "user" } });
    let error = rules
        .apply(&mut body, &EvalContext::new())
        .expect_err("结构不符必须报错");
    assert!(error.to_string().contains("messages"), "{error}");
}

#[test]
fn for_each_propagates_a_failing_child_rule() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"messages",
             "then":[{"type":"remove","path":"meta.flag"}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "messages": [{ "meta": "不是对象" }] });
    assert!(
        rules.apply(&mut body, &EvalContext::new()).is_err(),
        "子规则失败不能被吞掉"
    );
}

#[test]
fn for_each_can_nest() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"for_each","path":"groups",
             "then":[{"type":"for_each","path":"items",
                      "then":[{"type":"remove","path":"tmp"}]}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({
        "groups": [
            { "items": [{ "tmp": 1 }, { "keep": 2 }] },
            { "items": [{ "tmp": 3 }] },
        ]
    });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["groups"][0]["items"][0].get("tmp"), None);
    assert_eq!(body["groups"][0]["items"][1]["keep"], json!(2));
    assert_eq!(body["groups"][1]["items"][0].get("tmp"), None);
}

// ==================== $value 路径 ====================

#[test]
fn a_value_path_reads_a_field_of_the_current_root() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"when",
             "cond":{"eq":["$value.role","assistant"]},
             "then":[{"type":"set","path":"hit","value":true}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "role": "assistant" });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["hit"], json!(true));
}

#[test]
fn a_value_path_reads_a_nested_field() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"when",
             "cond":{"eq":["$value.meta.role","assistant"]},
             "then":[{"type":"set","path":"hit","value":true}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "meta": { "role": "assistant" } });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert_eq!(body["hit"], json!(true));
}

#[test]
fn a_value_path_that_does_not_resolve_is_not_an_error() {
    let rules = RuleSet::from_json(
        r#"{"rules":[{"type":"when",
             "cond":{"eq":["$value.missing.deep","assistant"]},
             "then":[{"type":"set","path":"hit","value":true}]}]}"#,
    )
    .expect("compile");
    let mut body = json!({ "role": "assistant" });
    rules.apply(&mut body, &EvalContext::new()).expect("apply");
    assert!(body.get("hit").is_none(), "取不到值时条件不成立");
}
