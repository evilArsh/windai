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
    assert_eq!(path::get(&v, &path::segments("a.b.c")).unwrap(), &json!(42));
}

#[test]
fn path_get_missing_key() {
    let v = json!({"a": 1});
    assert!(path::get(&v, &path::segments("b")).is_err());
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
    // HashMap::remove ignores missing keys — succeeds silently
    let mut v = json!({"x": 1});
    assert!(path::remove(&mut v, &path::segments("missing")).is_ok());
}

#[test]
fn path_remove_through_non_object() {
    let mut v = json!({"a": 1});
    assert!(path::remove(&mut v, &path::segments("a.b")).is_err());
}

// ==================== condition compile errors ====================

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

// ==================== condition eval ====================

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

// ==================== RuleSet parse ====================

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
    assert!(rs.apply(&mut json!({}), &EvalContext::new()).is_err());
}

#[test]
fn map_value_no_match_no_default() {
    let rs = RuleSet::from_json(
        r#"{"rules": [{"type": "map_value", "path": "x", "mappings": {"a": {"y": 1}}}]}"#,
    )
    .unwrap();
    let mut body = json!({"x": "unknown"});
    rs.apply(&mut body, &EvalContext::new()).unwrap();
    assert_eq!(body["x"], "unknown"); // source preserved (remove_source defaults false)
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
    assert_eq!(body["deep"]["other"], 3); // existing sibling preserved
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
    // expr that ignores $value entirely
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

// ==================== chained rules ====================

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

// ==================== misc ====================

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
