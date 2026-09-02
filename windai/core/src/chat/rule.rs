use crate::error::Result;
use crate::models::JsonRule;
use serde_json::Value;
use wind_ai::{model::AdapterType, provider::adapter::get_default_endpoint};
use wind_rule::{EvalContext, RuleSet};

/// 构建传递给JsonRule转换函数的上下文对象。
fn build_context(
    provider_name: &str,
    model_name: &str,
    endpoint: &str,
    adapter: &str,
) -> EvalContext {
    EvalContext::from(serde_json::json!({
        "provider": provider_name,
        "model": model_name,
        "endpoint": endpoint,
        "adapter": adapter,
    }))
}

pub fn build_rule(rule: Option<&JsonRule>) -> Result<Option<RuleSet>> {
    let rule = match rule {
        Some(v) if !v.json_rule.is_empty() => Some(RuleSet::from_json(&v.json_rule)?),
        _ => None,
    };
    Ok(rule)
}

/// 执行 Json规则 转换
/// - rule 为空时，不做处理
pub fn apply_json_rule(
    rule: Option<&RuleSet>,
    body: &mut Value,
    adapter: AdapterType,
    provider_name: &str,
    model_name: &str,
    endpoint: Option<&str>,
) {
    let rule = match rule {
        Some(rule) => rule,
        None => return,
    };
    let context = build_context(
        provider_name,
        model_name,
        endpoint
            .as_deref()
            .unwrap_or(get_default_endpoint(adapter).as_str()),
        &adapter.to_string(),
    );

    let _ = rule.apply(body, &context);
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use wind_ai::{chat::build_request, message::ReqConfig, provider::adapter::get_chat_adapter};

    const REASONING_RULE: &str = r#"{
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
    }"#;

    // ---- helpers ----

    fn make_rule(adapter: AdapterType, json_rule: &str) -> JsonRule {
        JsonRule {
            id: 0,
            provider_id: 0,
            adapter,
            json_rule: json_rule.to_string(),
            active: true,
            created_at: 0,
        }
    }

    /// 通过真实的 adapter 生成请求体，保证规则作用在真实字段名上
    fn build_body(adapter: AdapterType, config: &ReqConfig) -> Value {
        let chat_adapter = get_chat_adapter(adapter);
        build_request(chat_adapter.as_ref(), "test-model", config, &[], None).unwrap()
    }

    /// 编译 JSON 规则字符串并应用到请求体上（等价于生产路径）
    fn apply_rule_json(
        body: &mut Value,
        adapter: AdapterType,
        rule_json: &str,
        provider: &str,
        model: &str,
        endpoint: Option<&str>,
    ) {
        let rule_set = build_rule(Some(&make_rule(adapter, rule_json)))
            .unwrap()
            .unwrap();
        apply_json_rule(Some(&rule_set), body, adapter, provider, model, endpoint);
    }

    // ---- build_rule 转换 ----

    #[test]
    fn build_rule_filters_empty_and_parses() {
        // None -> None
        assert!(build_rule(None).unwrap().is_none());
        // 空规则字符串 -> None
        assert!(
            build_rule(Some(&make_rule(AdapterType::OpenAICompletion, "")))
                .unwrap()
                .is_none()
        );
        // 合法规则 -> Some(RuleSet)
        assert!(
            build_rule(Some(&make_rule(
                AdapterType::OpenAICompletion,
                REASONING_RULE
            )))
            .unwrap()
            .is_some()
        );
        // 非法 JSON -> Err
        assert!(build_rule(Some(&make_rule(AdapterType::OpenAICompletion, "not json"))).is_err());
    }

    // ---- apply_json_rule 边界 ----

    #[test]
    fn apply_rule_none_or_empty_is_noop() {
        // rule 为 None 时不做任何处理
        let mut body = json!({"model": "test-model"});
        apply_json_rule(
            None,
            &mut body,
            AdapterType::OpenAICompletion,
            "p",
            "m",
            None,
        );
        assert_eq!(body, json!({"model": "test-model"}));

        // 空规则集编译为 Some，但 apply 无副作用
        let mut body = json!({"model": "test-model"});
        apply_rule_json(
            &mut body,
            AdapterType::OpenAICompletion,
            r#"{"rules": []}"#,
            "p",
            "m",
            None,
        );
        assert_eq!(body, json!({"model": "test-model"}));
    }

    // ---- OpenAICompletion 集成（/chat/completions） ----

    #[test]
    fn test_json_rule_reasoning_enabled() {
        let mut config = ReqConfig::default();
        config.reasoning = Some(true);
        let mut req_body = build_body(AdapterType::OpenAICompletion, &config);
        // 前置：adapter 生成了可被规则消费的 reasoning_effort 字段
        assert_eq!(req_body["reasoning_effort"], "medium");

        apply_rule_json(
            &mut req_body,
            AdapterType::OpenAICompletion,
            REASONING_RULE,
            "provider_name",
            "model_name",
            None,
        );

        // 映射生效：medium -> thinking.type=enabled
        assert_eq!(req_body["thinking"]["type"], "enabled");
        // remove_source=true 移除源字段
        assert!(req_body.get("reasoning_effort").is_none());
    }

    #[test]
    fn test_json_rule_reasoning_disabled() {
        // reasoning 未开启时 adapter 不生成 reasoning_effort
        let mut config = ReqConfig::default();
        config.reasoning = Some(false);
        let mut req_body = build_body(AdapterType::OpenAICompletion, &config);
        assert!(req_body.get("reasoning_effort").is_none());

        apply_rule_json(
            &mut req_body,
            AdapterType::OpenAICompletion,
            REASONING_RULE,
            "provider_name",
            "model_name",
            None,
        );

        // 无源值匹配时走 default 分支
        assert_eq!(req_body["thinking"]["type"], "disabled");
    }

    #[test]
    fn completion_compute_caps_max_tokens() {
        let mut config = ReqConfig::default();
        config.max_tokens = Some(8192);
        let mut req_body = build_body(AdapterType::OpenAICompletion, &config);
        assert_eq!(req_body["max_completion_tokens"], 8192);

        apply_rule_json(
            &mut req_body,
            AdapterType::OpenAICompletion,
            r#"{"rules": [{"type": "compute", "path": "max_completion_tokens", "expr": "min($value, 2048)"}]}"#,
            "p",
            "m",
            None,
        );

        assert_eq!(req_body["max_completion_tokens"], 2048);
    }

    #[test]
    fn completion_set_and_remove_fields() {
        let mut config = ReqConfig::default();
        config.stream = Some(false);
        config.temperature = Some(0.5);
        let mut req_body = build_body(AdapterType::OpenAICompletion, &config);
        assert_eq!(req_body["stream"], false);
        assert_eq!(req_body["temperature"], 0.5);

        // 真实调优场景：强制流式输出 + 去掉 temperature
        apply_rule_json(
            &mut req_body,
            AdapterType::OpenAICompletion,
            r#"{"rules": [
                {"type": "set", "path": "stream", "value": true},
                {"type": "remove", "path": "temperature"}
            ]}"#,
            "p",
            "m",
            None,
        );

        assert_eq!(req_body["stream"], true);
        assert!(req_body.get("temperature").is_none());
    }

    // ---- OpenAIResponse 集成（/responses） ----

    #[test]
    fn responses_set_reasoning_effort() {
        let mut config = ReqConfig::default();
        config.reasoning = Some(true);
        let mut req_body = build_body(AdapterType::OpenAIResponse, &config);
        // responses API 用嵌套对象 reasoning.effort，而非 completion 的 reasoning_effort 字符串
        assert_eq!(req_body["reasoning"]["effort"], "medium");

        apply_rule_json(
            &mut req_body,
            AdapterType::OpenAIResponse,
            r#"{"rules": [{"type": "set", "path": "reasoning.effort", "value": "high"}]}"#,
            "p",
            "m",
            None,
        );

        assert_eq!(req_body["reasoning"]["effort"], "high");
    }

    #[test]
    fn responses_compute_caps_max_output_tokens() {
        let mut config = ReqConfig::default();
        config.max_tokens = Some(4096);
        let mut req_body = build_body(AdapterType::OpenAIResponse, &config);
        assert_eq!(req_body["max_output_tokens"], 4096);

        apply_rule_json(
            &mut req_body,
            AdapterType::OpenAIResponse,
            r#"{"rules": [{"type": "compute", "path": "max_output_tokens", "expr": "min($value, 1024)"}]}"#,
            "p",
            "m",
            None,
        );

        assert_eq!(req_body["max_output_tokens"], 1024);
    }

    // ---- context 注入（provider/model/endpoint/adapter） ----

    #[test]
    fn context_injects_provider_and_model() {
        let mut body = json!({"model": "test-model"});
        apply_rule_json(
            &mut body,
            AdapterType::OpenAICompletion,
            r#"{"rules": [{"type": "when", "cond": {"and": [
                {"eq": ["$ctx.provider", "deepseek"]},
                {"eq": ["$ctx.model", "deepseek-r1"]}
            ]}, "then": [{"type": "set", "path": "x", "value": "r1"}]}]}"#,
            "deepseek",
            "deepseek-r1",
            None,
        );
        assert_eq!(body["x"], "r1");
    }

    #[test]
    fn context_endpoint_defaults_per_adapter() {
        let rule = r#"{"rules": [
            {"type": "when", "cond": {"eq": ["$ctx.endpoint", "/responses"]}, "then": [{"type": "set", "path": "x", "value": "resp"}]},
            {"type": "when", "cond": {"eq": ["$ctx.endpoint", "/chat/completions"]}, "then": [{"type": "set", "path": "y", "value": "completion"}]}
        ]}"#;

        // completion 缺省 endpoint -> 默认 /chat/completions
        let mut body = json!({});
        apply_rule_json(
            &mut body,
            AdapterType::OpenAICompletion,
            rule,
            "p",
            "m",
            None,
        );
        assert!(body.get("x").is_none());
        assert_eq!(body["y"], "completion");

        // responses 缺省 endpoint -> 默认 /responses
        let mut body = json!({});
        apply_rule_json(&mut body, AdapterType::OpenAIResponse, rule, "p", "m", None);
        assert_eq!(body["x"], "resp");
        assert!(body.get("y").is_none());
    }

    #[test]
    fn context_custom_endpoint_overrides_default() {
        let mut body = json!({});
        apply_rule_json(
            &mut body,
            AdapterType::OpenAICompletion,
            r#"{"rules": [{"type": "when", "cond": {"eq": ["$ctx.endpoint", "https://proxy.example/v1"]}, "then": [{"type": "set", "path": "x", "value": 1}]}]}"#,
            "p",
            "m",
            Some("https://proxy.example/v1"),
        );
        assert_eq!(body["x"], 1);
    }

    #[test]
    fn context_injects_adapter_name() {
        let rule = r#"{"rules": [
            {"type": "when", "cond": {"eq": ["$ctx.adapter", "OpenAICompletion"]}, "then": [{"type": "set", "path": "a", "value": 1}]},
            {"type": "when", "cond": {"eq": ["$ctx.adapter", "OpenAIResponse"]}, "then": [{"type": "set", "path": "b", "value": 1}]}
        ]}"#;

        let mut body = json!({});
        apply_rule_json(
            &mut body,
            AdapterType::OpenAICompletion,
            rule,
            "p",
            "m",
            None,
        );
        assert_eq!(body["a"], 1);
        assert!(body.get("b").is_none());

        let mut body = json!({});
        apply_rule_json(&mut body, AdapterType::OpenAIResponse, rule, "p", "m", None);
        assert_eq!(body["b"], 1);
        assert!(body.get("a").is_none());
    }

    // ---- 错误处理 ----

    #[test]
    fn rule_apply_error_is_silently_ignored() {
        // 路径中段是标量（x=42），set x.y 会失败；apply_json_rule 不传播错误、不 panic
        let mut body = json!({"x": 42});
        apply_rule_json(
            &mut body,
            AdapterType::OpenAICompletion,
            r#"{"rules": [{"type": "set", "path": "x.y", "value": 1}]}"#,
            "p",
            "m",
            None,
        );
        assert_eq!(body, json!({"x": 42}));
    }
}
