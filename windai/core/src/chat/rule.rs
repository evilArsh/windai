use crate::models::JsonRule;
use serde_json::Value;
use wind_ai::provider::adaptor::get_default_endpoint;
use wind_rule::{EvalContext, RuleSet};

/// 构建传递给JsonRule转换函数的上下文对象。
fn build_context(
    provider_name: &str,
    model_name: &str,
    endpoint: &str,
    adaptor: &str,
) -> EvalContext {
    EvalContext::from(serde_json::json!({
        "provider": provider_name,
        "model": model_name,
        "endpoint": endpoint,
        "adaptor": adaptor,
    }))
}

/// 执行 Json规则 转换
/// - json_rule 为空或者不存在时，返回原始数据
pub fn apply_json_rule(
    rule: &RuleSet,
    json_rule: &Option<JsonRule>,
    body: &mut Value,
    provider_name: &str,
    model_name: &str,
    endpoint: Option<&str>,
) {
    let (json_rule, adaptor) = match json_rule.as_ref() {
        Some(hook) if !hook.json_rule.is_empty() => (hook.json_rule.as_str(), hook.adaptor),
        _ => return,
    };
    let context = build_context(
        provider_name,
        model_name,
        endpoint.unwrap_or(get_default_endpoint(adaptor).as_str()),
        &adaptor.to_string(),
    );

    log::debug!(
        "[transform_json_rule]\nbody:\n{}\nrule:\n{}",
        serde_json::to_string_pretty(&body).unwrap_or_default(),
        json_rule
    );
    let _ = rule.apply(body, &context);
}
