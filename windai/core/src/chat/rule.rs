use crate::error::Result;
use crate::models::JsonRule;
use serde_json::Value;
use wind_ai::{model::AdaptorType, provider::adaptor::get_default_endpoint};
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

pub fn build_rule(rule: Option<JsonRule>) -> Result<Option<RuleSet>> {
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
    adaptor: AdaptorType,
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
            .unwrap_or(get_default_endpoint(adaptor).as_str()),
        &adaptor.to_string(),
    );

    let _ = rule.apply(body, &context);
}
