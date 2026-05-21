use crate::{error::CoreError, models::JsHookCode, storage::provider::ProviderService};
use serde_json::Value;
use wind_ai::{model::AdaptorType, provider::adaptor::get_default_endpoint};
use wind_js::JsEngine;

/// 通过（provider_id, adaptor）从 js_hooks 表中查找 JS 钩子代码。
pub async fn lookup_js_hook(
    svc: &ProviderService,
    provider_id: i64,
    adaptor: AdaptorType,
) -> Result<Option<JsHookCode>, CoreError> {
    let row = svc.get_js_hook_code(provider_id, adaptor).await?;
    Ok(row)
}

/// 构建传递给JS转换函数的上下文对象。
fn build_context(provider_name: &str, model_name: &str, endpoint: &str, adaptor: &str) -> Value {
    serde_json::json!({
        "provider": provider_name,
        "model": model_name,
        "endpoint": endpoint,
        "adaptor": adaptor,
    })
}

/// 使用提供的JsEngine实例执行JS转换。
fn transform_with_engine(
    js_engine: &JsEngine,
    js_code: &str,
    body: Value,
    context: Value,
) -> Result<Value, CoreError> {
    log::debug!(
        "[transform_by_js]\nbody:\n{}\ncode:\n{}",
        serde_json::to_string_pretty(&body).unwrap_or_default(),
        js_code
    );
    let res = js_engine
        .transform(js_code, body, context)
        .map_err(|e| CoreError::Js(e.to_string()));
    res
}

/// 执行 JavaScript 转换
/// - js_code 为空或者不存在时，返回原始数据
pub async fn apply_js_hook(
    js_engine: &JsEngine,
    js_hook: Option<&JsHookCode>,
    body: Value,
    provider_name: &str,
    model_name: &str,
    endpoint: Option<&str>,
) -> Result<Value, CoreError> {
    let (js_code, adaptor) = match js_hook.as_deref() {
        Some(hook) if !hook.js_code.is_empty() => (hook.js_code.as_ref(), hook.adaptor),
        _ => return Ok(body),
    };
    let context = build_context(
        provider_name,
        model_name,
        endpoint.unwrap_or(get_default_endpoint(adaptor).as_str()),
        &adaptor.to_string(),
    );
    transform_with_engine(js_engine, js_code, body, context)
}
