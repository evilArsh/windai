use rquickjs::{Context, Ctx, Runtime, Value};
use serde::Serialize;

#[derive(thiserror::Error, Debug, Serialize)]
pub enum JsError {
    #[error("Js runtime error: {0}")]
    Runtime(String),
    #[error("JSON serialization failed: {0}")]
    Serialize(String),
}

pub struct JsEngine {
    rt: Runtime,
}

impl JsEngine {
    pub fn new() -> Result<Self, JsError> {
        let rt = Runtime::new().map_err(|e| JsError::Runtime(e.to_string()))?;
        Ok(JsEngine { rt })
    }

    /// 用户在js代码中处理 `user_input` 和 `user_context` 数据。
    ///
    /// 用户输入的代码必须包含一个 transform 函数，并且返回一个有效值。
    /// ```js
    ///   ... // 其他代码
    ///   function transform(body, context) { ... return body; }
    /// ```
    /// ```
    pub fn transform(
        &self,
        js_code: &str,
        user_input: serde_json::Value,
        user_context: serde_json::Value,
    ) -> Result<serde_json::Value, JsError> {
        let context = Context::full(&self.rt).map_err(|e| JsError::Runtime(e.to_string()))?;
        let result: rquickjs::Result<serde_json::Value> = context.with(|js_ctx| {
            let globals = js_ctx.globals();
            globals.set("body", to_qjs(&js_ctx, user_input)?)?;
            globals.set("context", to_qjs(&js_ctx, user_context)?)?;

            let null_val = Value::new_null(js_ctx.clone());
            let _ = globals.set("eval", null_val.clone());

            js_ctx.eval::<rquickjs::Value, _>(js_code)?;

            if !globals.contains_key("transform")? {
                return Err(rquickjs::Error::new_from_js_message(
                    "function",
                    "global",
                    "JS code must define a 'transform(body, context)' function",
                ));
            }
            let transform: rquickjs::Function = globals.get("transform")?;
            let body_val: rquickjs::Value = globals.get("body")?;
            let ctx_val: rquickjs::Value = globals.get("context")?;
            let result = transform.call::<_, rquickjs::Value>((body_val, ctx_val))?;

            from_qjs(&js_ctx, result)
        });

        result.map_err(|e| JsError::Runtime(e.to_string()))
    }
}

fn to_qjs<'js>(ctx: &Ctx<'js>, value: serde_json::Value) -> rquickjs::Result<Value<'js>> {
    match value {
        serde_json::Value::Null => Ok(Value::new_null(ctx.clone())),
        serde_json::Value::Bool(b) => Ok(Value::new_bool(ctx.clone(), b)),
        serde_json::Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            Ok(Value::new_float(ctx.clone(), f))
        }
        serde_json::Value::String(s) => {
            let s = rquickjs::String::from_str(ctx.clone(), &s)?;
            Ok(s.into_value())
        }
        serde_json::Value::Array(arr) => {
            let js_arr = rquickjs::Array::new(ctx.clone())?;
            for (i, v) in arr.into_iter().enumerate() {
                js_arr.set(i, to_qjs(ctx, v)?)?;
            }
            Ok(js_arr.into_value())
        }
        serde_json::Value::Object(map) => {
            let obj = rquickjs::Object::new(ctx.clone())?;
            for (k, v) in map {
                obj.set(k, to_qjs(ctx, v)?)?;
            }
            Ok(obj.into_value())
        }
    }
}

fn from_qjs<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> rquickjs::Result<serde_json::Value> {
    use rquickjs::Type;
    match value.type_of() {
        Type::Bool => {
            let b: bool = value.as_bool().unwrap_or(false);
            Ok(serde_json::Value::Bool(b))
        }
        Type::Int => {
            let n: i32 = value.as_int().unwrap_or(0);
            Ok(serde_json::Value::Number(n.into()))
        }
        Type::Float => {
            let n: f64 = value.as_float().unwrap_or(0.0);
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(serde_json::Value::Number((n as i64).into()))
            } else {
                let nf = serde_json::Number::from_f64(n)
                    .unwrap_or_else(|| serde_json::Number::from_f64(0.0).unwrap());
                Ok(serde_json::Value::Number(nf))
            }
        }
        Type::String => {
            let s: rquickjs::String = value.clone().into_string().unwrap();
            Ok(serde_json::Value::String(s.to_string()?))
        }
        Type::Array => {
            let arr: rquickjs::Array = value.clone().into_array().unwrap();
            let len = arr.len();
            let mut vec = Vec::with_capacity(len);
            for i in 0..len {
                let v: Value = arr.get(i)?;
                vec.push(from_qjs(ctx, v)?);
            }
            Ok(serde_json::Value::Array(vec))
        }
        Type::Object => {
            let obj: rquickjs::Object = value.clone().into_object().unwrap();
            let mut map = serde_json::Map::new();
            for pair in obj.props::<Value, Value>() {
                let (k, v) = pair?;
                let key: String = k
                    .into_string()
                    .ok_or_else(|| {
                        rquickjs::Error::new_from_js_message(
                            "string",
                            "object key",
                            "key must be a string",
                        )
                    })?
                    .to_string()?;
                map.insert(key, from_qjs(ctx, v)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Type::Function | Type::Exception | Type::Null | Type::Undefined => {
            Err(rquickjs::Error::new_from_js_message(
                "object",
                "return value",
                "transform must return a plain object or value",
            ))
        }
        _ => Err(rquickjs::Error::new_from_js_message(
            "object",
            "return value",
            "unsupported JS value type from transform",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> serde_json::Value {
        serde_json::json!({
            "provider": "openrouter",
            "model": "deepseek-r1",
            "endpoint": "/chat/completions",
            "adaptor": "openai_completion"
        })
    }

    fn engine() -> JsEngine {
        JsEngine::new().unwrap()
    }

    // --- 基础变换 ---

    #[test]
    fn test_add_field() {
        let code = r#"
function transform(body, context) {
    body.stream = true;
    return body;
}"#;
        let body = serde_json::json!({"model": "gpt-4", "messages": []});
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["stream"], true);
        assert_eq!(result["model"], "gpt-4");
    }

    #[test]
    fn test_remove_field() {
        let code = r#"
function transform(body, context) {
    delete body.reasoning_effort;
    return body;
}"#;
        let body = serde_json::json!({"model": "gpt-4", "reasoning_effort": "high"});
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result.get("reasoning_effort"), None);
    }

    #[test]
    fn test_modify_field() {
        let code = r#"
function transform(body, context) {
    body.temperature = Math.min(body.temperature, 1.0);
    return body;
}"#;
        let body = serde_json::json!({"model": "gpt-4", "temperature": 2.0});
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["temperature"], 1);
    }

    #[test]
    fn test_idempotent_no_changes() {
        let code = r#"
function transform(body, context) {
    return body;
}"#;
        let body = serde_json::json!({"model": "gpt-4", "stream": false});
        let result = engine().transform(code, body.clone(), test_ctx()).unwrap();
        assert_eq!(result, body);
    }

    // --- 提供商适配场景 ---

    #[test]
    fn test_reasoning_to_think() {
        // 模拟将通用 reasoning 转为某提供商的 think 参数
        let code = r#"
function transform(body, context) {
    if (body.reasoning_effort) {
        body.extra_body = body.extra_body || {};
        body.extra_body.think = true;
        delete body.reasoning_effort;
    }
    return body;
}"#;
        let body = serde_json::json!({
            "model": "deepseek-r1",
            "reasoning_effort": "high",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["extra_body"]["think"], true);
        assert_eq!(result.get("reasoning_effort"), None);
    }

    #[test]
    fn test_provider_conditional_logic() {
        let code = r#"
function transform(body, context) {
    if (context.provider === "openrouter") {
        body.extra_headers = {"HTTP-Referer": "https://example.com"};
    }
    return body;
}"#;
        let body = serde_json::json!({"model": "gpt-4"});
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(
            result["extra_headers"]["HTTP-Referer"],
            "https://example.com"
        );
    }

    #[test]
    fn test_adaptor_specific_mapping() {
        let code = r#"
function transform(body, context) {
    if (context.adaptor === "openai_completion") {
        body.max_completion_tokens = body.max_tokens;
        delete body.max_tokens;
    } else if (context.adaptor === "openai_response") {
        body.max_output_tokens = body.max_tokens;
        delete body.max_tokens;
    }
    return body;
}"#;
        let body = serde_json::json!({"model": "gpt-4", "max_tokens": 4096});
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["max_completion_tokens"], 4096);
        assert_eq!(result.get("max_tokens"), None);
    }

    // --- 复杂结构 ---

    #[test]
    fn test_nested_object_manipulation() {
        let code = r#"
function transform(body, context) {
    body.reasoning = { type: "enabled", effort: "high" };
    body.nested.deep.value = 42;
    return body;
}"#;
        let body = serde_json::json!({
            "model": "deepseek-r1",
            "nested": { "deep": { "x": 1 } }
        });
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["reasoning"]["type"], "enabled");
        assert_eq!(result["nested"]["deep"]["value"], 42);
    }

    #[test]
    fn test_array_operations() {
        let code = r#"
function transform(body, context) {
    body.tools.push({ type: "function", name: "extra_tool" });
    return body;
}"#;
        let body = serde_json::json!({
            "model": "gpt-4",
            "tools": [{"type": "function", "name": "search"}]
        });
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["tools"].as_array().unwrap().len(), 2);
        assert_eq!(result["tools"][1]["name"], "extra_tool");
    }

    #[test]
    fn test_deep_clone_and_mutate() {
        let code = r#"
function transform(body, context) {
    var messages = JSON.parse(JSON.stringify(body.messages));
    messages[0].content = "modified";
    body.messages = messages;
    return body;
}"#;
        let body = serde_json::json!({
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let result = engine().transform(code, body, test_ctx()).unwrap();
        assert_eq!(result["messages"][0]["content"], "modified");
    }

    // --- 错误场景 ---

    #[test]
    fn test_missing_transform_errors() {
        let code = r#"// just a comment, no transform function"#;
        let body = serde_json::json!({});
        let err = engine().transform(code, body, test_ctx()).unwrap_err();
        assert!(err.to_string().contains("transform"));
    }

    #[test]
    fn test_syntax_error() {
        let code = r#"function transform( { return }"#;
        let body = serde_json::json!({});
        let err = engine().transform(code, body, test_ctx()).unwrap_err();
        assert!(matches!(err, JsError::Runtime(_)));
    }

    #[test]
    fn test_null_return_errors() {
        let code = r#"
function transform(body, context) {
    return null;
}"#;
        let body = serde_json::json!({});
        let err = engine().transform(code, body, test_ctx()).unwrap_err();
        assert!(err.to_string().contains("must return"));
    }

    #[test]
    fn test_undefined_return_errors() {
        let code = r#"
function transform(body, context) {
    // no return
}"#;
        let body = serde_json::json!({});
        let err = engine().transform(code, body, test_ctx()).unwrap_err();
        assert!(err.to_string().contains("must return"));
    }
}
