use super::{FacadeCall, FacadeError, throw};
use rquickjs::function::Rest;
use rquickjs::{Ctx, Error, Function, Object, Result as JsResult, String as JsString, Value};

/// `ctx.log` 的等级取值
const LOG_LEVELS: &[&str] = &["debug", "info", "warn", "error"];

/// `ctx.notify` 的等级取值
///
/// 它比日志少一个 `debug`
const NOTIFY_LEVELS: &[&str] = &["info", "warn", "error"];

pub fn emit<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Function<'js>> {
    let host = call.host.clone();
    let scope = call.scope.clone();
    Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, name: String, payload: Value<'js>| -> JsResult<()> {
            let payload = json_of(&ctx, "emit payload", payload)?;
            host.emit(&scope, &name, payload)
                .map_err(|error| throw(&ctx, error))
        },
    )
}

pub fn notify<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Function<'js>> {
    let host = call.host.clone();
    let scope = call.scope.clone();
    Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, options: Object<'js>| -> JsResult<()> {
            let text: String = options.get("text")?;
            let level = match options.get::<_, Option<String>>("level")? {
                Some(level) => {
                    level_of("notify level", "NotifyOptions.level", &level, NOTIFY_LEVELS)?
                }
                None => String::from("info"),
            };
            host.notify(&scope, &level, &text)
                .map_err(|error| throw(&ctx, error))
        },
    )
}

pub fn log<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Function<'js>> {
    let host = call.host.clone();
    Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, level: String, args: Rest<Value<'js>>| -> JsResult<()> {
            let level = level_of("log level", "LogLevel", &level, LOG_LEVELS)?;
            let mut parts = Vec::with_capacity(args.len());
            for arg in args.0 {
                parts.push(text_of(&ctx, arg)?);
            }
            host.log(&level, &parts.join(" "));
            Ok(())
        },
    )
}

fn level_of(
    field: &'static str,
    target: &'static str,
    level: &str,
    allowed: &[&str],
) -> JsResult<String> {
    if allowed.contains(&level) {
        return Ok(level.to_string());
    }
    Err(Error::new_from_js_message(
        field,
        target,
        format!("{level} is not one of {}", allowed.join(", ")),
    ))
}

fn json_of<'js>(ctx: &Ctx<'js>, field: &str, value: Value<'js>) -> JsResult<serde_json::Value> {
    let text = ctx
        .json_stringify(value)?
        .ok_or_else(|| FacadeError::Failed(format!("{field} is not serializable")))
        .map_err(|error| throw(ctx, error))?;
    let text = text.to_string()?;
    serde_json::from_str(&text)
        .map_err(|error| FacadeError::Failed(error.to_string()))
        .map_err(|error| throw(ctx, error))
}

fn text_of<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> JsResult<String> {
    match value.as_string() {
        Some(text) => text_of_string(ctx, text),
        None => match ctx.json_stringify(value)? {
            Some(text) => text_of_string(ctx, &text),
            None => Ok(String::from("undefined")),
        },
    }
}

fn text_of_string(ctx: &Ctx<'_>, text: &JsString<'_>) -> JsResult<String> {
    text.to_string().map_err(|error| {
        throw(
            ctx,
            FacadeError::Failed(format!("a log argument is not text: {error}")),
        )
    })
}
