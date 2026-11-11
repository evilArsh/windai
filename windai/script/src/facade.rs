use std::sync::{Arc, Mutex};

use rquickjs::function::Rest;
use rquickjs::{Ctx, Exception, Function, Object, Value as JsValue};
use serde_json::Value;

use crate::error::ScriptsError;
use crate::service::{ScriptHost, ScriptScope};

/// 单次 emit payload 或 notify text 的上限，字节
pub(crate) const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// handler 里记录的宿主调用错误
pub(crate) type SharedFailure = Arc<Mutex<Option<ScriptsError>>>;

pub(crate) fn new_failure() -> SharedFailure {
    Arc::new(Mutex::new(None))
}

fn clear_failure(failure: &SharedFailure) {
    *failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

fn record_failure(failure: &SharedFailure, error: ScriptsError) {
    *failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error);
}

/// 一次 handler 调用在脚本侧的身份与宿主入口
pub(crate) struct FacadeCall {
    pub(crate) mod_name: String,
    pub(crate) mod_version: u32,
    pub(crate) scope: ScriptScope,
    pub(crate) host: Arc<dyn ScriptHost>,
    pub(crate) failure: SharedFailure,
}

impl FacadeCall {
    /// 取走宿主调用记录的错误
    pub(crate) fn take_failure(&self) -> Option<ScriptsError> {
        self.failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

/// 构造 handler 的第二个参数
pub(crate) fn build_ctx<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> rquickjs::Result<Object<'js>> {
    let facade = Object::new(ctx.clone())?;
    let module = Object::new(ctx.clone())?;
    module.set("name", call.mod_name.as_str())?;
    module.set("version", call.mod_version)?;
    facade.set("mod", module)?;
    let scope = Object::new(ctx.clone())?;
    match &call.scope {
        ScriptScope::Global => {
            scope.set("kind", "global")?;
        }
        ScriptScope::Topic {
            topic_id,
            instance_id,
        } => {
            scope.set("kind", "topic")?;
            scope.set("topic_id", *topic_id)?;
            scope.set("instance_id", *instance_id)?;
        }
    }
    facade.set("scope", scope)?;
    facade.set("emit", emit_fn(ctx, call)?)?;
    facade.set("notify", notify_fn(ctx, call)?)?;
    facade.set("log", log_fn(ctx, call)?)?;
    Ok(facade)
}

fn emit_fn<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> rquickjs::Result<Function<'js>> {
    let inner = ctx.clone();
    let host = Arc::clone(&call.host);
    let failure = Arc::clone(&call.failure);
    Function::new(
        ctx.clone(),
        move |channel: String, payload: JsValue<'js>| -> rquickjs::Result<()> {
            clear_failure(&failure);
            let text = inner.json_stringify(payload).map_err(|error| {
                Exception::throw_message(
                    &inner,
                    &format!("the payload is not serializable: {error}"),
                )
            })?;
            let Some(text) = text else {
                return Err(Exception::throw_message(
                    &inner,
                    "the payload is not serializable",
                ));
            };
            let text = text.to_string().map_err(|error| {
                Exception::throw_message(&inner, &format!("the payload is not readable: {error}"))
            })?;
            if text.len() > MAX_PAYLOAD_BYTES {
                record_failure(
                    &failure,
                    ScriptsError::PayloadTooLarge {
                        limit: MAX_PAYLOAD_BYTES,
                    },
                );
                return Err(Exception::throw_message(
                    &inner,
                    "the payload exceeds 65536 bytes",
                ));
            }
            let payload: Value = serde_json::from_str(&text).map_err(|error| {
                Exception::throw_message(&inner, &format!("the payload is not json: {error}"))
            })?;
            if let Err(error) = host.emit(&channel, payload) {
                record_failure(&failure, error);
                return Err(Exception::throw_message(
                    &inner,
                    "the host refused the emit",
                ));
            }
            Ok(())
        },
    )
}

fn notify_fn<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> rquickjs::Result<Function<'js>> {
    let inner = ctx.clone();
    let host = Arc::clone(&call.host);
    let failure = Arc::clone(&call.failure);
    Function::new(
        ctx.clone(),
        move |options: Object<'js>| -> rquickjs::Result<()> {
            clear_failure(&failure);
            let text: String = options.get("text").map_err(|_| {
                Exception::throw_message(&inner, "the notification requires a text")
            })?;
            let level = match options.get::<_, Option<String>>("level") {
                Ok(Some(level)) => level,
                Ok(None) => "info".to_string(),
                Err(_) => {
                    return Err(Exception::throw_message(
                        &inner,
                        "the notification level is invalid",
                    ));
                }
            };
            if !matches!(level.as_str(), "info" | "warn" | "error") {
                return Err(Exception::throw_message(
                    &inner,
                    "the notification level is invalid",
                ));
            }
            if text.len() > MAX_PAYLOAD_BYTES {
                record_failure(
                    &failure,
                    ScriptsError::PayloadTooLarge {
                        limit: MAX_PAYLOAD_BYTES,
                    },
                );
                return Err(Exception::throw_message(
                    &inner,
                    "the notification exceeds 65536 bytes",
                ));
            }
            if let Err(error) = host.notify(&level, &text) {
                record_failure(&failure, error);
                return Err(Exception::throw_message(
                    &inner,
                    "the host refused the notification",
                ));
            }
            Ok(())
        },
    )
}

fn log_fn<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> rquickjs::Result<Function<'js>> {
    let inner = ctx.clone();
    let mod_name = call.mod_name.clone();
    Function::new(
        ctx.clone(),
        move |level: String, args: Rest<JsValue<'js>>| -> rquickjs::Result<()> {
            let level = match level.as_str() {
                "debug" => log::Level::Debug,
                "info" => log::Level::Info,
                "warn" => log::Level::Warn,
                "error" => log::Level::Error,
                _ => {
                    return Err(Exception::throw_message(
                        &inner,
                        "the log level is not known",
                    ));
                }
            };
            let mut message = String::new();
            for (index, arg) in args.0.into_iter().enumerate() {
                if index > 0 {
                    message.push(' ');
                }
                match arg.as_string().and_then(|text| text.to_string().ok()) {
                    Some(text) => message.push_str(&text),
                    None => match inner.json_stringify(arg) {
                        Ok(Some(text)) => {
                            message.push_str(&text.to_string().unwrap_or_default());
                        }
                        Ok(None) => message.push_str("<undefined>"),
                        Err(_) => message.push_str("<unserializable>"),
                    },
                }
            }
            log::log!(target: "mod", level, "{}: {}", mod_name, message);
            Ok(())
        },
    )
}

/// 把 handler 的返回值转成 JSON，`undefined` 与 `null` 都返回 `None`
pub(crate) fn outcome_of<'js>(
    ctx: &Ctx<'js>,
    value: JsValue<'js>,
) -> Result<Option<Value>, ScriptsError> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    let raw = ctx
        .json_stringify(value)
        .map_err(|error| ScriptsError::Handler(error.to_string()))?;
    let Some(raw) = raw else {
        return Err(ScriptsError::Handler(
            "the returned value is not serializable".to_string(),
        ));
    };
    let text = raw
        .to_string()
        .map_err(|error| ScriptsError::Handler(error.to_string()))?;
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|error| ScriptsError::Handler(format!("the returned value is not json: {error}")))
}
