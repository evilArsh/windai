use super::{FacadeCall, TranscriptEntry, throw};
use rquickjs::function::Opt;
use rquickjs::object::Property;
use rquickjs::{Array, Ctx, Error, Function, Object, Result as JsResult};

/// 子实例与父实例的关系取值
const MODES: &[&str] = &["sync", "background"];

/// 未给出关系时的取值
const DEFAULT_MODE: &str = "sync";

/// 条数能取到的最大值
const MAX_LIMIT: f64 = u32::MAX as f64;

fn limit_error(message: String) -> Error {
    Error::new_from_js_message("limit", "AgentApi.transcript.limit", message)
}

fn limit_of(limit: Option<f64>) -> JsResult<Option<u32>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    if !limit.is_finite() || limit.fract() != 0.0 {
        return Err(limit_error(format!(
            "{limit:?} is not a whole number of messages"
        )));
    }
    if limit < 0.0 || limit > MAX_LIMIT {
        return Err(limit_error(format!(
            "{limit:?} is not a number of messages between 0 and {}",
            u32::MAX
        )));
    }
    Ok(Some(limit as u32))
}

fn entry<'js>(ctx: &Ctx<'js>, entry: &TranscriptEntry) -> JsResult<Object<'js>> {
    let object = Object::new(ctx.clone())?;
    object.prop("message_id", Property::from(entry.message_id).enumerable())?;
    object.prop("role", Property::from(entry.role.as_str()).enumerable())?;
    object.prop("text", Property::from(entry.text.as_str()).enumerable())?;
    Ok(object)
}

pub fn build<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Object<'js>> {
    let object = Object::new(ctx.clone())?;

    let prompt_host = call.host.clone();
    let prompt_scope = call.scope.clone();
    let prompt = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, text: String| -> JsResult<()> {
            prompt_host
                .agent_prompt(&prompt_scope, &text)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    let draft_host = call.host.clone();
    let draft_scope = call.scope.clone();
    let draft = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, text: String| -> JsResult<()> {
            draft_host
                .agent_draft(&draft_scope, &text)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    let spawn_host = call.host.clone();
    let spawn_scope = call.scope.clone();
    let spawn = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, options: Object<'js>| -> JsResult<()> {
            let task: String = options.get("task")?;
            let mode = match options.get::<_, Option<String>>("mode")? {
                Some(mode) => {
                    if !MODES.contains(&mode.as_str()) {
                        return Err(Error::new_from_js_message(
                            "mode",
                            "SpawnOptions.mode",
                            format!("{mode} is not one of {}", MODES.join(", ")),
                        ));
                    }
                    mode
                }
                None => String::from(DEFAULT_MODE),
            };
            spawn_host
                .agent_spawn(&spawn_scope, &mode, &task)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    let transcript_host = call.host.clone();
    let transcript_scope = call.scope.clone();
    let transcript = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, limit: Opt<Option<f64>>| -> JsResult<Array<'js>> {
            let limit = limit_of(limit.into_inner().flatten())?;
            let entries = transcript_host.agent_transcript(&transcript_scope, limit);
            let array = Array::new(ctx.clone())?;
            for (index, value) in entries.iter().enumerate() {
                array.set(index, entry(&ctx, value)?)?;
            }
            Ok(array)
        },
    )?;

    object.prop("prompt", prompt)?;
    object.prop("draft", draft)?;
    object.prop("spawn", spawn)?;
    object.prop("transcript", transcript)?;
    Ok(object)
}
