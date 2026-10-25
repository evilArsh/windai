use super::{FacadeCall, FacadeError, HttpRequest, throw};
use rquickjs::function::Opt;
use rquickjs::{Array, Ctx, Error, Function, Object, Result as JsResult};

/// 允许的 HTTP 方法
const METHODS: &[&str] = &["GET", "POST", "PUT", "PATCH", "DELETE"];

/// `onError` 的取值
const ON_ERROR_VALUES: &[&str] = &["throw", "null"];

/// 未给出方法时的取值
const DEFAULT_METHOD: &str = "GET";

/// `timeoutMs` 的上界
const MAX_TIMEOUT_MS: f64 = u64::MAX as f64;

/// 一个地址失败时的处理方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum OnError {
    /// 整个调用抛出异常
    #[default]
    Throw,
    /// 该位置以空串占位，其余位置照常返回
    Null,
}

struct Init {
    request: HttpRequest,
    on_error: OnError,
}

fn init_of(init: Option<Object<'_>>) -> JsResult<Init> {
    let mut request = HttpRequest {
        method: String::from(DEFAULT_METHOD),
        ..HttpRequest::default()
    };
    let mut on_error = OnError::Throw;
    let Some(init) = init else {
        return Ok(Init { request, on_error });
    };

    if let Some(method) = init.get::<_, Option<String>>("method")? {
        let method = method.to_ascii_uppercase();
        if !METHODS.contains(&method.as_str()) {
            return Err(value_error("method", "HttpInit.method", &method, METHODS));
        }
        request.method = method;
    }
    if let Some(body) = init.get::<_, Option<String>>("body")? {
        request.body = Some(body);
    }
    if let Some(timeout) = init.get::<_, Option<f64>>("timeoutMs")? {
        if !timeout.is_finite() || timeout.fract() != 0.0 {
            return Err(timeout_error(format!(
                "{timeout} is not a whole number of milliseconds"
            )));
        }
        if timeout < 0.0 || timeout >= MAX_TIMEOUT_MS {
            return Err(timeout_error(format!(
                "{timeout} is not a number of milliseconds between 0 and {}",
                u64::MAX
            )));
        }
        request.timeout_ms = Some(timeout as u64);
    }
    if let Some(headers) = init.get::<_, Option<Object<'_>>>("headers")? {
        for entry in headers.props::<String, String>() {
            let (name, value) = entry?;
            request.headers.push((name, value));
        }
    }
    if let Some(failure) = init.get::<_, Option<String>>("onError")? {
        on_error = match failure.as_str() {
            "throw" => OnError::Throw,
            "null" => OnError::Null,
            other => {
                return Err(value_error(
                    "onError",
                    "HttpInit.onError",
                    other,
                    ON_ERROR_VALUES,
                ));
            }
        };
    }
    Ok(Init { request, on_error })
}

fn value_error(field: &'static str, target: &'static str, value: &str, allowed: &[&str]) -> Error {
    Error::new_from_js_message(
        field,
        target,
        format!("{value} is not one of {}", allowed.join(", ")),
    )
}

fn timeout_error(message: String) -> Error {
    Error::new_from_js_message("timeoutMs", "HttpInit.timeoutMs", message)
}

fn placeable(error: &FacadeError) -> bool {
    !matches!(error, FacadeError::Cancelled | FacadeError::Unsupported)
}

fn text_of(body: Vec<u8>, url: &str) -> Result<String, FacadeError> {
    String::from_utf8(body).map_err(|_| FacadeError::Failed(format!("{url} is not valid utf-8")))
}

pub fn build<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Object<'js>> {
    let object = Object::new(ctx.clone())?;

    let one_host = call.host.clone();
    let fetch_text = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, url: String, init: Opt<Option<Object<'js>>>| -> JsResult<String> {
            let mut request = init_of(init.into_inner().flatten())?.request;
            request.url = url.clone();
            let body = one_host
                .http_fetch(request)
                .map_err(|error| throw(&ctx, error))?;
            text_of(body, &url).map_err(|error| throw(&ctx, error))
        },
    )?;

    let many_host = call.host.clone();
    let fetch_many = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>,
              urls: Array<'js>,
              init: Opt<Option<Object<'js>>>|
              -> JsResult<Array<'js>> {
            let Init { request, on_error } = init_of(init.into_inner().flatten())?;
            let mut requests = Vec::new();
            for url in urls.iter::<String>() {
                let url = url?;
                let mut one = request.clone();
                one.url = url.clone();
                requests.push(one);
            }
            let addresses: Vec<String> = requests.iter().map(|one| one.url.clone()).collect();
            let answers = many_host.http_fetch_many(requests);
            if answers.len() != addresses.len() {
                return Err(throw(
                    &ctx,
                    FacadeError::Failed(format!(
                        "the host answered {} of {} addresses",
                        answers.len(),
                        addresses.len()
                    )),
                ));
            }
            let out = Array::new(ctx.clone())?;
            for (index, (url, answer)) in addresses.iter().zip(answers).enumerate() {
                let body = match answer {
                    Ok(body) => body,
                    Err(error) if on_error == OnError::Null && placeable(&error) => {
                        out.set(index, "")?;
                        continue;
                    }
                    Err(error) => return Err(throw(&ctx, error)),
                };
                let text = text_of(body, url).map_err(|error| throw(&ctx, error))?;
                out.set(index, text)?;
            }
            Ok(out)
        },
    )?;

    object.prop("fetchText", fetch_text)?;
    object.prop("fetchMany", fetch_many)?;
    Ok(object)
}
