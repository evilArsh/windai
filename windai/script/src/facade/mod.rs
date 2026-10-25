pub mod agent;
pub mod fs;
pub mod http;
pub mod misc;
pub mod state;

use rquickjs::{Ctx, Error, Exception, Object, Result as JsResult};
use std::path::PathBuf;
use std::sync::Arc;
use wind_event::envelope::Scope;
use wind_mod::capability::{CtxSet, Prop};

#[derive(Debug, Clone, thiserror::Error)]
pub enum FacadeError {
    #[error("this capability is not available in this deployment")]
    Unsupported,
    #[error("this deployment has no {capability} runtime")]
    NoRuntime { capability: &'static str },
    #[error("{path} escapes the private directory")]
    Path { path: String },
    #[error("{path} does not exist")]
    NotFound { path: String },
    #[error("the value of {key} is over the {limit} byte limit")]
    PayloadTooLarge { key: String, limit: usize },
    #[error("{target} is over the {limit} byte limit")]
    TooLarge { target: String, limit: usize },
    #[error("the call exceeded its budget")]
    Timeout,
    #[error("the call was cancelled")]
    Cancelled,
    #[error("{reason}")]
    Http { status: Option<u16>, reason: String },
    #[error("{0}")]
    Failed(String),
}

impl FacadeError {
    pub fn exception_name(&self) -> &'static str {
        match self {
            FacadeError::Unsupported => "CapabilityError",
            FacadeError::NoRuntime { .. } => "CapabilityError",
            FacadeError::Path { .. } => "PathError",
            FacadeError::NotFound { .. } => "NotFoundError",
            FacadeError::PayloadTooLarge { .. } => "PayloadTooLargeError",
            FacadeError::TooLarge { .. } => "TooLargeError",
            FacadeError::Timeout => "TimeoutError",
            FacadeError::Cancelled => "CancelledError",
            FacadeError::Http { .. } => "HttpError",
            FacadeError::Failed(_) => "FacadeError",
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            FacadeError::Http { status, .. } => *status,
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModIdentity {
    pub name: String,
    pub version: i64,
}

#[derive(Debug, Clone, Default)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    /// 本次请求的时间上限，毫秒
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TranscriptEntry {
    pub message_id: i64,
    pub role: String,
    pub text: String,
}

pub trait HostApi: Send + Sync {
    fn state_read(&self, key: &str) -> Option<serde_json::Value>;
    fn state_write(&self, key: &str, value: serde_json::Value) -> Result<(), FacadeError>;
    fn state_delete(&self, key: &str);
    fn state_keys(&self) -> Vec<String>;
    fn storage_read(&self, key: &str) -> Option<serde_json::Value>;
    fn storage_write(&self, key: &str, value: serde_json::Value);
    fn storage_delete(&self, key: &str);
    fn storage_keys(&self) -> Vec<String>;
    fn http_fetch(&self, init: HttpRequest) -> Result<Vec<u8>, FacadeError>;
    /// 结果与传入一一对应、顺序相同
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>>;
    fn agent_prompt(&self, scope: &Scope, text: &str) -> Result<(), FacadeError>;
    fn agent_draft(&self, scope: &Scope, text: &str) -> Result<(), FacadeError>;
    fn agent_spawn(&self, scope: &Scope, mode: &str, task: &str) -> Result<(), FacadeError>;
    fn agent_transcript(&self, scope: &Scope, limit: Option<u32>) -> Vec<TranscriptEntry>;
    fn fs_dir(&self, name: &str) -> PathBuf;
    fn fs_read(&self, name: &str, path: &str) -> Result<String, FacadeError>;
    fn fs_write(&self, name: &str, path: &str, data: &str) -> Result<(), FacadeError>;
    fn fs_list(&self, name: &str, path: &str) -> Result<Vec<String>, FacadeError>;
    fn fs_remove(&self, name: &str, path: &str) -> Result<(), FacadeError>;
    fn emit(
        &self,
        scope: &Scope,
        name: &str,
        payload: serde_json::Value,
    ) -> Result<(), FacadeError>;
    fn notify(&self, scope: &Scope, level: &str, text: &str) -> Result<(), FacadeError>;
    fn log(&self, level: &str, message: &str);
}

#[derive(Clone)]
pub struct FacadeCall {
    pub identity: ModIdentity,
    pub scope: Scope,
    pub props: CtxSet,
    pub host: Arc<dyn HostApi>,
}

impl FacadeCall {
    pub fn clone_for_thread(&self) -> Self {
        self.clone()
    }
}

pub fn build_ctx<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Object<'js>> {
    let object = Object::new(ctx.clone())?;

    let identity = Object::new(ctx.clone())?;
    identity.prop("name", call.identity.name.as_str())?;
    identity.prop("version", call.identity.version)?;
    object.prop("mod", identity)?;

    let scope = Object::new(ctx.clone())?;
    match &call.scope {
        Scope::Global => {
            scope.prop("kind", "global")?;
        }
        Scope::Topic { topic_id } => {
            scope.prop("kind", "topic")?;
            scope.prop("topic_id", *topic_id)?;
        }
    }
    object.prop("scope", scope)?;

    if call.props.has(Prop::State) {
        object.prop("state", state::build(ctx, call, false)?)?;
    }
    if call.props.has(Prop::Storage) {
        object.prop("storage", state::build(ctx, call, true)?)?;
    }
    if call.props.has(Prop::Http) {
        object.prop("http", http::build(ctx, call)?)?;
    }
    if call.props.has(Prop::Agent) {
        object.prop("agent", agent::build(ctx, call)?)?;
    }
    if call.props.has(Prop::Fs) {
        object.prop("fs", fs::build(ctx, call)?)?;
    }
    if call.props.has(Prop::Emit) {
        object.prop("emit", misc::emit(ctx, call)?)?;
    }
    if call.props.has(Prop::Notify) {
        object.prop("notify", misc::notify(ctx, call)?)?;
    }
    if call.props.has(Prop::Log) {
        object.prop("log", misc::log(ctx, call)?)?;
    }
    Ok(object)
}

pub(crate) fn throw<'js>(ctx: &Ctx<'js>, error: FacadeError) -> Error {
    let name = error.exception_name();
    let message = error.to_string();
    if let Some(status) = error.status() {
        if let Ok(exception) = Exception::from_message(ctx.clone(), &message) {
            let object = exception.as_object();
            let _ = object.set("name", name);
            let _ = object.set("status", status);
            return ctx.throw(exception.into_object().into_value());
        }
    }
    Exception::throw_dom(ctx, name, &message)
}
