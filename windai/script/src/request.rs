use super::circuit::Breaker;
use super::deployment::{DeployError, Deployment};
use super::facade::{FacadeCall, HostApi, ModIdentity};
use super::runtime::{CallHost, HostFactory, eval_handler};
use super::threads::{ThreadPool, ThreadPoolError};
use rquickjs::{CaughtError, Ctx, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use wind_core::error::CoreError;
use wind_core::llm::ports::ModRequests;
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, CallMode};
use wind_mod::capability::available;
use wind_mod::manifest::{CallableBy, Manifest, Request};

/// 一次 request 调用的默认时间上限
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// 打开 `Deployment` 时给出的期限，秒
const OPEN_BUDGET_SECS: u64 = 3_600;

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error(transparent)]
    Deploy(#[from] DeployError),
    #[error("{name} does not declare a request named {request}")]
    Undeclared { name: String, request: String },
    #[error("{request} of {name} is not callable by this caller")]
    NotCallable { name: String, request: String },
    #[error("{request} of {name} exceeded its budget")]
    Timeout { name: String, request: String },
    #[error("{0}")]
    Handler(String),
    #[error("{name} is suspended after repeated timeouts")]
    Suspended { name: String },
    #[error("the interpreter pool is at its limit")]
    Busy,
    #[error("the interpreter thread panicked before it could answer")]
    Panicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    /// agent 工具循环
    Agent,
    /// 前端组件
    Ui,
}

impl Caller {
    fn as_manifest(self) -> CallableBy {
        match self {
            Caller::Agent => CallableBy::Agent,
            Caller::Ui => CallableBy::Ui,
        }
    }
}

/// 调用 mod 的 request 的入口
pub struct RequestCaller {
    factory: Arc<dyn HostFactory>,
    pool: Arc<ThreadPool>,
    caller: Caller,
    breaker: Arc<Breaker>,
}

impl RequestCaller {
    pub fn for_agent(
        factory: Arc<dyn HostFactory>,
        pool: Arc<ThreadPool>,
        breaker: Arc<Breaker>,
    ) -> Self {
        Self {
            factory,
            pool,
            caller: Caller::Agent,
            breaker,
        }
    }

    pub fn for_ui(
        factory: Arc<dyn HostFactory>,
        pool: Arc<ThreadPool>,
        breaker: Arc<Breaker>,
    ) -> Self {
        Self {
            factory,
            pool,
            caller: Caller::Ui,
            breaker,
        }
    }

    pub async fn call(
        &self,
        scope: &Scope,
        name: &str,
        version: i64,
        request: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, RequestError> {
        self.call_as(scope, name, version, request, input, self.caller)
            .await
    }

    pub async fn call_as(
        &self,
        scope: &Scope,
        name: &str,
        version: i64,
        request: &str,
        input: serde_json::Value,
        caller: Caller,
    ) -> Result<serde_json::Value, RequestError> {
        if self.breaker.refuse(name) {
            return Err(RequestError::Suspended {
                name: name.to_string(),
            });
        }
        let cx: CallCx = CallCx {
            deadline: TokioInstant::now() + Duration::from_secs(OPEN_BUDGET_SECS),
            cancel: CancellationToken::new(),
            mode: CallMode::Request,
            scope: scope.clone(),
        };
        let (deployment, host) = self.factory.open(name, version, scope, &cx).await?;

        let host: Arc<dyn CallHost> = Arc::from(host);
        let declared = match declared_request(&deployment.manifest, request, caller) {
            Ok(declared) => declared,
            Err(error) => {
                host.finish().await?;
                return Err(error);
            }
        };
        let budget = Duration::from_millis(declared.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
        host.rebase(TokioInstant::now() + budget);

        let facade: Arc<dyn HostApi> = host.clone();
        let call = FacadeCall {
            identity: ModIdentity {
                name: deployment.name.clone(),
                version: deployment.version,
            },
            scope: scope.clone(),
            props: available(&deployment.manifest.capabilities, cx.mode),
            host: facade,
        };

        let outcome = self
            .run(&deployment, call, request, &declared.handler, budget, input)
            .await;

        host.finish().await?;
        outcome
    }

    async fn run(
        &self,
        deployment: &Deployment,
        call: FacadeCall,
        request: &str,
        handler: &str,
        budget: Duration,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, RequestError> {
        let job = Job {
            root: deployment.root.clone(),
            compiled: deployment.compiled.clone(),
            call,
            handler: handler.to_string(),
            input,
            deadline: Instant::now() + budget,
        };

        let settled = match self.pool.run_within(budget, move || run_request(job)).await {
            Ok(JobOutcome::Returned(value)) => Ok(value),
            Ok(JobOutcome::Handler(reason)) => Err(RequestError::Handler(reason)),
            Ok(JobOutcome::TimedOut) | Err(ThreadPoolError::Timeout) => {
                Err(RequestError::Timeout {
                    name: deployment.name.clone(),
                    request: request.to_string(),
                })
            }
            Err(ThreadPoolError::Busy) => Err(RequestError::Busy),
            Err(ThreadPoolError::HandlerPanicked) => Err(RequestError::Panicked),
        };

        match &settled {
            Ok(_) | Err(RequestError::Handler(_)) => self.breaker.record_success(&deployment.name),
            Err(RequestError::Timeout { .. }) => self.breaker.record_timeout(&deployment.name),
            Err(_) => {}
        }
        settled
    }
}

#[async_trait::async_trait]
impl ModRequests for RequestCaller {
    async fn call(
        &self,
        scope: &Scope,
        mod_name: &str,
        version: i64,
        request: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        self.call(scope, mod_name, version, request, input)
            .await
            .map_err(|error| CoreError::Denied(error.to_string()))
    }
}

/// 获取 manifest 中 request 定义的详细信息
fn declared_request<'a>(
    manifest: &'a Manifest,
    request: &str,
    caller: Caller,
) -> Result<&'a Request, RequestError> {
    let declared = manifest
        .requests
        .get(request)
        .ok_or_else(|| RequestError::Undeclared {
            name: manifest.name.clone(),
            request: request.to_string(),
        })?;
    if !declared.callable_by.contains(&caller.as_manifest()) {
        return Err(RequestError::NotCallable {
            name: manifest.name.clone(),
            request: request.to_string(),
        });
    }
    Ok(declared)
}

struct Job {
    root: PathBuf,
    compiled: BTreeMap<String, Vec<u8>>,
    call: FacadeCall,
    handler: String,
    input: serde_json::Value,
    deadline: Instant,
}

enum JobOutcome {
    Returned(serde_json::Value),
    Handler(String),
    TimedOut,
}

fn run_request(job: Job) -> JobOutcome {
    let Job {
        root,
        compiled,
        call,
        handler,
        input,
        deadline,
    } = job;

    let returned = eval_handler(&root, &compiled, &call, &handler, &input, deadline, json_of);

    if Instant::now() >= deadline {
        return JobOutcome::TimedOut;
    }
    match returned {
        Ok(value) => JobOutcome::Returned(value),
        Err(reason) => JobOutcome::Handler(reason),
    }
}

fn json_of<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> Result<serde_json::Value, String> {
    let text = ctx
        .json_stringify(value)
        .map_err(|error| CaughtError::from_error(ctx, error).to_string())?
        .ok_or_else(|| "the handler returned no serializable value".to_string())?;
    let text = text.to_string().map_err(|error| error.to_string())?;
    serde_json::from_str(&text).map_err(|error| error.to_string())
}
