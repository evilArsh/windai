use super::circuit::Breaker;
use super::deployment::{DeployError, Deployment};
use super::facade::{FacadeCall, HostApi, ModIdentity, build_ctx};
use super::loader::{ModuleLoader, ModuleResolver};
use super::threads::{ThreadPool, ThreadPoolError};
use rquickjs::loader::{ImportAttributes, Loader};
use rquickjs::module::Declared;
use rquickjs::{CaughtError, Context, Ctx, Function, Module, Result as JsResult, Runtime, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, HostError, ModHost};
use wind_event::subscription::Subscription;
use wind_mod::capability::available;
use wind_mod::package::SCRIPT_ENTRY;

/// 总调用时长 = 调用时长 + 额外准备时间 `TIMEOUT_SLACK`
const TIMEOUT_SLACK: Duration = Duration::from_millis(10);

/// 总内存 = 解释器线程数 × 64 MB
const INTERPRETER_MEMORY_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn interpreter(deadline: Instant) -> rquickjs::Result<Runtime> {
    let runtime = Runtime::new()?;
    runtime.set_memory_limit(INTERPRETER_MEMORY_BYTES);
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)));
    Ok(runtime)
}

/// 一次调用在调用方这一侧的接口
#[async_trait::async_trait]
pub trait CallHost: HostApi {
    /// 把本次调用的截止时刻改成 `deadline`
    ///
    /// 它影响之后经 `blocked` 的调用（`ctx.xxx`）能等多久，不影响解释器的中断期限：
    /// 后者由 `run` 用同一个预算单独设置。
    fn rebase(&self, deadline: TokioInstant);

    /// 结束本次调用：把改过的键写进存储，把变化了的对外状态推给 mod 事件总线
    async fn finish(&self) -> Result<(), DeployError>;
}

/// 产生 `Deployment` 和调用宿主的工厂接口
#[async_trait::async_trait]
pub trait HostFactory: Send + Sync {
    async fn open(
        &self,
        name: &str,
        version: i64,
        scope: &Scope,
        cx: &CallCx,
    ) -> Result<(Deployment, Box<dyn CallHost>), DeployError>;
}

pub struct ScriptHostOptions {
    pub factory: Arc<dyn HostFactory>,
    pub pool: Arc<ThreadPool>,
    pub breaker: Arc<Breaker>,
}

pub struct ScriptHost {
    factory: Arc<dyn HostFactory>,
    pool: Arc<ThreadPool>,
    breaker: Arc<Breaker>,
}

impl ScriptHost {
    pub fn new(options: ScriptHostOptions) -> Self {
        Self {
            factory: options.factory,
            pool: options.pool,
            breaker: options.breaker,
        }
    }
}

struct Job {
    root: PathBuf,
    compiled: BTreeMap<String, Vec<u8>>,
    call: FacadeCall,
    handler: String,
    payload: serde_json::Value,
    deadline: Instant,
}

enum JobOutcome {
    Decided(Option<serde_json::Value>),
    /// 抛错或返回值不合法
    Handler(String),
    TimedOut,
}

struct SharedLoader(Arc<Mutex<ModuleLoader>>);

impl SharedLoader {
    fn new(loader: Arc<Mutex<ModuleLoader>>) -> Self {
        Self(loader)
    }
}

impl Loader for SharedLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        attributes: Option<ImportAttributes<'js>>,
    ) -> JsResult<Module<'js, Declared>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .load(ctx, name, attributes)
    }
}

#[async_trait::async_trait]
impl ModHost for ScriptHost {
    /// 一次运行时 js 调用实现
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        let started = Instant::now();
        if self.breaker.refuse(&sub.mod_name) {
            return Err(HostError::Handler(format!(
                "{} is suspended after repeated timeouts",
                sub.mod_name
            )));
        }

        let scope = cx.scope.clone();
        let (deployment, host) = self
            .factory
            .open(&sub.mod_name, i64::from(sub.mod_version), &scope, &cx)
            .await
            .map_err(|error| HostError::Internal(error.to_string()))?;

        let host: Arc<dyn CallHost> = Arc::from(host);
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

        // 预算 = min(剩余总预算, 订阅声明的预算)
        let budget = cx.remaining().min(sub.timeout());
        let job = Job {
            root: deployment.root,
            compiled: deployment.compiled,
            call,
            handler: sub.handler.clone(),
            payload,
            deadline: Instant::now() + budget,
        };

        let outcome = self.pool.run_within(budget, move || run_job(job)).await;

        let settled = match outcome {
            Ok(JobOutcome::Decided(decided)) => {
                self.breaker.record_success(&sub.mod_name);
                Ok(decided)
            }
            Ok(JobOutcome::Handler(reason)) => {
                self.breaker.record_success(&sub.mod_name);
                Err(HostError::Handler(reason))
            }
            Ok(JobOutcome::TimedOut) | Err(ThreadPoolError::Timeout) => {
                // 只有用满 mod 预算才算它的超时
                if started.elapsed() + TIMEOUT_SLACK >= sub.timeout() {
                    self.breaker.record_timeout(&sub.mod_name);
                }
                Err(HostError::Handler(format!(
                    "{} did not return within {budget:?}",
                    sub.mod_name
                )))
            }
            Err(ThreadPoolError::Busy) => Err(HostError::Internal(
                "the interpreter pool is at its limit".to_string(),
            )),
            Err(ThreadPoolError::HandlerPanicked) => Err(HostError::Internal(
                "the interpreter thread panicked before it could answer".to_string(),
            )),
        };

        host.finish()
            .await
            .map_err(|error| HostError::Internal(error.to_string()))?;

        settled
    }
}

fn run_job(job: Job) -> JobOutcome {
    let Job {
        root,
        compiled,
        call,
        handler,
        payload,
        deadline,
    } = job;

    let decided = eval_handler(
        &root,
        &compiled,
        &call,
        &handler,
        &payload,
        deadline,
        |ctx, returned| outcome_of(ctx, returned),
    );

    if Instant::now() >= deadline {
        return JobOutcome::TimedOut;
    }
    match decided {
        Ok(outcome) => JobOutcome::Decided(outcome),
        Err(reason) => JobOutcome::Handler(reason),
    }
}

/// 执行一次 js 脚本
///
/// - `export *` 不转发 `default`，要显式 `export { default as run } from './run.js'`,
/// 否则 `handler` 不存在
pub(crate) fn eval_handler<R>(
    root: &Path,
    compiled: &BTreeMap<String, Vec<u8>>,
    call: &FacadeCall,
    handler: &str,
    args: &serde_json::Value,
    deadline: Instant,
    read: impl for<'js> FnOnce(&Ctx<'js>, Value<'js>) -> Result<R, String>,
) -> Result<R, String> {
    let runtime = interpreter(deadline).map_err(|error| error.to_string())?;

    let loader = Arc::new(Mutex::new(ModuleLoader::new(
        root.to_path_buf(),
        compiled.clone(),
    )));
    runtime.set_loader(
        ModuleResolver::new(),
        SharedLoader::new(Arc::clone(&loader)),
    );

    let context = Context::full(&runtime).map_err(|error| error.to_string())?;
    let args_json = serde_json::to_string(args).map_err(|error| error.to_string())?;

    context.with(|ctx| {
        let mut entry = SharedLoader::new(Arc::clone(&loader));
        let module = entry
            .load(&ctx, SCRIPT_ENTRY, None)
            .map_err(|error| error.to_string())?;
        let (module, _promise) = module.eval().map_err(|error| js_error(&ctx, error))?;
        while ctx.execute_pending_job() {}

        // 检查 js 中是否由导出 `handler` 函数名
        let namespace = module.namespace().map_err(|error| error.to_string())?;
        if !namespace
            .contains_key(handler)
            .map_err(|error| js_error(&ctx, error))?
        {
            return Err(format!("the script does not export {handler}"));
        }
        // 导出的 js 函数
        let handler: Function = namespace
            .get(handler)
            .map_err(|error| js_error(&ctx, error))?;

        // 创建注入到 js 侧的 ctx 对象
        let object = build_ctx(&ctx, call).map_err(|error| error.to_string())?;
        let payload: Value = ctx
            .json_parse(args_json.as_str())
            .map_err(|error| error.to_string())?;
        let returned: Value = handler
            .call((payload, object))
            .map_err(|error| js_error(&ctx, error))?;
        read(&ctx, returned)
    })
}

fn js_error(ctx: &Ctx<'_>, error: rquickjs::Error) -> String {
    CaughtError::from_error(ctx, error).to_string()
}

/// 解析 js 返回的改写载荷
///
/// 不返回值、返回 `null` 或 `undefined` 都表示不修改
fn outcome_of<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Option<serde_json::Value>, String> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    if let Some(object) = value.as_object()
        && object
            .contains_key("action")
            .map_err(|error| error.to_string())?
    {
        return Err("the action wrapper was removed; return the payload or nothing".to_string());
    }
    let text = ctx
        .json_stringify(value)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "the rewritten value is not serializable".to_string())?;
    let text = text.to_string().map_err(|error| error.to_string())?;
    let decoded = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    Ok(Some(decoded))
}
