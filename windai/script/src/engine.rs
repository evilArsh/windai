use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::{CaughtError, Context, Ctx, Function, Module, Runtime, Value as JsValue};
use serde_json::Value;

use crate::circuit::Breaker;
use crate::error::ScriptsError;
use crate::facade::{FacadeCall, build_ctx, new_failure, outcome_of};
use crate::loader::{ENTRY, ModLoader, ModResolver};
use crate::service::{ScriptCall, ScriptHost, ScriptScope, ScriptService};
use crate::threads::{ThreadPool, ThreadPoolError};

pub use crate::service::Config;

/// 连续超时的熔断阈值
const TIMEOUT_THRESHOLD: u32 = 3;

/// 语法校验时接收任意导入名
struct IgnoreResolver;

impl Resolver for IgnoreResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<String> {
        Ok(format!("{base}#{name}"))
    }
}

/// 用空模块满足语法校验期的导入解析
struct IgnoreLoader;

impl Loader for IgnoreLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<Module<'js, rquickjs::module::Declared>> {
        Module::declare(ctx.clone(), name, "export {};")
    }
}

/// 解释器线程上的一次执行
struct Job {
    mod_name: String,
    mod_version: u32,
    scope: ScriptScope,
    root: PathBuf,
    handler: String,
    input: Value,
    host: Arc<dyn ScriptHost>,
    deadline: Instant,
}

/// QuickJS 解释器池
struct QuickJsEngine {
    pool: ThreadPool,
    breaker: Breaker,
    memory_limit: usize,
}

/// 创建脚本服务
pub fn quickjs(config: Config) -> Result<Arc<dyn ScriptService>, ScriptsError> {
    if config.threads == 0 {
        return Err(ScriptsError::Config(
            "threads must be greater than zero".to_string(),
        ));
    }
    if config.memory_limit == 0 {
        return Err(ScriptsError::Config(
            "memory_limit must be greater than zero".to_string(),
        ));
    }
    Ok(Arc::new(QuickJsEngine {
        pool: ThreadPool::new(config.threads),
        breaker: Breaker::new(TIMEOUT_THRESHOLD),
        memory_limit: config.memory_limit,
    }))
}

#[async_trait]
impl ScriptService for QuickJsEngine {
    fn compile(&self, root: &Path, _handlers: &HashSet<String>) -> Result<(), ScriptsError> {
        let path = root.join("scripts/index.js");
        let source = std::fs::read_to_string(&path).map_err(|error| {
            ScriptsError::Compile(format!("{} is not readable: {error}", path.display()))
        })?;
        let runtime = Runtime::new().map_err(|error| ScriptsError::Internal(error.to_string()))?;
        runtime.set_memory_limit(self.memory_limit);
        runtime.set_loader(IgnoreResolver, IgnoreLoader);
        let context =
            Context::full(&runtime).map_err(|error| ScriptsError::Internal(error.to_string()))?;
        context.with(|ctx| {
            Module::declare(ctx, ENTRY, source)
                .map(|_| ())
                .map_err(|error| ScriptsError::Compile(error.to_string()))
        })
    }

    async fn call(&self, call: ScriptCall) -> Result<Option<Value>, ScriptsError> {
        if self.breaker.refuse(&call.mod_name) {
            return Err(ScriptsError::Suspended);
        }
        let budget = call
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if budget.is_zero() {
            self.breaker.record_timeout(&call.mod_name);
            return Err(ScriptsError::Timeout);
        }
        let mod_name = call.mod_name.clone();
        let memory_limit = self.memory_limit;
        let cancel = call.cancel.clone();
        let job = Job {
            mod_name: call.mod_name,
            mod_version: call.mod_version,
            scope: call.scope,
            root: call.root,
            handler: call.handler,
            input: call.input,
            host: call.host,
            deadline: call.deadline,
        };
        let outcome = tokio::select! {
            _ = cancel.cancelled() => {
                Err(ScriptsError::Internal("the call was cancelled".to_string()))
            }
            result = self.pool.run_within(budget, move || run_job(job, memory_limit)) => {
                match result {
                    Ok(outcome) => outcome,
                    Err(ThreadPoolError::Timeout) => Err(ScriptsError::Timeout),
                    Err(ThreadPoolError::Busy) => Err(ScriptsError::Busy),
                    Err(ThreadPoolError::HandlerPanicked) => Err(ScriptsError::Internal(
                        "the interpreter panicked".to_string(),
                    )),
                }
            }
        };
        match &outcome {
            Ok(_)
            | Err(ScriptsError::Handler(_))
            | Err(ScriptsError::Compile(_))
            | Err(ScriptsError::PayloadTooLarge { .. }) => {
                self.breaker.record_success(&mod_name);
            }
            Err(ScriptsError::Timeout) => self.breaker.record_timeout(&mod_name),
            _ => {}
        }
        outcome
    }
}

/// 在解释器线程上执行一次调用
fn run_job(job: Job, memory_limit: usize) -> Result<Option<Value>, ScriptsError> {
    let runtime = Runtime::new().map_err(|error| ScriptsError::Internal(error.to_string()))?;
    runtime.set_memory_limit(memory_limit);
    let deadline = job.deadline;
    runtime.set_interrupt_handler(Some(Box::new(move || {
        std::time::Instant::now() >= deadline
    })));
    let loader = ModLoader::new(&job.root);
    let source = loader
        .source(ENTRY)
        .map_err(|error| ScriptsError::Handler(error.to_string()))?;
    runtime.set_loader(ModResolver, loader);
    let context =
        Context::full(&runtime).map_err(|error| ScriptsError::Internal(error.to_string()))?;
    context.with(|ctx| run_in(&ctx, &job, source))
}

/// 在已建立的上下文里解析模块并调用 handler
fn run_in(ctx: &Ctx<'_>, job: &Job, source: String) -> Result<Option<Value>, ScriptsError> {
    let module = Module::declare(ctx.clone(), ENTRY, source)
        .map_err(|error| js_error(ctx, error, job.deadline))?;
    let (module, promise) = module
        .eval()
        .map_err(|error| js_error(ctx, error, job.deadline))?;
    drop(promise);
    while ctx.execute_pending_job() {}
    if std::time::Instant::now() >= job.deadline {
        return Err(ScriptsError::Timeout);
    }
    let namespace = module
        .namespace()
        .map_err(|error| ScriptsError::Internal(error.to_string()))?;
    if !namespace
        .contains_key(job.handler.as_str())
        .map_err(|error| ScriptsError::Internal(error.to_string()))?
    {
        return Err(ScriptsError::Handler(format!(
            "the mod does not export {}",
            job.handler
        )));
    }
    let handler: Function = namespace
        .get(job.handler.as_str())
        .map_err(|_| ScriptsError::Handler(format!("{} is not a function", job.handler)))?;
    let payload_json = serde_json::to_string(&job.input)
        .map_err(|error| ScriptsError::Handler(error.to_string()))?;
    let payload = ctx
        .json_parse(payload_json)
        .map_err(|error| js_error(ctx, error, job.deadline))?;
    let call = FacadeCall {
        mod_name: job.mod_name.clone(),
        mod_version: job.mod_version,
        scope: job.scope.clone(),
        host: Arc::clone(&job.host),
        failure: new_failure(),
    };
    let facade =
        build_ctx(ctx, &call).map_err(|error| ScriptsError::Internal(error.to_string()))?;
    let result: JsValue = match handler.call((payload, facade)) {
        Ok(result) => result,
        Err(error) => {
            if let Some(error) = call.take_failure() {
                return Err(error);
            }
            return Err(js_error(ctx, error, job.deadline));
        }
    };
    if let Some(error) = call.take_failure() {
        return Err(error);
    }
    outcome_of(ctx, result)
}

/// 把 QuickJS 错误映射成脚本错误，截止时间已过时按超时处理
fn js_error(ctx: &Ctx<'_>, error: rquickjs::Error, deadline: Instant) -> ScriptsError {
    if std::time::Instant::now() >= deadline {
        return ScriptsError::Timeout;
    }
    ScriptsError::Handler(CaughtError::from_error(ctx, error).to_string())
}
