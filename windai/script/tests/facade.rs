//! ctx 的装配：哪些属性存在、存在的是什么形状

use rquickjs::convert::List;
use rquickjs::{
    Context, Ctx, Error as JsError, FromJs, Result as JsResult, Runtime, Value as JsValue,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use wind_event::envelope::Scope;
use wind_event::host::CallMode;
use wind_mod::capability::{Capability, available};
use wind_script::facade::{
    FacadeCall, FacadeError, HostApi, HttpRequest, ModIdentity, TranscriptEntry, build_ctx,
};
/// 一个失败种类与它在 JS 侧的名字
type Failure = (fn() -> FacadeError, &'static str);

/// 一个最小的解释器往返，证明依赖接得上
#[test]
fn an_interpreter_can_be_created_and_run() {
    let rt = rquickjs::Runtime::new().expect("create runtime");
    let ctx = rquickjs::Context::full(&rt).expect("build context");
    let value: i32 = ctx
        .with(|ctx| ctx.eval::<i32, _>("1 + 1"))
        .expect("evaluate");
    assert_eq!(value, 2);
}

/// 一次求值的返回值，按 JSON 形态取出
#[derive(Debug)]
struct Json(Value);

impl<'js> FromJs<'js> for Json {
    fn from_js(ctx: &Ctx<'js>, value: JsValue<'js>) -> JsResult<Self> {
        let text = ctx.json_stringify(value)?;
        let text = match text {
            Some(text) => text.to_string()?,
            None => String::from("null"),
        };
        serde_json::from_str(&text)
            .map(Json)
            .map_err(|error| JsError::new_from_js_message("value", "json", error.to_string()))
    }
}

/// 一个只记内存的宿主实现，用来观察 ctx 把哪些调用送到了哪一边
#[derive(Default)]
struct Memory {
    /// 对外状态的当前值
    state: Mutex<HashMap<String, Value>>,
    /// 私有存储的当前值
    storage: Mutex<HashMap<String, Value>>,
    /// 记录发生过哪些非状态调用
    calls: Mutex<Vec<String>>,
    /// 让 notify 返回的失败，用来观察一条宿主失败如何到达 JS 侧
    notify_failure: Mutex<Option<fn() -> FacadeError>>,
}

impl Memory {
    /// 让接下来的一次 notify 以指定的失败返回
    fn fail_notify_with(&self, error: fn() -> FacadeError) {
        *self.notify_failure.lock().expect("lock the state") = Some(error);
    }
}
impl HostApi for Memory {
    fn state_read(&self, key: &str) -> Option<Value> {
        self.state.lock().expect("lock the state").get(key).cloned()
    }
    fn state_write(&self, key: &str, value: Value) -> Result<(), FacadeError> {
        self.state
            .lock()
            .expect("lock the state")
            .insert(key.to_string(), value);
        Ok(())
    }
    fn state_delete(&self, key: &str) {
        self.state.lock().expect("lock the state").remove(key);
    }
    fn state_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .state
            .lock()
            .expect("lock the state")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    }
    fn storage_read(&self, key: &str) -> Option<Value> {
        self.storage
            .lock()
            .expect("lock the state")
            .get(key)
            .cloned()
    }
    fn storage_write(&self, key: &str, value: Value) {
        self.storage
            .lock()
            .expect("lock the state")
            .insert(key.to_string(), value);
    }
    fn storage_delete(&self, key: &str) {
        self.storage.lock().expect("lock the state").remove(key);
    }
    fn storage_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .storage
            .lock()
            .expect("lock the state")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    }
    fn http_fetch(&self, _init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        self.calls
            .lock()
            .expect("lock the state")
            .push(String::from("http"));
        Ok(Vec::new())
    }
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        // 这个替身一个字节也不取回：每个地址各记一笔，响应体一律为空
        requests
            .into_iter()
            .map(|request| self.http_fetch(request))
            .collect()
    }
    fn agent_prompt(&self, _scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.calls
            .lock()
            .expect("lock the state")
            .push(format!("prompt:{text}"));
        Ok(())
    }
    fn agent_draft(&self, scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.calls
            .lock()
            .expect("lock the state")
            .push(format!("draft:{scope:?}:{text}"));
        Ok(())
    }
    fn agent_spawn(&self, _scope: &Scope, mode: &str, task: &str) -> Result<(), FacadeError> {
        self.calls
            .lock()
            .expect("lock the state")
            .push(format!("spawn:{mode}:{task}"));
        Ok(())
    }
    fn agent_transcript(&self, _scope: &Scope, _limit: Option<u32>) -> Vec<TranscriptEntry> {
        Vec::new()
    }
    fn fs_dir(&self, name: &str) -> PathBuf {
        PathBuf::from(name)
    }
    fn fs_read(&self, _name: &str, _path: &str) -> Result<String, FacadeError> {
        Ok(String::new())
    }
    fn fs_write(&self, _name: &str, _path: &str, _data: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn fs_list(&self, _name: &str, _path: &str) -> Result<Vec<String>, FacadeError> {
        Ok(Vec::new())
    }
    fn fs_remove(&self, _name: &str, _path: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn emit(&self, _scope: &Scope, name: &str, payload: Value) -> Result<(), FacadeError> {
        self.calls
            .lock()
            .expect("lock the state")
            .push(format!("emit:{name}:{payload}"));
        Ok(())
    }
    fn notify(&self, _scope: &Scope, level: &str, text: &str) -> Result<(), FacadeError> {
        self.calls
            .lock()
            .expect("lock the state")
            .push(format!("notify:{level}:{text}"));
        match self.notify_failure.lock().expect("lock the state").take() {
            Some(make) => Err(make()),
            None => Ok(()),
        }
    }
    fn log(&self, level: &str, message: &str) {
        self.calls
            .lock()
            .expect("lock the state")
            .push(format!("log:{level}:{message}"));
    }
}

/// 一次调用的 ctx 输入
fn call(declared: &[Capability], mode: CallMode, host: Arc<dyn HostApi>) -> FacadeCall {
    FacadeCall {
        identity: ModIdentity {
            name: "demo".to_string(),
            version: 1,
        },
        scope: Scope::Topic { topic_id: 7 },
        props: available(declared, mode),
        host,
    }
}

/// 在解释器里跑一段脚本并取出返回值
///
/// 解释器必须跑在没有 tokio 运行时的线程上
fn run<T>(call: &FacadeCall, source: &str) -> T
where
    T: for<'js> FromJs<'js> + Send + 'static,
{
    let call = call.clone_for_thread();
    let source = source.to_string();
    std::thread::spawn(move || {
        let rt = Runtime::new().expect("create runtime");
        let ctx = Context::full(&rt).expect("build context");
        ctx.with(|ctx| {
            let object = build_ctx(&ctx, &call).expect("assemble");
            ctx.globals().set("ctx", object).expect("attach");
            ctx.eval::<T, _>(source.as_str()).expect("evaluate")
        })
    })
    .join()
    .expect("the thread shuts down")
}

#[test]
fn an_intercept_call_has_no_io_properties_at_all() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(
        &[Capability::Http, Capability::Agent, Capability::Fs],
        CallMode::Intercept,
        host,
    );
    let present: List<(bool, bool, bool)> = run(
        &call,
        r#"[
            ("http" in ctx),
            ("agent" in ctx),
            ("fs" in ctx),
        ]"#,
    );
    assert_eq!(
        present.0,
        (false, false, false),
        "these three properties must be absent in an intercept call, not present but erroring"
    );
}

#[test]
fn a_declared_capability_appears_in_a_mode_that_allows_io() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[Capability::Http], CallMode::Request, host);
    let present: bool = run(&call, r#""http" in ctx"#);
    assert!(present);
}

#[test]
fn an_undeclared_capability_does_not_appear_at_all() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[], CallMode::Request, host);
    let present: List<(bool, bool, bool)> = run(
        &call,
        r#"[
            ("state" in ctx),
            ("storage" in ctx),
            ("http" in ctx),
        ]"#,
    );
    assert_eq!(present.0, (false, false, false));
}

#[test]
fn reading_an_absent_capability_throws_a_type_error() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[], CallMode::Request, host);
    let name: String = run(
        &call,
        r#"try { ctx.state.get("k"); "no throw" }
           catch (error) { error.name }"#,
    );
    assert_eq!(
        name, "TypeError",
        "an undeclared capability must be a property read failure"
    );
}

#[test]
fn emit_notify_and_log_are_present_without_any_declaration() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[], CallMode::Intercept, host);
    let present: List<(bool, bool, bool)> = run(
        &call,
        r#"[
            typeof ctx.emit === "function",
            typeof ctx.notify === "function",
            typeof ctx.log === "function",
        ]"#,
    );
    assert_eq!(present.0, (true, true, true));
}

#[test]
fn mod_and_scope_are_read_only() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[], CallMode::Request, host);
    let name: String = run(
        &call,
        r#"try { ctx.mod = null; "no throw" } catch (error) { error.name }"#,
    );
    assert_eq!(
        name, "TypeError",
        "the module is in strict mode, so assigning to a read-only property must throw"
    );
}

#[test]
fn state_round_trips_through_the_host() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[Capability::State], CallMode::Request, host.clone());
    let read_back: Json = run(
        &call,
        r#"ctx.state.set("items", { list: [1, 2] });
           ctx.state.get("items")"#,
    );
    assert_eq!(read_back.0, json!({ "list": [1, 2] }));
    assert_eq!(
        host.state_read("items"),
        Some(json!({ "list": [1, 2] })),
        "the write must land on the host side"
    );
}

#[test]
fn a_state_key_that_was_never_written_reads_back_as_undefined() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[Capability::State], CallMode::Request, host);
    let value: bool = run(&call, r#"ctx.state.get("missing") === undefined"#);
    assert!(
        value,
        "a missing key reads back as undefined without throwing"
    );
}

#[test]
fn state_and_storage_do_not_see_each_other() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(
        &[Capability::State, Capability::Storage],
        CallMode::Request,
        host,
    );
    let leaked: bool = run(
        &call,
        r#"ctx.state.set("k", 1);
           ctx.storage.get("k") === undefined"#,
    );
    assert!(
        leaked,
        "private storage and external state do not share a key space"
    );
}

#[test]
fn keys_lists_only_what_this_call_has_written_or_read() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    host.state_write("a", json!(1)).expect("write");
    host.state_write("b", json!(2)).expect("write");
    let call = call(&[Capability::State], CallMode::Request, host);
    let keys: Vec<String> = run(&call, r#"ctx.state.keys().sort()"#);
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn notify_reaches_the_host_with_its_level_and_text() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Observe, host);
    let _: () = run(&call, r#"ctx.notify({ text: "done", level: "warn" })"#);
    assert_eq!(
        memory
            .calls
            .lock()
            .expect("lock the state")
            .last()
            .map(String::as_str),
        Some("notify:warn:done")
    );
}

#[test]
fn notify_defaults_the_level_to_info() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let _: () = run(&call, r#"ctx.notify({ text: "hi" })"#);
    assert_eq!(
        memory
            .calls
            .lock()
            .expect("lock the state")
            .last()
            .map(String::as_str),
        Some("notify:info:hi")
    );
}

#[test]
fn emit_passes_the_name_and_the_payload_through() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let _: () = run(&call, r#"ctx.emit("notify", { n: 1 })"#);
    assert_eq!(
        memory
            .calls
            .lock()
            .expect("lock the state")
            .last()
            .map(String::as_str),
        Some(r#"emit:notify:{"n":1}"#)
    );
}

#[test]
fn emit_refuses_a_payload_with_no_json_form() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.emit("notify", () => 1); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "FacadeError",
        "a payload that cannot become JSON throws the ctx failure"
    );
    assert_eq!(seen.0.1, "emit payload is not serializable");
    assert!(
        memory.calls.lock().expect("lock the state").is_empty(),
        "an event with an invalid payload must not reach the host"
    );
}

#[test]
fn log_carries_its_level_and_a_joined_message() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Intercept, host);
    let _: () = run(&call, r#"ctx.log("warn", "a", 2)"#);
    let seen = memory.calls.lock().expect("lock the state");
    assert_eq!(seen.last().map(String::as_str), Some("log:warn:a 2"));
}

#[test]
fn a_notify_without_a_text_is_refused() {
    let host: Arc<dyn HostApi> = Arc::new(Memory::default());
    let call = call(&[], CallMode::Request, host);
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.notify({ level: "info" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "a missing text is a property read failure"
    );
    assert!(
        seen.0.1.contains("'undefined'"),
        "the reason is that text is absent, not that ctx.notify is missing: {}",
        seen.0.1
    );
}

#[test]
fn a_notify_level_that_is_not_one_of_the_three_is_refused() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.notify({ text: "x", level: "debug" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "notify accepts only info / warn / error"
    );
    assert!(
        seen.0.1.contains("debug is not one of info, warn, error"),
        "what is refused is this level itself: {}",
        seen.0.1
    );
    assert!(
        memory.calls.lock().expect("lock the state").is_empty(),
        "a refused notification must not reach the host"
    );
}

#[test]
fn a_log_level_outside_the_four_is_refused() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.log("verbose", "x"); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "log accepts only debug / info / warn / error"
    );
    assert!(
        seen.0
            .1
            .contains("verbose is not one of debug, info, warn, error"),
        "what is refused is this level itself: {}",
        seen.0.1
    );
    assert!(
        memory.calls.lock().expect("lock the state").is_empty(),
        "a refused log must not reach the host"
    );
}

#[test]
fn a_facade_failure_arrives_as_a_named_exception() {
    let failures: [Failure; 5] = [
        (|| FacadeError::Unsupported, "CapabilityError"),
        (
            || FacadeError::Path {
                path: String::from("../x"),
            },
            "PathError",
        ),
        (|| FacadeError::Timeout, "TimeoutError"),
        (|| FacadeError::Cancelled, "CancelledError"),
        (|| FacadeError::Failed(String::from("boom")), "FacadeError"),
    ];
    for (make, expected) in failures {
        let memory = Arc::new(Memory::default());
        memory.fail_notify_with(make);
        let host: Arc<dyn HostApi> = memory.clone();
        let call = call(&[], CallMode::Request, host);
        let seen: List<(String, String)> = run(
            &call,
            r#"try { ctx.notify({ text: "x" }); ["no throw", ""] }
               catch (error) { [error.name, error.message] }"#,
        );
        assert_eq!(
            seen.0.0, expected,
            "a host failure arrives on the JS side under this name"
        );
        assert_eq!(
            seen.0.1,
            make().to_string(),
            "the exception message is the failure description"
        );
    }
}

#[test]
fn a_facade_failure_is_an_error_instance_but_not_a_type_error() {
    let memory = Arc::new(Memory::default());
    memory.fail_notify_with(|| FacadeError::Unsupported);
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let seen: List<(bool, bool, bool)> = run(
        &call,
        r#"try { ctx.notify({ text: "x" }); [false, false, false] }
           catch (error) { [
               error instanceof Error,
               error instanceof DOMException,
               error instanceof TypeError,
           ] }"#,
    );
    assert_eq!(
        seen.0,
        (true, true, false),
        "the thrown value is a catchable Error subclass but not the engine's TypeError, so its kind can only be judged by name"
    );
}

#[test]
fn a_log_argument_that_is_not_text_is_refused() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.log("warn", "\ud800"); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "FacadeError",
        "an argument that cannot become text errors instead of collapsing into an empty fragment"
    );
    assert!(
        seen.0.1.contains("is not text"),
        "the failure message points at that argument: {}",
        seen.0.1
    );
    assert!(
        memory.calls.lock().expect("lock the state").is_empty(),
        "a failed log does not produce a log line missing a fragment"
    );
}

#[test]
fn a_lone_surrogate_inside_a_non_string_argument_still_logs() {
    let memory = Arc::new(Memory::default());
    let host: Arc<dyn HostApi> = memory.clone();
    let call = call(&[], CallMode::Request, host);
    let _: () = run(&call, r#"ctx.log("warn", { s: "\ud800" })"#);
    assert_eq!(
        memory
            .calls
            .lock()
            .expect("lock the state")
            .last()
            .map(String::as_str),
        Some(r#"log:warn:{"s":"\ud800"}"#),
        "other values go through JSON, and text after escaping is always valid"
    );
}
