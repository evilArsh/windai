//! `ctx.http` 的形状与出网参数的装配
//!
//! ctx 不认识网络，只把参数解成宿主认识的一份；测试替身记下请求并按地址作答

use rquickjs::convert::List;
use rquickjs::{Context, FromJs, Runtime};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use wind_event::envelope::Scope;
use wind_event::host::CallMode;
use wind_mod::capability::{Capability, available};
use wind_script::facade::{
    FacadeCall, FacadeError, HostApi, HttpRequest, ModIdentity, TranscriptEntry, build_ctx,
};

/// 一个只记内存的宿主实现
///
/// 出网记录每次请求并按地址作答，批量走一次调用
struct Memory {
    /// 不在 `bodies` 里的地址取这一段响应体
    default_body: Vec<u8>,
    /// 每个地址各自的响应体
    bodies: Mutex<HashMap<String, Vec<u8>>>,
    /// 每个地址各自的失败，它优先于响应体
    failures: Mutex<HashMap<String, Maker>>,
    /// 每次批量出网收到多少个地址，按调用顺序
    batches: Mutex<Vec<usize>>,
    /// 到达宿主的出网请求，按调用顺序
    requests: Mutex<Vec<HttpRequest>>,
}

/// 造一次失败的入口
type Maker = fn() -> FacadeError;

/// 一个地址与它要给的失败
type Failure = (&'static str, Maker);

impl Memory {
    /// 一个总以这段文本作答的宿主实现
    fn returning_http(body: &str) -> Self {
        Self {
            default_body: body.as_bytes().to_vec(),
            bodies: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            batches: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// 让这些地址各自的响应体是这一段文本
    fn returning(self, bodies: &[(&str, &str)]) -> Self {
        let mut map = self.bodies.lock().expect("lock the state");
        for (url, body) in bodies {
            map.insert((*url).to_string(), body.as_bytes().to_vec());
        }
        drop(map);
        self
    }

    /// 让这个地址以一段不是合法文本的字节作答
    fn returning_raw(self, url: &str, bytes: &[u8]) -> Self {
        self.bodies
            .lock()
            .expect("lock the state")
            .insert(url.to_string(), bytes.to_vec());
        self
    }

    /// 让这些地址以指定的失败作答
    fn failing(self, failures: &[Failure]) -> Self {
        let mut map = self.failures.lock().expect("lock the state");
        for (url, make) in failures {
            map.insert((*url).to_string(), *make);
        }
        drop(map);
        self
    }

    /// 到过宿主的请求，按调用顺序
    fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("lock the state").clone()
    }

    /// 到过宿主的地址，按调用顺序
    fn urls(&self) -> Vec<String> {
        self.requests()
            .into_iter()
            .map(|request| request.url)
            .collect()
    }

    /// 每次批量出网收到的地址个数，按调用顺序
    fn batches(&self) -> Vec<usize> {
        self.batches.lock().expect("lock the state").clone()
    }

    /// 一个地址的作答：被点名失败的地址交出对应的失败，其余交出响应体
    fn answer(&self, request: &HttpRequest) -> Result<Vec<u8>, FacadeError> {
        if let Some(make) = self
            .failures
            .lock()
            .expect("lock the state")
            .get(&request.url)
        {
            return Err(make());
        }
        let body = self
            .bodies
            .lock()
            .expect("lock the state")
            .get(&request.url)
            .cloned()
            .unwrap_or_else(|| self.default_body.clone());
        Ok(body)
    }
}

impl HostApi for Memory {
    fn state_read(&self, _key: &str) -> Option<Value> {
        None
    }
    fn state_write(&self, _key: &str, _value: Value) -> Result<(), FacadeError> {
        Ok(())
    }
    fn state_delete(&self, _key: &str) {}
    fn state_keys(&self) -> Vec<String> {
        Vec::new()
    }
    fn storage_read(&self, _key: &str) -> Option<Value> {
        None
    }
    fn storage_write(&self, _key: &str, _value: Value) {}
    fn storage_delete(&self, _key: &str) {}
    fn storage_keys(&self) -> Vec<String> {
        Vec::new()
    }
    fn http_fetch(&self, init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        self.requests
            .lock()
            .expect("lock the state")
            .push(init.clone());
        self.answer(&init)
    }
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        self.batches
            .lock()
            .expect("lock the state")
            .push(requests.len());
        requests
            .into_iter()
            .map(|request| self.http_fetch(request))
            .collect()
    }
    fn agent_prompt(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn agent_draft(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn agent_spawn(&self, _scope: &Scope, _mode: &str, _task: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn agent_transcript(&self, _scope: &Scope, _limit: Option<u32>) -> Vec<TranscriptEntry> {
        Vec::new()
    }
    fn fs_dir(&self, name: &str) -> PathBuf {
        PathBuf::from(name)
    }
    fn fs_read(&self, _name: &str, _path: &str) -> Result<String, FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn fs_write(&self, _name: &str, _path: &str, _data: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn fs_list(&self, _name: &str, _path: &str) -> Result<Vec<String>, FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn fs_remove(&self, _name: &str, _path: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn emit(&self, _scope: &Scope, _name: &str, _payload: Value) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn notify(&self, _scope: &Scope, _level: &str, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn log(&self, _level: &str, _message: &str) {}
}

/// 一次只带出网能力的 ctx 调用
fn call(declared: &[Capability], mode: CallMode, host: Arc<dyn HostApi>) -> FacadeCall {
    FacadeCall {
        identity: ModIdentity {
            name: String::from("demo"),
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
fn fetch_text_returns_the_body_the_host_produced() {
    let host = Arc::new(Memory::returning_http("hello"));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let body: String = run(&call, r#"ctx.http.fetchText("https://example.com/a")"#);
    assert_eq!(body, "hello");
    assert_eq!(
        host.requests().first().map(|request| request.url.as_str()),
        Some("https://example.com/a")
    );
}

#[test]
fn fetch_text_passes_the_method_headers_and_body_through() {
    let host = Arc::new(Memory::returning_http("ok"));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let _: String = run(
        &call,
        r#"ctx.http.fetchText("https://example.com/a", {
               method: "post",
               headers: { "x-k": "v" },
               body: "{}",
               timeoutMs: 500,
           })"#,
    );
    let request = host.requests().remove(0);
    assert_eq!(
        request.method, "POST",
        "the method's case is normalised by the host to uppercase"
    );
    assert_eq!(request.headers, vec![("x-k".to_string(), "v".to_string())]);
    assert_eq!(request.body.as_deref(), Some("{}"));
    assert_eq!(request.timeout_ms, Some(500));
}

#[test]
fn a_method_the_facade_does_not_know_is_refused_before_it_reaches_the_host() {
    let host = Arc::new(Memory::returning_http("ok"));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.http.fetchText("https://a", { method: "TRACE" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "a refused method is an argument failure"
    );
    assert!(
        seen.0.1.contains("TRACE is not one of"),
        "what is refused is this method itself: {}",
        seen.0.1
    );
    assert!(
        host.requests().is_empty(),
        "a refused method must not reach the host"
    );
}

#[test]
fn a_timeout_that_is_not_a_whole_number_of_milliseconds_is_refused() {
    for value in ["-1", "1e300", "0/0", "Infinity", "500.5"] {
        let host = Arc::new(Memory::returning_http("ok"));
        let call = call(&[Capability::Http], CallMode::Request, host.clone());
        let source = format!(
            r#"try {{ ctx.http.fetchText("https://a", {{ timeoutMs: {value} }}); ["no throw", ""] }}
               catch (error) {{ [error.name, error.message] }}"#
        );
        let seen: List<(String, String)> = run(&call, &source);
        assert_eq!(
            seen.0.0, "TypeError",
            "with timeoutMs {value} it is an argument failure; what is thrown is {}",
            seen.0.1
        );
        assert!(
            host.requests().is_empty(),
            "with timeoutMs {value} nothing may reach the host"
        );
    }
}

#[test]
fn a_response_body_that_is_not_text_is_a_failure() {
    let host = Arc::new(Memory::returning_http("ok").returning_raw("https://a", &[0xff, 0xfe]));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.http.fetchText("https://a"); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_ne!(
        seen.0.0, "no throw",
        "a response body that cannot be read as text is a failure"
    );
    assert!(
        seen.0.1.contains("https://a is not valid utf-8"),
        "the failure message names that address: {}",
        seen.0.1
    );
}

#[test]
fn fetch_many_answers_in_the_input_order() {
    let host = Arc::new(Memory::returning_http("none").returning(&[
        ("https://a", "body-a"),
        ("https://b", "body-b"),
        ("https://c", "body-c"),
    ]));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let bodies: Vec<String> = run(
        &call,
        r#"ctx.http.fetchMany(["https://c", "https://a", "https://b"])"#,
    );
    assert_eq!(
        bodies,
        ["body-c", "body-a", "body-b"],
        "each position holds the response body of its own address"
    );
    assert_eq!(
        host.urls(),
        ["https://c", "https://a", "https://b"],
        "the order handed to the host is the input order"
    );
    assert_eq!(
        host.batches(),
        [3],
        "the whole list goes through one host call"
    );
}

#[test]
fn fetch_many_gives_every_url_the_same_options() {
    let host = Arc::new(Memory::returning_http("b"));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let bodies: Vec<String> = run(
        &call,
        r#"ctx.http.fetchMany(["https://a", "https://b"], { method: "post", body: "x" })"#,
    );
    assert_eq!(bodies, ["b", "b"]);
    let requests = host.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(request.method, "POST");
        assert_eq!(request.body.as_deref(), Some("x"));
    }
}

#[test]
fn an_http_call_in_an_intercept_handler_cannot_even_be_written() {
    let host = Arc::new(Memory::returning_http("ok"));
    let call = call(&[Capability::Http], CallMode::Intercept, host);
    let shape: String = run(&call, r#"typeof ctx.http"#);
    assert_eq!(shape, "undefined");
}

#[test]
fn an_init_that_is_omitted_or_undefined_or_null_is_the_same_thing() {
    let host = Arc::new(Memory::returning_http("ok"));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let done: i32 = run(
        &call,
        r#"ctx.http.fetchText("https://a");
           ctx.http.fetchText("https://b", undefined);
           ctx.http.fetchText("https://c", null);
           ctx.http.fetchMany(["https://d"]);
           ctx.http.fetchMany(["https://e"], undefined);
           1"#,
    );
    assert_eq!(done, 1);
    let requests = host.requests();
    assert_eq!(
        requests.len(),
        5,
        "init may be omitted entirely; both methods must still be callable"
    );
    for request in &requests {
        assert_eq!(request.method, "GET", "the default method is GET");
        assert!(request.headers.is_empty());
        assert!(request.body.is_none());
        assert!(request.timeout_ms.is_none());
    }
}

#[test]
fn http_exposes_exactly_two_methods() {
    let host = Arc::new(Memory::returning_http("ok"));
    let call = call(&[Capability::Http], CallMode::Request, host);
    let keys: Vec<String> = run(&call, "Object.getOwnPropertyNames(ctx.http).sort()");
    assert_eq!(keys, ["fetchMany", "fetchText"]);
}

#[test]
fn fetch_many_places_an_empty_string_where_a_request_failed() {
    let host = Arc::new(
        Memory::returning_http("none")
            .returning(&[("https://a", "body-a"), ("https://c", "body-c")])
            .failing(&[("https://b", || FacadeError::Timeout)]),
    );
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let bodies: Vec<String> = run(
        &call,
        r#"ctx.http.fetchMany(["https://a", "https://b", "https://c"], { onError: "null" })"#,
    );
    assert_eq!(
        bodies,
        ["body-a", "", "body-c"],
        "a failed position holds an empty string; the other positions carry the real response body"
    );
    assert_eq!(
        host.urls(),
        ["https://a", "https://b", "https://c"],
        "one failing address does not interrupt the remaining addresses"
    );
    assert_eq!(
        host.batches(),
        [3],
        "the whole list still goes through one host call"
    );
}

#[test]
fn fetch_many_throws_on_a_failed_address_by_default_and_when_asked_to() {
    for args in [
        r#"["https://a", "https://b", "https://c"]"#,
        r#"["https://a", "https://b", "https://c"], { onError: "throw" }"#,
    ] {
        let host = Arc::new(
            Memory::returning_http("ok").failing(&[("https://b", || FacadeError::Timeout)]),
        );
        let call = call(&[Capability::Http], CallMode::Request, host.clone());
        let source = format!(
            r#"try {{ ctx.http.fetchMany({args}); ["no throw", ""] }}
               catch (error) {{ [error.name, error.message] }}"#
        );
        let seen: List<(String, String)> = run(&call, &source);
        assert_eq!(
            seen.0.0, "TimeoutError",
            "with args {args} the host failure arrives on the JS side under its name; what is thrown is {}",
            seen.0.1
        );
    }
}

#[test]
fn fetch_many_throws_when_the_round_is_cancelled_whatever_the_options_say() {
    let host =
        Arc::new(Memory::returning_http("ok").failing(&[("https://b", || FacadeError::Cancelled)]));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.http.fetchMany(["https://a", "https://b"], { onError: "null" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "CancelledError",
        "a call cancelled by the user must not collapse into an empty position; what is thrown is {}",
        seen.0.1
    );
}

#[test]
fn fetch_many_throws_when_the_deployment_has_no_http_whatever_the_options_say() {
    let host = Arc::new(
        Memory::returning_http("ok").failing(&[("https://b", || FacadeError::Unsupported)]),
    );
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.http.fetchMany(["https://a", "https://b"], { onError: "null" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "CapabilityError",
        "this assembly lacks this capability, so it is not that one request failed; what is thrown is {}",
        seen.0.1
    );
}

#[test]
fn fetch_text_throws_even_when_the_options_say_null() {
    let host =
        Arc::new(Memory::returning_http("ok").failing(&[("https://a", || FacadeError::Timeout)]));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.http.fetchText("https://a", { onError: "null" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TimeoutError",
        "a single address has no position to place a placeholder in; a failure is a failure, and what is thrown is {}",
        seen.0.1
    );
}

#[test]
fn an_on_error_that_is_neither_throw_nor_null_is_refused() {
    let host = Arc::new(Memory::returning_http("ok"));
    let call = call(&[Capability::Http], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.http.fetchMany(["https://a"], { onError: "ignore" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "a refused value is an argument failure"
    );
    assert!(
        seen.0.1.contains("ignore is not one of throw, null"),
        "what is refused is this value itself: {}",
        seen.0.1
    );
    assert!(
        host.requests().is_empty(),
        "a refused value must not reach the host"
    );
    assert!(
        host.batches().is_empty(),
        "a refused value does not produce a batched network call"
    );
}
