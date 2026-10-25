//! `ctx.agent` 的四个方法与参数校验
//!
//! ctx 不认识 agent 系统，只把解好的参数原样交给宿主；测试替身按结构记下每次
//! 调用，要断的正是每个实参

use rquickjs::convert::List;
use rquickjs::{
    Context, Ctx, Error as JsError, FromJs, Result as JsResult, Runtime, Value as JsValue,
};
use serde_json::{Value, json};
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

/// 一次到达宿主的调用
#[derive(Debug, Clone, PartialEq)]
struct Seen {
    /// 本次调用所在的作用域
    scope: Scope,
    /// 被调用的方法，以及它收到的实参
    method: Method,
}

/// 被调用的方法与它的实参
#[derive(Debug, Clone, PartialEq)]
enum Method {
    /// 追加一条用户消息，带那段文本
    Prompt(String),
    /// 提供一段待确认文本，带那段文本
    Draft(String),
    /// 派生一个子实例，带它的关系与任务
    Spawn {
        /// 子实例与父实例的关系
        mode: String,
        /// 交给子实例的初始任务
        task: String,
    },
    /// 读最近的消息，带要求的条数
    Transcript(Option<u32>),
}

/// 一个只记内存的宿主实现
///
/// 转录内容由它给出，ctx 只负责把取得的条目原样装成 JS 数组
struct Memory {
    /// 到达宿主的调用，按调用顺序
    seen: Mutex<Vec<Seen>>,
    /// 转录要交回的条目
    transcript: Mutex<Vec<TranscriptEntry>>,
    /// 接下来一次调用的失败，用掉就没了
    failure: Mutex<Option<fn() -> FacadeError>>,
}

impl Memory {
    /// 一个转录里没有条目的宿主实现
    fn new() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
            transcript: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
        }
    }

    /// 一个以这些条目作答的宿主实现
    fn holding(entries: Vec<TranscriptEntry>) -> Self {
        let memory = Self::new();
        *memory.transcript.lock().expect("lock the state") = entries;
        memory
    }

    /// 一个接下来的一次调用会失败的宿主实现
    fn failing(self, make: fn() -> FacadeError) -> Self {
        *self.failure.lock().expect("lock the state") = Some(make);
        self
    }

    /// 到过宿主的调用，按调用顺序
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock the state").clone()
    }

    /// 记一次到达宿主的调用
    fn record(&self, scope: &Scope, method: Method) {
        self.seen.lock().expect("lock the state").push(Seen {
            scope: scope.clone(),
            method,
        });
    }

    /// 一次预置的失败，用掉就没了
    fn answer(&self) -> Result<(), FacadeError> {
        match self.failure.lock().expect("lock the state").take() {
            Some(make) => Err(make()),
            None => Ok(()),
        }
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
    fn http_fetch(&self, _init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn http_fetch_many(&self, _requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        Vec::new()
    }
    fn agent_prompt(&self, scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.record(scope, Method::Prompt(text.to_string()));
        self.answer()
    }
    fn agent_draft(&self, scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.record(scope, Method::Draft(text.to_string()));
        self.answer()
    }
    fn agent_spawn(&self, scope: &Scope, mode: &str, task: &str) -> Result<(), FacadeError> {
        self.record(
            scope,
            Method::Spawn {
                mode: mode.to_string(),
                task: task.to_string(),
            },
        );
        self.answer()
    }
    fn agent_transcript(&self, scope: &Scope, limit: Option<u32>) -> Vec<TranscriptEntry> {
        self.record(scope, Method::Transcript(limit));
        self.transcript.lock().expect("lock the state").clone()
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

/// 本次调用所在的话题
fn topic() -> Scope {
    Scope::Topic { topic_id: 7 }
}

/// 一次只带出 agent 能力的 ctx 调用
fn call(declared: &[Capability], mode: CallMode, host: Arc<dyn HostApi>) -> FacadeCall {
    FacadeCall {
        identity: ModIdentity {
            name: String::from("demo"),
            version: 1,
        },
        scope: topic(),
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
fn prompt_and_draft_reach_the_host_as_two_different_calls() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let _: () = run(
        &call,
        r#"ctx.agent.draft("suggest one line");
           ctx.agent.prompt("say one line");"#,
    );
    assert_eq!(
        host.seen(),
        vec![
            Seen {
                scope: topic(),
                method: Method::Draft(String::from("suggest one line")),
            },
            Seen {
                scope: topic(),
                method: Method::Prompt(String::from("say one line")),
            },
        ],
        "each method makes its own call and the texts do not cross over"
    );
}

#[test]
fn spawn_carries_the_mode_and_the_task() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let _: () = run(
        &call,
        r#"ctx.agent.spawn({ task: "research", mode: "background" })"#,
    );
    assert_eq!(
        host.seen(),
        vec![Seen {
            scope: topic(),
            method: Method::Spawn {
                mode: String::from("background"),
                task: String::from("research"),
            },
        }],
        "the mode and the task are handed to the host unchanged"
    );
}

#[test]
fn spawn_defaults_to_sync() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let _: () = run(&call, r#"ctx.agent.spawn({ task: "research" })"#);
    assert_eq!(
        host.seen(),
        vec![Seen {
            scope: topic(),
            method: Method::Spawn {
                mode: String::from("sync"),
                task: String::from("research"),
            },
        }],
        "without a given mode the child is in sync with the parent"
    );
}

#[test]
fn spawn_refuses_a_mode_it_does_not_know() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.agent.spawn({ task: "x", mode: "parallel" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "a refused mode value is an argument failure"
    );
    assert!(
        seen.0.1.contains("parallel is not one of sync, background"),
        "what is refused is this value itself, not that ctx.agent.spawn is missing: {}",
        seen.0.1
    );
    assert!(
        host.seen().is_empty(),
        "a refused mode value must not reach the host"
    );
}

#[test]
fn spawn_refuses_an_object_without_a_task() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let seen: List<(String, String)> = run(
        &call,
        r#"try { ctx.agent.spawn({ mode: "sync" }); ["no throw", ""] }
           catch (error) { [error.name, error.message] }"#,
    );
    assert_eq!(
        seen.0.0, "TypeError",
        "a missing task is a property read failure"
    );
    assert!(
        seen.0.1.contains("'undefined'"),
        "the reason is that task is absent: {}",
        seen.0.1
    );
    assert!(
        host.seen().is_empty(),
        "with no task to dispatch nothing may reach the host"
    );
}

#[test]
fn spawn_refuses_a_call_without_an_object() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let name: String = run(
        &call,
        r#"try { ctx.agent.spawn(); "no throw" } catch (error) { error.name }"#,
    );
    assert_eq!(
        name, "TypeError",
        "the options are required; without them there is no task"
    );
    assert!(host.seen().is_empty());
}

#[test]
fn transcript_returns_the_entries_the_host_gave() {
    let host = Arc::new(Memory::holding(vec![
        TranscriptEntry {
            message_id: 7,
            role: String::from("user"),
            text: String::from("question"),
        },
        TranscriptEntry {
            message_id: 6,
            role: String::from("assistant"),
            text: String::from("answer"),
        },
    ]));
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let entries: Vec<List<(i64, String, String)>> = run(
        &call,
        r#"ctx.agent.transcript(5).map((entry) => [entry.message_id, entry.role, entry.text])"#,
    );
    assert_eq!(
        entries.len(),
        2,
        "however many entries the host gives, that many are built; it does not pad to the requested count"
    );
    assert_eq!(
        entries[0].0,
        (7, String::from("user"), String::from("question")),
        "each entry is built from its own three fields, in the order the host gave"
    );
    assert_eq!(
        entries[1].0,
        (6, String::from("assistant"), String::from("answer"))
    );
    assert_eq!(
        host.seen(),
        vec![Seen {
            scope: topic(),
            method: Method::Transcript(Some(5)),
        }],
        "the requested count is handed to the host unchanged"
    );
}

#[test]
fn a_transcript_entry_carries_exactly_the_three_contract_fields() {
    let host = Arc::new(Memory::holding(vec![TranscriptEntry {
        message_id: 7,
        role: String::from("user"),
        text: String::from("question"),
    }]));
    let call = call(&[Capability::Agent], CallMode::Request, host);
    let names: Vec<String> = run(
        &call,
        "Object.getOwnPropertyNames(ctx.agent.transcript(1)[0]).sort()",
    );
    assert_eq!(names, ["message_id", "role", "text"]);
}

#[test]
fn transcript_hands_the_limit_through_as_given() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let done: i32 = run(
        &call,
        r#"ctx.agent.transcript();
           ctx.agent.transcript(undefined);
           ctx.agent.transcript(null);
           1"#,
    );
    assert_eq!(
        done, 1,
        "the count may be omitted entirely, or given as undefined or null"
    );
    assert_eq!(
        host.seen(),
        vec![
            Seen {
                scope: topic(),
                method: Method::Transcript(None),
            };
            3
        ],
        "without a count it hands None: both the default and the cap are the host's policy, ctx does not decide them"
    );
}

#[test]
fn a_limit_that_is_not_a_whole_number_of_messages_is_refused() {
    let cases = [
        ("-1", "is not a number of messages between 0 and 4294967295"),
        (
            "1e300",
            "is not a number of messages between 0 and 4294967295",
        ),
        (
            "4294967296",
            "is not a number of messages between 0 and 4294967295",
        ),
        ("Infinity", "is not a whole number of messages"),
        ("0/0", "is not a whole number of messages"),
        ("500.5", "is not a whole number of messages"),
    ];
    for (value, fragment) in cases {
        let host = Arc::new(Memory::new());
        let call = call(&[Capability::Agent], CallMode::Request, host.clone());
        let source = format!(
            r#"try {{ ctx.agent.transcript({value}); ["no throw", ""] }}
               catch (error) {{ [error.name, error.message] }}"#
        );
        let seen: List<(String, String)> = run(&call, &source);
        assert_eq!(
            seen.0.0, "TypeError",
            "with limit {value} it is an argument failure; what is thrown is {}",
            seen.0.1
        );
        assert!(
            seen.0.1.contains("'limit'"),
            "the failure message points at that argument: {}",
            seen.0.1
        );
        assert!(
            seen.0.1.contains(fragment),
            "the failure message with limit {value}: {}",
            seen.0.1
        );
        assert!(
            host.seen().is_empty(),
            "with limit {value} nothing may reach the host"
        );
    }
}

#[test]
fn a_limit_at_the_top_of_its_range_is_taken() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let _: () = run(&call, "ctx.agent.transcript(4294967295)");
    assert_eq!(
        host.seen(),
        vec![Seen {
            scope: topic(),
            method: Method::Transcript(Some(u32::MAX)),
        }],
        "the upper bound itself is accepted; what is refused is the one above it"
    );
}

#[test]
fn a_limit_that_is_not_a_number_is_refused() {
    for value in ["\"5\"", "true", "{}"] {
        let host = Arc::new(Memory::new());
        let call = call(&[Capability::Agent], CallMode::Request, host.clone());
        let source = format!(
            r#"try {{ ctx.agent.transcript({value}); "no throw" }}
               catch (error) {{ error.name }}"#
        );
        let name: String = run(&call, &source);
        assert_eq!(
            name, "TypeError",
            "with limit {value} it is an argument failure"
        );
        assert!(
            host.seen().is_empty(),
            "with limit {value} nothing may reach the host"
        );
    }
}

#[test]
fn a_transcript_entry_serialises_its_three_fields() {
    let host = Arc::new(Memory::holding(vec![TranscriptEntry {
        message_id: 7,
        role: String::from("user"),
        text: String::from("question"),
    }]));
    let call = call(&[Capability::Agent], CallMode::Request, host.clone());
    let entry: Json = run(&call, "ctx.agent.transcript(1)[0]");
    assert_eq!(
        entry.0,
        json!({ "message_id": 7, "role": "user", "text": "question" }),
        "the entry's fields take part in serialisation; otherwise JSON.stringify yields only an empty object"
    );
    let spread: Json = run(&call, "({ ...ctx.agent.transcript(1)[0] })");
    assert_eq!(
        spread.0,
        json!({ "message_id": 7, "role": "user", "text": "question" }),
        "spreading an entry likewise yields the three fields"
    );
    let name: String = run(
        &call,
        r#"try { const entry = ctx.agent.transcript(1)[0]; entry.role = "system"; "no throw" }
           catch (error) { error.name }"#,
    );
    assert_eq!(
        name, "TypeError",
        "enumerable does not mean writable: assigning a field must still throw"
    );
}

#[test]
fn agent_exposes_exactly_four_methods() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Request, host);
    let keys: Vec<String> = run(&call, "Object.getOwnPropertyNames(ctx.agent).sort()");
    assert_eq!(keys, ["draft", "prompt", "spawn", "transcript"]);
}

#[test]
fn the_agent_capability_is_absent_in_an_intercept_handler() {
    let host = Arc::new(Memory::new());
    let call = call(&[Capability::Agent], CallMode::Intercept, host);
    let shape: String = run(&call, r#"typeof ctx.agent"#);
    assert_eq!(shape, "undefined");
}

#[test]
fn a_failure_from_the_host_arrives_as_a_named_exception() {
    let failures: [Failure; 2] = [
        (|| FacadeError::Cancelled, "CancelledError"),
        (|| FacadeError::Unsupported, "CapabilityError"),
    ];
    for (make, expected) in failures {
        let host = Arc::new(Memory::new().failing(make));
        let call = call(&[Capability::Agent], CallMode::Request, host);
        let seen: List<(String, String)> = run(
            &call,
            r#"try { ctx.agent.prompt("x"); ["no throw", ""] }
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
