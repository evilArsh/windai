//! 验证 `AllowPolicy` 的判定顺序与 mod 拦截对调用集合的影响。

use std::sync::{Arc, Mutex};
use std::time::Duration;
use wind_core::llm::directory;
use wind_core::llm::policy::{Whitelisted, decide_calls};
use wind_core::models::AllowPolicy;
use wind_event::context::DispatchCx;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase as SubPhase, Subscription};
use wind_wire::tool::FunctionCall;

/// 记录被派发过哪些调用的 `ModHost` 替身
/// 该替身按构造时给定的判定作答，用于观察派发到宿主的是哪些调用
#[derive(Default)]
struct RecordingHost {
    /// 被派发过的 call_id
    seen: Mutex<Vec<String>>,
    /// 被派发进来的载荷
    payloads: Mutex<Vec<serde_json::Value>>,
    /// 判为删除的 call_id
    drop: Vec<&'static str>,
    /// 判为挂起的 call_id
    suspended: Vec<&'static str>,
    /// call_id 的改名表
    rename: Vec<(&'static str, &'static str)>,
}

impl RecordingHost {
    /// 一律放行
    fn passing() -> Self {
        Self::default()
    }

    /// 删掉指定的调用
    fn dropping(call_id: &'static str) -> Self {
        Self {
            drop: vec![call_id],
            ..Self::passing()
        }
    }

    /// 挂起指定的调用
    fn suspending(call_id: &'static str) -> Self {
        Self {
            suspended: vec![call_id],
            ..Self::passing()
        }
    }

    /// 把指定调用改名后返回。这是对调用清单的改写
    fn renaming(call_id: &'static str, name: &'static str) -> Self {
        Self {
            rename: vec![(call_id, name)],
            ..Self::passing()
        }
    }

    /// 被派发过的 call_id
    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("the record is usable").clone()
    }

    /// 被派发进来的载荷
    fn payloads(&self) -> Vec<serde_json::Value> {
        self.payloads.lock().expect("the record is usable").clone()
    }
}

#[async_trait::async_trait]
impl ModHost for RecordingHost {
    async fn call(
        &self,
        _sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        let calls = payload["calls"].as_array().cloned().unwrap_or_default();
        self.payloads
            .lock()
            .expect("the record is usable")
            .push(payload.clone());
        for call in &calls {
            if let Some(id) = call["id"].as_str() {
                self.seen
                    .lock()
                    .expect("the record is usable")
                    .push(id.to_string());
            }
        }
        let kept: Vec<serde_json::Value> = calls
            .into_iter()
            .filter(|call| !self.drop.contains(&call["id"].as_str().unwrap_or_default()))
            .map(|mut call| {
                if let Some(id) = call["id"].as_str()
                    && let Some((_, name)) = self.rename.iter().find(|(from, _)| *from == id)
                {
                    call["name"] = serde_json::json!(name);
                }
                call
            })
            .collect();
        // 载荷形状须与派发时一致。否则改写后的值无法解出声明类型
        Ok(Some(serde_json::json!({
            "instance_id": payload["instance_id"],
            "calls": kept,
            "suspended": self.suspended,
        })))
    }
}

/// 全部订阅者都启用
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 组装一条拦截订阅
fn subscription(event: &'static str) -> Subscription {
    Subscription {
        mod_name: "policy".into(),
        mod_version: 1,
        event,
        phase: Some(SubPhase::Guard),
        handler: "onEvent".into(),
        timeout_ms: 1000,
        registration_seq: 1,
    }
}

/// 组装一个按给定订阅表派发的事件总线
fn bus_with(host: Arc<dyn ModHost>, subs: Vec<Subscription>) -> Arc<Dispatcher> {
    let dispatcher = Dispatcher::new(directory(), Arc::new(AllEnabled), host, Limits::default());
    dispatcher.rebuild_index(subs);
    Arc::new(dispatcher)
}

/// 顶层操作的 `DispatchCx`
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("llm"),
        Duration::from_secs(5),
    )
}

/// 用例使用的实例标识
///
/// `INSTANCE` 与 `cx()` 的话题标识取不同值。两者混淆时相关用例会失败
const INSTANCE: i64 = 7;

/// 构造一条模型返回的调用
fn call(id: &str, name: &str) -> FunctionCall {
    FunctionCall {
        id: id.into(),
        name: name.into(),
        arguments: "{}".into(),
    }
}

#[test]
fn a_star_admits_everything() {
    let policy = AllowPolicy {
        allow: vec!["*".to_string()],
    };
    assert_eq!(policy.admits("anything"), Whitelisted::Hit);
}

#[test]
fn an_empty_list_admits_nothing() {
    // 空名单与 `*` 在字符串处理上只差一个分支，语义却相反
    let policy = AllowPolicy { allow: Vec::new() };
    assert_eq!(policy.admits("anything"), Whitelisted::Miss);
}

#[test]
fn a_named_entry_admits_only_itself() {
    let policy = AllowPolicy {
        allow: vec!["fs__read".to_string()],
    };
    assert_eq!(policy.admits("fs__read"), Whitelisted::Hit);
    assert_eq!(policy.admits("fs__write"), Whitelisted::Miss);
}

#[test]
fn the_stored_policy_is_the_one_the_chain_reads() {
    // 放行名单即话题中存储的 `AllowPolicy`，序列化结果须与存储列的形状一致
    let policy = AllowPolicy {
        allow: vec!["*".into()],
    };
    assert_eq!(policy.admits("anything"), Whitelisted::Hit);
    assert_eq!(
        serde_json::to_string(&policy).expect("serialize"),
        r#"{"allow":["*"]}"#
    );
}

#[tokio::test]
async fn a_hit_call_is_never_dispatched_to_any_mod() {
    let host = Arc::new(RecordingHost::default());
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy {
        allow: vec!["fs__read".to_string()],
    };
    let calls = vec![call("c-1", "fs__read")];

    let outcome = decide_calls(&bus, &cx(), &policy, calls, INSTANCE).await;

    assert!(
        host.seen().is_empty(),
        "a whitelisted call must not be dispatched to any mod: {:?}",
        host.seen()
    );
    assert_eq!(
        outcome.admitted.len(),
        1,
        "it goes straight into the executed set"
    );
    assert!(outcome.pending.is_empty());
}

#[tokio::test]
async fn a_mod_drops_a_miss_by_rewriting_the_call_list() {
    // 载荷可改写。删除一个调用意味着它不会被采纳，也就不会产生结果
    let host = Arc::new(RecordingHost::dropping("c-1"));
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy { allow: Vec::new() };

    let outcome = decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read")],
        INSTANCE,
    )
    .await;

    assert_eq!(host.seen().len(), 1, "a miss must be dispatched to the mod");
    assert!(
        outcome.admitted.is_empty(),
        "the dropped call is not admitted"
    );
}

#[tokio::test]
async fn a_rewrite_that_changes_the_call_list_takes_effect() {
    // 载荷可改写。改名后的调用就是被执行的那个
    let host = Arc::new(RecordingHost::renaming("c-1", "fs__write"));
    let bus = bus_with(host, vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy { allow: Vec::new() };

    let plan = decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read")],
        INSTANCE,
    )
    .await;

    assert!(plan.pending.is_empty());
    assert_eq!(plan.admitted.len(), 1);
    assert_eq!(plan.admitted[0].id, "c-1");
    assert_eq!(
        plan.admitted[0].name, "fs__write",
        "the rewritten name is adopted"
    );
    assert_eq!(plan.admitted[0].arguments, "{}");
}

#[tokio::test]
async fn a_batch_splits_between_the_whitelist_and_the_chain() {
    let host = Arc::new(RecordingHost::passing());
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy {
        allow: vec!["fs__read".to_string()],
    };

    let outcome = decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read"), call("c-2", "fs__write")],
        INSTANCE,
    )
    .await;

    assert_eq!(outcome.admitted.len(), 2, "both are admitted");
    assert_eq!(
        host.seen(),
        vec!["c-2".to_string()],
        "only the miss is dispatched"
    );
}

#[tokio::test]
async fn a_call_a_mod_suspends_becomes_pending() {
    // 未命中且 mod 挂起等待外部答复时，该调用进入等待集合，不被拒绝
    let host = Arc::new(RecordingHost::suspending("c-1"));
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy { allow: Vec::new() };

    let outcome = decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read")],
        INSTANCE,
    )
    .await;

    assert!(outcome.admitted.is_empty());
    assert_eq!(
        outcome.pending.len(),
        1,
        "the deferred call enters the pending set"
    );
    assert_eq!(outcome.pending[0].id, "c-1");
}

#[tokio::test]
async fn an_unanswered_miss_is_admitted_rather_than_left_pending() {
    // 无人判定时，未命中的调用直接放行。该调用进入执行集合，不进入等待集合
    let host = Arc::new(RecordingHost::passing());
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy { allow: Vec::new() };

    let outcome = decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read")],
        INSTANCE,
    )
    .await;

    assert_eq!(
        outcome.admitted.len(),
        1,
        "a miss nobody judged is admitted"
    );
    assert!(
        outcome.pending.is_empty(),
        "it does not enter the pending set"
    );
}

#[tokio::test]
async fn a_suspension_leaves_the_other_misses_admitted() {
    // 挂起逐调用生效。被点名的调用进入等待集合，同批其余未命中的调用照常放行
    let host = Arc::new(RecordingHost::suspending("c-1"));
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy { allow: Vec::new() };

    let outcome = decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read"), call("c-2", "fs__write")],
        INSTANCE,
    )
    .await;

    assert_eq!(
        outcome.admitted.len(),
        1,
        "a call that was not named is admitted as usual"
    );
    assert_eq!(outcome.admitted[0].id, "c-2");
    assert_eq!(
        outcome.pending.len(),
        1,
        "the deferred call enters the pending set"
    );
    assert_eq!(outcome.pending[0].id, "c-1");
}

#[tokio::test]
async fn the_dispatched_payload_carries_the_instance_not_the_topic() {
    // 载荷中的实例标识由调用方提供。作用域里的话题标识不是实例标识
    let host = Arc::new(RecordingHost::passing());
    let bus = bus_with(host.clone(), vec![subscription("core/llm.toolcall")]);
    let policy = AllowPolicy { allow: Vec::new() };

    decide_calls(
        &bus,
        &cx(),
        &policy,
        vec![call("c-1", "fs__read")],
        INSTANCE,
    )
    .await;

    let payloads = host.payloads();
    assert_eq!(payloads.len(), 1, "the miss was dispatched once");
    assert_eq!(
        payloads[0]["instance_id"],
        serde_json::json!(INSTANCE),
        "the payload must carry the instance id given by the caller"
    );
    assert_ne!(
        payloads[0]["instance_id"],
        serde_json::json!(1),
        "the topic id must not appear in this field"
    );
}
