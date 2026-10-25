//! 端到端：一轮真实往返跑在一棵真树上
//!
//! 这组用例使用 `wind-runtime` 的装配本身。话题、模型与实例由夹具直接写入数据库，
//! 一轮从一次等待的答复发起
//!
//! 只覆盖经答复唤醒的那一轮，验证一次运行能在真树上跑完

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use wind_core::agent::behavior::Outcome;
use wind_core::agent::domain::AgentDomain;
use wind_core::agent::tree::phase::Phase;
use wind_core::db::Db;
use wind_core::llm::events::{CORE_LLM_TOOLCALL, ToolCalls};
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentPhase, AgentRole, AllowPolicy, BlockKind, ModelCreate,
    ProviderCreate, TopicCreate, TopicUpdate, WaitOpen,
};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModHost};
use wind_event::subscription::Subscription;
use wind_mod::dirs::AppDirs;
use wind_runtime::{Assembly, RuntimeError};

/// 等一轮跑完的时限
const PATIENCE: Duration = Duration::from_secs(10);

const POLL: Duration = Duration::from_millis(5);

/// 夹具里 mod 的名字
const MOD: &str = "guard";

/// 写入数据库的工具结果的块类型
///
/// 块类型在存储里是裸字符串
/// 本装配的 request 通道给出的拒绝原因
///
/// 没有脚本运行时，一次 mod request 的结果就是该拒绝
const SEAM_REFUSAL: &str = "no script runtime";

/// 回查事件名时一次读取的日志条数
const LOG_LIMIT: i64 = 4096;

/// 模型会返回的两个工具名
///
/// 它们来自 mod 声明为 agent 可调的 request，模型看到的就是这两个名字
const READ: &str = "m_guard__read";
const WRITE: &str = "m_guard__write";

/// 订阅工具调用并声明两个 agent 可调 request 的清单
const MOD_MANIFEST: &str = r#"{"name":"guard","version":1,
    "requests":{"read":{"handler":"onRead","callableBy":["agent"]},
                "write":{"handler":"onWrite","callableBy":["agent"]}},
    "events":{"on":{"core/llm.toolcall":{"handler":"onToolCall","phase":"guard"}}}}"#;

/// 一个记录被派发过什么、并对工具调用作出判定的宿主替身
///
/// 它按构造时给定的规则作答，用来观察谁被派发、谁没有；其余载荷原样放回
#[derive(Default)]
struct RecordingHost {
    /// 被派发过的事件名，按到达顺序
    seen: Mutex<Vec<String>>,
    /// 试图用改写删掉哪些调用标识
    rewrite_away: Vec<&'static str>,
}

impl RecordingHost {
    /// 一律放行
    fn passing() -> Self {
        Self::default()
    }

    /// 用一次改写试图删掉指定的调用
    fn rewriting_away(call_id: &'static str) -> Self {
        Self {
            rewrite_away: vec![call_id],
            ..Self::passing()
        }
    }

    /// 被派发过的事件名
    fn seen_names(&self) -> Vec<String> {
        self.seen.lock().expect("the record is usable").clone()
    }

    /// 被派发进来的次数
    fn calls(&self) -> usize {
        self.seen.lock().expect("the record is usable").len()
    }
}

#[async_trait::async_trait]
impl ModHost for RecordingHost {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.seen
            .lock()
            .expect("the record is usable")
            .push(sub.event.to_string());
        if sub.event != "core/llm.toolcall" {
            return Ok(None);
        }
        let calls = payload["calls"].as_array().cloned().unwrap_or_default();
        let kept: Vec<serde_json::Value> = calls
            .into_iter()
            .filter(|call| {
                !self
                    .rewrite_away
                    .contains(&call["id"].as_str().unwrap_or_default())
            })
            .collect();
        // 载荷的形状必须与派发进来时一致，否则折叠得到的值无法解析为声明类型
        Ok(Some(serde_json::json!({
            "instance_id": payload["instance_id"],
            "calls": kept,
        })))
    }
}

/// 一套以装配本身驱动的夹具
struct Rig {
    /// 临时目录必须被持有，否则库文件随目录一起删除
    _tmp: TempDir,
    /// 应用目录，重开时使用同一份
    dirs: AppDirs,
    /// 被驱动的装配
    assembly: Assembly,
    server: Server,
    /// 本用例自己持有的记录宿主
    host: Arc<RecordingHost>,
    topic_id: i64,
    instance_id: i64,
    /// 唤醒一轮所用的挂起句柄
    handle: String,
    manifest: String,
    /// 跑完一轮时从数据库读到的、真正执行过的调用
    executed: Mutex<Vec<String>>,
}

impl Rig {
    /// 建一套夹具，包含临时库、真装配、一个记录宿主与一个按次序应答的模型
    ///
    /// 提供商、模型、话题与实例都由夹具直接写入数据库。装配不是本文件的主题，而话题
    /// 绑定模型与实例行的写入接口在用例层都不存在。
    ///
    /// 模型应答按请求次序取，用完之后一直用最后一项
    async fn build(
        policy: AllowPolicy,
        manifest: &str,
        replies: Vec<String>,
        host: Arc<RecordingHost>,
    ) -> Self {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let dirs = AppDirs::under(tmp.path());
        let server = Server::serve(replies).await;

        let db = Db::open(&dirs.db_file()).await.expect("open database");
        db.init_schema().await.expect("create table");
        let storage = Storage::new(db);
        let provider = storage
            .provider_create(ProviderCreate {
                name: "local".into(),
                base_url: server.base_url(),
                ..Default::default()
            })
            .await
            .expect("create provider");
        let model = storage
            .model_create(ModelCreate::new(
                provider,
                "local-model".into(),
                AdapterType::OpenAICompletion,
            ))
            .await
            .expect("build model");
        let topic_id = storage
            .topic_create(TopicCreate {
                label: "test".into(),
                ..Default::default()
            })
            .await
            .expect("create topic");
        storage
            .topic_update(
                topic_id,
                TopicUpdate {
                    tool_approval_policy: Some(policy),
                    ..Default::default()
                },
            )
            .await
            .expect("set the allow policy");
        // 话题绑定模型。用例层没有绑定模型的接口，夹具直接改该列
        sqlx::query("UPDATE topics SET model_id = ? WHERE id = ?")
            .bind(model)
            .bind(topic_id)
            .execute(storage.pool())
            .await
            .expect("bind the model");
        if !manifest.is_empty() {
            let mod_id = storage
                .mod_install(MOD, manifest, None)
                .await
                .expect("install the mod");
            storage
                .mod_enable(topic_id, mod_id)
                .await
                .expect("enable the mod");
        }

        // 主实例先写一行，再转入等待外部答复。根实例的行没有生产者，而恢复一棵挂起的
        // 树是让节点开始运行的现有入口
        let handle = "h-1".to_string();
        let instance_id = storage
            .agent_create(AgentInstanceCreate {
                parent_id: None,
                topic_id,
                role: AgentRole::Main,
                mode: None,
                phase: AgentPhase::Running,
                phase_payload: None,
            })
            .await
            .expect("create instance");
        storage
            .agent_save_phase(
                instance_id,
                AgentPhase::Suspended,
                Some(&suspended_payload(instance_id, &handle)),
            )
            .await
            .expect("suspend the instance");
        storage
            .wait_open(WaitOpen {
                handle: handle.to_string(),
                instance_id,
                kind: "approval".to_string(),
                message_id: 0,
                payload: serde_json::json!({ "pending_call_ids": [] }).clone(),
            })
            .await
            .expect("open the wait");

        let assembly = Assembly::open(dirs.clone(), None, Some(host.clone()), None)
            .await
            .expect("assemble");
        // 交付追踪默认关闭，而判据读取总线自己的交付记录
        assembly.bus().set_tracking(true);
        Self {
            _tmp: tmp,
            dirs,
            assembly,
            server,
            host,
            topic_id,
            instance_id,
            handle,
            manifest: manifest.to_string(),
            executed: Mutex::new(Vec::new()),
        }
    }

    /// 本用例自己持有的记录宿主
    fn mod_host(&self) -> &Arc<RecordingHost> {
        &self.host
    }

    /// 唤醒一轮并等它跑完
    ///
    /// 从树上的 Phase 读出结果，节点进入终态就是这一轮结束。已完成与已失败各对应
    /// 一种交回给树的结果。
    ///
    /// 唤醒通过答复一次等待实现，因此跑到的是恢复之后的续行
    async fn run_one_round(&self) -> Result<Outcome<AgentDomain>, String> {
        let tree = self
            .assembly
            .tree_of(self.topic_id)
            .await
            .map_err(|error| error.to_string())?;
        tree.store
            .answer(&self.cx(), &self.handle, Vec::new(), "rig")
            .await
            .map_err(|error| error.to_string())?;
        let settled = timeout(PATIENCE, async {
            loop {
                let nodes = tree.hub.snapshot().await.expect("query the snapshot");
                if !nodes.is_empty() && nodes.iter().all(|node| node.phase.is_terminal()) {
                    return nodes;
                }
                sleep(POLL).await;
            }
        })
        .await
        .map_err(|_| "this round did not finish within the deadline".to_string())?;

        let calls = self.committed_calls().await;
        self.executed
            .lock()
            .expect("the record is usable")
            .extend(calls);
        match settled.first().map(|node| node.phase.clone()) {
            Some(Phase::Done(outcome)) => Ok(Outcome::Completed(outcome)),
            Some(Phase::Failed(failure)) => Ok(Outcome::Failed(failure)),
            other => Err(format!("this round did not finish normally: {other:?}")),
        }
    }

    /// 真正执行过的调用，按写入数据库的工具结果识别
    ///
    /// 只算 request 通道真的被走到的结果。模型编造的名字（清单里没有它）也会留下形状
    /// 相同的工具结果块，只按块类型识别会把编造的算成执行过的
    async fn committed_calls(&self) -> Vec<String> {
        let storage = self.assembly.storage();
        let turns = storage
            .turn_list_by_instance(self.instance_id)
            .await
            .expect("read this instance's turn");
        let turn_ids: Vec<i64> = turns.iter().map(|turn| turn.id).collect();
        let messages = storage
            .message_list_by_turns(&turn_ids)
            .await
            .expect("read the turn messages");
        let message_ids: Vec<i64> = messages.iter().map(|message| message.id).collect();
        storage
            .block_list_by_messages(&message_ids)
            .await
            .expect("read the committed block")
            .into_iter()
            .filter(|block| block.kind == BlockKind::ToolResult)
            .filter(|block| seam_denied(block.data.as_ref()))
            .filter_map(|block| block.call_id)
            .collect()
    }

    /// 跑过一轮之后真正执行过的调用
    fn executed_calls(&self) -> Vec<String> {
        self.executed.lock().expect("the record is usable").clone()
    }

    /// 总线真的派发过什么，按 (事件名, 接收方) 列出
    ///
    /// 派发记录只带事件序号，事件名要从写入磁盘的日志里回查，而日志要等帧设施
    /// 写盘之后才有行
    async fn delivered(&self) -> Vec<(String, String)> {
        self.settle().await;
        let until = self.log_max_seq().await;
        let named: HashMap<u64, String> = self
            .assembly
            .storage()
            .event_log_range(Some(self.topic_id), 0, LOG_LIMIT, until)
            .await
            .expect("read the log")
            .into_iter()
            .map(|record| (record.seq.0, record.name))
            .collect();
        self.assembly
            .bus()
            .delivery_records()
            .iter()
            .filter_map(|record| {
                named
                    .get(&record.event_seq.0)
                    .map(|name| (name.clone(), record.target.name().to_string()))
            })
            .collect()
    }

    /// 直接派发一次工具调用
    ///
    /// 用来断言白名单命中时一次宿主调用都没有。断言零次的前提是订阅确实在索引里、确实能被调用
    async fn probe_tool_call(&self) {
        let payload = ToolCalls {
            instance_id: self.instance_id,
            calls: Vec::new(),
            suspended: Vec::new(),
        };
        self.assembly
            .bus()
            .decide(&CORE_LLM_TOOLCALL, &self.cx(), payload)
            .await;
    }

    /// 清单能不能通过校验
    ///
    /// 校验使用总线上的目录，安装期与运行期解析清单使用同一个目录
    fn subscription_accepted(&self) -> bool {
        accepts(self, &self.manifest)
    }

    /// 总线当前发放到的序号
    fn sequence(&self) -> u64 {
        self.assembly.bus().current_seq().0
    }

    /// 把已经产生的日志写入磁盘
    ///
    /// 帧设施平常只把记录留在内存里，日志表要等写盘之后才有行
    async fn settle(&self) {
        self.assembly.hub().flush().await.expect("write the state");
    }

    /// 数据库中事件日志的最大序号
    async fn log_max_seq(&self) -> u64 {
        self.assembly
            .storage()
            .event_log_max_seq()
            .await
            .expect("read the max log sequence")
    }

    /// 丢弃装配，在同一个应用目录上重开
    async fn restart(self) -> Rig {
        let Rig {
            _tmp,
            dirs,
            assembly,
            server,
            host,
            topic_id,
            instance_id,
            handle,
            manifest,
            executed,
        } = self;
        assembly.shutdown().await.expect("stop");
        drop(assembly);
        let assembly = Assembly::open(dirs.clone(), None, Some(host.clone()), None)
            .await
            .expect("reopen the assembly");
        Rig {
            _tmp,
            dirs,
            assembly,
            server,
            host,
            topic_id,
            instance_id,
            handle,
            manifest,
            executed,
        }
    }

    /// 一轮所处的操作上下文
    fn cx(&self) -> DispatchCx {
        DispatchCx::root(
            Origin::Agent,
            Scope::Topic {
                topic_id: self.topic_id,
            },
            Source::core("agent"),
            Duration::from_millis(50),
        )
    }
}

/// 一份清单能不能被总线的目录接受
fn accepts(rig: &Rig, manifest: &str) -> bool {
    wind_mod::manifest::parse(manifest, rig.assembly.bus().directory()).is_ok()
}

/// 一条工具结果是不是 request 通道交回的拒绝结果
///
/// 载荷里要同时有拒绝标记与 request 通道给出的原因，缺一个就不算
fn seam_denied(data: Option<&serde_json::Value>) -> bool {
    let Some(data) = data else {
        return false;
    };
    data["refused"] == serde_json::json!(true)
        && data["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains(SEAM_REFUSAL))
}

/// 一次挂起 Phase 写入数据库的载荷
///
/// 只有渲染一侧使用挂在哪条消息上，本夹具不为它写真实消息
fn suspended_payload(instance_id: i64, handle: &str) -> String {
    serde_json::json!({
        "instance_id": instance_id,
        "kind": "approval",
        "message_id": 0,
        "handle": handle,
    })
    .to_string()
}

/// 只订阅一条链上事件的清单
fn manifest_subscribing_to(event: &str) -> String {
    serde_json::json!({
        "name": MOD,
        "version": 1,
        "events": {"on": {(event): {"handler": "onEvent", "phase": "guard"}}},
    })
    .to_string()
}

/// 一个不做任何声明的装配
async fn bare_assembly() -> (TempDir, Assembly) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let assembly = Assembly::open(AppDirs::under(tmp.path()), None, None, None)
        .await
        .expect("assemble");
    (tmp, assembly)
}

/// 一个宿主会用改写试图删掉指定调用的夹具，模型返回两条调用
async fn rig_with_a_mod_rewriting_away(call_id: &'static str) -> Rig {
    let host = Arc::new(RecordingHost::rewriting_away(call_id));
    let replies = vec![
        calls_reply(&[("c-1", READ), ("c-2", WRITE)]),
        text_reply("all done"),
    ];
    Rig::build(
        AllowPolicy { allow: Vec::new() },
        MOD_MANIFEST,
        replies,
        host,
    )
    .await
}

/// 一个按给定白名单放行的夹具，模型返回命中的调用
async fn rig_with_policy(policy: AllowPolicy) -> Rig {
    let host = Arc::new(RecordingHost::passing());
    let replies = vec![calls_reply(&[("c-1", READ)]), text_reply("done")];
    Rig::build(policy, MOD_MANIFEST, replies, host).await
}

/// 一个没有 mod 会判定的夹具，模型返回命中的调用
///
/// 宿主与 `rig_with_policy` 相同，差别只在断言看的是整轮的调用次数
async fn rig_with_policy_and_a_recording_host(policy: AllowPolicy) -> Rig {
    rig_with_policy(policy).await
}

/// 一个由声明了某条链上事件订阅的清单驱动的夹具
async fn rig_declaring_a_subscription_to(event: &str) -> Rig {
    let host = Arc::new(RecordingHost::passing());
    let replies = vec![text_reply("ok")];
    Rig::build(
        AllowPolicy { allow: Vec::new() },
        &manifest_subscribing_to(event),
        replies,
        host,
    )
    .await
}

#[tokio::test]
async fn a_rewrite_drops_one_of_the_calls_the_model_returned() {
    // 载荷可改写。mod 用改写删掉 c-2 之后，只有 c-1 被执行
    let rig = rig_with_a_mod_rewriting_away("c-2").await;
    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(
        rig.mod_host()
            .seen_names()
            .iter()
            .any(|name| name == "core/llm.toolcall"),
        "this scenario must actually go through the mod's rewrite"
    );
    assert_eq!(
        rig.executed_calls(),
        vec!["c-1".to_string()],
        "the rewritten list is what executes"
    );
    assert!(matches!(outcome, Outcome::Completed(_)));
    assert_eq!(
        rig.server.requests(),
        2,
        "both results were returned to the model together once, so this round reached a second round trip"
    );
}

#[tokio::test]
async fn a_whitelisted_call_never_reaches_a_mod() {
    let rig = rig_with_policy(AllowPolicy {
        allow: vec![READ.to_string()],
    })
    .await;
    let _ = rig.run_one_round().await.expect("run to completion");
    assert!(
        rig.mod_host()
            .seen_names()
            .iter()
            .all(|name| name != "core/llm.toolcall"),
        "when whitelisted, not even one dispatch happens"
    );
    // 判据是总线的派发记录里没有它，因此也读总线自己的记录
    assert!(
        rig.delivered()
            .await
            .iter()
            .all(|(name, _)| name != "core/llm.toolcall"),
        "the bus must not deliver this tier to any mod: {:?}",
        rig.delivered().await
    );
    assert_eq!(
        rig.executed_calls(),
        vec!["c-1".to_string()],
        "truncation is not skipping: the whitelisted entry still executes as usual"
    );
    // 两个读口都确实有效。探一次派发之后，两边都会立刻出现它
    rig.probe_tool_call().await;
    assert!(
        rig.delivered()
            .await
            .iter()
            .any(|(name, target)| name == "core/llm.toolcall" && target == MOD),
        "the probe dispatch must appear in the delivery record, showing that this tier's subscription is indeed in the index"
    );
}

#[tokio::test]
async fn the_declarative_tier_reaches_no_mod_host() {
    // 建立解释器就发生在宿主调用里，没有宿主调用等价于没有唤醒 JS
    let rig = rig_with_policy_and_a_recording_host(AllowPolicy {
        allow: vec![READ.to_string()],
    })
    .await;
    let _ = rig.run_one_round().await.expect("run to completion");
    // 整轮没有一次宿主调用，而这批调用确实执行了
    assert_eq!(
        rig.mod_host().calls(),
        0,
        "when whitelisted and nobody subscribes, there should be no host call"
    );
    assert_eq!(rig.executed_calls(), vec!["c-1".to_string()]);
    // 上面那个 0 是有效读数。探一次派发会立即引起一次宿主调用，说明该档订阅确实在
    // 索引里，整轮的零次不是因为这条订阅没有进索引
    rig.probe_tool_call().await;
    assert_eq!(
        rig.mod_host().calls(),
        1,
        "the probe dispatch must reach the host"
    );
}

#[tokio::test]
async fn a_mod_can_subscribe_to_the_chain() {
    let rig = rig_declaring_a_subscription_to("core/llm.request").await;
    assert!(
        rig.subscription_accepted(),
        "the interception points on the chain must be open to mods"
    );
    // 判据有效。目录里没有的事件名同样进不去
    assert!(
        !accepts(&rig, &manifest_subscribing_to("core/llm.nowhere")),
        "an event name not in the directory must be refused, otherwise the previous assertion says nothing"
    );
    let _ = rig.run_one_round().await.expect("run to completion");
    assert!(
        rig.mod_host()
            .seen_names()
            .iter()
            .any(|name| name == "core/llm.request"),
        "only entering the subscription index counts: the dispatch on the chain must reach this mod"
    );
}

#[tokio::test]
async fn shutting_down_twice_is_safe() {
    let (_tmp, assembly) = bare_assembly().await;
    assembly.shutdown().await.expect("stop");
    assembly.shutdown().await.expect("shut down twice");
    assert!(
        matches!(assembly.tree_of(1).await, Err(RuntimeError::Stopped)),
        "taking a tree after shutdown must be refused, otherwise nobody would stop the newly built one"
    );
}

#[tokio::test]
async fn shutting_down_stops_the_trees_the_table_was_holding() {
    let (_tmp, assembly) = bare_assembly().await;
    let tree = assembly.tree_of(1).await.expect("build");
    assert!(!tree.hub.is_stopped(), "the obtained tree is alive");

    assembly.shutdown().await.expect("stop");
    assert!(
        tree.hub.is_stopped(),
        "shutdown must stop that tree; dropping it from the table alone would leave it running forever"
    );
}

#[tokio::test]
async fn a_round_on_a_real_tree_leaves_a_sequence_a_restart_does_not_reuse() {
    let rig = rig_with_a_mod_rewriting_away("c-2").await;
    let _ = rig.run_one_round().await.expect("run to completion");
    rig.settle().await;
    let watermark = rig.log_max_seq().await;
    assert!(
        watermark > 0,
        "after one round there must be a record in the log"
    );
    assert!(
        rig.sequence() >= watermark,
        "the bus-issued sequence must not be lower than the maximum sequence already in the log, actual {} and {watermark}",
        rig.sequence()
    );

    let again = rig.restart().await;
    assert!(
        again.sequence() >= watermark,
        "after a restart the sequence is not reused, actual {} and {watermark}",
        again.sequence()
    );
}

/// 一次 SSE 应答
fn calls_reply(calls: &[(&str, &str)]) -> String {
    let body: String = calls
        .iter()
        .map(|(id, name)| {
            delta_line(serde_json::json!({
                "role": "assistant",
                "tool_calls": [{
                    "index": 0,
                    "id": id,
                    "type": "function",
                    "function": { "name": name, "arguments": "{}" },
                }],
            }))
        })
        .collect();
    format!("{body}{DONE}")
}

/// 一次纯文本的 SSE 应答
fn text_reply(text: &str) -> String {
    format!(
        "{}{DONE}",
        delta_line(serde_json::json!({ "role": "assistant", "content": text }))
    )
}

/// 流结束的哨兵行
const DONE: &str = "data: [DONE]\n\n";

/// 由一个增量片段拼成的数据行
fn delta_line(delta: serde_json::Value) -> String {
    let chunk = serde_json::json!({
        "id": "x",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "local-model",
        "choices": [{ "index": 0, "delta": delta }],
    });
    format!("data: {chunk}\n\n")
}

/// 一个按请求次数给出不同响应体的最小 HTTP 应答器
struct Server {
    base_url: String,
    requests: Arc<AtomicUsize>,
}

impl Server {
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// 启动服务并逐个处理连接
    ///
    /// 只服务本文件的用例。不分块、不使用 keep-alive，写完就关闭。应答按请求次序取，
    /// 用完之后一直用最后一项
    async fn serve(replies: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("has a local address")
        );
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let index = counted.fetch_add(1, Ordering::SeqCst);
                drain_request(&mut socket).await;
                if let Some(reply) = replies.get(index).or_else(|| replies.last()) {
                    let _ = respond(&mut socket, reply).await;
                }
                let _ = socket.shutdown().await;
            }
        });
        Self { base_url, requests }
    }
}

/// 把一次请求读完，头部与实体都要读取
///
/// 不读实体的话，请求方在仍在发送时会看到连接被关闭
async fn drain_request(socket: &mut TcpStream) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let Ok(read) = socket.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = head_end(&buffer) {
            let head = String::from_utf8_lossy(&buffer[..end]);
            if buffer.len() >= end + body_len(&head) {
                return;
            }
        }
    }
}

/// 头部结束的位置，即空行之后的偏移
fn head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| at + 4)
}

/// 头部里声明的请求体长度
fn body_len(head: &str) -> usize {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .find_map(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0)
}

/// 写回一次应答。先写头部再写实体，之后关闭连接
async fn respond(socket: &mut TcpStream, body: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await
}
