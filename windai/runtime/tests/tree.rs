//! 每个话题一棵树：懒建、按话题分开，实例创建经过用例层

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use wind_core::db::Db;
use wind_core::models::AgentInstanceCreate;
use wind_core::models::{AgentMode, AgentPhase, AgentRole};
use wind_core::storage::Storage;
use wind_event::dispatch::Dispatcher;
use wind_event::host::{CallCx, HostError, ModHost};
use wind_event::subscription::Subscription;
use wind_mod::dirs::AppDirs;
use wind_runtime::{Assembly, TreeHandle};

/// 订阅实例创建事件的清单
const GUARD_MANIFEST: &str = r#"{"name":"guard","version":1,
    "events":{"on":{"core/agent.create":{"handler":"onCreate","phase":"guard"}}}}"#;

/// 等待一个条件成立的时限，超过即判定条件不会成立
const PATIENCE: Duration = Duration::from_secs(2);

const POLL: Duration = Duration::from_millis(5);

/// 记下逐次派发的事件名
#[derive(Default)]
struct Recorder {
    /// 收到的事件名，按到达顺序
    seen: Mutex<Vec<String>>,
}

impl Recorder {
    fn seen_names(&self) -> Vec<String> {
        self.seen
            .lock()
            .map(|seen| seen.clone())
            .unwrap_or_default()
    }

    /// 某个事件名被派发的次数
    fn count_of(&self, event: &str) -> usize {
        self.seen_names()
            .iter()
            .filter(|name| name.as_str() == event)
            .count()
    }
}

#[async_trait::async_trait]
impl ModHost for Recorder {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(sub.event.to_string());
        }
        // 只记录事件名，载荷原样放回
        Ok(Some(payload))
    }
}

/// 一个不接脚本运行时、总线上也没有宿主的装配
async fn bare_assembly() -> (TempDir, Assembly) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let assembly = Assembly::open(AppDirs::under(tmp.path()), None, None, None)
        .await
        .expect("assemble");
    (tmp, assembly)
}

/// 本文件用到的全部话题
///
/// 宿主只在使能集合覆盖的作用域里被唤醒，用到的每个话题都要使能
const TOPICS: [i64; 2] = [1, 2];

/// 一个装有订阅实例创建事件的 mod 的装配，以及它自己持有的记录宿主
///
/// 使能集合与订阅索引都在装配时从数据库读取，这个 mod 必须先写入数据库再装配
async fn recording_assembly() -> (TempDir, Arc<Recorder>, Assembly) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let mod_id = storage
        .mod_install("guard", GUARD_MANIFEST, None)
        .await
        .expect("install the mod");
    for topic in TOPICS {
        storage
            .mod_enable(topic, mod_id)
            .await
            .expect("enable the mod");
    }

    let recorder = Arc::new(Recorder::default());
    let assembly = Assembly::open(dirs, None, Some(recorder.clone()), None)
        .await
        .expect("assemble");
    (tmp, recorder, assembly)
}

/// 一棵树上根节点的标识，按顺序
async fn root_ids(tree: &TreeHandle) -> Vec<i64> {
    tree.hub
        .snapshot()
        .await
        .expect("query the snapshot")
        .iter()
        .filter(|node| node.parent.is_none())
        .map(|node| node.id)
        .collect()
}

/// 等总线 `Arc` 的引用计数落到预期值，超时返回当时的计数
async fn wait_for_bus_refs(bus: &Arc<Dispatcher>, want: usize) -> usize {
    timeout(PATIENCE, async {
        loop {
            if Arc::strong_count(bus) == want {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await
    .unwrap_or(());
    Arc::strong_count(bus)
}

#[tokio::test]
async fn asking_twice_gives_the_same_tree() {
    let (_tmp, recorder, assembly) = recording_assembly().await;
    let first = assembly.tree_of(1).await.expect("build");
    let second = assembly.tree_of(1).await.expect("build");
    assert!(
        Arc::ptr_eq(&first.store, &second.store),
        "a topic has only one tree"
    );
    // 取树不创建实例。第二次取用即使又建了一棵树，也没有可观测的副作用
    assert_eq!(
        recorder.count_of("core/agent.create"),
        0,
        "the root instance is created by the first submit; taking a tree must not dispatch a create event"
    );
}

/// 空话题取树不创建根实例
#[tokio::test]
async fn an_empty_topic_starts_without_a_root() {
    let (_tmp, assembly) = bare_assembly().await;
    let one = assembly.tree_of(1).await.expect("build");
    let two = assembly.tree_of(2).await.expect("build");
    assert!(
        !Arc::ptr_eq(&one.store, &two.store),
        "each of the two topics has its own tree"
    );

    assert!(
        root_ids(&one).await.is_empty(),
        "an empty topic has no root node"
    );
    assert!(
        root_ids(&two).await.is_empty(),
        "an empty topic has no root node"
    );
    for topic in [1, 2] {
        assert!(
            assembly
                .storage()
                .agent_list_by_topic(topic)
                .await
                .expect("list the instances")
                .is_empty(),
            "taking a tree must not write instance rows; the root instance row is created by the first submit"
        );
    }
}

#[tokio::test]
async fn two_racers_leave_one_tree_and_the_other_one_stops() {
    let (_tmp, assembly) = bare_assembly().await;
    // 自己持有总线的一份引用，读取它的引用计数
    let bus = assembly.bus().clone();

    // 先量出一棵存活的树在总线上多占用的引用计数，包括树本身与句柄持有的引用
    let before = Arc::strong_count(&bus);
    let _first = assembly.tree_of(1).await.expect("build");
    let per_tree = Arc::strong_count(&bus) - before;
    assert!(
        per_tree > 0,
        "a live tree must hold an extra reference count on the bus"
    );

    // 两路同时取一个还没有树的话题。两路都会在第一次轮询时发现表里没有树并开始建树，
    // 而登记要等树自己的答复，树的事件循环在同一线程上无法运行，因此这里的竞跑是
    // 构造出来的，不是偶然发生的
    let before = Arc::strong_count(&bus);
    let (one, two) = tokio::join!(assembly.tree_of(2), assembly.tree_of(2));
    let one = one.expect("build");
    let two = two.expect("build");
    assert!(
        Arc::ptr_eq(&one.store, &two.store),
        "both racers get the same tree, the one registered first"
    );

    // 两路持有同一个句柄，只保留一路，使增量与先前量出的一棵树可比
    drop(two);

    // 输掉的一路没有进表，外部也拿不到它的句柄，只能靠它不再占用总线的引用计数确认它已停止
    let after = wait_for_bus_refs(&bus, before + per_tree).await;
    assert_eq!(
        after - before,
        per_tree,
        "after the race the bus should have only one extra tree; the extra one was not stopped and would keep running"
    );
}

#[tokio::test]
async fn taking_a_tree_creates_no_instance() {
    // 取树只恢复已有的实例，实例创建属于第一次提交
    let (_tmp, recorder, assembly) = recording_assembly().await;
    let _ = assembly.tree_of(1).await.expect("build");
    let seen = recorder.seen_names();
    assert!(
        seen.iter().all(|name| name != "core/agent.create"),
        "taking a tree must not dispatch a create event, actual dispatches: {seen:?}"
    );
}

#[tokio::test]
async fn a_restored_background_node_stays_detached() {
    let (_tmp, assembly) = bare_assembly().await;
    let root = assembly
        .storage()
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");
    let background = assembly
        .storage()
        .agent_create(AgentInstanceCreate {
            parent_id: Some(root),
            topic_id: 1,
            role: AgentRole::Child,
            mode: Some(AgentMode::Background),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");

    let tree = assembly.tree_of(1).await.expect("build");
    let nodes = tree.hub.snapshot().await.expect("query the snapshot");
    let detached_of = |id: i64| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .expect("find the node")
            .detached
    };
    assert!(
        !detached_of(root),
        "a sync-mode node does not enter detached"
    );
    assert!(
        detached_of(background),
        "a background-mode node must be restored as detached according to the instance row's run mode"
    );
}

#[tokio::test]
async fn a_build_that_fails_leaves_no_hub_running() {
    let (_tmp, _recorder, assembly) = recording_assembly().await;
    let bus = assembly.bus().clone();
    let base = Arc::strong_count(&bus);

    // Phase 名无法解析的一行会让恢复在第四步失败
    let instance = assembly
        .storage()
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create the instance row");
    sqlx::query("UPDATE agent_instances SET phase = 'bogus' WHERE id = ?")
        .bind(instance)
        .execute(assembly.storage().pool())
        .await
        .expect("corrupt the phase name");
    let failed = assembly.tree_of(1).await;
    assert!(
        failed.is_err(),
        "taking a tree must report an error when a Phase name cannot be parsed, and must not hand out a tree that cannot be restored"
    );

    // 新树没有进表，本路也已经丢弃它，只能靠它不再占用总线的引用计数确认已停止。
    // 停不掉的话它会一直运行：命令通道由它自己的存储持有，丢掉句柄关不掉它
    let after = wait_for_bus_refs(&bus, base).await;
    assert_eq!(
        after, base,
        "a failed build must not leave a running hub behind"
    );
}
