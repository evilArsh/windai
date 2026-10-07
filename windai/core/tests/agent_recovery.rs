//! `AgentStore::load_snapshots` 的存储契约：相位、父子、detached 都要能从库里重建

#[path = "./common/lib.rs"]
mod common;

use common::{init_test_core, seed_chat_fixture};
use std::path::Path;
use wind_ai::message::Content;
use wind_core::agent::domain::AgentSpec;
use wind_core::agent::helper;
use wind_core::agent::store::AgentStore;
use wind_core::agent::tree::phase::{CancelOrigin, Phase};
use wind_core::models::{AgentInstance, AgentMode, AgentStatus, UpdateInstance};

/// 实例当前的两个消息指针
async fn pointers(core: &wind_core::WindCore, instance_id: i64) -> (Option<i64>, Option<i64>) {
    let instance = helper::get_instance_by_id(core.storage(), instance_id)
        .await
        .expect("instance");
    (instance.user_id, instance.assistant_id)
}

/// 投一条用户输入，与生产路径一样经 `start_round` 落库并记进实例指针
async fn seed_input(
    core: &wind_core::WindCore,
    topic_id: i64,
    instance: &AgentInstance,
    text: &str,
) -> i64 {
    helper::start_round(
        core.storage(),
        Path::new("/tmp"),
        topic_id,
        instance.clone(),
        &[Content::new_text(text.to_string())],
    )
    .await
    .expect("start round");
    pointers(core, instance.id)
        .await
        .0
        .expect("本轮输入必须记进 user_id")
}

/// 走一轮真实对话：落一条用户输入并开出本轮 assistant
async fn seed_turn(
    core: &wind_core::WindCore,
    topic_id: i64,
    instance: &AgentInstance,
    text: &str,
) {
    seed_input(core, topic_id, instance, text).await;
    helper::open_assistant(core.storage(), instance.id)
        .await
        .expect("open assistant");
}

async fn set_status(core: &wind_core::WindCore, instance_id: i64, status: AgentStatus) {
    core.storage()
        .agent()
        .update_instance(
            instance_id,
            UpdateInstance {
                status: Some(status),
                ..Default::default()
            },
        )
        .await
        .expect("update instance");
}

#[tokio::test]
async fn load_snapshots_rebuilds_phases_parents_and_detached() {
    let core = init_test_core().await;
    let (_, topic) = seed_chat_fixture(&core, "snapshots").await;
    let main = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main instance");

    let child = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        main.id,
        1,
        AgentMode::Background,
    )
    .await
    .expect("child instance");
    // 子实例必须跑过一轮，恢复才能重建它的相位载荷
    seed_turn(&core, topic.id, &child, "child task").await;
    let (_, assistant_id) = pointers(&core, child.id).await;
    let assistant_id = assistant_id.expect("子实例本轮的 assistant");

    core.storage()
        .agent()
        .update_instance(
            main.id,
            UpdateInstance {
                status: Some(AgentStatus::Cancelling),
                ..Default::default()
            },
        )
        .await
        .expect("mark cancelling");
    core.storage()
        .agent()
        .update_instance(
            child.id,
            UpdateInstance {
                status: Some(AgentStatus::Finished),
                ..Default::default()
            },
        )
        .await
        .expect("mark finished");
    // update_instance 的 mode 字段为 None 时不写列，create_child_instance 已经写好 Background

    let store = AgentStore::new(core.storage().clone());
    let snaps = store.load_snapshots(topic.id).await.expect("snapshots");
    assert_eq!(snaps.len(), 2, "idle 之外的实例都要进树");

    let node = |id: i64| snaps.iter().find(|s| s.id == id).expect("node").clone();

    assert_eq!(node(main.id).children, vec![child.id], "父子关系要重建");
    assert!(node(main.id).parent.is_none(), "主实例是根");
    assert!(matches!(
        node(main.id).phase,
        Phase::Cancelling(CancelOrigin::External)
    ));
    assert!(node(main.id).children.contains(&child.id));

    assert_eq!(node(child.id).parent, Some(main.id));
    assert!(
        node(child.id).detached,
        "background 实例必须重建为 detached"
    );
    match node(child.id).phase {
        Phase::Done(outcome) => assert_eq!(outcome.message_id, assistant_id),
        other => panic!("终态必须从实例本轮的 assistant 消息重建: {other:?}"),
    }
}

/// 父被跳过时，它的后代也必须一起丢掉：留着会变成没有根的孤儿
#[tokio::test]
async fn load_snapshots_drops_nodes_whose_parent_is_missing() {
    let core = init_test_core().await;
    let (_, topic) = seed_chat_fixture(&core, "orphan").await;
    let main = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main instance");
    let child = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        main.id,
        1,
        AgentMode::Sync,
    )
    .await
    .expect("child instance");
    let grandchild = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        child.id,
        1,
        AgentMode::Sync,
    )
    .await
    .expect("grandchild instance");
    seed_turn(&core, topic.id, &child, "child task").await;
    seed_turn(&core, topic.id, &grandchild, "grandchild task").await;
    // 主实例停在 waiting_approval 又没有助手消息：它会被跳过，后代随之失去父
    set_status(&core, main.id, AgentStatus::WaitingApproval).await;
    set_status(&core, child.id, AgentStatus::WaitingChild).await;
    set_status(&core, grandchild.id, AgentStatus::Running).await;

    let store = AgentStore::new(core.storage().clone());
    let snaps = store.load_snapshots(topic.id).await.expect("snapshots");
    assert!(
        snaps.is_empty(),
        "父不在结果集里的节点连同后代都要丢掉: {snaps:?}"
    );
}

/// 恢复出的待消费输入：完结的实例上还压着没人作答的输入才派生，取消的一律不派生
#[tokio::test]
async fn load_snapshots_derives_pending_only_for_a_finished_instance_with_an_unconsumed_input() {
    let core = init_test_core().await;
    let (_, topic) = seed_chat_fixture(&core, "pending").await;
    let main = wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main instance");
    let child = wind_core::agent::helper::create_child_instance(
        core.storage(),
        topic.id,
        main.id,
        1,
        AgentMode::Sync,
    )
    .await
    .expect("child instance");

    // 主实例：一轮已经跑完，之后又投递进来一条还没被消费的用户消息
    seed_turn(&core, topic.id, &main, "first").await;
    let queued = seed_input(&core, topic.id, &main, "queued").await;
    let (bound_user, bound_assistant) = pointers(&core, main.id).await;
    assert_eq!(bound_user, Some(queued), "实例空闲时新到的输入顶替 user_id");
    assert!(bound_assistant.is_some(), "assistant_id 停在本轮助手消息上");
    set_status(&core, main.id, AgentStatus::Finished).await;

    // 子实例：同样的形状，但它已经被取消
    seed_input(&core, topic.id, &child, "cancelled").await;
    set_status(&core, child.id, AgentStatus::Cancelled).await;

    let store = AgentStore::new(core.storage().clone());
    let snaps = store.load_snapshots(topic.id).await.expect("snapshots");
    let node = |id: i64| {
        snaps
            .iter()
            .find(|snap| snap.id == id)
            .expect("node")
            .clone()
    };

    assert_eq!(
        node(main.id).pending,
        Some(AgentSpec {
            instance_id: main.id,
            message_id: None,
        }),
        "完结且压着未消费输入（{queued}）的实例必须派生"
    );
    assert!(
        node(child.id).pending.is_none(),
        "取消的实例不得派生，否则恢复出来的树永远不安静"
    );
}

#[tokio::test]
async fn an_idle_instance_does_not_enter_the_tree() {
    let core = init_test_core().await;
    let (_, topic) = seed_chat_fixture(&core, "idle").await;
    wind_core::agent::helper::get_or_create_main_instance(core.storage(), topic.id)
        .await
        .expect("main instance");

    let store = AgentStore::new(core.storage().clone());
    let snaps = store.load_snapshots(topic.id).await.expect("snapshots");
    assert!(snaps.is_empty(), "只有 idle 实例时树是空的: {snaps:?}");
}
