//! `AgentStore::load_snapshots` 的存储契约：相位、父子、detached 都要能从库里重建

#[path = "./common/lib.rs"]
mod common;

use common::{init_test_core, seed_chat_fixture};
use wind_core::agent::domain::AgentSpec;
use wind_core::agent::store::AgentStore;
use wind_core::agent::tree::phase::{CancelOrigin, Phase};
use wind_core::models::{AgentMode, AgentStatus, CreateMessage, UpdateInstance};

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
    // 子实例必须有一条 assistant 消息，恢复才能重建它的相位载荷
    let user = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: 1,
            is_boundary: false,
            is_excluded: false,
            instance_id: child.id,
        })
        .await
        .expect("user message");
    let assistant = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: Some(user.id),
            model_id: 1,
            is_boundary: false,
            is_excluded: false,
            instance_id: child.id,
        })
        .await
        .expect("assistant message");

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
        Phase::Done(outcome) => assert_eq!(outcome.message_id, assistant.id),
        other => panic!("终态必须从最后一条 assistant 消息重建: {other:?}"),
    }
}

/// 给实例补一条用户消息，恢复把它当成还没被消费的输入
async fn seed_user_message(core: &wind_core::WindCore, instance_id: i64) -> i64 {
    core.storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: 1,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("user message")
        .id
}

/// 给实例补一对 user/assistant 消息，恢复才能重建它的相位载荷
async fn seed_messages(core: &wind_core::WindCore, instance_id: i64) {
    let user = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            model_id: 1,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("user message");
    core.storage()
        .message()
        .create(CreateMessage {
            from_id: Some(user.id),
            model_id: 1,
            is_boundary: false,
            is_excluded: false,
            instance_id,
        })
        .await
        .expect("assistant message");
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
    seed_messages(&core, child.id).await;
    seed_messages(&core, grandchild.id).await;
    // 主实例停在 waiting_approval 又没有消息：它会被跳过，后代随之失去父
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

/// 恢复出的待消费输入：完结的实例末条消息停在 user 才派生，取消的一律不派生
#[tokio::test]
async fn load_snapshots_derives_pending_only_for_a_finished_instance_with_a_trailing_user() {
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
    seed_messages(&core, main.id).await;
    let queued = seed_user_message(&core, main.id).await;
    set_status(&core, main.id, AgentStatus::Finished).await;

    // 子实例：同样的形状，但它已经被取消
    seed_user_message(&core, child.id).await;
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
        "完结且末条是 user（{queued}）的实例必须派生待消费输入"
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
