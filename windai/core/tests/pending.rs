//! 验证下一轮输入队列的持久化。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::sync::Arc;
use wind_core::agent::store::AgentStore;
use wind_core::models::{
    AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, PendingCreate, PendingKind,
};
use wind_core::storage::Storage;
use wind_event::def::Directory;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Subscription};
use wind_wire::message::Content;

/// 不启用任何 mod
struct NoEnablement;

impl ModEnablement for NoEnablement {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::none()
    }
}

/// 不会被调用的宿主
struct NoHost;

#[async_trait::async_trait]
impl ModHost for NoHost {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        Err(HostError::Internal("the test dispatches nothing".into()))
    }
}

/// 一条没有订阅者的事件总线
fn dispatcher() -> Arc<Dispatcher> {
    Arc::new(Dispatcher::new(
        Directory::default(),
        Arc::new(NoEnablement),
        Arc::new(NoHost),
        Limits::default(),
    ))
}

/// 绑定话题 `topic_id` 的存储入口
fn store_for(storage: &Storage, topic_id: i64) -> AgentStore {
    AgentStore::new(storage.clone(), topic_id, dispatcher())
}

#[tokio::test]
async fn a_pending_row_round_trips_with_its_content() {
    let (_dir, storage) = store().await;
    let contents = vec![Content::new_text("hello".into())];
    let id = storage
        .pending_insert(PendingCreate {
            instance_id: 1,
            kind: PendingKind::User,
            source_id: None,
            payload: contents.to_vec(),
        })
        .await
        .expect("write");
    let rows = storage
        .pending_list_by_instance(1)
        .await
        .expect("list pending rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].payload, contents);
}

#[tokio::test]
async fn an_already_queued_source_is_not_inserted_twice() {
    let (_dir, storage) = store().await;
    let contents = [Content::new_text("result".into())];
    let first = storage
        .pending_insert_if_absent(PendingCreate {
            instance_id: 1,
            kind: PendingKind::BackgroundResult,
            source_id: Some(7),
            payload: contents.to_vec(),
        })
        .await
        .expect("write");
    assert!(first.is_some());
    let second = storage
        .pending_insert_if_absent(PendingCreate {
            instance_id: 1,
            kind: PendingKind::BackgroundResult,
            source_id: Some(7),
            payload: contents.to_vec(),
        })
        .await
        .expect("write");
    assert_eq!(second, None);
    assert_eq!(
        storage
            .pending_list_by_instance(1)
            .await
            .expect("list pending rows")
            .len(),
        1
    );
}

#[tokio::test]
async fn deleting_a_pending_row_removes_it_from_the_queue() {
    let (_dir, storage) = store().await;
    let id = storage
        .pending_insert(PendingCreate {
            instance_id: 1,
            kind: PendingKind::User,
            source_id: None,
            payload: [Content::new_text("a".into())].to_vec(),
        })
        .await
        .expect("write");
    storage.pending_delete(&[id]).await.expect("delete");
    assert!(
        storage
            .pending_list_by_instance(1)
            .await
            .expect("list pending rows")
            .is_empty()
    );
}

#[tokio::test]
async fn a_restored_done_node_rediscovers_the_input_it_never_consumed() {
    let (_dir, storage) = store().await;
    let instance = storage
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Done,
            phase_payload: Some(r#"{"message_id": 1}"#.to_string()),
        })
        .await
        .expect("create instance");
    let pending = storage
        .pending_insert(PendingCreate {
            instance_id: instance,
            kind: PendingKind::User,
            source_id: None,
            payload: [Content::new_text("continue".into())].to_vec(),
        })
        .await
        .expect("enqueue");
    let snapshots = store_for(&storage, 1)
        .load_snapshots()
        .await
        .expect("recover");
    assert_eq!(snapshots[0].pending.len(), 1);
    assert_eq!(snapshots[0].pending[0].pending_id, Some(pending));
}
