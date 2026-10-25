//! 派生调用的配对存储

use super::Storage;
use super::TableName;
use super::utils::{ensure_lte_one, execute_allowing_zero_rows, execute_expecting_rows, now_ts};
use crate::db::{DbDriver, DbRow};
use crate::delete_from;
use crate::error::Result;
use crate::insert;
use crate::models::{AgentMode, AgentPhase, SpawnCall};
use crate::select_fields;
use crate::update_fields;
use sqlx::{FromRow, QueryBuilder, Row};

fn select_spawn_calls<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::SPAWN_CALLS,
        (
            "parent_id",
            "call_id",
            "child_id",
            "created_at",
            "result_delivered_at"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for SpawnCall {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            parent_id: row.try_get("parent_id")?,
            call_id: row.try_get("call_id")?,
            child_id: row.try_get("child_id")?,
            created_at: row.try_get("created_at")?,
            result_delivered_at: row.try_get("result_delivered_at")?,
        })
    }
}

impl Storage {
    /// 记录父实例与子实例的调用配对
    pub async fn spawn_call_insert(
        &self,
        parent_id: i64,
        call_id: &str,
        child_id: i64,
    ) -> Result<()> {
        execute_expecting_rows(
            &self.executor,
            insert!(
                TableName::SPAWN_CALLS,
                ("parent_id", parent_id),
                ("call_id", call_id),
                ("child_id", child_id),
                ("created_at", now_ts()),
                ("result_delivered_at", Option::<i64>::None),
            ),
        )
        .await
    }

    /// 按父实例与调用标识取出配对，不存在时为 None
    pub async fn spawn_call_get(&self, parent_id: i64, call_id: &str) -> Result<Option<SpawnCall>> {
        let rows = self
            .executor
            .fetch_all(
                select_spawn_calls()
                    .push(" WHERE parent_id = ")
                    .push_bind(parent_id)
                    .push(" AND call_id = ")
                    .push_bind(call_id)
                    .build_query_as::<SpawnCall>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出父实例的配对，按创建时间升序
    pub async fn spawn_call_list_by_parent(&self, parent_id: i64) -> Result<Vec<SpawnCall>> {
        self.executor
            .fetch_all(
                select_spawn_calls()
                    .push(" WHERE parent_id = ")
                    .push_bind(parent_id)
                    .push(" ORDER BY created_at ASC, call_id ASC")
                    .build_query_as::<SpawnCall>(),
            )
            .await
    }

    /// 列出 topic 内已结束但未交付的后台子实例配对
    pub async fn spawn_call_list_undelivered_background(
        &self,
        topic_id: i64,
    ) -> Result<Vec<SpawnCall>> {
        let mut qb = QueryBuilder::<DbDriver>::new(
            "SELECT s.parent_id, s.call_id, s.child_id, s.created_at, s.result_delivered_at FROM ",
        );
        qb.push(TableName::SPAWN_CALLS)
            .push(" s JOIN ")
            .push(TableName::AGENT_INSTANCES)
            .push(" i ON i.id = s.child_id WHERE i.topic_id = ")
            .push_bind(topic_id)
            .push(" AND i.mode = ")
            .push_bind(AgentMode::Background.as_ref())
            .push(" AND s.result_delivered_at IS NULL")
            .push(" AND i.phase IN (")
            .push_bind(AgentPhase::Done.as_ref())
            .push(", ")
            .push_bind(AgentPhase::Failed.as_ref())
            .push(", ")
            .push_bind(AgentPhase::Cancelled.as_ref())
            .push(", ")
            .push_bind(AgentPhase::Aborted.as_ref())
            .push(") ORDER BY s.child_id ASC");
        self.executor
            .fetch_all(qb.build_query_as::<SpawnCall>())
            .await
    }

    /// 按子实例取出配对，不存在时为 None
    pub async fn spawn_call_get_by_child(&self, child_id: i64) -> Result<Option<SpawnCall>> {
        let rows = self
            .executor
            .fetch_all(
                select_spawn_calls()
                    .push(" WHERE child_id = ")
                    .push_bind(child_id)
                    .build_query_as::<SpawnCall>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 写入结果交付时间，配对不存在时报错
    pub async fn spawn_call_mark_delivered(&self, child_id: i64) -> Result<()> {
        let mut qb = update_fields!(
            TableName::SPAWN_CALLS,
            ("result_delivered_at", Some(now_ts())),
        );
        qb.push(" WHERE child_id = ").push_bind(child_id);
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 删除子实例的配对，不存在不报错
    pub async fn spawn_call_delete_by_child(&self, child_id: i64) -> Result<()> {
        let mut qb = delete_from!(TableName::SPAWN_CALLS);
        qb.push(" WHERE child_id = ").push_bind(child_id);
        execute_allowing_zero_rows(&self.executor, qb).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::error::CoreError;
    use crate::models::{AgentInstanceCreate, AgentMode, AgentPhase, AgentRole};
    use tempfile::TempDir;

    /// 一个已建表、位于临时目录的空库
    ///
    /// 返回的临时目录必须由调用方持有。临时目录一旦丢弃，数据库文件随之删除
    async fn storage() -> (TempDir, Storage) {
        let dir = TempDir::new().expect("create temp dir");
        let db = Db::open(&dir.path().join("t.db"))
            .await
            .expect("open database");
        db.init_schema().await.expect("create table");
        (dir, Storage::new(db))
    }

    #[tokio::test]
    async fn a_pair_round_trips_by_parent_and_call() {
        let (_dir, storage) = storage().await;
        storage
            .spawn_call_insert(10, "c-1", 20)
            .await
            .expect("write");

        let pair = storage
            .spawn_call_get(10, "c-1")
            .await
            .expect("query")
            .expect("must be found");
        assert_eq!(pair.parent_id, 10);
        assert_eq!(pair.call_id, "c-1");
        assert_eq!(pair.child_id, 20);
        assert_eq!(
            pair.result_delivered_at, None,
            "the newly created pair is not yet delivered"
        );
    }

    #[tokio::test]
    async fn a_parent_lists_only_its_own_pairs() {
        let (_dir, storage) = storage().await;
        storage
            .spawn_call_insert(10, "c-1", 20)
            .await
            .expect("write");
        storage
            .spawn_call_insert(10, "c-2", 21)
            .await
            .expect("write");
        storage
            .spawn_call_insert(11, "c-1", 22)
            .await
            .expect("write");

        let rows = storage.spawn_call_list_by_parent(10).await.expect("query");
        assert_eq!(
            rows.len(),
            2,
            "pairs of other parent instances are excluded"
        );
        assert_eq!(rows[0].call_id, "c-1");
        assert_eq!(rows[1].call_id, "c-2");
    }

    #[tokio::test]
    async fn a_child_belongs_to_at_most_one_call() {
        let (_dir, storage) = storage().await;
        storage
            .spawn_call_insert(10, "c-1", 20)
            .await
            .expect("write");
        let error = storage
            .spawn_call_insert(10, "c-2", 20)
            .await
            .expect_err("reject a duplicate child");
        assert!(matches!(error, CoreError::Database(_)), "{error}");
    }

    #[tokio::test]
    async fn a_pair_can_be_found_from_its_child_and_marked_delivered() {
        let (_dir, storage) = storage().await;
        storage
            .spawn_call_insert(10, "c-1", 20)
            .await
            .expect("write");

        let pair = storage
            .spawn_call_get_by_child(20)
            .await
            .expect("query")
            .expect("must be found");
        assert_eq!(pair.call_id, "c-1");

        storage
            .spawn_call_mark_delivered(20)
            .await
            .expect("mark the result delivered");
        let pair = storage
            .spawn_call_get_by_child(20)
            .await
            .expect("query")
            .expect("must be found");
        assert!(
            pair.result_delivered_at.is_some(),
            "the delivery marker must be written to the database"
        );
    }

    #[tokio::test]
    async fn only_undelivered_terminal_background_children_are_listed() {
        let (_dir, storage) = storage().await;
        let parent = storage
            .agent_create(AgentInstanceCreate {
                parent_id: None,
                topic_id: 1,
                role: AgentRole::Main,
                mode: Some(AgentMode::Sync),
                phase: AgentPhase::Done,
                phase_payload: None,
            })
            .await
            .expect("create parent instance");
        let done = storage
            .agent_create(AgentInstanceCreate {
                parent_id: Some(parent),
                topic_id: 1,
                role: AgentRole::Child,
                mode: Some(AgentMode::Background),
                phase: AgentPhase::Done,
                phase_payload: None,
            })
            .await
            .expect("create child instance");
        let running = storage
            .agent_create(AgentInstanceCreate {
                parent_id: Some(parent),
                topic_id: 1,
                role: AgentRole::Child,
                mode: Some(AgentMode::Background),
                phase: AgentPhase::Running,
                phase_payload: None,
            })
            .await
            .expect("create child instance");
        let delivered = storage
            .agent_create(AgentInstanceCreate {
                parent_id: Some(parent),
                topic_id: 1,
                role: AgentRole::Child,
                mode: Some(AgentMode::Background),
                phase: AgentPhase::Done,
                phase_payload: None,
            })
            .await
            .expect("create child instance");
        let sync = storage
            .agent_create(AgentInstanceCreate {
                parent_id: Some(parent),
                topic_id: 1,
                role: AgentRole::Child,
                mode: Some(AgentMode::Sync),
                phase: AgentPhase::Done,
                phase_payload: None,
            })
            .await
            .expect("create child instance");
        let elsewhere = storage
            .agent_create(AgentInstanceCreate {
                parent_id: Some(parent),
                topic_id: 2,
                role: AgentRole::Child,
                mode: Some(AgentMode::Background),
                phase: AgentPhase::Done,
                phase_payload: None,
            })
            .await
            .expect("create child instance");
        for (call_id, child) in [
            ("done", done),
            ("running", running),
            ("delivered", delivered),
            ("sync", sync),
            ("elsewhere", elsewhere),
        ] {
            storage
                .spawn_call_insert(parent, call_id, child)
                .await
                .expect("record the pair");
        }

        // 已交付的配对不属于补发范围
        storage
            .spawn_call_mark_delivered(delivered)
            .await
            .expect("mark the result delivered");
        let rows = storage
            .spawn_call_list_undelivered_background(1)
            .await
            .expect("query");
        assert_eq!(
            rows.len(),
            1,
            "only undelivered terminal background children in this topic are included"
        );
        assert_eq!(rows[0].child_id, done);
        assert_eq!(rows[0].parent_id, parent);

        let elsewhere_rows = storage
            .spawn_call_list_undelivered_background(2)
            .await
            .expect("query");
        assert_eq!(
            elsewhere_rows.len(),
            1,
            "results appear only in the scan of their own topic"
        );
        assert_eq!(elsewhere_rows[0].child_id, elsewhere);
    }

    #[tokio::test]
    async fn deleting_a_pair_that_does_not_exist_is_not_an_error() {
        let (_dir, storage) = storage().await;
        storage
            .spawn_call_delete_by_child(404)
            .await
            .expect("tolerate a missing pair");
    }
}
