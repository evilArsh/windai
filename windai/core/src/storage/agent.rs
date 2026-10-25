use super::Storage;
use super::TableName;
use super::utils::{
    decode_enum, ensure_lte_one, execute_allowing_zero_rows, execute_expecting_rows, now_ts,
};
use crate::db::{DbDriver, DbRow};
use crate::delete_by_id;
use crate::error::Result;
use crate::insert;
use crate::models::{AgentInstance, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole};
use crate::select_fields;
use crate::update_strict;
use sqlx::{FromRow, QueryBuilder, Row};

fn select_instances<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::AGENT_INSTANCES,
        (
            "id",
            "parent_id",
            "topic_id",
            "role",
            "mode",
            "phase",
            "phase_payload",
            "created_at"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for AgentInstance {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        const ROLE: &str = "agent_instances.role";
        const MODE: &str = "agent_instances.mode";
        const PHASE: &str = "agent_instances.phase";
        let role: String = row.try_get("role")?;
        let mode: Option<String> = row.try_get("mode")?;
        let phase: String = row.try_get("phase")?;
        Ok(Self {
            id: row.try_get("id")?,
            parent_id: row.try_get("parent_id")?,
            topic_id: row.try_get("topic_id")?,
            role: decode_enum(ROLE, &role).map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            mode: mode
                .map(|text| decode_enum(MODE, &text))
                .transpose()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            phase: decode_enum(PHASE, &phase)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            phase_payload: row.try_get("phase_payload")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl Storage {
    /// 插入实例并返回新 id
    pub async fn agent_create(&self, input: AgentInstanceCreate) -> Result<i64> {
        let mut qb = insert!(
            TableName::AGENT_INSTANCES,
            ("parent_id", input.parent_id),
            ("topic_id", input.topic_id),
            ("role", input.role.to_string()),
            ("mode", input.mode.map(|mode| mode.to_string())),
            ("phase", input.phase.to_string()),
            ("phase_payload", input.phase_payload),
            ("created_at", now_ts()),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 该 topic 没有主实例时补建，返回已有或新建的 id
    pub async fn agent_ensure_main(
        &self,
        topic_id: i64,
        mode: AgentMode,
        phase: AgentPhase,
    ) -> Result<i64> {
        let role = AgentRole::Main.to_string();
        let mode = mode.to_string();
        let phase = phase.to_string();
        self.with_tx(move |storage| async move {
            let mut insert = insert!(
                TableName::AGENT_INSTANCES,
                ("parent_id", Option::<i64>::None),
                ("topic_id", topic_id),
                ("role", role.clone()),
                ("mode", Some(mode)),
                ("phase", phase),
                ("phase_payload", Option::<&str>::None),
                ("created_at", now_ts()),
            );
            insert.push(" ON CONFLICT DO NOTHING");
            execute_allowing_zero_rows(&storage.executor, insert).await?;
            storage
                .executor
                .fetch_one_scalar(
                    select_fields!(TableName::AGENT_INSTANCES, ("id",))
                        .push(" WHERE topic_id = ")
                        .push_bind(topic_id)
                        .push(" AND role = ")
                        .push_bind(role)
                        .build_query_scalar::<i64>(),
                )
                .await
        })
        .await
    }

    /// 删除实例，记录不存在时报错
    pub async fn agent_delete(&self, id: i64) -> Result<()> {
        execute_expecting_rows(
            &self.executor,
            delete_by_id!(TableName::AGENT_INSTANCES, id),
        )
        .await
    }

    /// 覆盖实例的阶段与载荷，记录不存在时报错
    pub async fn agent_save_phase(
        &self,
        id: i64,
        phase: AgentPhase,
        phase_payload: Option<&str>,
    ) -> Result<()> {
        execute_expecting_rows(
            &self.executor,
            update_strict!(
                TableName::AGENT_INSTANCES,
                id,
                ("phase", phase.to_string()),
                ("phase_payload", phase_payload)
            ),
        )
        .await
    }

    /// 按 id 取出实例，不存在时为 None
    pub async fn agent_get(&self, id: i64) -> Result<Option<AgentInstance>> {
        let rows = self
            .executor
            .fetch_all(
                select_instances()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<AgentInstance>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出 topic 下的实例，按 id 升序
    pub async fn agent_list_by_topic(&self, topic_id: i64) -> Result<Vec<AgentInstance>> {
        self.executor
            .fetch_all(
                select_instances()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<AgentInstance>(),
            )
            .await
    }
}
