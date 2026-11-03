use crate::error::Result;
use crate::utils::{ensure_affected, now_ts};
use crate::{AgentInstance, ChildStateRow, Storage};
use sqlx::{QueryBuilder, Sqlite};
use wind_wire::{AgentMode, AgentRole};

impl Storage {
    pub async fn get_instance_by_id(&self, instance_id: i64) -> Result<Option<AgentInstance>> {
        let instance = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, AgentInstance>(
                    "SELECT id, parent_id, topic_id, role, mode, phase, phase_payload, created_at
                    FROM agent_instances
                    WHERE id = ?",
                )
                .bind(instance_id),
            )
            .await?;
        Ok(instance)
    }

    /// 创建 agent 实例
    pub async fn create_agent_instance(
        &self,
        topic_id: i64,
        parent_id: Option<i64>,
        role: AgentRole,
        mode: Option<AgentMode>,
        phase: &str,
        phase_payload: Option<String>,
    ) -> Result<i64> {
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO agent_instances
                        (parent_id, topic_id, role, mode, phase, phase_payload, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    RETURNING id",
                )
                .bind(parent_id)
                .bind(topic_id)
                .bind(role.as_ref())
                .bind(mode.map(|mode| mode.as_ref().to_string()))
                .bind(phase)
                .bind(phase_payload)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
    }

    pub async fn update_instance_phase(
        &self,
        instance_id: i64,
        phase: &str,
        phase_payload: Option<String>,
    ) -> Result<()> {
        let result = self
            .executor
            .execute(
                sqlx::query(
                    "UPDATE agent_instances
                    SET phase = ?, phase_payload = ?
                    WHERE id = ?",
                )
                .bind(phase)
                .bind(phase_payload)
                .bind(instance_id),
            )
            .await?;
        ensure_affected(result)
    }

    /// 写一行派生调用的配对
    ///
    /// 一行 = 父实例的一次 tool_call id 及其 spawn 出的子实例。
    ///
    /// 子实例的运行结果是父实例 tool_call 结果
    ///
    /// 父实例唤醒时按 (parent_id, call_id) 取 child_id，用它回补 tool_result；
    /// 后台子实例终止时按 child_id 取 parent_id，用它把结果写入父实例的待消费队列，
    /// 同事务写 result_delivered_at。该列为空即未交付，重复的终止事件不会再投递。
    pub async fn create_spawn_call_map(
        &self,
        parent_instance_id: i64,
        call_id: &str,
        child_instance_id: i64,
    ) -> Result<()> {
        self.executor
            .execute(
                sqlx::query(
                    "INSERT INTO spawn_calls
                        (parent_id, call_id, child_id, created_at)
                    VALUES (?, ?, ?, ?)",
                )
                .bind(parent_instance_id)
                .bind(call_id)
                .bind(child_instance_id)
                .bind(now_ts()),
            )
            .await?;
        Ok(())
    }

    /// 按调用标识读取子实例状态与收尾回复
    pub async fn list_child_states(
        &self,
        parent_id: i64,
        call_ids: &[&str],
    ) -> Result<Vec<ChildStateRow>> {
        if call_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT s.call_id,
                    i.id AS child_id,
                    i.mode,
                    i.phase,
                    i.phase_payload,
                    b.content AS reply_part
            FROM spawn_calls s
            JOIN agent_instances i ON i.id = s.child_id
            LEFT JOIN block b
                ON b.message_id = CASE
                    WHEN json_valid(i.phase_payload)
                    THEN json_extract(i.phase_payload, '$.message_id')
                    ELSE NULL
                END
                AND b.kind = 'text'
                AND b.finished = TRUE
            WHERE s.parent_id = ",
        );
        query.push_bind(parent_id);
        query.push(" AND s.call_id IN (");
        {
            let mut separated = query.separated(", ");
            for call_id in call_ids {
                separated.push_bind(*call_id);
            }
        }
        query.push(") ORDER BY s.call_id, b.seq");
        let rows = self.executor.fetch_all(query.build_query_as()).await?;
        Ok(rows)
    }
}
