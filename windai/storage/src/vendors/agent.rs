use crate::error::Result;
use crate::utils::now_ts;
use crate::{AgentInstance, Storage};
use wind_wire::{AgentMode, AgentPhase, AgentRole};

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
        phase: AgentPhase,
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
                .bind(phase.as_ref())
                .bind(phase_payload)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
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
}
