use crate::error::Result;
use crate::{AgentInstance, Storage};
use wind_wire::{AgentMode, AgentPhase, AgentRole};

impl Storage {
    pub async fn get_instance_by_id(&self, _instance_id: i64) -> Result<Option<AgentInstance>> {
        todo!()
    }

    /// TODO: 创建 main agent 实例,
    pub async fn create_agent_instance(
        &self,
        topic_id: i64,
        parent_id: Option<i64>,
        role: AgentRole,
        mode: Option<AgentMode>,
        phase: AgentPhase,
        phase_payload: Option<String>,
    ) -> Result<i64> {
        // AgentRole::Main
        todo!()
    }

    /// TODO: 写一行派生调用的配对
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
        _call_id: &str,
        child_instance_id: i64,
    ) -> Result<()> {
        todo!()
    }
}
