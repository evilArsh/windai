use crate::error::Result;
use crate::{AgentInstance, Storage};
use wind_wire::{AgentMode, AgentPhase, AgentRole};

impl Storage {
    pub async fn get_instance_by_id(&self, _instance_id: i64) -> Result<Option<AgentInstance>> {
        todo!()
    }

    /// TODO: 创建 main agent 实例
    pub async fn create_main_agent_instance(
        &self,
        topic_id: i64,
        parent_id: Option<i64>,
        mode: Option<AgentMode>,
    ) -> Result<i64> {
        // AgentRole::Main
        todo!()
    }

    /// TODO: 创建 子 agent 实例
    pub async fn create_child_agent_instance(
        &self,
        topic_id: i64,
        parent_id: Option<i64>,
        mode: Option<AgentMode>,
    ) -> Result<i64> {
        // AgentRole::Child
        todo!()
    }
}
