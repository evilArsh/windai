use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{
    AgentDefinition, AgentInstance, AgentRole, CreateAgentDefinition, UpdateAgentDefinition,
};

use crate::dto::{ApiResponse, map_core_error};

pub struct AgentStorageFacade {
    core: Arc<WindCore>,
}

impl AgentStorageFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_agent_definitions(&self) -> ApiResponse<Vec<AgentDefinition>> {
        match self.core.storage().agent().list_definitions().await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_agent_definition(
        &self,
        input: CreateAgentDefinition,
    ) -> ApiResponse<AgentDefinition> {
        match self.core.storage().agent().create_definition(input).await {
            Ok(d) => ApiResponse::ok(d),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_agent_definition(&self, id: i64) -> ApiResponse<AgentDefinition> {
        match self.core.storage().agent().get_definition(id).await {
            Ok(Some(d)) => ApiResponse::ok(d),
            Ok(None) => ApiResponse::not_found("agent definition not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_agent_definition(
        &self,
        id: i64,
        input: UpdateAgentDefinition,
    ) -> ApiResponse<AgentDefinition> {
        if let Err(e) = self
            .core
            .storage()
            .agent()
            .update_definition(id, input)
            .await
        {
            return map_core_error(e);
        }
        self.get_agent_definition(id).await
    }

    pub async fn delete_agent_definition(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().agent().delete_definition(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_agent_definition_by_key(&self, key: String) -> ApiResponse<AgentDefinition> {
        match self
            .core
            .storage()
            .agent()
            .get_definition_by_key(&key)
            .await
        {
            Ok(Some(d)) => ApiResponse::ok(d),
            Ok(None) => ApiResponse::not_found("agent definition not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_agent_definitions_by_topic(
        &self,
        topic_id: i64,
    ) -> ApiResponse<Vec<AgentDefinition>> {
        match self
            .core
            .storage()
            .agent()
            .list_sub_definitions_by_topic(topic_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn clone_agent_definition(
        &self,
        agent_id: i64,
        owner_topic_id: i64,
    ) -> ApiResponse<AgentDefinition> {
        match self
            .core
            .storage()
            .agent()
            .clone_definition_for_topic(agent_id, owner_topic_id)
            .await
        {
            Ok(d) => ApiResponse::ok(d),
            Err(e) => map_core_error(e),
        }
    }

    /// 列出话题下的子 Agent 实例
    pub async fn list_instances_by_topic(&self, topic_id: i64) -> ApiResponse<Vec<AgentInstance>> {
        // 先确认话题存在，与 agent-maps 保持同样的 404 语义
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Err(err) => return map_core_error(err),
        }
        match self
            .core
            .storage()
            .agent()
            .list_child_instances_by_topic(topic_id)
            .await
        {
            Ok(instances) => ApiResponse::ok(instances),
            Err(err) => map_core_error(err),
        }
    }

    /// 获取单个子 Agent 实例，主实例不对外暴露
    pub async fn get_instance(&self, instance_id: i64) -> ApiResponse<AgentInstance> {
        let instance = match self.core.storage().agent().get_instance(instance_id).await {
            Ok(Some(instance)) => instance,
            Ok(None) => return ApiResponse::not_found("agent instance not found"),
            Err(err) => return map_core_error(err),
        };
        if instance.role == AgentRole::Main {
            return ApiResponse::not_found("agent instance not found");
        }
        ApiResponse::ok(instance)
    }
}
