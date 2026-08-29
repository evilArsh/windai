use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{
    AgentBinding, AgentDefinition, CreateAgentBinding, CreateAgentDefinition, UpdateAgentBinding,
    UpdateAgentDefinition,
};

use crate::dto::envelope::{ApiResponse, map_core_error};

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
            .list_definitions_by_topic(topic_id)
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

    pub async fn create_agent_binding(
        &self,
        input: CreateAgentBinding,
    ) -> ApiResponse<AgentBinding> {
        match self.core.storage().agent().create_binding(input).await {
            Ok(b) => ApiResponse::ok(b),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_agent_binding(&self, id: i64) -> ApiResponse<AgentBinding> {
        match self.core.storage().agent().get_binding(id).await {
            Ok(Some(b)) => ApiResponse::ok(b),
            Ok(None) => ApiResponse::not_found("agent binding not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_agent_binding(
        &self,
        id: i64,
        input: UpdateAgentBinding,
    ) -> ApiResponse<AgentBinding> {
        if let Err(e) = self.core.storage().agent().update_binding(id, input).await {
            return map_core_error(e);
        }
        self.get_agent_binding(id).await
    }

    pub async fn delete_agent_binding(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().agent().get_binding(id).await {
            Ok(None) => return ApiResponse::not_found("agent binding not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        match self.core.storage().agent().delete_binding(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_agent_binding_by_agent(
        &self,
        agent_id: i64,
        parent_topic_id: i64,
    ) -> ApiResponse<AgentBinding> {
        match self
            .core
            .storage()
            .agent()
            .get_binding_by_agent_id(parent_topic_id, agent_id)
            .await
        {
            Ok(Some(b)) => ApiResponse::ok(b),
            Ok(None) => ApiResponse::not_found("agent binding not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_agent_bindings_by_topic(
        &self,
        topic_id: i64,
    ) -> ApiResponse<Vec<AgentBinding>> {
        match self
            .core
            .storage()
            .agent()
            .list_bindings_by_topic(topic_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_main_binding(&self, topic_id: i64) -> ApiResponse<AgentBinding> {
        match self.core.storage().agent().get_main_binding(topic_id).await {
            Ok(Some(b)) => ApiResponse::ok(b),
            Ok(None) => ApiResponse::not_found("agent binding not found"),
            Err(e) => map_core_error(e),
        }
    }
}
