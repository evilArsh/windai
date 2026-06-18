use crate::{
    db::{DbDriver, DbPool},
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::agent::{
        AgentDefinition, CreateAgentDefinition, CreateTopicAgentBinding, TopicAgentBinding,
        TopicAgentBindingRole, UpdateAgentDefinition, UpdateTopicAgentBinding,
    },
    select_fields,
    storage::next_id,
    update,
};

use super::utils::{self, ensure_affected};

pub struct AgentStorage {
    db: DbPool,
}

impl AgentStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub async fn create_definition(&self, data: CreateAgentDefinition) -> Result<i64> {
        if data.key.trim().is_empty() {
            return Err(CoreError::Validation("agent key cannot be empty".into()));
        }
        if data.name.trim().is_empty() {
            return Err(CoreError::Validation("agent name cannot be empty".into()));
        }

        let id = next_id();
        insert!(
            "agent_definitions",
            ("id", id),
            ("key", data.key),
            ("name", data.name),
            ("description", data.description),
            ("scope", data.scope.to_string()),
            ("owner_topic_id", data.owner_topic_id),
            ("cloned_from_agent_id", data.cloned_from_agent_id),
            ("role", data.role.to_string()),
            ("active", data.active.unwrap_or(true)),
            ("data", utils::map_to_str_default(Some(&data.data))?)
        )
        .build()
        .execute(&self.db)
        .await?;
        Ok(id)
    }

    pub async fn update_definition(&self, id: i64, data: UpdateAgentDefinition) -> Result<()> {
        let mut qb = update!(
            "agent_definitions",
            id,
            ("key", data.key),
            ("name", data.name),
            ("description", data.description),
            ("scope", data.scope.map(|v| v.to_string())),
            ("owner_topic_id", data.owner_topic_id),
            ("cloned_from_agent_id", data.cloned_from_agent_id),
            ("role", data.role.map(|v| v.to_string())),
            ("active", data.active),
            ("data", utils::map_to_str_optional(data.data.as_ref())?)
        );
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
    }

    pub async fn delete_definition(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("agent_definitions", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn get_definition(&self, id: i64) -> Result<Option<AgentDefinition>> {
        let row = get_by_id!("agent_definitions", id)
            .build_query_as::<AgentDefinition>()
            .fetch_optional(&self.db)
            .await?;
        Ok(row)
    }

    pub async fn get_definition_by_key(&self, key: &str) -> Result<Option<AgentDefinition>> {
        let row = Self::select_definitions()
            .push(" WHERE key = ")
            .push_bind(key)
            .build_query_as::<AgentDefinition>()
            .fetch_optional(&self.db)
            .await?;
        Ok(row)
    }

    pub async fn list_definitions(&self) -> Result<Vec<AgentDefinition>> {
        let rows = Self::select_definitions()
            .push(" ORDER BY id DESC ")
            .build_query_as::<AgentDefinition>()
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    pub async fn create_binding(&self, data: CreateTopicAgentBinding) -> Result<i64> {
        let id = next_id();
        insert!(
            "topic_agent_bindings",
            ("id", id),
            ("topic_id", data.topic_id),
            ("agent_id", data.agent_id),
            ("binding_role", data.binding_role.to_string()),
            ("alias", data.alias),
            ("model_id", data.model_id),
            ("chat_config_id", data.chat_config_id),
            ("enabled", data.enabled.unwrap_or(true)),
            ("config", utils::map_to_str_default(Some(&data.config))?)
        )
        .build()
        .execute(&self.db)
        .await?;
        Ok(id)
    }

    pub async fn update_binding(&self, id: i64, data: UpdateTopicAgentBinding) -> Result<()> {
        let mut qb = update!(
            "topic_agent_bindings",
            id,
            ("agent_id", data.agent_id),
            ("binding_role", data.binding_role.map(|v| v.to_string())),
            ("alias", data.alias),
            ("model_id", data.model_id),
            ("chat_config_id", data.chat_config_id),
            ("enabled", data.enabled),
            ("config", utils::map_to_str_optional(data.config.as_ref())?)
        );
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
    }

    pub async fn delete_binding(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("topic_agent_bindings", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn get_binding(&self, id: i64) -> Result<Option<TopicAgentBinding>> {
        let row = get_by_id!("topic_agent_bindings", id)
            .build_query_as::<TopicAgentBinding>()
            .fetch_optional(&self.db)
            .await?;
        Ok(row)
    }

    pub async fn get_main_binding(&self, topic_id: i64) -> Result<Option<TopicAgentBinding>> {
        let row = Self::select_bindings()
            .push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .push(" AND binding_role = ")
            .push_bind(TopicAgentBindingRole::Main.to_string())
            .push(" AND enabled = ")
            .push_bind(true)
            .build_query_as::<TopicAgentBinding>()
            .fetch_optional(&self.db)
            .await?;
        Ok(row)
    }

    pub async fn list_bindings_by_topic(&self, topic_id: i64) -> Result<Vec<TopicAgentBinding>> {
        let rows = Self::select_bindings()
            .push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .push(" ORDER BY id ASC ")
            .build_query_as::<TopicAgentBinding>()
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    fn select_definitions<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            "agent_definitions",
            (
                "id",
                "key",
                "name",
                "description",
                "scope",
                "owner_topic_id",
                "cloned_from_agent_id",
                "role",
                "active",
                "data",
                "created_at"
            )
        )
    }

    fn select_bindings<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            "topic_agent_bindings",
            (
                "id",
                "topic_id",
                "agent_id",
                "binding_role",
                "alias",
                "model_id",
                "chat_config_id",
                "enabled",
                "config",
                "created_at"
            )
        )
    }
}
