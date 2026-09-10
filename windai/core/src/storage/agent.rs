use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected, ensure_lte_one, next_id, now_ts},
};
use crate::{
    db::DbDriver,
    delete_by_id, delete_from,
    error::{CoreError, Result},
    get_by_id, insert,
    models::{
        AgentStatus, ToolApprovalPolicy,
        agent::{
            AgentBinding, AgentDefinition, AgentRole, CreateAgentBinding, CreateAgentDefinition,
            UpdateAgentBinding, UpdateAgentDefinition,
        },
    },
    select_fields,
    storage::TableName,
    update,
};
#[derive(Clone)]
pub struct AgentStorage {
    executor: StorageExecutor,
}

impl AgentStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    /// 创建新的 AgentDefinition
    pub async fn create_definition(&self, data: CreateAgentDefinition) -> Result<AgentDefinition> {
        if data.key.trim().is_empty() {
            return Err(CoreError::Validation("agent key cannot be empty".into()));
        }
        if data.name.trim().is_empty() {
            return Err(CoreError::Validation("agent name cannot be empty".into()));
        }
        let id = next_id();
        let now = now_ts();
        let active = data.active.unwrap_or(true);
        let mut qb = insert!(
            TableName::AGENT_DEFINITION,
            ("id", id),
            ("key", data.key.clone()),
            ("name", data.name.clone()),
            ("description", data.description.clone()),
            ("owner_topic_id", data.owner_topic_id),
            ("cloned_from_id", data.cloned_from_id),
            ("active", active),
            ("data", utils::map_to_str_default(Some(&data.data))?),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(AgentDefinition {
            id,
            key: data.key,
            name: data.name,
            description: data.description,
            owner_topic_id: data.owner_topic_id,
            cloned_from_id: data.cloned_from_id,
            active: active,
            data: data.data,
            created_at: now,
        })
    }

    /// 更新 AgentDefinition
    pub async fn update_definition(&self, id: i64, data: UpdateAgentDefinition) -> Result<()> {
        if data
            .name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(CoreError::Validation("agent name cannot be empty".into()));
        }
        let mut qb = update!(
            TableName::AGENT_DEFINITION,
            id,
            ("name", data.name),
            ("description", data.description),
            ("owner_topic_id", data.owner_topic_id),
            ("cloned_from_id", data.cloned_from_id),
            ("active", data.active),
            ("data", utils::map_to_str_optional(data.data.as_ref())?)
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn delete_definition(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!(TableName::AGENT_DEFINITION, id);
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn get_definition(&self, id: i64) -> Result<Option<AgentDefinition>> {
        let row = self
            .executor
            .fetch_optional(
                get_by_id!(TableName::AGENT_DEFINITION, id).build_query_as::<AgentDefinition>(),
            )
            .await?;
        Ok(row)
    }

    pub async fn batch_get_definitions(&self, ids: &[i64]) -> Result<Vec<AgentDefinition>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = Self::select_definitions();
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<AgentDefinition>())
            .await?;
        Ok(rows)
    }

    /// 删除topic特有的agent定义
    pub(crate) async fn batch_delete_definitions_by_topics(&self, topic_ids: &[i64]) -> Result<()> {
        if topic_ids.is_empty() {
            return Ok(());
        }
        let mut qb = delete_from!(TableName::AGENT_DEFINITION);
        qb.push(" WHERE owner_topic_id IN ( ");
        let mut separated = qb.separated(", ");
        for id in topic_ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn get_definition_by_key(&self, key: &str) -> Result<Option<AgentDefinition>> {
        let row = self
            .executor
            .fetch_optional(
                Self::select_definitions()
                    .push(" WHERE key = ")
                    .push_bind(key)
                    .build_query_as::<AgentDefinition>(),
            )
            .await?;
        Ok(row)
    }

    pub async fn list_definitions(&self) -> Result<Vec<AgentDefinition>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_definitions()
                    .push(" ORDER BY id DESC ")
                    .build_query_as::<AgentDefinition>(),
            )
            .await?;
        Ok(rows)
    }

    /// 创建新的 TopicAgentBinding
    pub async fn create_binding(&self, data: CreateAgentBinding) -> Result<AgentBinding> {
        // TODO: 校验topic_id是否已有agent_id绑定
        let id = next_id();
        let now = now_ts();
        let status = AgentStatus::Idle;
        let policy = ToolApprovalPolicy::default();
        let enabled = data.enabled.unwrap_or(true);
        let mut qb = insert!(
            TableName::TOPIC_AGENT_BINDINGS,
            ("id", id),
            ("topic_id", data.topic_id),
            ("agent_id", data.agent_id),
            ("role", data.role.to_string()),
            ("model_id", data.model_id),
            ("chat_config_id", data.chat_config_id),
            ("status", status.to_string()),
            ("tool_approval_policy", serde_json::to_string(&policy)?),
            ("enabled", enabled),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(AgentBinding {
            id,
            topic_id: data.topic_id,
            agent_id: data.agent_id,
            mode: None,
            role: data.role,
            status,
            model_id: data.model_id,
            tool_approval_policy: Some(policy),
            chat_config_id: data.chat_config_id,
            enabled,
            created_at: now,
        })
    }

    /// 更新 TopicAgentBinding
    pub async fn update_binding(&self, id: i64, data: UpdateAgentBinding) -> Result<()> {
        let mut qb = update!(
            TableName::TOPIC_AGENT_BINDINGS,
            id,
            ("agent_id", data.agent_id),
            ("role", data.role.map(|v| v.to_string())),
            ("model_id", data.model_id),
            ("chat_config_id", data.chat_config_id),
            ("status", data.status.map(|v| v.to_string())),
            ("mode", data.mode.map(|v| v.to_string())),
            (
                "tool_approval_policy",
                utils::map_to_str_optional(data.tool_approval_policy.as_ref())?
            ),
            ("enabled", data.enabled)
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn delete_bindings(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        self.executor
            .with_tx(|executor| async move {
                // 1. 删 chat_configs：
                //    DELETE FROM chat_configs
                //    WHERE id IN (
                //        SELECT chat_config_id FROM topic_agent_bindings
                //        WHERE id IN (?, ?, ...)
                //    )
                let mut qb = delete_from!(TableName::CHAT_CONFIGS);
                qb.push(" WHERE id IN ( SELECT chat_config_id FROM ")
                    .push(TableName::TOPIC_AGENT_BINDINGS)
                    .push(" WHERE id IN (");
                {
                    let mut separated = qb.separated(", ");
                    for id in ids {
                        separated.push_bind(*id);
                    }
                }
                qb.push(") )");
                executor.execute(qb.build()).await?;

                // 2. 删 messages：
                //    DELETE FROM messages WHERE binding_id IN (?, ?, ...)
                let mut qb = delete_from!(TableName::MESSAGES);
                qb.push(" WHERE binding_id IN (");
                {
                    let mut separated = qb.separated(", ");
                    for id in ids {
                        separated.push_bind(*id);
                    }
                }
                qb.push(")");
                executor.execute(qb.build()).await?;

                // 3. 删 topic_agent_bindings：
                //    DELETE FROM topic_agent_bindings WHERE id IN (?, ?, ...)
                let mut qb = delete_from!(TableName::TOPIC_AGENT_BINDINGS);
                qb.push(" WHERE id IN (");
                {
                    let mut separated = qb.separated(", ");
                    for id in ids {
                        separated.push_bind(*id);
                    }
                }
                qb.push(")");
                executor.execute(qb.build()).await?;

                Ok(())
            })
            .await
    }

    pub async fn get_binding(&self, id: i64) -> Result<Option<AgentBinding>> {
        let row = self
            .executor
            .fetch_optional(
                get_by_id!(TableName::TOPIC_AGENT_BINDINGS, id).build_query_as::<AgentBinding>(),
            )
            .await?;
        Ok(row)
    }

    pub async fn get_bindings_by_topic(&self, topic_id: i64) -> Result<Vec<AgentBinding>> {
        let mut qb = Self::select_bindings();
        qb.push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .push(" ORDER BY id ASC ");
        let row = self
            .executor
            .fetch_all(qb.build_query_as::<AgentBinding>())
            .await?;

        Ok(row)
    }

    pub(crate) async fn batch_get_bindings_by_topics(
        &self,
        topic_ids: &[i64],
    ) -> Result<Vec<AgentBinding>> {
        let mut qb = Self::select_bindings();
        qb.push(" WHERE topic_id IN ( ");
        let mut separated = qb.separated(", ");
        for id in topic_ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        qb.push(" ORDER BY id ASC ");

        let row = self
            .executor
            .fetch_all(qb.build_query_as::<AgentBinding>())
            .await?;

        Ok(row)
    }

    /// 每个Topic下的同一个AgentDefinition只能被一个AgentBinding绑定。
    pub async fn get_binding_by_agent_id(
        &self,
        topic_id: i64,
        agent_id: i64,
    ) -> Result<Option<AgentBinding>> {
        ensure_lte_one(
            self.executor
                .fetch_all(
                    Self::select_bindings()
                        .push(" WHERE agent_id = ")
                        .push_bind(agent_id)
                        .push(" AND topic_id = ")
                        .push_bind(topic_id)
                        .push(" AND enabled = ")
                        .push_bind(true)
                        .build_query_as::<AgentBinding>(),
                )
                .await?,
            Some(format!(
                "(topic_id = {}, agent_id = {})",
                topic_id, agent_id
            )),
        )
    }

    pub async fn get_main_binding(&self, topic_id: i64) -> Result<Option<AgentBinding>> {
        ensure_lte_one(
            self.executor
                .fetch_all(
                    Self::select_bindings()
                        .push(" WHERE topic_id = ")
                        .push_bind(topic_id)
                        .push(" AND role = ")
                        .push_bind(AgentRole::Main.to_string())
                        .push(" AND enabled = ")
                        .push_bind(true)
                        .build_query_as::<AgentBinding>(),
                )
                .await?,
            Some(format!(
                "Only one main binding allowed in a topic, topic_id: {}",
                topic_id
            )),
        )
    }

    pub async fn list_bindings_by_topic(&self, topic_id: i64) -> Result<Vec<AgentBinding>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_bindings()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<AgentBinding>(),
            )
            .await?;
        Ok(rows)
    }

    /// 查找topic下绑定的所有AgentDefinition
    ///
    /// 过滤掉主Agent和已禁用的绑定
    pub async fn list_sub_definitions_by_topic(&self, topic: i64) -> Result<Vec<AgentDefinition>> {
        // FIXME: tablename is hardcoded
        let mut qb = sqlx::QueryBuilder::new(
            r#"
            SELECT
                agent_definitions.id AS id,
                agent_definitions.key AS key,
                agent_definitions.name AS name,
                agent_definitions.description AS description,
                agent_definitions.owner_topic_id AS owner_topic_id,
                agent_definitions.cloned_from_id AS cloned_from_id,
                agent_definitions.active AS active,
                agent_definitions.data AS data,
                agent_definitions.created_at AS created_at
            FROM "#,
        );
        qb.push(TableName::AGENT_DEFINITION)
            .push(" INNER JOIN ")
            .push(TableName::TOPIC_AGENT_BINDINGS)
            .push(
                r#" ON agent_definitions.id = topic_agent_bindings.agent_id
            WHERE
                topic_agent_bindings.role <> 'main'
                AND topic_agent_bindings.enabled = 1
                AND topic_agent_bindings.topic_id =
            "#,
            );
        qb.push_bind(topic)
            .push(" ORDER BY topic_agent_bindings.id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<AgentDefinition>())
            .await?;
        Ok(rows)
    }

    /// 复制一份`agent_id`给新的`owner_topic_id`
    pub async fn clone_definition_for_topic(
        &self,
        agent_id: i64,
        owner_topic_id: i64,
    ) -> Result<AgentDefinition> {
        let source = self
            .get_definition(agent_id)
            .await?
            .ok_or_else(|| CoreError::RowNotFound(format!("agent definition {agent_id}")))?;
        if source.owner_topic_id == Some(owner_topic_id) {
            return Ok(source);
        }

        let cloned_key = format!("{}-topic-{}", source.key, owner_topic_id);
        self.create_definition(CreateAgentDefinition {
            key: cloned_key,
            name: source.name,
            description: source.description,
            owner_topic_id: Some(owner_topic_id),
            cloned_from_id: Some(source.id),
            active: Some(source.active),
            data: source.data,
        })
        .await
    }

    fn select_definitions<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::AGENT_DEFINITION,
            (
                "id",
                "key",
                "name",
                "description",
                "scope",
                "owner_topic_id",
                "cloned_from_id",
                "active",
                "data",
                "created_at"
            )
        )
    }

    fn select_bindings<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::TOPIC_AGENT_BINDINGS,
            (
                "id",
                "topic_id",
                "agent_id",
                "role",
                "model_id",
                "chat_config_id",
                "status",
                "mode",
                "tool_approval_policy",
                "enabled",
                "created_at"
            )
        )
    }
}
