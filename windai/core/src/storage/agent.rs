use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected, ensure_lte_one, next_id, now_ts},
};
use crate::{
    db::DbDriver,
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::{
        AgentStatus, CreateTopicAgentMap, TopicAgentMap,
        agent::{
            AgentDefinition, AgentInstance, AgentRole, CreateAgentDefinition, CreateInstance,
            UpdateAgentDefinition, UpdateInstance,
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

    /// 删除 topic 特有的 agent 定义
    pub(crate) async fn batch_delete_definitions_by_topics(&self, topic_ids: &[i64]) -> Result<()> {
        utils::batch_delete_in(
            &self.executor,
            TableName::AGENT_DEFINITION,
            "owner_topic_id",
            topic_ids,
        )
        .await
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

    /// 创建新的 Agent 实例
    pub async fn create_instance(&self, data: CreateInstance) -> Result<AgentInstance> {
        let id = next_id();
        let now = now_ts();
        let role = data.role.unwrap_or(AgentRole::Child);
        let status = data.status.unwrap_or(AgentStatus::Idle);
        let mut qb = insert!(
            TableName::AGENT_INSTANCES,
            ("id", id),
            ("parent_id", data.parent_id),
            ("topic_id", data.topic_id),
            ("agent_id", data.agent_id),
            ("role", role.to_string()),
            ("mode", data.mode.map(|v| v.to_string())),
            ("status", status.to_string()),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(AgentInstance {
            id,
            parent_id: data.parent_id,
            topic_id: data.topic_id,
            agent_id: data.agent_id,
            mode: data.mode,
            role,
            status,
            created_at: now,
        })
    }

    /// 更新 Agent 实例
    pub async fn update_instance(&self, id: i64, data: UpdateInstance) -> Result<()> {
        let mut qb = update!(
            TableName::AGENT_INSTANCES,
            id,
            ("agent_id", data.agent_id),
            ("status", data.status.map(|v| v.to_string())),
            ("mode", data.mode.map(|v| v.to_string())),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn delete_instances(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.executor
            .with_tx(|executor| async move {
                utils::batch_delete_in(
                    &executor,
                    TableName::TOOL_APPROVAL_REQUESTS,
                    "instance_id",
                    ids,
                )
                .await?;
                utils::batch_delete_in(&executor, TableName::MESSAGES, "instance_id", ids).await?;
                utils::batch_delete_in(&executor, TableName::AGENT_INSTANCES, "id", ids).await?;
                Ok(())
            })
            .await
    }

    pub async fn get_instance(&self, id: i64) -> Result<Option<AgentInstance>> {
        let row = self
            .executor
            .fetch_optional(
                get_by_id!(TableName::AGENT_INSTANCES, id).build_query_as::<AgentInstance>(),
            )
            .await?;
        Ok(row)
    }

    pub(crate) async fn batch_get_instances_by_topics(
        &self,
        topic_ids: &[i64],
    ) -> Result<Vec<AgentInstance>> {
        if topic_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = Self::select_instances();
        qb.push(" WHERE topic_id IN ( ");
        let mut separated = qb.separated(", ");
        for id in topic_ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        qb.push(" ORDER BY id ASC ");

        let row = self
            .executor
            .fetch_all(qb.build_query_as::<AgentInstance>())
            .await?;

        Ok(row)
    }

    /// 获取 topic 的主 Agent 实例
    pub async fn get_main_instance(&self, topic_id: i64) -> Result<Option<AgentInstance>> {
        ensure_lte_one(
            self.executor
                .fetch_all(
                    Self::select_instances()
                        .push(" WHERE topic_id = ")
                        .push_bind(topic_id)
                        .push(" AND role = ")
                        .push_bind(AgentRole::Main.to_string())
                        .build_query_as::<AgentInstance>(),
                )
                .await?,
            Some(format!(
                "Only one main instance allowed in a topic, topic_id: {}",
                topic_id
            )),
        )
    }

    pub async fn list_instances_by_topic(&self, topic_id: i64) -> Result<Vec<AgentInstance>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_instances()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<AgentInstance>(),
            )
            .await?;
        Ok(rows)
    }

    /// 获取 topic 下的子 Agent 实例，不含主 Agent
    pub async fn list_child_instances_by_topic(&self, topic_id: i64) -> Result<Vec<AgentInstance>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_instances()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" AND role <> ")
                    .push_bind(AgentRole::Main.to_string())
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<AgentInstance>(),
            )
            .await?;
        Ok(rows)
    }

    /// 查找 topic 能力列表中的 AgentDefinition
    pub async fn list_sub_definitions_by_topic(
        &self,
        topic_id: i64,
    ) -> Result<Vec<AgentDefinition>> {
        let mut qb = Self::select_definitions();
        qb.push(" WHERE id IN (SELECT agent_id FROM ")
            .push(TableName::TOPIC_AGENT_MAPS)
            .push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .push(") ORDER BY id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<AgentDefinition>())
            .await?;
        Ok(rows)
    }

    /// 复制一份 `agent_id` 给新的 `owner_topic_id`
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

    async fn get_agent_map_by_agent(
        &self,
        topic_id: i64,
        agent_id: i64,
    ) -> Result<Option<TopicAgentMap>> {
        self.executor
            .fetch_optional(
                Self::select_agent_maps()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" AND agent_id = ")
                    .push_bind(agent_id)
                    .build_query_as::<TopicAgentMap>(),
            )
            .await
    }

    /// 创建 topic 与 AgentDefinition 的能力映射
    pub async fn create_topic_agent_map(&self, data: CreateTopicAgentMap) -> Result<TopicAgentMap> {
        if self.get_definition(data.agent_id).await?.is_none() {
            return Err(CoreError::Validation(format!(
                "Agent definition not found: {}",
                data.agent_id
            )));
        }
        if self
            .get_agent_map_by_agent(data.topic_id, data.agent_id)
            .await?
            .is_some()
        {
            return Err(CoreError::Validation(format!(
                "Agent {} is already mapped in topic {}",
                data.agent_id, data.topic_id
            )));
        }

        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            TableName::TOPIC_AGENT_MAPS,
            ("id", id),
            ("topic_id", data.topic_id),
            ("agent_id", data.agent_id),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(TopicAgentMap {
            id,
            topic_id: data.topic_id,
            agent_id: data.agent_id,
            created_at: now,
        })
    }

    /// 获取 topic 的能力映射列表
    pub async fn list_agent_maps_by_topic(&self, topic_id: i64) -> Result<Vec<TopicAgentMap>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_agent_maps()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<TopicAgentMap>(),
            )
            .await?;
        Ok(rows)
    }

    /// 获取单个 topic 能力映射
    pub async fn get_agent_map(&self, id: i64) -> Result<Option<TopicAgentMap>> {
        self.executor
            .fetch_optional(
                Self::select_agent_maps()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<TopicAgentMap>(),
            )
            .await
    }

    /// 删除 topic 能力映射
    pub async fn delete_topic_agent_map(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!(TableName::TOPIC_AGENT_MAPS, id);
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    /// 删除 topic 的能力映射，供删除 topic 时级联调用
    pub(crate) async fn batch_delete_agent_maps_by_topics(&self, topic_ids: &[i64]) -> Result<()> {
        utils::batch_delete_in(
            &self.executor,
            TableName::TOPIC_AGENT_MAPS,
            "topic_id",
            topic_ids,
        )
        .await
    }

    pub fn select_definitions<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::AGENT_DEFINITION,
            (
                "id",
                "key",
                "name",
                "description",
                "owner_topic_id",
                "cloned_from_id",
                "active",
                "data",
                "created_at"
            )
        )
    }

    fn select_instances<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::AGENT_INSTANCES,
            (
                "id",
                "parent_id",
                "topic_id",
                "agent_id",
                "role",
                "status",
                "mode",
                "created_at"
            )
        )
    }

    fn select_agent_maps<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::TOPIC_AGENT_MAPS,
            ("id", "topic_id", "agent_id", "created_at")
        )
    }
}
