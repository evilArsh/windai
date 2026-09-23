use super::{
    agent::AgentStorage,
    executor::StorageExecutor,
    utils::{self, ensure_affected, now_ts},
};
use crate::{
    db::DbDriver,
    error::{CoreError, Result},
    insert,
    models::{CreateInstance, CreateTopic, Topic, UpdateTopic},
    select_fields,
    storage::TableName,
    update,
};

#[derive(Clone)]
pub struct TopicStorage {
    executor: StorageExecutor,
}

impl TopicStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    /// 创建话题
    ///
    /// 指定 `agent_id` 时在同一事务内创建绑定该 Agent 的主实例，
    /// Agent 不存在或已禁用则整体拒绝；未指定时不创建实例，主实例留给首次对话懒创建
    pub async fn create(&self, data: CreateTopic) -> Result<Topic> {
        self.executor
            .with_tx(|executor| async move {
                if let Some(agent_id) = data.agent_id {
                    let agent = AgentStorage::new(executor.clone());
                    Self::ensure_agent_assignable(&agent, agent_id).await?;
                    let topic = Self::insert_topic(&executor, data).await?;
                    agent
                        .create_instance(CreateInstance::new_main(topic.id, Some(agent_id)))
                        .await?;
                    return Ok(topic);
                }

                Self::insert_topic(&executor, data).await
            })
            .await
    }

    /// 校验 Agent 可用于绑定主实例
    async fn ensure_agent_assignable(agent: &AgentStorage, agent_id: i64) -> Result<()> {
        let definition = agent.get_definition(agent_id).await?.ok_or_else(|| {
            CoreError::Validation(format!("Agent definition not found: {agent_id}"))
        })?;
        if !definition.active {
            return Err(CoreError::Validation(format!(
                "agent {} is disabled",
                definition.key
            )));
        }
        Ok(())
    }

    /// 插入话题行
    async fn insert_topic(executor: &StorageExecutor, data: CreateTopic) -> Result<Topic> {
        let parent_id = data.parent_id;
        let now = now_ts();
        let mut qb = insert!(
            TableName::TOPICS,
            ("parent_id", parent_id),
            ("model_id", data.model_id),
            (
                "tool_approval_policy",
                data.tool_approval_policy
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?
            ),
            ("label", data.label.clone()),
            ("icon", data.icon.clone()),
            ("created_at", now),
        );
        qb.push(" RETURNING id");
        let id: i64 = executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await?;

        Ok(Topic {
            id,
            parent_id,
            label: data.label,
            icon: data.icon,
            created_at: now,
            model_id: data.model_id,
            tool_approval_policy: data.tool_approval_policy,
        })
    }

    pub async fn update(&self, id: i64, data: UpdateTopic) -> Result<()> {
        let mut qb = update!(
            TableName::TOPICS,
            id,
            ("parent_id", data.parent_id),
            ("label", data.label),
            ("icon", data.icon),
            ("model_id", data.model_id),
            (
                "tool_approval_policy",
                data.tool_approval_policy
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?
            ),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    /// 获取所有 topic
    ///
    /// 目前只获取根 topic
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let mut qb = Self::select_topic();
        qb.push(" WHERE parent_id IS NULL ");
        qb.push(" ORDER BY id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<Topic>())
            .await?;

        Ok(rows)
    }

    /// 获取指定父话题下的直接子话题
    pub async fn list_child_topics(&self, parent_id: i64) -> Result<Vec<Topic>> {
        let mut qb = Self::select_topic();
        qb.push(" WHERE parent_id = ").push_bind(parent_id);
        qb.push(" ORDER BY id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<Topic>())
            .await?;

        Ok(rows)
    }

    /// 获取 topic
    pub async fn get_topic(&self, id: i64) -> Result<Option<Topic>> {
        let mut qb = Self::select_topic();
        qb.push(" WHERE id = ").push_bind(id);
        let row = self
            .executor
            .fetch_optional(qb.build_query_as::<Topic>())
            .await?;

        Ok(row)
    }

    /// 删除 topic 及其直接关联数据
    ///
    /// 子 topic 不在此处级联，调用方需一并传入其 id
    pub async fn delete_topics(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.executor
            .with_tx(|executor| async move {
                let agent = AgentStorage::new(executor.clone());
                let instance_ids = agent
                    .batch_get_instances_by_topics(ids)
                    .await?
                    .into_iter()
                    .map(|b| b.id)
                    .collect::<Vec<i64>>();

                // 删除只属于该 topic 的 agent_definitions
                agent.batch_delete_definitions_by_topics(ids).await?;
                // 删除该 topic 的能力映射
                agent.batch_delete_agent_maps_by_topics(ids).await?;
                // 删除该 topic 的 instance，连带 messages / tool_approval_requests
                agent.delete_instances(&instance_ids).await?;
                // 兜底删除该 topic 下残留的审批记录
                utils::batch_delete_in(
                    &executor,
                    TableName::TOOL_APPROVAL_REQUESTS,
                    "topic_id",
                    ids,
                )
                .await?;
                // 删除 topic 自身
                utils::batch_delete_in(&executor, TableName::TOPICS, "id", ids).await?;
                Ok(())
            })
            .await
    }
    fn select_topic<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::TOPICS,
            (
                "id",
                "parent_id",
                "label",
                "icon",
                "model_id",
                "tool_approval_policy",
                "created_at"
            )
        )
    }
}
