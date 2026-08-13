use super::{executor::StorageExecutor, utils::ensure_affected};
use crate::{
    db::DbDriver,
    error::Result,
    insert,
    models::{ChatConfig, CreateTopic, Topic, UpdateTopic},
    select_fields,
    storage::{TableName, next_id, now_ts},
    update, update_fields,
};
use sqlx::QueryBuilder;
use wind_ai::message::ReqConfig;

#[derive(Clone)]
pub struct TopicStorage {
    executor: StorageExecutor,
}

impl TopicStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    async fn batch_delete_by_ids(
        executor: &StorageExecutor,
        table: &str,
        column: &str,
        ids: &[i64],
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new(format!("DELETE FROM {} WHERE {} IN (", table, column));
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        executor.execute(builder.build()).await?;

        Ok(())
    }
    pub async fn create(&self, data: CreateTopic) -> Result<Topic> {
        let id = next_id();
        let parent_id = data.parent_id;
        let binding_id = data.binding_id;
        let now = now_ts();
        let mut qb = insert!(
            TableName::TOPICS,
            ("id", id),
            ("parent_id", parent_id),
            ("binding_id", binding_id),
            ("label", data.label.clone()),
            ("icon", data.icon.clone()),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;

        Ok(Topic {
            id,
            parent_id,
            binding_id,
            label: data.label,
            icon: data.icon,
            created_at: now,
        })
    }

    pub async fn update(&self, id: i64, data: UpdateTopic) -> Result<()> {
        let mut qb = update!(
            TableName::TOPICS,
            id,
            ("parent_id", data.parent_id),
            ("label", data.label),
            ("icon", data.icon),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    /// 获取所有 topic
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let mut qb = Self::select_topic();
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

    pub async fn get_topic_by_binding_id(
        &self,
        parent_topic: i64,
        binding_id: i64,
    ) -> Result<Option<Topic>> {
        let mut qb = Self::select_topic();
        qb.push(" WHERE binding_id = ").push_bind(binding_id);
        qb.push(" AND parent_id = ").push_bind(parent_topic);
        let row = self
            .executor
            .fetch_optional(qb.build_query_as::<Topic>())
            .await?;

        Ok(row)
    }

    pub async fn delete_topics(&self, ids: &[i64]) -> Result<()> {
        self.executor
            .with_tx(|executor| async move {
                Self::batch_delete_by_ids(&executor, TableName::MESSAGES, "topic_id", ids).await?;

                if !ids.is_empty() {
                    let mut qb = QueryBuilder::new("DELETE FROM ");
                    qb.push(TableName::CHAT_CONFIGS)
                        .push(" WHERE id IN (SELECT chat_config_id FROM ")
                        .push(TableName::TOPIC_AGENT_BINDINGS)
                        .push(" WHERE parent_topic_id IN (");
                    let mut separated = qb.separated(", ");
                    for id in ids {
                        separated.push_bind(*id);
                    }
                    separated.push_unseparated(")) ");
                    executor.execute(qb.build()).await?;
                }

                Self::batch_delete_by_ids(
                    &executor,
                    TableName::TOPIC_AGENT_BINDINGS,
                    "parent_topic_id",
                    ids,
                )
                .await?;
                Self::batch_delete_by_ids(&executor, TableName::TOPICS, "id", ids).await?;
                Self::batch_delete_by_ids(
                    &executor,
                    TableName::TOOL_APPROVAL_REQUESTS,
                    "topic_id",
                    ids,
                )
                .await?;
                Ok(())
            })
            .await
    }

    pub async fn create_chat_config(&self, config: ReqConfig) -> Result<ChatConfig> {
        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            TableName::CHAT_CONFIGS,
            ("id", id),
            ("temperature", config.temperature),
            ("top_p", config.top_p),
            ("max_tokens", config.max_tokens),
            ("stream", config.stream),
            ("presence_penalty", config.presence_penalty),
            ("frequency_penalty", config.frequency_penalty),
            ("parallel_tool_calls", config.parallel_tool_calls),
            ("reasoning", config.reasoning),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(ChatConfig {
            id,
            data: config,
            created_at: now,
        })
    }

    pub async fn update_chat_config(&self, id: i64, config: ReqConfig) -> Result<()> {
        ensure_affected(
            self.executor
                .execute(
                    update_fields!(
                        TableName::CHAT_CONFIGS,
                        ("temperature", config.temperature),
                        ("top_p", config.top_p),
                        ("max_tokens", config.max_tokens),
                        ("stream", config.stream),
                        ("presence_penalty", config.presence_penalty),
                        ("frequency_penalty", config.frequency_penalty),
                        ("parallel_tool_calls", config.parallel_tool_calls),
                        ("reasoning", config.reasoning),
                        ("updated_at", Some(now_ts()))
                    )
                    .push(" WHERE id =  ")
                    .push_bind(id)
                    .build(),
                )
                .await?,
        )
    }

    pub async fn get_chat_config(&self, id: i64) -> Result<Option<ChatConfig>> {
        let row = self
            .executor
            .fetch_optional(
                select_fields!(
                    TableName::CHAT_CONFIGS,
                    (
                        "id",
                        "temperature",
                        "top_p",
                        "max_tokens",
                        "stream",
                        "presence_penalty",
                        "frequency_penalty",
                        "parallel_tool_calls",
                        "reasoning",
                        "created_at"
                    )
                )
                .push(" WHERE id = ")
                .push_bind(id)
                .build_query_as::<ChatConfig>(),
            )
            .await?;

        Ok(row)
    }

    fn select_topic<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::TOPICS,
            ("id", "parent_id", "label", "icon", "created_at")
        )
    }
}
