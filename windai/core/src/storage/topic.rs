use crate::{
    db::{DbDriver, DbPool},
    error::Result,
    get_by_id, insert,
    models::{ChatConfig, CreateTopic, Topic, UpdateTopic},
    select_fields,
    storage::{next_id, now_ts},
    update, update_fields,
};
use sqlx::{QueryBuilder, Row};
use wind_ai::message::ReqConfig;

use super::utils;
pub struct TopicStorage {
    db: DbPool,
}

impl TopicStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    async fn get_next_topic_index<'e, E>(executor: E, parent_id: Option<i64>) -> Result<i64>
    where
        E: sqlx::Executor<'e, Database = DbDriver>,
    {
        let mut qb = select_fields!("topics", ("COALESCE(MAX(topic_index), 0)"));
        match parent_id {
            Some(parent_id) => qb.push(" WHERE parent_id = ").push_bind(parent_id),
            None => qb.push(" WHERE parent_id IS NULL "),
        };
        let row = qb.build().fetch_one(executor).await?;
        Ok(row.try_get(0).unwrap_or(0) + 10)
    }

    async fn batch_delete_by_ids<'e, E>(
        executor: E,
        table: &str,
        column: &str,
        ids: &[i64],
    ) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = DbDriver>,
    {
        if ids.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new(format!("DELETE FROM {} WHERE {} IN (", table, column));
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        builder.build().execute(executor).await?;

        Ok(())
    }

    pub async fn create(&self, data: CreateTopic) -> Result<i64> {
        let mut tx = self.db.begin().await?;

        let id = next_id();
        let next_index = Self::get_next_topic_index(&mut *tx, data.parent_id).await?;
        let mut qb = insert!(
            "topics",
            ("id", id),
            ("parent_id", data.parent_id),
            ("chat_config_id", data.chat_config_id),
            ("label", data.label),
            ("icon", data.icon),
            ("max_context", data.max_context.or_else(|| Some(999))),
            ("topic_index", next_index),
            (
                "mcp_server_ids",
                utils::vec_to_str_default(data.mcp_server_ids.as_deref())?
            )
        );
        qb.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn update(&self, id: i64, data: UpdateTopic) -> Result<()> {
        let mut qb = update!(
            "topics",
            id,
            ("parent_id", data.parent_id),
            ("label", data.label),
            ("icon", data.icon),
            ("max_context", data.max_context),
            (
                "auto_approves",
                utils::vec_to_str_optional(data.auto_approves.as_deref())?
            ),
            (
                "mcp_server_ids",
                utils::vec_to_str_optional(data.mcp_server_ids.as_deref())?
            ),
        );
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    /// 获取所有 topic
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let mut qb = select_fields!(
            "topics",
            (
                "id",
                "parent_id",
                "chat_config_id",
                "label",
                "icon",
                "max_context",
                "topic_index",
                "auto_approves",
                "mcp_server_ids",
                "created_at"
            )
        );
        qb.push(" ORDER BY topic_index ASC ");
        let rows = qb.build_query_as::<Topic>().fetch_all(&self.db).await?;

        Ok(rows)
    }

    /// 获取 topic
    pub async fn get_topic(&self, id: i64) -> Result<Option<Topic>> {
        let mut qb = get_by_id!(
            "topics",
            id,
            (
                "id",
                "parent_id",
                "chat_config_id",
                "label",
                "icon",
                "max_context",
                "topic_index",
                "auto_approves",
                "mcp_server_ids",
                "created_at"
            )
        );
        let row = qb
            .build_query_as::<Topic>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    pub async fn delete_topics(&self, ids: &[i64]) -> Result<()> {
        let mut tx = self.db.begin().await?;

        Self::batch_delete_by_ids(&mut *tx, "chat_configs", "topic_id", ids).await?;
        Self::batch_delete_by_ids(&mut *tx, "messages", "topic_id", ids).await?;
        Self::batch_delete_by_ids(&mut *tx, "topics", "id", ids).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_chat_config(&self, topic_id: i64, config: ReqConfig) -> Result<i64> {
        let id = next_id();
        insert!(
            "chat_configs",
            ("id", id),
            ("topic_id", topic_id),
            ("temperature", config.temperature),
            ("top_p", config.top_p),
            ("max_tokens", config.max_tokens),
            ("stream", config.stream),
            ("presence_penalty", config.presence_penalty),
            ("frequency_penalty", config.frequency_penalty),
            ("parallel_tool_calls", config.parallel_tool_calls),
            ("reasoning", config.reasoning)
        )
        .build()
        .execute(&self.db)
        .await?;

        Ok(id)
    }

    pub async fn update_chat_config(&self, topic_id: i64, config: ReqConfig) -> Result<()> {
        update_fields!(
            "chat_configs",
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
        .push(" WHERE topic_id =  ")
        .push_bind(topic_id)
        .build()
        .execute(&self.db)
        .await?;

        Ok(())
    }

    pub async fn get_chat_config(&self, topic_id: i64) -> Result<Option<ChatConfig>> {
        let row = select_fields!(
            "chat_configs",
            (
                "id",
                "topic_id",
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
        .push(" WHERE topic_id = ")
        .push_bind(topic_id)
        .build_query_as::<ChatConfig>()
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }
}
