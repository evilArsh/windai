use super::repo::TopicRepo;
use crate::db;
use crate::error::{CoreError, Result};
use crate::models::{ChatConfig, CreateTopic, McpServerParam, Topic, UpdateTopic};
use sqlx::SqlitePool;
use wind_ai::message::ReqConfig;

pub struct TopicService {
    repo: TopicRepo,
}

impl TopicService {
    pub fn new(db: SqlitePool) -> Self {
        Self {
            repo: TopicRepo::new(db),
        }
    }

    /// 创建一个 topic
    pub async fn create_topic(&self, data: CreateTopic) -> Result<Topic> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        let next_index = self.repo.get_next_topic_index(data.parent_id).await?;
        let id = self
            .repo
            .create_topic(
                &mut tx,
                data.parent_id,
                data.chat_config_id,
                &data.label,
                data.icon.as_deref(),
                data.max_context.unwrap_or(999),
                next_index,
            )
            .await?;
        tx.commit().await?;

        self.repo
            .get_topic(id)
            .await?
            .ok_or(CoreError::NotFound("created topic".into()))
    }

    /// 获取所有 topic
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        self.repo.list_topics().await
    }

    /// 获取 topic
    pub async fn get_topic(&self, id: i64) -> Result<Option<Topic>> {
        self.repo.get_topic(id).await
    }

    /// 更新 topic
    pub async fn update_topic(&self, id: i64, data: UpdateTopic) -> Result<()> {
        let current = self
            .get_topic(id)
            .await?
            .ok_or(CoreError::NotFound(format!("topic {id}")))?;

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update_topic(
                &mut tx,
                id,
                data.parent_id.or(current.parent_id),
                data.label.as_deref().unwrap_or(&current.label),
                data.icon.as_deref().or(current.icon.as_deref()),
                data.max_context.or(current.max_context),
            )
            .await?;
        tx.commit().await?;

        Ok(())
    }

    /// 删除 topic
    pub async fn delete_topics(&self, ids: &[i64]) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete_topics(&mut tx, ids).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn set_mcp_servers(&self, topic_id: i64, server_ids: Vec<i64>) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .set_mcp_servers(&mut tx, topic_id, &server_ids)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 查询指定 topic 下关联的 MCP 服务信息
    /// - 每个 topic 有独立的 MCP 服务列表引用，这些服务列表可动态变更，并且共享全局 MCP 服务
    pub async fn list_mcp_servers(&self, topic_id: i64) -> Result<Vec<McpServerParam>> {
        self.repo.list_mcp_servers(topic_id).await
    }

    pub async fn create_chat_config(&self, topic_id: i64, config: ReqConfig) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .create_chat_config(&mut tx, topic_id, &config)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_chat_config(&self, topic_id: i64) -> Result<Option<ChatConfig>> {
        self.repo.get_chat_config(topic_id).await
    }
}
