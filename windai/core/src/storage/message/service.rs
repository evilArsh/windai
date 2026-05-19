use super::repo::MessageRepo;
use crate::db;
use crate::error::{CoreError, Result};
use crate::models::{CreateMessage, Message, UpdateMessage};
use sqlx::SqlitePool;

pub struct MessageService {
    repo: MessageRepo,
}

impl MessageService {
    pub fn new(db: SqlitePool) -> Self {
        Self {
            repo: MessageRepo::new(db),
        }
    }

    /// 保存一条消息
    pub async fn create(&self, data: CreateMessage) -> Result<Message> {
        let index = self.repo.get_next_index(data.topic_id).await?;

        let mut tx = db::begin_tx(&self.repo.db).await?;
        let id = self.repo.create(&mut tx, index, data).await?;
        tx.commit().await?;

        self.repo
            .get(id)
            .await?
            .ok_or(CoreError::NotFound("created provider".into()))
    }

    /// 更新消息
    pub async fn update(&self, id: i64, data: UpdateMessage) -> Result<()> {
        let current = self
            .get(id)
            .await?
            .ok_or(crate::error::CoreError::NotFound(format!("message {id}")))?;

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update(
                &mut tx,
                id,
                data.from_id.or(current.from_id),
                data.stream.unwrap_or_else(|| current.stream),
                data.content_json
                    .unwrap_or_else(|| {
                        serde_json::to_string(&current.content)
                            .unwrap_or_else(|_| String::from("[]"))
                    })
                    .as_str(),
                data.model_id.unwrap_or_else(|| current.model_id),
                data.topic_id.unwrap_or_else(|| current.topic_id),
                current.index,
                data.is_boundary.unwrap_or_else(|| current.is_boundary),
                data.is_excluded.unwrap_or_else(|| current.is_excluded),
                data.input_tokens.unwrap_or_else(|| current.input_tokens),
                data.output_tokens.unwrap_or_else(|| current.output_tokens),
            )
            .await?;
        tx.commit().await?;

        Ok(())
    }

    /// 使用消息 id 查询消息
    pub async fn get(&self, id: i64) -> Result<Option<Message>> {
        self.repo.get(id).await
    }

    /// 查询 topic_id 下所有的消息
    pub async fn list_by_topic(&self, topic_id: i64) -> Result<Vec<Message>> {
        self.repo.list_by_topic(topic_id).await
    }
}
