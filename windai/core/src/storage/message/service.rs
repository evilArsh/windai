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

    /// 保存一条消息。
    ///
    /// - 用户消息（from_id 为 None）：`is_excluded = true`
    /// - 助手消息（from_id 指向用户消息）：新消息和用户消息的 `is_excluded` 均为 `false`
    /// - from_id 指向的消息不存在或不是用户消息：返回错误
    pub async fn create(&self, data: CreateMessage) -> Result<Message> {
        let index = self.repo.get_next_index(data.topic_id).await?;

        let (is_excluded, user_msg_id) = match data.from_id {
            Some(from_id) => {
                let parent = self
                    .repo
                    .get(from_id)
                    .await?
                    .ok_or(CoreError::NotFound(format!("message {from_id}")))?;
                if parent.from_id.is_some() {
                    return Err(CoreError::Validation(
                        "from_id must reference a user message".into(),
                    ));
                }
                (false, Some(from_id))
            }
            None => (true, None),
        };

        let mut tx = db::begin_tx(&self.repo.db).await?;
        if let Some(uid) = user_msg_id {
            self.repo.update_is_excluded(&mut tx, uid, false).await?;
        }
        let id = self.repo.create(&mut tx, index, is_excluded, data).await?;
        tx.commit().await?;

        self.repo
            .get(id)
            .await?
            .ok_or(CoreError::NotFound("created message".into()))
    }

    /// 批量创建助手消息，所有消息的 from_id 必须一致且指向有效的用户消息，
    /// index 以当前最大 index 为基准逐一递增
    pub async fn batch_create_assistant(&self, data: Vec<CreateMessage>) -> Result<Vec<Message>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let from_id = data[0].from_id;
        if !data.iter().all(|m| m.from_id == from_id) {
            return Err(CoreError::Validation(
                "all messages must have the same from_id".into(),
            ));
        }

        let user_msg_id = from_id.ok_or(CoreError::Validation(
            "from_id is required for assistant messages".into(),
        ))?;
        let user_msg = self
            .repo
            .get(user_msg_id)
            .await?
            .ok_or(CoreError::NotFound(format!("user message {user_msg_id}")))?;
        if user_msg.from_id.is_some() {
            return Err(CoreError::Validation(
                "from_id must reference a user message".into(),
            ));
        }

        let base_index = self.repo.get_next_index(user_msg.topic_id).await?;
        let rows: Vec<(i64, CreateMessage)> = data
            .into_iter()
            .enumerate()
            .map(|(i, msg)| (base_index + i as i64, msg))
            .collect();

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update_is_excluded(&mut tx, user_msg_id, false)
            .await?;
        let ids = self.repo.batch_create(&mut tx, false, rows).await?;
        tx.commit().await?;
        Ok(self.repo.batch_get(&ids).await?)
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
                current.from_id,
                current.stream,
                data.content_json
                    .unwrap_or_else(|| {
                        serde_json::to_string(&current.content)
                            .unwrap_or_else(|_| String::from("[]"))
                    })
                    .as_str(),
                data.model_id.unwrap_or_else(|| current.model_id),
                current.topic_id,
                current.index,
                current.is_boundary,
                current.is_excluded,
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

    /// 批量查询消息
    pub async fn batch_get(&self, ids: &[i64]) -> Result<Vec<Message>> {
        self.repo.batch_get(ids).await
    }

    /// 查询 topic_id 下所有的消息
    pub async fn list_by_topic(&self, topic_id: i64) -> Result<Vec<Message>> {
        self.repo.list_by_topic(topic_id).await
    }

    /// 删除一条消息
    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
}
