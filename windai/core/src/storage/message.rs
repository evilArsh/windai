use super::{
    executor::StorageExecutor,
    now_ts,
    utils::{self, ensure_affected},
};
use crate::{
    db::DbDriver,
    delete_by_id,
    error::Result,
    insert,
    models::{CreateMessage, Message, UpdateMessage},
    select_fields,
    storage::{TableName, next_id},
    update,
};
use sqlx::QueryBuilder;

#[derive(Clone)]
pub struct MessageStorage {
    executor: StorageExecutor,
}
impl MessageStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    fn select_common<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::MESSAGES,
            (
                "id",
                "from_id",
                "stream",
                "content",
                "model_id",
                "topic_id",
                "is_boundary",
                "is_excluded",
                "input_tokens",
                "output_tokens",
                "created_at"
            )
        )
    }
    // async fn update_is_excluded_inner(
    //     executor: &StorageExecutor,
    //     id: i64,
    //     is_excluded: bool,
    // ) -> Result<()> {
    //     let mut qb = update!(TableName::MESSAGES, id, ("is_excluded", Some(is_excluded)));
    //     ensure_affected(executor.execute(qb.build()).await?)
    // }

    /// 保存一条消息。
    pub async fn create(&self, data: CreateMessage) -> Result<Message> {
        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            TableName::MESSAGES,
            ("id", id),
            ("from_id", data.from_id),
            ("stream", data.stream),
            ("content", utils::vec_to_str_default(Some(&data.content))?),
            ("model_id", data.model_id),
            ("topic_id", data.topic_id),
            ("is_boundary", data.is_boundary),
            ("is_excluded", data.is_exclude),
            ("input_tokens", data.input_tokens),
            ("output_tokens", data.output_tokens),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;

        Ok(Message {
            id,
            from_id: data.from_id,
            stream: data.stream,
            content: data.content,
            model_id: data.model_id,
            topic_id: data.topic_id,
            is_boundary: data.is_boundary,
            is_excluded: data.is_exclude,
            input_tokens: data.input_tokens,
            output_tokens: data.output_tokens,
            created_at: now,
        })
    }

    /// 更新消息
    pub async fn update(&self, id: i64, data: UpdateMessage) -> Result<()> {
        let mut qb = update!(
            TableName::MESSAGES,
            id,
            (
                "content",
                utils::vec_to_str_optional(data.content.as_deref())?
            ),
            ("model_id", data.model_id),
            ("input_tokens", data.input_tokens),
            ("output_tokens", data.output_tokens),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn get(&self, id: i64) -> Result<Option<Message>> {
        let row = self
            .executor
            .fetch_optional(
                Self::select_common()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<Message>(),
            )
            .await?;
        Ok(row)
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!(TableName::MESSAGES, id);
        // TODO: 将用户消息或者助手消息is_excluded 设置为 1
        ensure_affected(self.executor.execute(qb.build()).await?)
    }
    /// 查询 topic_id 下所有的消息
    pub async fn list_by_topic(&self, topic_id: i64) -> Result<Vec<Message>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_common()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<Message>(),
            )
            .await?;

        Ok(rows)
    }

    /// 查询 topic_id 下所有消息
    ///
    /// 获取从最新一条含有 is_boundary = true 的消息开始往后所有的消息
    pub async fn list_contexts(&self, topic_id: i64) -> Result<Vec<Message>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_common()
                    .push(" WHERE topic_id = ")
                    .push_bind(topic_id)
                    .push(" AND id > COALESCE((SELECT MAX(id) FROM ")
                    .push(TableName::MESSAGES)
                    .push(" WHERE is_boundary = 1 AND topic_id = ")
                    .push_bind(topic_id)
                    .push("), 0)")
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<Message>(),
            )
            .await?;

        Ok(rows)
    }
}
