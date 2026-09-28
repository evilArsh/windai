use super::{
    executor::StorageExecutor,
    utils::{self, batch_delete_in, now_ts},
};
use crate::{
    db::DbDriver,
    error::Result,
    insert,
    models::{CreateMessageContent, MessageContent},
    select_fields,
    storage::TableName,
};
use sqlx::QueryBuilder;

const IN_CHUNK: usize = 1000;

#[derive(Clone)]
pub struct MessageContentStorage {
    executor: StorageExecutor,
}
impl MessageContentStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    fn select_common<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::MESSAGE_CONTENTS,
            (
                "id",
                "message_id",
                "role",
                "content",
                "reasoning_content",
                "tool_calls",
                "input_tokens",
                "output_tokens",
                "created_at"
            )
        )
    }

    /// 追加一条内容块
    pub async fn create(&self, data: CreateMessageContent) -> Result<MessageContent> {
        let now = now_ts();
        let message_id = data.message_id;
        let mut qb = insert!(
            TableName::MESSAGE_CONTENTS,
            ("message_id", message_id),
            ("role", data.data.role.to_string()),
            (
                "content",
                utils::vec_to_str_default(Some(&data.data.content))?
            ),
            ("reasoning_content", data.data.reasoning_content.clone()),
            (
                "tool_calls",
                utils::map_to_str_optional(data.data.tool_calls.as_ref())?
            ),
            ("input_tokens", data.data.input_tokens),
            ("output_tokens", data.data.output_tokens),
            ("created_at", data.data.created_at),
            ("updated_at", now),
        );
        qb.push(" RETURNING id");
        let id: i64 = self
            .executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await?;

        Ok(MessageContent {
            id,
            message_id,
            data: data.data,
        })
    }

    /// 查询单条消息的全部内容，按 id 升序
    pub async fn list_by_message(&self, message_id: i64) -> Result<Vec<MessageContent>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_common()
                    .push(" WHERE message_id = ")
                    .push_bind(message_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<MessageContent>(),
            )
            .await?;
        Ok(rows)
    }

    /// 批量查询多条消息的内容
    ///
    /// 按 `message_id` 升序、组内 `id` 升序返回
    pub async fn list_by_messages(&self, message_ids: &[i64]) -> Result<Vec<MessageContent>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for chunk in message_ids.chunks(IN_CHUNK) {
            let mut qb = Self::select_common();
            qb.push(" WHERE message_id IN (");
            {
                let mut separated = qb.separated(", ");
                for id in chunk {
                    separated.push_bind(*id);
                }
            }
            qb.push(") ORDER BY message_id ASC, id ASC ");
            rows.extend(
                self.executor
                    .fetch_all(qb.build_query_as::<MessageContent>())
                    .await?,
            );
        }
        Ok(rows)
    }

    /// 删除指定消息的全部内容
    pub async fn delete_by_messages(&self, message_ids: &[i64]) -> Result<()> {
        batch_delete_in(
            &self.executor,
            TableName::MESSAGE_CONTENTS,
            "message_id",
            message_ids,
        )
        .await
    }

    /// 删除指定实例下全部消息的内容
    pub async fn delete_by_instances(&self, instance_ids: &[i64]) -> Result<()> {
        if instance_ids.is_empty() {
            return Ok(());
        }
        let mut qb: QueryBuilder<'_, DbDriver> = QueryBuilder::new("DELETE FROM ");
        qb.push(TableName::MESSAGE_CONTENTS)
            .push(" WHERE message_id IN (SELECT id FROM ")
            .push(TableName::MESSAGES)
            .push(" WHERE instance_id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in instance_ids {
                separated.push_bind(*id);
            }
        }
        qb.push("))");
        self.executor.execute(qb.build()).await?;
        Ok(())
    }
}
