use super::{
    now_ts,
    utils::{self, ensure_affected},
};
use crate::{
    db::{DbDriver, DbPool, DbRow},
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert, insert_fields,
    models::{CreateMessage, Message, UpdateMessage},
    select_fields,
    storage::next_id,
    update,
};
use sqlx::{QueryBuilder, Row};

struct PreparedMessage {
    id: i64,
    message_index: i64,
    content_str: String,
    tools_allowed_str: String,
    tools_denied_str: String,
    is_excluded: bool,
    original: CreateMessage,
}

pub struct MessageStorage {
    db: DbPool,
}
impl MessageStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    fn select_common<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            "messages",
            (
                "id",
                "from_id",
                "stream",
                "content",
                "model_id",
                "topic_id",
                "message_index",
                "is_boundary",
                "is_excluded",
                "input_tokens",
                "output_tokens",
                "tools_allowed",
                "tools_denied",
                "created_at"
            )
        )
    }
    async fn update_is_excluded_inner<'e, E>(executor: E, id: i64, is_excluded: bool) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = DbDriver>,
    {
        let mut qb = update!("messages", id, ("is_excluded", Some(is_excluded)));
        qb.build().execute(executor).await?;
        Ok(())
    }

    async fn get_inner<'e, E>(executor: E, id: i64) -> Result<Option<Message>>
    where
        E: sqlx::Executor<'e, Database = DbDriver>,
    {
        let row = Self::select_common()
            .push(" WHERE id = ")
            .push_bind(id)
            .build_query_as::<Message>()
            .fetch_optional(executor)
            .await?;
        Ok(row)
    }

    async fn get_next_index<'e, E>(executor: E, topic_id: i64) -> Result<i64>
    where
        E: sqlx::Executor<'e, Database = DbDriver>,
    {
        let row = select_fields!("messages", ("COALESCE(MAX(message_index), 0)"))
            .push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .build()
            .fetch_one(executor)
            .await?;
        Ok(row.try_get(0).unwrap_or(0) + 10)
    }

    /// 保存一条消息。
    ///
    /// - 用户消息（from_id 为 None）：`is_excluded = true`
    /// - 助手消息（from_id 指向用户消息）：新消息和用户消息的 `is_excluded` 均为 `false`
    /// - from_id 指向的消息不存在或不是用户消息：返回错误
    pub async fn create(&self, data: CreateMessage) -> Result<i64> {
        let mut tx = self.db.begin().await?;

        let index = Self::get_next_index(&mut *tx, data.topic_id).await?;
        let (is_excluded, user_msg_id) = match data.from_id {
            Some(from_id) => {
                let parent = Self::get_inner(&mut *tx, from_id)
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
        if let Some(uid) = user_msg_id {
            Self::update_is_excluded_inner(&mut *tx, uid, false).await?;
        };
        let id = next_id();
        insert!(
            "messages",
            ("id", id),
            ("from_id", data.from_id),
            ("stream", data.stream),
            ("content", utils::vec_to_str_default(Some(&data.content))?),
            ("model_id", data.model_id),
            ("topic_id", data.topic_id),
            ("message_index", index),
            ("is_boundary", data.is_boundary),
            ("is_excluded", is_excluded),
            ("input_tokens", data.input_tokens),
            ("output_tokens", data.output_tokens),
            (
                "tools_allowed",
                utils::vec_to_str_default(data.tools_allowed.as_deref())?
            ),
            (
                "tools_denied",
                utils::vec_to_str_default(data.tools_denied.as_deref())?
            ),
        )
        .build()
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(id)
    }

    /// 更新消息
    pub async fn update(&self, id: i64, data: UpdateMessage) -> Result<()> {
        let mut qb = update!(
            "messages",
            id,
            (
                "content",
                utils::vec_to_str_optional(data.content.as_deref())?
            ),
            ("model_id", data.model_id),
            ("input_tokens", data.input_tokens),
            ("output_tokens", data.output_tokens),
            (
                "tools_allowed",
                utils::vec_to_str_optional(data.tools_allowed.as_deref())?
            ),
            (
                "tools_denied",
                utils::vec_to_str_optional(data.tools_denied.as_deref())?
            ),
        );
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<Message>> {
        Self::get_inner(&self.db, id).await
    }

    /// 查询 topic_id 下所有的消息
    pub async fn list_by_topic(&self, topic_id: i64) -> Result<Vec<Message>> {
        let rows = Self::select_common()
            .push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .push(" ORDER BY message_index ASC ")
            .build_query_as::<Message>() // 使用 build_query_as 触发 FromRow
            .fetch_all(&self.db)
            .await?;

        Ok(rows)
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut tx = self.db.begin().await?;

        let msg = get_by_id!("messages", id, ("id", "from_id", "message_index"))
            .build()
            .map(|row: DbRow| {
                (
                    row.get::<i64, _>("id"),
                    row.get::<Option<i64>, _>("from_id"),
                    row.get::<i64, _>("message_index"),
                )
            })
            .fetch_optional(&mut *tx)
            .await?;

        let (msg_id, from_id, msg_index) = match msg {
            Some(m) => m,
            None => return Ok(()),
        };

        match from_id {
            // 删除助手消息: 若该用户消息仅此一条助手回复，则排除用户消息；
            // 若有多条，则恢复下一条助手消息（索引大于当前消息的第一条）
            Some(user_msg_id) => {
                let count: i64 = select_fields!("messages", ("COUNT(*)"))
                    .push(" WHERE from_id = ")
                    .push_bind(user_msg_id)
                    .build()
                    .map(|row: DbRow| row.get(0))
                    .fetch_one(&mut *tx)
                    .await?;
                if count == 1 {
                    Self::update_is_excluded_inner(&mut *tx, user_msg_id, true).await?;
                } else {
                    let next_id = select_fields!("messages", ("id"))
                        .push(" WHERE from_id = ")
                        .push_bind(user_msg_id)
                        .push(" AND message_index > ")
                        .push_bind(msg_index)
                        .push(" ORDER BY message_index ASC LIMIT 1 ")
                        .build()
                        .map(|row: DbRow| row.get::<i64, _>("id"))
                        .fetch_optional(&mut *tx)
                        .await?;
                    if let Some(next_id) = next_id {
                        Self::update_is_excluded_inner(&mut *tx, next_id, false).await?;
                    }
                }
            }
            // 删除用户消息: 排除其所有助手回复
            None => {
                sqlx::query(
                    "UPDATE messages SET is_excluded = 1, updated_at = ? WHERE from_id = ?",
                )
                .bind(super::now_ts())
                .bind(msg_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        delete_by_id!("messages", id)
            .build()
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    /// 批量创建助手消息，所有消息的 from_id 必须一致且指向有效的用户消息，
    /// index 以当前最大 index 为基准逐一递增
    pub async fn batch_create_assistant(&self, data: Vec<CreateMessage>) -> Result<Vec<i64>> {
        let mut tx = self.db.begin().await?;

        let user_msg_id =
            data.iter()
                .filter_map(|c| c.from_id)
                .next()
                .ok_or(CoreError::Validation(
                    "need at least one message with from_id".into(),
                ))?;
        // 验证所有消息的 from_id 都相同
        if !data.iter().all(|m| m.from_id == Some(user_msg_id)) {
            return Err(CoreError::Validation(
                "all messages must have the same from_id".into(),
            ));
        }
        let user_msg = Self::get_inner(&mut *tx, user_msg_id)
            .await?
            .ok_or(CoreError::NotFound(format!(
                "user message id: {user_msg_id}"
            )))?;
        if user_msg.from_id.is_some() {
            return Err(CoreError::Validation(
                "from_id must reference a user message".into(),
            ));
        }
        Self::update_is_excluded_inner(&mut *tx, user_msg_id, false).await?;

        let base_index = Self::get_next_index(&mut *tx, user_msg.topic_id).await?;
        let mut prepared_rows: Vec<PreparedMessage> = Vec::with_capacity(data.len());
        for (index, msg) in data.into_iter().enumerate() {
            prepared_rows.push(PreparedMessage {
                id: next_id(),
                message_index: base_index + index as i64,
                content_str: utils::vec_to_str_default(Some(&msg.content))?,
                tools_allowed_str: utils::vec_to_str_default(msg.tools_allowed.as_deref())?,
                tools_denied_str: utils::vec_to_str_default(msg.tools_denied.as_deref())?,
                // 第一条默认作为消息上下文
                is_excluded: index == 0,
                original: msg,
            });
        }
        insert_fields!(
            "messages",
            (
                "id",
                "from_id",
                "stream",
                "content",
                "model_id",
                "topic_id",
                "message_index",
                "is_boundary",
                "is_excluded",
                "input_tokens",
                "output_tokens",
                "tools_allowed",
                "tools_denied",
                "created_at"
            )
        )
        .push_values(prepared_rows.iter(), |mut b, item: &PreparedMessage| {
            b.push_bind(item.id);
            b.push_bind(item.original.from_id);
            b.push_bind(item.original.stream);
            b.push_bind(&item.content_str);
            b.push_bind(item.original.model_id);
            b.push_bind(item.original.topic_id);
            b.push_bind(item.message_index);
            b.push_bind(false); // is_boundary
            b.push_bind(item.is_excluded);
            b.push_bind(item.original.input_tokens);
            b.push_bind(item.original.output_tokens);
            b.push_bind(&item.tools_allowed_str);
            b.push_bind(&item.tools_denied_str);
            b.push_bind(now_ts());
        })
        .build()
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(prepared_rows.iter().map(|d| d.id).collect())
    }

    pub async fn batch_get(&self, ids: &[i64]) -> Result<Vec<Message>> {
        if ids.is_empty() {
            return Err(CoreError::Validation("message ids are empty".into()));
        }
        let mut qb = Self::select_common();
        qb.push(" WHERE id IN ( ");
        let mut separated = qb.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");

        let rows = qb
            .build_query_as::<Message>() // 使用 build_query_as 触发 FromRow
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }
}
