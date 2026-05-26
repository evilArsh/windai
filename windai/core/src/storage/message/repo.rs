use crate::models::Message;
use crate::{error::Result, models::CreateMessage};
use sqlx::{QueryBuilder, Row, SqlitePool, Transaction};

pub struct MessageRepo {
    pub(crate) db: SqlitePool,
}

impl MessageRepo {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    pub async fn create(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        message_index: i64,
        data: CreateMessage,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"INSERT INTO messages 
            (from_id, stream, content, model_id, topic_id, message_index, is_boundary, is_excluded, input_tokens, output_tokens)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(data.from_id)
        .bind(data.stream)
        .bind(data.content_json)
        .bind(data.model_id)
        .bind(data.topic_id)
        .bind(message_index)
        .bind(data.is_boundary)
        .bind(data.is_excluded)
        .bind(data.input_tokens)
        .bind(data.output_tokens)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }

    pub async fn update(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
        from_id: Option<i64>,
        stream: bool,
        content_json: &str,
        model_id: i64,
        topic_id: i64,
        message_index: i64,
        is_boundary: bool,
        is_excluded: bool,
        input_tokens: i32,
        output_tokens: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE messages SET
            from_id = ?, stream = ?, content = ?, model_id = ?, topic_id = ?, message_index = ?, is_boundary = ?, is_excluded = ?, input_tokens = ?, output_tokens = ?,
            updated_at = strftime('%s', 'now')
            WHERE id = ?"#,
        )
        .bind(from_id)
        .bind(stream)
        .bind(content_json)
        .bind(model_id)
        .bind(topic_id)
        .bind(message_index)
        .bind(is_boundary)
        .bind(is_excluded)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<Message>> {
        let row = sqlx::query(
            r#"SELECT
            id, from_id, stream, content, model_id, topic_id, message_index, is_boundary, is_excluded, input_tokens, output_tokens,
            created_at
            FROM messages WHERE id = ?"#,
        )
        .bind(id)
        .map(Self::row_to_message)
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }

    pub async fn batch_get(&self, ids: &[i64]) -> Result<Vec<Message>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let mut builder = QueryBuilder::new(
            "SELECT id, from_id, stream, content, model_id, topic_id, message_index, is_boundary, is_excluded, input_tokens, output_tokens, created_at FROM messages WHERE id IN (",
        );
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");

        let rows = builder
            .build()
            .map(Self::row_to_message)
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    pub async fn list_by_topic(&self, topic_id: i64) -> Result<Vec<Message>> {
        let rows = sqlx::query(
            r#"SELECT
            id, from_id, stream, content, model_id, topic_id, message_index, is_boundary, is_excluded, input_tokens, output_tokens,
            created_at
            FROM messages WHERE topic_id = ?
            ORDER BY message_index ASC"#,
        )
        .bind(topic_id)
        .map(Self::row_to_message)
        .fetch_all(&self.db)
        .await?;

        Ok(rows)
    }

    pub async fn delete(&self, tx: &mut Transaction<'_, sqlx::Sqlite>, id: i64) -> Result<()> {
        let msg = sqlx::query("SELECT id, from_id, message_index FROM messages WHERE id = ?")
            .bind(id)
            .map(|row: sqlx::sqlite::SqliteRow| {
                (
                    row.get::<i64, _>(0),
                    row.get::<Option<i64>, _>(1),
                    row.get::<i64, _>(2),
                )
            })
            .fetch_optional(&mut **tx)
            .await?;

        let (msg_id, from_id, msg_index) = match msg {
            Some(m) => m,
            None => return Ok(()),
        };

        match from_id {
            // 删除助手消息: 若该用户消息仅此一条助手回复，则排除用户消息；
            // 若有多条，则恢复下一条助手消息（索引大于当前消息的第一条）
            Some(user_msg_id) => {
                let count: i64 = sqlx::query("SELECT COUNT(*) FROM messages WHERE from_id = ?")
                    .bind(user_msg_id)
                    .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
                    .fetch_one(&mut **tx)
                    .await?;

                if count == 1 {
                    sqlx::query("UPDATE messages SET is_excluded = 1 WHERE id = ?")
                        .bind(user_msg_id)
                        .execute(&mut **tx)
                        .await?;
                } else {
                    let next_id: Option<i64> = sqlx::query(
                        "SELECT id FROM messages WHERE from_id = ? AND message_index > ? ORDER BY message_index ASC LIMIT 1",
                    )
                    .bind(user_msg_id)
                    .bind(msg_index)
                    .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
                    .fetch_optional(&mut **tx)
                    .await?;

                    if let Some(next_id) = next_id {
                        sqlx::query("UPDATE messages SET is_excluded = 0 WHERE id = ?")
                            .bind(next_id)
                            .execute(&mut **tx)
                            .await?;
                    }
                }
            }
            // 删除用户消息: 排除其所有助手回复
            None => {
                sqlx::query("UPDATE messages SET is_excluded = 1 WHERE from_id = ?")
                    .bind(msg_id)
                    .execute(&mut **tx)
                    .await?;
            }
        }

        sqlx::query("DELETE FROM messages WHERE id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    pub async fn batch_create(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        data: Vec<(i64, CreateMessage)>,
    ) -> Result<Vec<i64>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let mut builder = QueryBuilder::new(
            "INSERT INTO messages (from_id, stream, content, model_id, topic_id, message_index, is_boundary, is_excluded, input_tokens, output_tokens)",
        );
        builder.push_values(data.iter(), |mut b, item: &(i64, CreateMessage)| {
            b.push_bind(item.1.from_id);
            b.push_bind(item.1.stream);
            b.push_bind(&item.1.content_json);
            b.push_bind(item.1.model_id);
            b.push_bind(item.1.topic_id);
            b.push_bind(item.0);
            b.push_bind(item.1.is_boundary);
            b.push_bind(item.1.is_excluded);
            b.push_bind(item.1.input_tokens);
            b.push_bind(item.1.output_tokens);
        });
        let query = builder.build();
        let result = query.execute(&mut **tx).await?;
        let last_id = result.last_insert_rowid();
        let first_id = last_id - data.len() as i64 + 1;
        // FIXME: 需要表中的id为自增
        Ok((first_id..=last_id).collect())
    }

    pub async fn get_next_index(&self, topic_id: i64) -> Result<i64> {
        let row = sqlx::query(
            r#"SELECT COALESCE(MAX(message_index), 0) FROM messages WHERE topic_id = ?"#,
        )
        .bind(topic_id)
        .fetch_one(&self.db)
        .await?;

        Ok(row.try_get(0).unwrap_or(0) + 10)
    }

    fn row_to_message(row: sqlx::sqlite::SqliteRow) -> Message {
        Message {
            id: row.get(0),
            from_id: row.get(1),
            stream: row.get(2),
            content: serde_json::from_str(row.get(3)).unwrap_or_default(),
            model_id: row.get(4),
            topic_id: row.get(5),
            index: row.get(6),
            is_boundary: row.get(7),
            is_excluded: row.get(8),
            input_tokens: row.get(9),
            output_tokens: row.get(10),
            created_at: row.get(11),
        }
    }
}
