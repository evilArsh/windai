use crate::models::Message;
use crate::{error::Result, models::CreateMessage};
use sqlx::{Row, SqlitePool, Transaction};

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

    pub async fn list_by_topic(&self, topic_id: i64) -> Result<Vec<Message>> {
        let rows = sqlx::query(
            r#"select
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
