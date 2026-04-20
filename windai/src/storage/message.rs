use crate::{
    dto::chat::MessageResponse,
    models::{chat::Message, model::AdaptorType},
    storage::{Storage, StorageError, lock_db},
};
use std::str::FromStr;

fn row_to_message(row: &rusqlite::Row<'_>) -> Result<Message, rusqlite::Error> {
    Ok(Message {
        id: row.get(0)?,
        from_id: row.get(1)?,
        role: row.get(2)?,
        raw_content: row.get(3)?,
        content: row.get(4)?,
        reasoning_content: row.get(5)?,
        transcript: row.get(6)?,
        content_type: row.get::<_, String>(7)?.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e))
        })?,
        model_id: row.get(8)?,
        topic_id: row.get(9)?,
        index: row.get(10)?,
        stream: row.get(11)?,
        is_boundary: row.get(12)?,
        input_tokens: row.get(13)?,
        output_tokens: row.get(14)?,
        created_at: row.get(15)?,
    })
}

impl Storage {
    /// 创建消息
    /// index 自动取当前 topic 下最大 index + 10，首次插入时为 10
    pub fn create_message(&self, msg: &Message) -> Result<i64, StorageError> {
        let conn = lock_db!(&self);
        let max_index: i64 = conn.query_row(
            "SELECT COALESCE(MAX(index), 0) FROM messages WHERE topic_id = ?1",
            [msg.topic_id],
            |row| row.get(0),
        )?;
        let new_index = max_index + 10;
        let row_count = conn.execute(
            "INSERT INTO messages (from_id, role, raw_content, content, reasoning_content, transcript, content_type, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            (
                msg.from_id,
                &msg.role,
                &msg.raw_content,
                &msg.content,
                &msg.reasoning_content,
                &msg.transcript,
                msg.content_type.to_string(),
                msg.model_id,
                msg.topic_id,
                new_index,
                msg.stream,
                msg.is_boundary,
                msg.input_tokens,
                msg.output_tokens,
            ),
        )?;
        if row_count == 0 {
            return Err(StorageError::Internal("failed to insert message".into()));
        }
        Ok(conn.last_insert_rowid())
    }

    /// 根据 id 查询消息
    pub fn get_message(&self, id: i64) -> Result<Option<Message>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, from_id, role, raw_content, content, reasoning_content, transcript, content_type, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at
            FROM messages WHERE id = ?1",
        )?;
        let msg = stmt.query_row([id], row_to_message).ok();
        Ok(msg)
    }

    /// 根据 topic_id 查询该会话下的所有消息，按 index 排序
    pub fn list_messages_by_topic(&self, topic_id: i64) -> Result<Vec<Message>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, from_id, role, raw_content, content, reasoning_content, content_type, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at
            FROM messages WHERE topic_id = ?1 ORDER BY index ASC",
        )?;
        let msgs = stmt
            .query_map([topic_id], row_to_message)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(msgs)
    }

    /// 根据 topic_id 查询该会话下的所有消息，包括模型和提供商信息，按 index 排序。
    pub fn list_chat_messages_by_topic(
        &self,
        topic_id: i64,
    ) -> Result<Vec<MessageResponse>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT m.id, m.from_id, m.role, m.raw_content, m.content, m.reasoning_content, m.transcript,
                m.content_type, m.model_id, m.topic_id, m.index, m.stream, m.is_boundary,
                m.input_tokens, m.output_tokens, m.created_at,
                mo.adaptor, mo.name, p.name, p.id
            FROM messages m
            LEFT JOIN models mo ON m.model_id = mo.id
            LEFT JOIN providers p ON mo.provider_id = p.id
            WHERE m.topic_id = ?1 ORDER BY m.index ASC",
        )?;
        let msgs = stmt
            .query_map([topic_id], |row| {
                let adaptor: AdaptorType = AdaptorType::from_str(&row.get::<_, String>(16)?)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            16,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?;
                Ok(MessageResponse {
                    base: Message {
                        id: row.get(0)?,
                        from_id: row.get(1)?,
                        role: row.get(2)?,
                        raw_content: row.get(3)?,
                        content: row.get(4)?,
                        reasoning_content: row.get(5)?,
                        transcript: row.get(6)?,
                        content_type: row.get::<_, String>(7)?.parse().map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                7,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?,
                        model_id: row.get(8)?,
                        topic_id: row.get(9)?,
                        index: row.get(10)?,
                        stream: row.get(11)?,
                        is_boundary: row.get(12)?,
                        input_tokens: row.get(13)?,
                        output_tokens: row.get(14)?,
                        created_at: row.get(15)?,
                    },
                    model_name: row.get(17)?,
                    provider_name: row.get(18)?,
                    provider_id: row.get(19)?,
                    adaptor,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(msgs)
    }

    /// 查询所有消息
    pub fn list_all_messages(&self) -> Result<Vec<Message>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, from_id, role, raw_content, content, reasoning_content, content_type, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at
            FROM messages ORDER BY created_at DESC",
        )?;
        let msgs = stmt
            .query_map([], row_to_message)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(msgs)
    }

    /// 更新消息
    pub fn update_message(&self, msg: &Message) -> Result<(), StorageError> {
        let conn = lock_db!(&self);
        conn.execute(
            "UPDATE messages SET from_id = ?1, role = ?2, raw_content = ?3,
            content = ?4, reasoning_content = ?5, transcript = ?6, content_type = ?7, model_id = ?8, topic_id = ?9,
            index = ?10, stream = ?11, is_boundary = ?12, input_tokens = ?13, output_tokens = ?14,
            updated_at = strftime('%s', 'now') WHERE id = ?15",
            (
                msg.from_id,
                &msg.role,
                &msg.raw_content,
                &msg.content,
                &msg.reasoning_content,
                &msg.transcript,
                msg.content_type.to_string(),
                msg.model_id,
                msg.topic_id,
                msg.index,
                msg.stream,
                msg.is_boundary,
                msg.input_tokens,
                msg.output_tokens,
                msg.id,
            ),
        )?;
        Ok(())
    }

    /// 根据 id 删除消息
    pub fn delete_message(&self, id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute("DELETE FROM messages WHERE id = ?1", [id])?)
    }

    /// 根据 topic_id 删除该会话下的所有消息
    pub fn delete_messages_by_topic(&self, topic_id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute("DELETE FROM messages WHERE topic_id = ?1", [topic_id])?)
    }
}
