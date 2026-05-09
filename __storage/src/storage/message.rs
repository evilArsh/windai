use super::{Storage, StorageError, lock_db};
use crate::{api::response::ChatMessage, storage::utils::value_or_none};
use std::str::FromStr;
use windai_domain::{adaptor::AdaptorType, chat::Message};
fn row_to_message(row: &rusqlite::Row<'_>) -> Result<Message, rusqlite::Error> {
    let content_json: String = row.get(4)?;
    let content = serde_json::from_str(&content_json).unwrap_or_default();
    Ok(Message {
        id: row.get(0)?,
        from_id: row.get(1)?,
        role: row.get::<_, String>(2)?.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e))
        })?,
        raw_content: row.get(3)?,
        content,
        reasoning_content: row.get(5)?,
        transcript: row.get(6)?,
        model_id: row.get(7)?,
        topic_id: row.get(8)?,
        index: row.get(9)?,
        stream: row.get(10)?,
        is_boundary: row.get(11)?,
        input_tokens: row.get(12)?,
        output_tokens: row.get(13)?,
        created_at: row.get(14)?,
    })
}

impl Storage {
    pub fn create_message(&self, msg: &mut Message) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let max_index: i64 = conn.query_row(
            "SELECT COALESCE(MAX(index), 0) FROM messages WHERE topic_id = ?1",
            [msg.topic_id],
            |row| row.get(0),
        )?;
        let new_index = max_index + 10;
        let content_json = serde_json::to_string(&msg.content)?;
        let row_count = conn.execute(
            "INSERT INTO messages (from_id, role, raw_content, content, reasoning_content, transcript,
            model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            (
                msg.from_id,
                &msg.role.to_string(),
                &msg.raw_content,
                &content_json,
                &msg.reasoning_content,
                &msg.transcript,
                msg.model_id,
                msg.topic_id,
                new_index,
                msg.stream,
                msg.is_boundary,
                msg.input_tokens,
                msg.output_tokens,
                msg.created_at,
            ),
        )?;
        msg.id = conn.last_insert_rowid();
        msg.index = new_index;
        Ok(row_count)
    }
    /// 批量创建消息
    /// - 消息可能属于不同的 topic_id
    /// - index 值：按 topic_id 分组后，分别取当前 topic 下最大 index + 10 递增
    /// - 传入的 msg 中 id 和 index 值会被忽略，插入后 id 和 index 会被设置为新值
    pub fn create_messages(&self, mut msgs: Vec<&mut Message>) -> Result<(), StorageError> {
        if msgs.is_empty() {
            return Ok(());
        }
        let mut topic_groups: Vec<(i64, Vec<usize>)> = Vec::new();
        for (i, msg) in msgs.iter().enumerate() {
            if let Some((_, indices)) = topic_groups
                .iter_mut()
                .find(|(tid, _)| *tid == msg.topic_id)
            {
                indices.push(i);
            } else {
                topic_groups.push((msg.topic_id, vec![i]));
            }
        }
        let mut conn = lock_db!(&self);
        let tx = conn.transaction()?;
        for (topic_id, indices) in &topic_groups {
            let max_index: i64 = tx.query_row(
                "SELECT COALESCE(MAX(index), 0) FROM messages WHERE topic_id = ?1",
                [*topic_id],
                |row| row.get(0),
            )?;
            for (offset, msg_idx) in indices.iter().enumerate() {
                let msg = &mut msgs[*msg_idx];
                let new_index = max_index + 10 * (offset as i64 + 1);
                let content_json = serde_json::to_string(&msg.content)?;
                let _ = tx.execute(
                    "INSERT INTO messages (from_id, role, raw_content, content, reasoning_content, transcript,
                    model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    (
                        msg.from_id,
                        &msg.role.to_string(),
                        &msg.raw_content,
                        &content_json,
                        &msg.reasoning_content,
                        &msg.transcript,
                        msg.model_id,
                        msg.topic_id,
                        new_index,
                        msg.stream,
                        msg.is_boundary,
                        msg.input_tokens,
                        msg.output_tokens,
                        msg.created_at,
                    ),
                )?;
                msg.id = tx.last_insert_rowid();
                msg.index = new_index;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// 根据 id 查询消息
    pub fn get_message(&self, id: i64) -> Result<Option<Message>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, from_id, role, raw_content, content, reasoning_content, transcript, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at
            FROM messages WHERE id = ?1",
        )?;

        let result = stmt.query_row([id], row_to_message);
        value_or_none(result)
    }

    /// 根据 topic_id 查询该会话下的所有消息，按 index 排序
    pub fn list_messages_by_topic(&self, topic_id: i64) -> Result<Vec<Message>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, from_id, role, raw_content, content, reasoning_content, transcript, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at
            FROM messages WHERE topic_id = ?1 ORDER BY index ASC",
        )?;
        let msgs = stmt
            .query_map([topic_id], row_to_message)?
            .collect::<Result<Vec<Message>, rusqlite::Error>>()?; // 保证所有数据都正常才返回
        Ok(msgs)
    }

    /// 根据 topic_id 查询该会话下的所有消息，包括模型和提供商信息，按 index 排序。
    pub fn list_chat_messages_by_topic(
        &self,
        topic_id: i64,
    ) -> Result<Vec<ChatMessage>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT m.id, m.from_id, m.role, m.raw_content, m.content, m.reasoning_content, m.transcript,
                m.model_id, m.topic_id, m.index, m.stream, m.is_boundary,
                m.input_tokens, m.output_tokens, m.created_at,
                mo.adaptor, mo.name, p.name, p.id
            FROM messages m
            LEFT JOIN models mo ON m.model_id = mo.id
            LEFT JOIN providers p ON mo.provider_id = p.id
            WHERE m.topic_id = ?1 ORDER BY m.index ASC",
        )?;
        let msgs = stmt
            .query_map([topic_id], |row| {
                let adaptor: AdaptorType = AdaptorType::from_str(&row.get::<_, String>(15)?)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            15,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?;
                let content_json: String = row.get(4)?;
                let content = serde_json::from_str(&content_json).unwrap_or_default();
                Ok(ChatMessage {
                    base: Message {
                        id: row.get(0)?,
                        from_id: row.get(1)?,
                        role: row.get::<_, String>(2)?.parse().map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?,
                        raw_content: row.get(3)?,
                        content,
                        reasoning_content: row.get(5)?,
                        transcript: row.get(6)?,
                        model_id: row.get(7)?,
                        topic_id: row.get(8)?,
                        index: row.get(9)?,
                        stream: row.get(10)?,
                        is_boundary: row.get(11)?,
                        input_tokens: row.get(12)?,
                        output_tokens: row.get(13)?,
                        created_at: row.get(14)?,
                    },
                    model_name: row.get(16)?,
                    provider_name: row.get(17)?,
                    provider_id: row.get(18)?,
                    adaptor,
                })
            })?
            .collect::<Result<Vec<ChatMessage>, rusqlite::Error>>()?;
        Ok(msgs)
    }

    /// 查询所有消息
    pub fn list_all_messages(&self) -> Result<Vec<Message>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, from_id, role, raw_content, content, reasoning_content, transcript, model_id, topic_id, index, stream, is_boundary, input_tokens, output_tokens, created_at
            FROM messages ORDER BY created_at DESC",
        )?;
        let msgs = stmt
            .query_map([], row_to_message)?
            .collect::<Result<Vec<Message>, rusqlite::Error>>()?;
        Ok(msgs)
    }

    /// 更新消息
    pub fn update_message(&self, msg: &Message) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let content_json = serde_json::to_string(&msg.content)?;
        Ok(conn.execute(
            "UPDATE messages SET from_id = ?1, role = ?2, raw_content = ?3,
            content = ?4, reasoning_content = ?5, transcript = ?6, model_id = ?7, topic_id = ?8,
            index = ?9, stream = ?10, is_boundary = ?11, input_tokens = ?12, output_tokens = ?13,
            updated_at = strftime('%s', 'now') WHERE id = ?14",
            (
                msg.from_id,
                &msg.role.to_string(),
                &msg.raw_content,
                &content_json,
                &msg.reasoning_content,
                &msg.transcript,
                msg.model_id,
                msg.topic_id,
                msg.index,
                msg.stream,
                msg.is_boundary,
                msg.input_tokens,
                msg.output_tokens,
                msg.id,
            ),
        )?)
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
