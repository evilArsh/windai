use crate::storage::utils::value_or_none;

use super::{Storage, StorageError, lock_db};
use wind_domain::chat::Topic;

fn row_to_topic(row: &rusqlite::Row<'_>) -> Result<Topic, rusqlite::Error> {
    Ok(Topic {
        id: row.get(0)?,
        parent_id: row.get(1)?,
        label: row.get(2)?,
        icon: row.get(3)?,
        created_at: row.get(4)?,
        max_context: row.get(5)?,
        index: row.get(6)?,
    })
}

impl Storage {
    /// 创建话题
    /// - 创建成功后将 id 设置到 topic 中
    pub fn create_topic(&self, topic: &mut Topic) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let row_count = conn.execute(
            "INSERT INTO topics (parent_id, label, icon, max_context, index)
            VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                topic.parent_id,
                &topic.label,
                &topic.icon,
                topic.max_context,
                topic.index,
            ),
        )?;
        topic.id = conn.last_insert_rowid();
        Ok(row_count)
    }

    /// 根据 id 查询话题
    pub fn get_topic(&self, id: i64) -> Result<Option<Topic>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, parent_id, label, icon, created_at, max_context, index
            FROM topics WHERE id = ?1",
        )?;
        value_or_none(stmt.query_row([id], row_to_topic))
    }

    /// 查询所有话题，按创建时间倒序
    pub fn list_all_topics(&self) -> Result<Vec<Topic>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, parent_id, label, icon, created_at, max_context, index
            FROM topics ORDER BY created_at DESC",
        )?;
        let topics = stmt
            .query_map([], row_to_topic)?
            .collect::<Result<Vec<Topic>, rusqlite::Error>>()?;
        Ok(topics)
    }

    /// 更新话题
    pub fn update_topic(&self, topic: &Topic) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute(
            "UPDATE topics SET parent_id = ?1, label = ?2, icon = ?3,
            max_context = ?4, index = ?5,
            updated_at = strftime('%s', 'now') WHERE id = ?6",
            (
                topic.parent_id,
                &topic.label,
                &topic.icon,
                topic.max_context,
                topic.index,
                topic.id,
            ),
        )?)
    }

    /// 根据 id 删除话题
    pub fn delete_topic(&self, id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute("DELETE FROM topics WHERE id = ?1", [id])?)
    }
}
