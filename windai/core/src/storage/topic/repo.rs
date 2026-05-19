use crate::models::Topic;
use crate::{error::Result, models::ChatConfig};
use sqlx::{Row, SqlitePool, Transaction};
use wind_ai::message::ReqConfig;

pub struct TopicRepo {
    pub(crate) db: SqlitePool,
}

impl TopicRepo {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    pub async fn get_next_topic_index(&self, topic_id: Option<i64>) -> Result<i64> {
        let row =
            sqlx::query(r#"SELECT COALESCE(MAX(topic_index), 0) FROM topics WHERE id = ?"#)
                .bind(topic_id)
                .fetch_one(&self.db)
                .await?;

        Ok(row.try_get(0).unwrap_or(0) + 10)
    }

    pub async fn create_topic(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        parent_id: Option<i64>,
        chat_config_id: i64,
        label: &str,
        icon: Option<&str>,
        max_context: i32,
        topic_index: i64,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"INSERT INTO topics 
            (parent_id, chat_config_id, label, icon, max_context, topic_index)
            VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind(parent_id)
        .bind(chat_config_id)
        .bind(label)
        .bind(icon)
        .bind(max_context)
        .bind(topic_index)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }

    pub async fn update_topic(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
        parent_id: Option<i64>,
        label: &str,
        icon: Option<&str>,
        max_context: Option<i32>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE topics SET
            parent_id = ?, label = ?, icon = ?, max_context = ?
            WHERE id = ?"#,
        )
        .bind(parent_id)
        .bind(label)
        .bind(icon)
        .bind(max_context)
        .bind(id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn get_topic(&self, id: i64) -> Result<Option<Topic>> {
        let row = sqlx::query(
            r#"SELECT
            id, parent_id, chat_config_id, label, icon, max_context, topic_index, created_at
            FROM topics WHERE id = ?"#,
        )
        .bind(id)
        .map(|row: sqlx::sqlite::SqliteRow| Topic {
            id: row.get(0),
            parent_id: row.get(1),
            chat_config_id: row.get(2),
            label: row.get(3),
            icon: row.get(4),
            max_context: row.get(5),
            index: row.get(6),
            created_at: row.get(7),
        })
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }

    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let rows = sqlx::query(
            r#"SELECT
            id, parent_id, chat_config_id, label, icon, max_context, topic_index, created_at
            FROM topics ORDER BY topic_index ASC"#,
        )
        .map(|row: sqlx::sqlite::SqliteRow| Topic {
            id: row.get(0),
            parent_id: row.get(1),
            chat_config_id: row.get(2),
            label: row.get(3),
            icon: row.get(4),
            max_context: row.get(5),
            index: row.get(6),
            created_at: row.get(7),
        })
        .fetch_all(&self.db)
        .await?;

        Ok(rows)
    }

    async fn batch_delete_by_ids(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        table: &str,
        column: &str,
        ids: &[i64],
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "DELETE FROM {} WHERE {} IN ({})",
            table, column, placeholders
        );
        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }
        query.execute(&mut **tx).await?;

        Ok(())
    }

    pub async fn delete_topics(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        ids: &[i64],
    ) -> Result<()> {
        self.batch_delete_by_ids(tx, "topic_mcp_servers", "topic_id", ids)
            .await?;
        self.batch_delete_by_ids(tx, "chat_configs", "topic_id", ids)
            .await?;
        self.batch_delete_by_ids(tx, "messages", "topic_id", ids)
            .await?;
        self.batch_delete_by_ids(tx, "topics", "id", ids).await?;

        Ok(())
    }
    // --- MCP server bindings ---

    pub async fn set_mcp_servers(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        topic_id: i64,
        server_ids: &[i64],
    ) -> Result<()> {
        let existing: Vec<i64> = sqlx::query(
            "SELECT server_id FROM topic_mcp_servers WHERE topic_id = ?",
        )
        .bind(topic_id)
        .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
        .fetch_all(&mut **tx)
        .await?;

        let to_remove: Vec<i64> = existing
            .iter()
            .filter(|id| !server_ids.contains(id))
            .copied()
            .collect();
        let to_add: Vec<i64> = server_ids
            .iter()
            .filter(|id| !existing.contains(id))
            .copied()
            .collect();

        if !to_remove.is_empty() {
            let placeholders = to_remove.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "DELETE FROM topic_mcp_servers WHERE topic_id = ? AND server_id IN ({})",
                placeholders
            );
            let mut query = sqlx::query(&sql).bind(topic_id);
            for id in &to_remove {
                query = query.bind(id);
            }
            query.execute(&mut **tx).await?;
        }

        if !to_add.is_empty() {
            let placeholders: Vec<String> = to_add
                .iter()
                .map(|_| format!("({}, ?)", topic_id))
                .collect();
            let sql = format!(
                "INSERT INTO topic_mcp_servers (topic_id, server_id) VALUES {}",
                placeholders.join(", ")
            );
            let mut query = sqlx::query(&sql);
            for id in &to_add {
                query = query.bind(id);
            }
            query.execute(&mut **tx).await?;
        }

        Ok(())
    }

    pub async fn list_mcp_servers(&self, topic_id: i64) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT server_id FROM topic_mcp_servers WHERE topic_id = ?")
            .bind(topic_id)
            .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
            .fetch_all(&self.db)
            .await?;

        Ok(rows)
    }

    // --- Chat Config bindings ---

    pub async fn create_chat_config(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        topic_id: i64,
        chat_config: &ReqConfig,
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO chat_configs
            (topic_id, temperature, top_p, max_tokens, stream, presence_penalty, frequency_penalty, parallel_tool_calls, reasoning)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic_id) DO UPDATE SET
            temperature = excluded.temperature,
            top_p = excluded.top_p,
            max_tokens = excluded.max_tokens,
            stream = excluded.stream,
            presence_penalty = excluded.presence_penalty,
            frequency_penalty = excluded.frequency_penalty,
            parallel_tool_calls = excluded.parallel_tool_calls,
            reasoning = excluded.reasoning"#,
        )
        .bind(topic_id)
        .bind(chat_config.temperature)
        .bind(chat_config.top_p)
        .bind(chat_config.max_tokens)
        .bind(chat_config.stream)
        .bind(chat_config.presence_penalty)
        .bind(chat_config.frequency_penalty)
        .bind(chat_config.parallel_tool_calls)
        .bind(chat_config.reasoning)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn get_chat_config(&self, topic_id: i64) -> Result<Option<ChatConfig>> {
        let row = sqlx::query(
            r#"SELECT
            id, topic_id, temperature, top_p, max_tokens, stream, presence_penalty, frequency_penalty, parallel_tool_calls, reasoning,
            created_at
            FROM chat_configs WHERE topic_id = ?"#,
        )
        .bind(topic_id)
        .map(|row:sqlx::sqlite::SqliteRow| {
            ChatConfig{
                id:row.get(0),
                topic_id: row.get(1),
                data:ReqConfig {
                    temperature: row.get(2),
                    top_p: row.get(3),
                    max_tokens: row.get(4),
                    stream: row.get(5),
                    presence_penalty: row.get(6),
                    frequency_penalty: row.get(7),
                    parallel_tool_calls: row.get(8),
                    reasoning: row.get(9),
                },
                created_at: row.get(10)
            }
        })
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }
}
