use crate::error::{Result, StorageError};
use crate::utils::{conflict, is_unique_violation, not_found, now_ms};
use crate::{McpServerRecord, Storage};
use wind_wire::McpDefinition;

impl Storage {
    /// 新建独立配置，revision 从 1 开始
    pub async fn create_mcp_server(&self, definition: &McpDefinition) -> Result<McpServerRecord> {
        let config = serde_json::to_string(&definition.config)?;
        let now = now_ms();
        let inserted = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, McpServerRecord>(
                    "INSERT INTO mcp_servers
                        (name, description, config, revision, created_at, updated_at)
                     VALUES (?, ?, ?, 1, ?, ?)
                     RETURNING id, name, description, config, revision, created_at, updated_at",
                )
                .bind(&definition.name)
                .bind(definition.description.as_deref())
                .bind(&config)
                .bind(now)
                .bind(now),
            )
            .await;
        match inserted {
            Ok(Some(record)) => Ok(record),
            Ok(None) => Err(StorageError::Internal("insert returned no row".into())),
            Err(error) if is_unique_violation(&error) => Err(conflict(
                "mcp server",
                &definition.name,
                "the name is already configured",
            )),
            Err(error) => Err(error),
        }
    }

    /// 用新的 revision 替换配置，旧 revision 冲突时不覆盖
    pub async fn update_mcp_server(
        &self,
        id: i64,
        expected_revision: u32,
        definition: &McpDefinition,
    ) -> Result<McpServerRecord> {
        self.with_tx(async move |storage| {
            let config = serde_json::to_string(&definition.config)?;
            let updated = storage
                .executor
                .fetch_optional(
                    sqlx::query_as::<_, McpServerRecord>(
                        "UPDATE mcp_servers
                            SET name = ?, description = ?, config = ?,
                                revision = revision + 1, updated_at = ?
                          WHERE id = ? AND revision = ?
                          RETURNING id, name, description, config, revision, created_at, updated_at",
                    )
                    .bind(&definition.name)
                    .bind(definition.description.as_deref())
                    .bind(&config)
                    .bind(now_ms())
                    .bind(id)
                    .bind(i64::from(expected_revision)),
                )
                .await;
            match updated {
                Ok(Some(record)) => Ok(record),
                Ok(None) => {
                    if storage.mcp_server(id).await?.is_some() {
                        Err(conflict(
                            "mcp server",
                            id,
                            format!("revision {expected_revision} is stale"),
                        ))
                    } else {
                        Err(not_found("mcp server", id))
                    }
                }
                Err(error) if is_unique_violation(&error) => Err(conflict(
                    "mcp server",
                    &definition.name,
                    "the name is already configured",
                )),
                Err(error) => Err(error),
            }
        })
        .await
    }

    /// 读取一条独立配置
    pub async fn mcp_server(&self, id: i64) -> Result<Option<McpServerRecord>> {
        let record = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, McpServerRecord>(
                    "SELECT id, name, description, config, revision, created_at, updated_at
                       FROM mcp_servers
                      WHERE id = ?",
                )
                .bind(id),
            )
            .await?;
        Ok(record)
    }

    /// 全部独立配置，按 id 升序
    pub async fn list_mcp_servers(&self) -> Result<Vec<McpServerRecord>> {
        let records = self
            .executor
            .fetch_all(sqlx::query_as::<_, McpServerRecord>(
                "SELECT id, name, description, config, revision, created_at, updated_at
                   FROM mcp_servers
                  ORDER BY id",
            ))
            .await?;
        Ok(records)
    }

    /// 在话题中启用独立配置，返回关系是否变化
    pub async fn enable_mcp_server(&self, topic_id: i64, id: i64) -> Result<bool> {
        self.with_tx(async move |storage| {
            storage.ensure_mcp_targets(topic_id, id).await?;
            let inserted = storage
                .executor
                .execute(
                    sqlx::query(
                        "INSERT INTO topic_mcp_servers (topic_id, server_id)
                         VALUES (?, ?)
                         ON CONFLICT (topic_id, server_id) DO NOTHING",
                    )
                    .bind(topic_id)
                    .bind(id),
                )
                .await?;
            Ok(inserted.rows_affected() == 1)
        })
        .await
    }

    /// 在话题中禁用独立配置，返回关系是否变化
    pub async fn disable_mcp_server(&self, topic_id: i64, id: i64) -> Result<bool> {
        self.with_tx(async move |storage| {
            storage.ensure_mcp_targets(topic_id, id).await?;
            let deleted = storage
                .executor
                .execute(
                    sqlx::query(
                        "DELETE FROM topic_mcp_servers
                          WHERE topic_id = ? AND server_id = ?",
                    )
                    .bind(topic_id)
                    .bind(id),
                )
                .await?;
            Ok(deleted.rows_affected() == 1)
        })
        .await
    }

    /// 话题启用的独立配置，按配置 id 升序
    pub async fn enabled_mcp_servers(&self, topic_id: i64) -> Result<Vec<McpServerRecord>> {
        let records = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, McpServerRecord>(
                    "SELECT s.id, s.name, s.description, s.config, s.revision,
                            s.created_at, s.updated_at
                       FROM topic_mcp_servers ts
                       JOIN mcp_servers s ON s.id = ts.server_id
                      WHERE ts.topic_id = ?
                      ORDER BY s.id",
                )
                .bind(topic_id),
            )
            .await?;
        Ok(records)
    }

    /// 列出启用该配置的 topic，按 id 升序
    pub async fn topics_with_mcp_server(&self, id: i64) -> Result<Vec<i64>> {
        let rows = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, (i64,)>(
                    "SELECT topic_id
                       FROM topic_mcp_servers
                      WHERE server_id = ?
                      ORDER BY topic_id",
                )
                .bind(id),
            )
            .await?;
        Ok(rows.into_iter().map(|(topic_id,)| topic_id).collect())
    }

    /// 删除独立配置与启用关系，返回受影响的话题
    pub async fn delete_mcp_server(&self, id: i64) -> Result<Vec<i64>> {
        self.with_tx(async move |storage| {
            let topics: Vec<i64> = storage
                .executor
                .fetch_all(
                    sqlx::query_as::<_, (i64,)>(
                        "SELECT topic_id
                           FROM topic_mcp_servers
                          WHERE server_id = ?
                          ORDER BY topic_id",
                    )
                    .bind(id),
                )
                .await?
                .into_iter()
                .map(|(topic_id,)| topic_id)
                .collect();
            let deleted = storage
                .executor
                .execute(sqlx::query("DELETE FROM mcp_servers WHERE id = ?").bind(id))
                .await?;
            if deleted.rows_affected() == 0 {
                return Err(not_found("mcp server", id));
            }
            Ok(topics)
        })
        .await
    }

    async fn ensure_mcp_targets(&self, topic_id: i64, id: i64) -> Result<()> {
        let exists = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, (bool, bool)>(
                    "SELECT EXISTS(SELECT 1 FROM topics WHERE id = ?),
                            EXISTS(SELECT 1 FROM mcp_servers WHERE id = ?)",
                )
                .bind(topic_id)
                .bind(id),
            )
            .await?;
        let (topic_exists, server_exists) = exists.unwrap_or((false, false));
        if !topic_exists {
            return Err(not_found("topic", topic_id));
        }
        if !server_exists {
            return Err(not_found("mcp server", id));
        }
        Ok(())
    }
}
