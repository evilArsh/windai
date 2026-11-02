use serde_json::Value;

use crate::Storage;
use crate::error::Result;
use crate::utils::{ensure_affected, now_ts};

impl Storage {
    pub async fn create_external_wait(
        &self,
        instance_id: i64,
        message_id: i64,
        payload: Value,
    ) -> Result<i64> {
        let payload = serde_json::to_string(&payload)?;
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO external_waits
                        (instance_id, message_id, payload_json, created_at)
                     VALUES (?, ?, ?, ?)
                     RETURNING id",
                )
                .bind(instance_id)
                .bind(message_id)
                .bind(payload)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
    }

    pub async fn answer_external_wait(&self, id: i64, decisions: Value) -> Result<()> {
        let decisions = serde_json::to_string(&decisions)?;
        let result = self
            .executor
            .execute(
                sqlx::query(
                    "UPDATE external_waits
                    SET answered_at = ?, decision_json = ?
                      WHERE id = ? AND answered_at IS NULL",
                )
                .bind(now_ts())
                .bind(decisions)
                .bind(id),
            )
            .await?;
        ensure_affected(result)
    }
}
