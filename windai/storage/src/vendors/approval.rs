use crate::error::Result;
use crate::utils::now_ts;
use crate::{ExternalWait, Storage};
use serde_json::Value;

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

    pub async fn get_external_wait(&self, id: i64) -> Result<Option<ExternalWait>> {
        let wait = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, ExternalWait>(
                    "SELECT id, instance_id, message_id, payload_json, created_at,
                            answered_at, decision_json
                       FROM external_waits
                      WHERE id = ?",
                )
                .bind(id),
            )
            .await?;
        Ok(wait)
    }

    /// 记录一次等待的答复
    ///
    /// 只有尚未答复的等待会被更新，返回是否由本次调用写入
    pub async fn answer_external_wait(&self, id: i64, decisions: Value) -> Result<bool> {
        let decisions = serde_json::to_string(&decisions)?;
        let answered = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, (i64,)>(
                    "UPDATE external_waits
                        SET answered_at = ?, decision_json = ?
                      WHERE id = ? AND answered_at IS NULL
                      AND decision_json IS NULL
                    RETURNING id",
                )
                .bind(now_ts())
                .bind(decisions)
                .bind(id),
            )
            .await?;
        Ok(answered.is_some())
    }
}
