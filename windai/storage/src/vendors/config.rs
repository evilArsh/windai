use crate::error::Result;
use crate::utils::now_ts;
use crate::{Storage, Topic};

impl Storage {
    pub async fn create_topic(
        &self,
        parent_id: Option<i64>,
        label: &str,
        model_id: Option<i64>,
        icon: Option<&str>,
    ) -> Result<i64> {
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO topics
                        (parent_id, label, model_id, icon, created_at)
                     VALUES (?, ?, ?, ?, ?)
                     RETURNING id",
                )
                .bind(parent_id)
                .bind(label)
                .bind(model_id)
                .bind(icon)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
    }

    pub async fn get_topic_by_id(&self, topic_id: i64) -> Result<Option<Topic>> {
        let topic = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, Topic>(
                    "SELECT id, parent_id, label, model_id, icon
                       FROM topics
                      WHERE id = ?",
                )
                .bind(topic_id),
            )
            .await?;
        Ok(topic)
    }

    pub async fn get_topic_by_instance(&self, instance_id: i64) -> Result<Option<Topic>> {
        let topic = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, Topic>(
                    "SELECT t.id, t.parent_id, t.label, t.model_id, t.icon
                       FROM topics t
                       JOIN agent_instances i ON i.topic_id = t.id
                      WHERE i.id = ?",
                )
                .bind(instance_id),
            )
            .await?;
        Ok(topic)
    }
}
