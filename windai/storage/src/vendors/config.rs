use crate::db::DbRow;
use crate::error::Result;
use crate::utils::now_ts;
use crate::{Model, Provider, Rule, Storage, Topic};
use serde_json::Value;
use sqlx::{FromRow, Row};
use wind_wire::AdapterType;

/// 模型及其提供商与启用凭证
#[derive(Debug, Clone, PartialEq)]
pub struct ModelConfig {
    pub model: Model,
    /// 提供商缺失时为空
    pub provider: Option<Provider>,
    pub api_key: Option<String>,
}

impl<'r> FromRow<'r, DbRow> for ModelConfig {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let model = Model::from_row(row)?;
        let provider_id: Option<i64> = row.try_get("provider_pk")?;
        let provider = match provider_id {
            Some(id) => Some(Provider {
                id,
                name: row.try_get("provider_name")?,
                alias: row.try_get("provider_alias")?,
                description: row.try_get("provider_description")?,
                base_url: row.try_get("provider_base_url")?,
                doc: row.try_get("provider_doc")?,
                active: row.try_get("provider_active")?,
                created_at: row.try_get("provider_created_at")?,
                updated_at: row.try_get("provider_updated_at")?,
            }),
            None => None,
        };
        Ok(Self {
            model,
            provider,
            api_key: row.try_get("api_key")?,
        })
    }
}

impl Storage {
    pub async fn create_topic(
        &self,
        parent_id: Option<i64>,
        label: &str,
        model_id: Option<i64>,
        icon: Option<&str>,
        workspace: Option<&str>,
    ) -> Result<i64> {
        self.with_tx(async move |storage| {
            let id = storage
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
            let workspace = workspace.map(str::to_owned).unwrap_or_else(|| {
                default_topics_root()
                    .join(id.to_string())
                    .to_string_lossy()
                    .into_owned()
            });
            storage
                .executor
                .execute(
                    sqlx::query("UPDATE topics SET workspace = ? WHERE id = ?")
                        .bind(&workspace)
                        .bind(id),
                )
                .await?;
            Ok(id)
        })
        .await
    }

    pub async fn get_topic_by_id(&self, topic_id: i64) -> Result<Option<Topic>> {
        let topic = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, Topic>(
                    "SELECT id, parent_id, label, model_id, icon, workspace
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
                    "SELECT t.id, t.parent_id, t.label, t.model_id, t.icon, t.workspace
                    FROM topics t
                    JOIN agent_instances i ON i.topic_id = t.id
                    WHERE i.id = ?",
                )
                .bind(instance_id),
            )
            .await?;
        Ok(topic)
    }

    /// 读取模型、其提供商与启用凭证
    pub async fn get_model_config(&self, model_id: i64) -> Result<Option<ModelConfig>> {
        let config = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, ModelConfig>(
                    "SELECT m.id, m.provider_id, m.name, m.alias, m.adapter,
                        m.modalities, m.active, m.icon, m.endpoint, m.config,
                        m.frequency, m.created_at, m.updated_at,
                        p.id AS provider_pk,
                        p.name AS provider_name,
                        p.alias AS provider_alias,
                        p.description AS provider_description,
                        p.base_url AS provider_base_url,
                        p.doc AS provider_doc,
                        p.active AS provider_active,
                        p.created_at AS provider_created_at,
                        p.updated_at AS provider_updated_at,
                        c.key AS api_key
                    FROM models m
                    LEFT JOIN providers p ON p.id = m.provider_id
                    LEFT JOIN credentials c ON c.id = (
                        SELECT id
                        FROM credentials
                        WHERE provider_id = p.id AND active = TRUE
                        ORDER BY id
                        LIMIT 1
                    )
                    WHERE m.id = ?",
                )
                .bind(model_id),
            )
            .await?;
        Ok(config)
    }

    pub async fn list_rules(&self, provider_id: i64, adapter: AdapterType) -> Result<Vec<Rule>> {
        let rules = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, Rule>(
                    "SELECT id, provider_id, adapter, json_rule, created_at, updated_at
                    FROM json_rule
                    WHERE provider_id = ? AND adapter = ?
                    ORDER BY id ASC",
                )
                .bind(provider_id)
                .bind(adapter.as_ref()),
            )
            .await?;
        Ok(rules)
    }

    pub async fn create_provider(&self, name: &str, base_url: &str) -> Result<i64> {
        let now = now_ts();
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO providers
                        (name, base_url, active, created_at, updated_at)
                    VALUES (?, ?, TRUE, ?, ?)
                    RETURNING id",
                )
                .bind(name)
                .bind(base_url)
                .bind(now)
                .bind(now),
            )
            .await?;
        Ok(id)
    }

    pub async fn create_model(
        &self,
        provider_id: i64,
        name: &str,
        adapter: AdapterType,
        endpoint: Option<&str>,
        config: Option<&Value>,
    ) -> Result<i64> {
        let config = config.map(serde_json::to_string).transpose()?;
        let now = now_ts();
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO models
                        (provider_id, name, adapter, active, endpoint, config, frequency,
                         created_at, updated_at)
                    VALUES (?, ?, ?, TRUE, ?, ?, 0, ?, ?)
                    RETURNING id",
                )
                .bind(provider_id)
                .bind(name)
                .bind(adapter.as_ref())
                .bind(endpoint)
                .bind(config)
                .bind(now)
                .bind(now),
            )
            .await?;
        Ok(id)
    }

    pub async fn create_credential(&self, provider_id: i64, key: &str) -> Result<i64> {
        let now = now_ts();
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO credentials
                        (provider_id, key, active, created_at, updated_at)
                    VALUES (?, ?, TRUE, ?, ?)
                    RETURNING id",
                )
                .bind(provider_id)
                .bind(key)
                .bind(now)
                .bind(now),
            )
            .await?;
        Ok(id)
    }

    pub async fn create_rule(
        &self,
        provider_id: i64,
        adapter: AdapterType,
        json_rule: &str,
    ) -> Result<i64> {
        let now = now_ts();
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO json_rule
                        (provider_id, adapter, json_rule, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    RETURNING id",
                )
                .bind(provider_id)
                .bind(adapter.as_ref())
                .bind(json_rule)
                .bind(now)
                .bind(now),
            )
            .await?;
        Ok(id)
    }
}

/// 话题工作目录的默认根
fn default_topics_root() -> std::path::PathBuf {
    let root = match std::env::var("WIND_ROOT_DIR") {
        Ok(value) => std::path::PathBuf::from(value),
        Err(_) => dirs::home_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join(".windai"),
    };
    root.join("topics")
}
