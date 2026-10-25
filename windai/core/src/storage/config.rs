use super::Storage;
use super::TableName;
use super::utils::{decode_adapter, ensure_lte_one, execute_expecting_rows, now_ts};
use crate::db::{DbDriver, DbRow};
use crate::error::{CoreError, Result};
use crate::insert;
use crate::models::{
    AllowPolicy, CredentialUpdate, Credentials, Model, ModelCreate, ModelUpdate, Provider,
    ProviderCreate, ProviderUpdate, Topic, TopicCreate, TopicUpdate,
};
use crate::select_fields;
use crate::{delete_by_id, update};
use sqlx::{FromRow, QueryBuilder, Row};

fn select_topics<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::TOPICS,
        (
            "id",
            "parent_id",
            "label",
            "model_id",
            "tool_approval_policy",
            "icon"
        )
    )
}

fn select_providers<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::PROVIDERS,
        (
            "id",
            "name",
            "alias",
            "description",
            "base_url",
            "doc",
            "active"
        )
    )
}

fn select_credentials<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::CREDENTIALS,
        ("id", "provider_id", "key", "created_at", "active")
    )
}

fn select_models<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::MODELS,
        (
            "id",
            "provider_id",
            "name",
            "alias",
            "adapter",
            "modalities",
            "active",
            "icon",
            "endpoint",
            "config",
            "frequency"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for Topic {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        const COLUMN: &str = "topics.tool_approval_policy";
        let raw: String = row.try_get("tool_approval_policy")?;
        Ok(Self {
            id: row.try_get("id")?,
            parent_id: row.try_get("parent_id")?,
            label: row.try_get("label")?,
            model_id: row.try_get("model_id")?,
            tool_approval_policy: decode_policy(COLUMN, &raw)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            icon: row.try_get("icon")?,
        })
    }
}

impl<'s> FromRow<'s, DbRow> for Provider {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            alias: row.try_get("alias")?,
            description: row.try_get("description")?,
            base_url: row.try_get("base_url")?,
            doc: row.try_get("doc")?,
            active: row.try_get("active")?,
        })
    }
}

impl<'s> FromRow<'s, DbRow> for Credentials {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            provider_id: row.try_get("provider_id")?,
            key: row.try_get("key")?,
            created_at: row.try_get("created_at")?,
            active: row.try_get("active")?,
        })
    }
}

impl<'s> FromRow<'s, DbRow> for Model {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        const ADAPTER: &str = "models.adapter";
        const MODALITIES: &str = "models.modalities";
        const CONFIG: &str = "models.config";
        let adapter: String = row.try_get("adapter")?;
        let modalities: Option<String> = row.try_get("modalities")?;
        let config: Option<String> = row.try_get("config")?;
        Ok(Self {
            id: row.try_get("id")?,
            provider_id: row.try_get("provider_id")?,
            name: row.try_get("name")?,
            alias: row.try_get("alias")?,
            adapter: decode_adapter(ADAPTER, &adapter)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            modalities: decode_json(MODALITIES, modalities.as_deref())
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            active: row.try_get("active")?,
            icon: row.try_get("icon")?,
            endpoint: row.try_get("endpoint")?,
            config: decode_json(CONFIG, config.as_deref())
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            frequency: row.try_get("frequency")?,
        })
    }
}

impl Storage {
    /// 插入 topic 并返回新 id
    pub async fn topic_create(&self, input: TopicCreate) -> Result<i64> {
        let policy = serde_json::to_string(&input.tool_approval_policy)?;
        let now = now_ts();
        let mut qb = insert!(
            TableName::TOPICS,
            ("parent_id", input.parent_id),
            ("label", input.label),
            ("model_id", input.model_id),
            ("tool_approval_policy", policy),
            ("icon", input.icon),
            ("created_at", now),
            ("updated_at", now),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 按给出的字段更新 topic，空更新直接返回
    pub async fn topic_update(&self, id: i64, update: TopicUpdate) -> Result<()> {
        if update.is_empty() {
            return Ok(());
        }
        let policy = update
            .tool_approval_policy
            .map(|policy| serde_json::to_string(&policy))
            .transpose()?;
        execute_expecting_rows(
            &self.executor,
            update!(
                TableName::TOPICS,
                id,
                ("parent_id", update.parent_id),
                ("label", update.label),
                ("model_id", update.model_id),
                ("tool_approval_policy", policy),
                ("icon", update.icon),
            ),
        )
        .await
    }

    /// 删除 topic，不存在时报错
    pub async fn topic_delete(&self, id: i64) -> Result<()> {
        execute_expecting_rows(&self.executor, delete_by_id!(TableName::TOPICS, id)).await
    }

    /// 按 id 取出 topic，不存在时为 None
    pub async fn topic_get(&self, id: i64) -> Result<Option<Topic>> {
        let rows = self
            .executor
            .fetch_all(
                select_topics()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<Topic>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出全部 topic，按 id 升序
    pub async fn topic_list(&self) -> Result<Vec<Topic>> {
        self.executor
            .fetch_all(
                select_topics()
                    .push(" ORDER BY id ASC")
                    .build_query_as::<Topic>(),
            )
            .await
    }

    /// 列出直接子 topic，按 id 升序
    pub async fn topic_children_of(&self, parent_id: i64) -> Result<Vec<Topic>> {
        self.executor
            .fetch_all(
                select_topics()
                    .push(" WHERE parent_id = ")
                    .push_bind(parent_id)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<Topic>(),
            )
            .await
    }

    /// 插入 provider 并返回新 id
    pub async fn provider_create(&self, input: ProviderCreate) -> Result<i64> {
        let now = now_ts();
        let mut qb = insert!(
            TableName::PROVIDERS,
            ("name", input.name),
            ("alias", input.alias),
            ("description", input.description),
            ("base_url", input.base_url),
            ("doc", input.doc),
            ("active", true),
            ("created_at", now),
            ("updated_at", now),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 按给出的字段更新 provider，空更新直接返回
    pub async fn provider_update(&self, id: i64, update: ProviderUpdate) -> Result<()> {
        if update.is_empty() {
            return Ok(());
        }
        execute_expecting_rows(
            &self.executor,
            update!(
                TableName::PROVIDERS,
                id,
                ("name", update.name),
                ("alias", update.alias),
                ("description", update.description),
                ("base_url", update.base_url),
                ("doc", update.doc),
                ("active", update.active),
            ),
        )
        .await
    }

    /// 删除 provider，不存在时报错
    pub async fn provider_delete(&self, id: i64) -> Result<()> {
        execute_expecting_rows(&self.executor, delete_by_id!(TableName::PROVIDERS, id)).await
    }

    /// 按 id 取出 provider，不存在时为 None
    pub async fn provider_get(&self, id: i64) -> Result<Option<Provider>> {
        let rows = self
            .executor
            .fetch_all(
                select_providers()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<Provider>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出全部 provider，按 id 升序
    pub async fn provider_list(&self) -> Result<Vec<Provider>> {
        self.executor
            .fetch_all(
                select_providers()
                    .push(" ORDER BY id ASC")
                    .build_query_as::<Provider>(),
            )
            .await
    }

    /// 插入凭据并返回新 id
    pub async fn credential_create(&self, provider_id: i64, secret: &str) -> Result<i64> {
        let now = now_ts();
        let mut qb = insert!(
            TableName::CREDENTIALS,
            ("provider_id", provider_id),
            ("key", secret),
            ("active", true),
            ("created_at", now),
            ("updated_at", now),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 按给出的字段更新凭据，空更新直接返回
    pub async fn credential_update(&self, id: i64, update: CredentialUpdate) -> Result<()> {
        if update.is_empty() {
            return Ok(());
        }
        execute_expecting_rows(
            &self.executor,
            update!(TableName::CREDENTIALS, id, ("active", update.active)),
        )
        .await
    }

    /// 删除凭据，不存在时报错
    pub async fn credential_delete(&self, id: i64) -> Result<()> {
        execute_expecting_rows(&self.executor, delete_by_id!(TableName::CREDENTIALS, id)).await
    }

    /// 按 id 取出凭据，不存在时为 None
    pub async fn credential_get(&self, id: i64) -> Result<Option<Credentials>> {
        let rows = self
            .executor
            .fetch_all(
                select_credentials()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<Credentials>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出 provider 的凭据，按 id 升序
    pub async fn credential_list_by_provider(&self, provider_id: i64) -> Result<Vec<Credentials>> {
        self.executor
            .fetch_all(
                select_credentials()
                    .push(" WHERE provider_id = ")
                    .push_bind(provider_id)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<Credentials>(),
            )
            .await
    }

    /// 插入模型并返回新 id
    pub async fn model_create(&self, input: ModelCreate) -> Result<i64> {
        let modalities = input.modalities.unwrap_or_else(|| serde_json::json!([]));
        let modalities = serde_json::to_string(&modalities)?;
        let config = input
            .config
            .map(|config| serde_json::to_string(&config))
            .transpose()?;
        let now = now_ts();
        let mut qb = insert!(
            TableName::MODELS,
            ("provider_id", input.provider_id),
            ("name", input.name),
            ("alias", input.alias),
            ("adapter", input.adapter.to_string()),
            ("modalities", modalities),
            ("active", true),
            ("icon", input.icon),
            ("endpoint", input.endpoint),
            ("config", config),
            ("frequency", 0_i64),
            ("created_at", now),
            ("updated_at", now),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 按给出的字段更新模型，空更新直接返回
    pub async fn model_update(&self, id: i64, update: ModelUpdate) -> Result<()> {
        if update.is_empty() {
            return Ok(());
        }
        let modalities = update
            .modalities
            .map(|modalities| serde_json::to_string(&modalities))
            .transpose()?;
        let config = update
            .config
            .map(|config| serde_json::to_string(&config))
            .transpose()?;
        execute_expecting_rows(
            &self.executor,
            update!(
                TableName::MODELS,
                id,
                ("provider_id", update.provider_id),
                ("name", update.name),
                ("alias", update.alias),
                ("adapter", update.adapter.map(|adapter| adapter.to_string())),
                ("modalities", modalities),
                ("active", update.active),
                ("icon", update.icon),
                ("endpoint", update.endpoint),
                ("config", config),
            ),
        )
        .await
    }

    /// 删除模型，不存在时报错
    pub async fn model_delete(&self, id: i64) -> Result<()> {
        execute_expecting_rows(&self.executor, delete_by_id!(TableName::MODELS, id)).await
    }

    /// 按 id 取出模型，不存在时为 None
    pub async fn model_get(&self, id: i64) -> Result<Option<Model>> {
        let rows = self
            .executor
            .fetch_all(
                select_models()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<Model>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出 provider 的模型，按 id 升序
    pub async fn model_list_by_provider(&self, provider_id: i64) -> Result<Vec<Model>> {
        self.executor
            .fetch_all(
                select_models()
                    .push(" WHERE provider_id = ")
                    .push_bind(provider_id)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<Model>(),
            )
            .await
    }
}

fn decode_json(column: &str, text: Option<&str>) -> Result<Option<serde_json::Value>> {
    text.map(|raw| {
        serde_json::from_str(raw).map_err(|e| CoreError::MalformedColumn {
            column: column.into(),
            detail: e.to_string(),
        })
    })
    .transpose()
}

fn decode_policy(column: &str, text: &str) -> Result<AllowPolicy> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| CoreError::MalformedColumn {
            column: column.into(),
            detail: e.to_string(),
        })?;
    let items = value
        .get("allow")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CoreError::MalformedColumn {
            column: column.into(),
            detail: "expected an object holding an `allow` array".into(),
        })?;
    let mut allow = Vec::with_capacity(items.len());
    for item in items {
        let name = item.as_str().ok_or_else(|| CoreError::MalformedColumn {
            column: column.into(),
            detail: "every entry of `allow` must be a string".into(),
        })?;
        allow.push(name.to_string());
    }
    Ok(AllowPolicy { allow })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个密钥为 `sk-secret` 的凭证
    fn credential() -> Credentials {
        Credentials {
            id: 1,
            provider_id: 2,
            key: "sk-secret".into(),
            created_at: 0,
            active: true,
        }
    }

    #[test]
    fn a_credential_never_prints_its_secret() {
        let credential = credential();

        let debug = format!("{credential:?}");
        assert!(
            !debug.contains("sk-secret"),
            "Debug leaked the secret: {debug}"
        );
        assert!(
            debug.contains("*********"),
            "Debug should mask with asterisks by length: {debug}"
        );

        let display = format!("{credential}");
        assert!(
            !display.contains("sk-secret"),
            "Display leaked the secret: {display}"
        );

        let json = serde_json::to_string(&credential).expect("serialize");
        assert!(
            !json.contains("sk-secret"),
            "Serialize leaked the secret: {json}"
        );
        assert!(
            json.contains("*********"),
            "Serialize should mask with asterisks by length: {json}"
        );

        assert_eq!(
            credential.key, "sk-secret",
            "the type still holds the original inside"
        );
    }

    #[test]
    fn a_deserialized_credential_holds_a_mask_not_the_secret() {
        // 载荷读回时密钥是掩码，事件无法用载荷改写密钥
        let json = r#"{"id":1,"provider_id":2,"key":"*********","created_at":0,"active":true}"#;
        let credential: Credentials = serde_json::from_str(json).expect("deserialize");
        assert_eq!(credential.key, "*********");
    }
}
