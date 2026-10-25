//! 提供商结构规则的存储

use super::Storage;
use super::TableName;
use super::utils::{decode_adapter, execute_expecting_rows, now_ts};
use crate::db::{DbDriver, DbRow};
use crate::error::Result;
use crate::insert;
use crate::models::{AdapterType, RuleCreate, RuleRow, RuleUpdate};
use crate::select_fields;
use crate::update;
use sqlx::{FromRow, QueryBuilder, Row};

fn select_rules<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::JSON_RULE,
        ("id", "provider_id", "adapter", "json_rule")
    )
}

impl<'s> FromRow<'s, DbRow> for RuleRow {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        const ADAPTER: &str = "json_rule.adapter";
        let adapter: String = row.try_get("adapter")?;
        Ok(Self {
            id: row.try_get("id")?,
            provider_id: row.try_get("provider_id")?,
            adapter: decode_adapter(ADAPTER, &adapter)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            json_rule: row.try_get("json_rule")?,
        })
    }
}

impl Storage {
    /// 列出 provider 与适配器下启用的规则，按 id 升序
    pub async fn rule_list_active(
        &self,
        provider_id: i64,
        adapter: AdapterType,
    ) -> Result<Vec<RuleRow>> {
        self.executor
            .fetch_all(
                select_rules()
                    .push(" WHERE provider_id = ")
                    .push_bind(provider_id)
                    .push(" AND adapter = ")
                    .push_bind(adapter.to_string())
                    .push(" AND active = ")
                    .push_bind(true)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<RuleRow>(),
            )
            .await
    }

    /// 插入规则并返回新 id
    pub async fn rule_create(&self, input: RuleCreate) -> Result<i64> {
        let now = now_ts();
        let mut qb = insert!(
            TableName::JSON_RULE,
            ("provider_id", input.provider_id),
            ("adapter", input.adapter.to_string()),
            ("json_rule", input.json_rule),
            ("active", input.active),
            ("created_at", now),
            ("updated_at", now),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 按给出的字段更新规则，空更新直接返回
    pub async fn rule_update(&self, id: i64, update: RuleUpdate) -> Result<()> {
        if update.is_empty() {
            return Ok(());
        }
        execute_expecting_rows(
            &self.executor,
            update!(
                TableName::JSON_RULE,
                id,
                ("adapter", update.adapter.map(|adapter| adapter.to_string())),
                ("json_rule", update.json_rule),
                ("active", update.active),
            ),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::error::CoreError;
    use tempfile::TempDir;

    /// 一条设置字段的规则原文
    fn rule_json(path: &str) -> String {
        format!(r#"{{"rules":[{{"type":"set","path":"{path}","value":1}}]}}"#)
    }

    /// 一个已建表、位于临时目录的空库
    ///
    /// 返回的临时目录必须由调用方持有。临时目录一旦丢弃，数据库文件随之删除
    async fn storage() -> (TempDir, Storage) {
        let dir = TempDir::new().expect("create temp dir");
        let db = Db::open(&dir.path().join("t.db"))
            .await
            .expect("open database");
        db.init_schema().await.expect("create table");
        (dir, Storage::new(db))
    }

    #[tokio::test]
    async fn only_active_rules_of_that_provider_and_adapter_are_listed() {
        let (_dir, storage) = storage().await;
        let wanted = storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("stream").to_string(),
                active: true,
            })
            .await
            .expect("write");
        storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("temperature").to_string(),
                active: false,
            })
            .await
            .expect("write");
        storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAIResponse,
                json_rule: rule_json("reasoning").to_string(),
                active: true,
            })
            .await
            .expect("write");
        storage
            .rule_create(RuleCreate {
                provider_id: 2,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("top_p").to_string(),
                active: true,
            })
            .await
            .expect("write");

        let rows = storage
            .rule_list_active(1, AdapterType::OpenAICompletion)
            .await
            .expect("query");
        assert_eq!(
            rows.len(),
            1,
            "disabled rules, other adapters, and other providers are all excluded"
        );
        assert_eq!(rows[0].id, wanted);
        assert_eq!(rows[0].provider_id, 1);
        assert_eq!(rows[0].adapter, AdapterType::OpenAICompletion);
        assert_eq!(rows[0].json_rule, rule_json("stream"));
    }

    #[tokio::test]
    async fn the_list_is_ordered_by_id() {
        let (_dir, storage) = storage().await;
        let first = storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("a").to_string(),
                active: true,
            })
            .await
            .expect("write");
        let second = storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("b").to_string(),
                active: true,
            })
            .await
            .expect("write");

        let ids: Vec<i64> = storage
            .rule_list_active(1, AdapterType::OpenAICompletion)
            .await
            .expect("query")
            .into_iter()
            .map(|row| row.id)
            .collect();
        assert_eq!(
            ids,
            vec![first, second],
            "the order is the order in which rules take effect"
        );
    }

    #[tokio::test]
    async fn editing_a_rule_replaces_its_adapter_and_text() {
        let (_dir, storage) = storage().await;
        let id = storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("stream").to_string(),
                active: true,
            })
            .await
            .expect("write");

        storage
            .rule_update(
                id,
                RuleUpdate {
                    adapter: Some(AdapterType::OpenAIResponse),
                    json_rule: Some(rule_json("reasoning").to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("update");

        let rows = storage
            .rule_list_active(1, AdapterType::OpenAIResponse)
            .await
            .expect("query");
        assert_eq!(rows.len(), 1, "the rule moved to the new adapter");
        assert_eq!(rows[0].json_rule, rule_json("reasoning"));
    }

    #[tokio::test]
    async fn deactivating_a_rule_takes_it_out_of_the_list_and_back() {
        let (_dir, storage) = storage().await;
        let id = storage
            .rule_create(RuleCreate {
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: rule_json("stream").to_string(),
                active: true,
            })
            .await
            .expect("write");

        storage
            .rule_update(
                id,
                RuleUpdate {
                    active: Some(false),
                    ..Default::default()
                },
            )
            .await
            .expect("disable");
        let rows = storage
            .rule_list_active(1, AdapterType::OpenAICompletion)
            .await
            .expect("query");
        assert!(
            rows.is_empty(),
            "once disabled it no longer appears among active rules"
        );

        storage
            .rule_update(
                id,
                RuleUpdate {
                    active: Some(true),
                    ..Default::default()
                },
            )
            .await
            .expect("enable");
        let rows = storage
            .rule_list_active(1, AdapterType::OpenAICompletion)
            .await
            .expect("query");
        assert_eq!(rows.len(), 1, "it comes back after being enabled");
    }

    #[tokio::test]
    async fn switching_a_rule_that_does_not_exist_is_reported() {
        let (_dir, storage) = storage().await;
        let error = storage
            .rule_update(
                404,
                RuleUpdate {
                    active: Some(false),
                    ..Default::default()
                },
            )
            .await
            .expect_err("report a missing rule");
        assert!(matches!(error, CoreError::NoRowsAffected), "{error}");
    }
}
