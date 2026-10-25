use super::Storage;
use super::TableName;
use super::utils::{
    de_str_to, ensure_lte_one, execute_allowing_zero_rows, execute_expecting_rows, now_ts,
};
use crate::db::{DbDriver, DbRow};
use crate::error::Result;
use crate::insert;
use crate::models::{CompiledScripts, Mod, ModStateEntry, ModVersion};
use crate::select_fields;
use crate::update;
use crate::{delete_by_id, delete_from};
use sqlx::{FromRow, QueryBuilder, Row};
use std::collections::BTreeMap;

fn select_mods<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::MODS,
        ("id", "name", "current_version", "created_at")
    )
}

impl<'s> FromRow<'s, DbRow> for Mod {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            current_version: row.try_get("current_version")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

fn select_versions<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::MOD_VERSIONS,
        (
            "mod_id",
            "version",
            "manifest",
            "compiled",
            "engine_fingerprint",
            "created_at"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for ModVersion {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        let raw: Option<String> = row.try_get("compiled")?;
        let compiled: Option<serde_json::Value> = match raw {
            Some(text) => {
                Some(de_str_to(&text).map_err(|error| sqlx::Error::Decode(Box::new(error)))?)
            }
            None => None,
        };
        Ok(Self {
            mod_id: row.try_get("mod_id")?,
            version: row.try_get("version")?,
            manifest: row.try_get("manifest")?,
            compiled,
            engine_fingerprint: row.try_get("engine_fingerprint")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

fn select_states<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::MOD_STATE,
        (
            "mod_id",
            "scope_kind",
            "scope_id",
            "kind",
            "key",
            "value_json",
            "updated_at"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for ModStateEntry {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        let raw: String = row.try_get("value_json")?;
        Ok(Self {
            mod_id: row.try_get("mod_id")?,
            scope_kind: row.try_get("scope_kind")?,
            scope_id: row.try_get("scope_id")?,
            kind: row.try_get("kind")?,
            key: row.try_get("key")?,
            value: de_str_to(&raw).map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

fn select_enabled<'a>() -> QueryBuilder<'a, DbDriver> {
    let mut qb = QueryBuilder::new("SELECT ");
    qb.push(TableName::TOPIC_MODS).push(".topic_id, ");
    qb.push(TableName::MODS).push(".name FROM ");
    qb.push(TableName::TOPIC_MODS)
        .push(" JOIN ")
        .push(TableName::MODS)
        .push(" ON ")
        .push(TableName::MODS)
        .push(".id = ")
        .push(TableName::TOPIC_MODS)
        .push(".mod_id");
    qb
}

#[derive(Debug)]
struct CurrentManifestRow {
    id: i64,
    manifest: String,
}

impl<'s> FromRow<'s, DbRow> for CurrentManifestRow {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            manifest: row.try_get("manifest")?,
        })
    }
}

#[derive(Debug)]
struct EnabledRow {
    topic_id: i64,
    name: String,
}

impl<'s> FromRow<'s, DbRow> for EnabledRow {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            topic_id: row.try_get("topic_id")?,
            name: row.try_get("name")?,
        })
    }
}

impl Storage {
    /// 建立 mod 并发布初始版本，返回新 id
    pub async fn mod_install(
        &self,
        name: &str,
        manifest: &str,
        compiled: Option<&CompiledScripts>,
    ) -> Result<i64> {
        self.with_tx(|storage| async move {
            let now = now_ts();
            let mut qb = insert!(
                TableName::MODS,
                ("name", name),
                ("current_version", 1),
                ("created_at", now),
                ("updated_at", now),
            );
            qb.push(" RETURNING id");
            let id: i64 = storage
                .executor
                .fetch_one_scalar(qb.build_query_scalar::<i64>())
                .await?;
            storage.mod_publish(id, 1, manifest, compiled).await?;
            Ok(id)
        })
        .await
    }

    /// 写入指定版本，编译产物随版本保存
    pub async fn mod_publish(
        &self,
        mod_id: i64,
        version: i64,
        manifest: &str,
        compiled: Option<&CompiledScripts>,
    ) -> Result<()> {
        let encoded = match compiled {
            Some(compiled) => Some(serde_json::to_string(&compiled.modules)?),
            None => None,
        };
        let fingerprint = compiled.map(|compiled| compiled.fingerprint.as_str());
        execute_expecting_rows(
            &self.executor,
            insert!(
                TableName::MOD_VERSIONS,
                ("mod_id", mod_id),
                ("version", version),
                ("manifest", manifest),
                ("compiled", encoded),
                ("engine_fingerprint", fingerprint),
                ("created_at", now_ts()),
            ),
        )
        .await
    }

    /// 发布新版本并把它设为当前版本
    pub async fn mod_publish_current(
        &self,
        mod_id: i64,
        version: i64,
        manifest: &str,
        compiled: Option<&CompiledScripts>,
    ) -> Result<()> {
        self.with_tx(|storage| async move {
            storage
                .mod_publish(mod_id, version, manifest, compiled)
                .await?;
            storage.mod_set_current_version(mod_id, version).await
        })
        .await
    }

    /// 按 mod 与版本号取出，不存在时为 None
    pub async fn mod_get_version(&self, mod_id: i64, version: i64) -> Result<Option<ModVersion>> {
        let rows = self
            .executor
            .fetch_all(
                select_versions()
                    .push(" WHERE mod_id = ")
                    .push_bind(mod_id)
                    .push(" AND version = ")
                    .push_bind(version)
                    .build_query_as::<ModVersion>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 切换 mod 的当前版本，mod 不存在时报错
    pub async fn mod_set_current_version(&self, mod_id: i64, version: i64) -> Result<()> {
        execute_expecting_rows(
            &self.executor,
            update!(TableName::MODS, mod_id, ("current_version", Some(version))),
        )
        .await
    }

    /// 按名字取出 mod，不存在时为 None
    pub async fn mod_get_by_name(&self, name: &str) -> Result<Option<Mod>> {
        let rows = self
            .executor
            .fetch_all(
                select_mods()
                    .push(" WHERE name = ")
                    .push_bind(name)
                    .build_query_as::<Mod>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出 mod 的版本，按版本号升序
    pub async fn mod_list_versions(&self, mod_id: i64) -> Result<Vec<ModVersion>> {
        self.executor
            .fetch_all(
                select_versions()
                    .push(" WHERE mod_id = ")
                    .push_bind(mod_id)
                    .push(" ORDER BY version ASC")
                    .build_query_as::<ModVersion>(),
            )
            .await
    }

    /// 返回每个 mod 当前版本的清单
    pub async fn mod_current_manifests(&self) -> Result<Vec<(i64, String)>> {
        let mut qb = QueryBuilder::<DbDriver>::new("SELECT ");
        qb.push(TableName::MODS)
            .push(".id AS id, ")
            .push(TableName::MOD_VERSIONS)
            .push(".manifest AS manifest FROM ")
            .push(TableName::MODS)
            .push(" JOIN ")
            .push(TableName::MOD_VERSIONS)
            .push(" ON ")
            .push(TableName::MOD_VERSIONS)
            .push(".mod_id = ")
            .push(TableName::MODS)
            .push(".id AND ")
            .push(TableName::MOD_VERSIONS)
            .push(".version = ")
            .push(TableName::MODS)
            .push(".current_version");
        let rows: Vec<CurrentManifestRow> = self
            .executor
            .fetch_all(qb.build_query_as::<CurrentManifestRow>())
            .await?;
        Ok(rows.into_iter().map(|row| (row.id, row.manifest)).collect())
    }

    /// 在 topic 启用 mod，重复启用不报错
    pub async fn mod_enable(&self, topic_id: i64, mod_id: i64) -> Result<()> {
        let mut qb = insert!(
            TableName::TOPIC_MODS,
            ("topic_id", topic_id),
            ("mod_id", mod_id),
        );
        qb.push(" ON CONFLICT (topic_id, mod_id) DO NOTHING");
        execute_allowing_zero_rows(&self.executor, qb).await
    }

    /// 在 topic 停用 mod，未启用时报错
    pub async fn mod_disable(&self, topic_id: i64, mod_id: i64) -> Result<()> {
        let mut qb = delete_from!(TableName::TOPIC_MODS);
        qb.push(" WHERE topic_id = ")
            .push_bind(topic_id)
            .push(" AND mod_id = ")
            .push_bind(mod_id);
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 列出 topic 启用的 mod 名字，按名字升序
    pub async fn mod_enabled_in(&self, topic_id: i64) -> Result<Vec<String>> {
        let rows = self
            .executor
            .fetch_all_rows(
                QueryBuilder::<DbDriver>::new("SELECT m.name AS name FROM ")
                    .push(TableName::TOPIC_MODS)
                    .push(" t JOIN ")
                    .push(TableName::MODS)
                    .push(" m ON m.id = t.mod_id WHERE t.topic_id = ")
                    .push_bind(topic_id)
                    .push(" ORDER BY m.name ASC")
                    .build(),
            )
            .await?;
        rows.into_iter()
            .map(|row| Ok(row.try_get("name")?))
            .collect()
    }

    /// 列出启用该 mod 的 topic，按 id 升序
    pub async fn mod_topics(&self, mod_id: i64) -> Result<Vec<i64>> {
        let rows = self
            .executor
            .fetch_all_rows(
                select_fields!(TableName::TOPIC_MODS, ("topic_id",))
                    .push(" WHERE mod_id = ")
                    .push_bind(mod_id)
                    .push(" ORDER BY topic_id ASC")
                    .build(),
            )
            .await?;
        rows.into_iter()
            .map(|row| Ok(row.try_get("topic_id")?))
            .collect()
    }

    /// 返回全部 topic 与各自启用的 mod 名字
    pub async fn mod_enabled_all(&self) -> Result<Vec<(i64, String)>> {
        let rows: Vec<EnabledRow> = self
            .executor
            .fetch_all(select_enabled().build_query_as::<EnabledRow>())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.topic_id, row.name))
            .collect())
    }

    /// 写入状态键值，已存在时覆盖
    pub async fn mod_put_state(
        &self,
        mod_id: i64,
        scope_kind: &str,
        scope_id: i64,
        kind: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        let encoded = serde_json::to_string(&value)?;
        let mut qb = insert!(
            TableName::MOD_STATE,
            ("mod_id", mod_id),
            ("scope_kind", scope_kind),
            ("scope_id", scope_id),
            ("kind", kind),
            ("key", key),
            ("value_json", encoded),
            ("updated_at", now_ts()),
        );
        qb.push(
            " ON CONFLICT (mod_id, scope_kind, scope_id, kind, key) \
             DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
        );
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 按作用域、类型与 key 取出状态，不存在时为 None
    pub async fn mod_get_state(
        &self,
        mod_id: i64,
        scope_kind: &str,
        scope_id: i64,
        kind: &str,
        key: &str,
    ) -> Result<Option<ModStateEntry>> {
        let rows = self
            .executor
            .fetch_all(
                select_states()
                    .push(" WHERE mod_id = ")
                    .push_bind(mod_id)
                    .push(" AND scope_kind = ")
                    .push_bind(scope_kind)
                    .push(" AND scope_id = ")
                    .push_bind(scope_id)
                    .push(" AND kind = ")
                    .push_bind(kind)
                    .push(" AND key = ")
                    .push_bind(key)
                    .build_query_as::<ModStateEntry>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 删除状态键值，不存在不报错
    pub async fn mod_delete_state(
        &self,
        mod_id: i64,
        scope_kind: &str,
        scope_id: i64,
        kind: &str,
        key: &str,
    ) -> Result<()> {
        let mut qb = delete_from!(TableName::MOD_STATE);
        qb.push(" WHERE mod_id = ")
            .push_bind(mod_id)
            .push(" AND scope_kind = ")
            .push_bind(scope_kind)
            .push(" AND scope_id = ")
            .push_bind(scope_id)
            .push(" AND kind = ")
            .push_bind(kind)
            .push(" AND key = ")
            .push_bind(key);
        execute_allowing_zero_rows(&self.executor, qb).await
    }

    /// 在单个事务内批量写入与删除状态
    pub async fn mod_write_state(
        &self,
        mod_id: i64,
        scope_kind: &str,
        scope_id: i64,
        upserts: &[(&str, String, serde_json::Value)],
        deletes: &[(&str, String)],
    ) -> Result<()> {
        if upserts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let encoded: Vec<String> = upserts
            .iter()
            .map(|(_, _, value)| serde_json::to_string(value))
            .collect::<std::result::Result<_, _>>()?;
        self.with_tx(|storage| async move {
            if !upserts.is_empty() {
                let mut qb = QueryBuilder::<DbDriver>::new("INSERT INTO ");
                qb.push(TableName::MOD_STATE);
                qb.push(" (mod_id, scope_kind, scope_id, kind, key, value_json, updated_at) ");
                let now = now_ts();
                qb.push_values(upserts.iter().zip(encoded), |mut row, ((kind, key, _), value)| {
                    row.push_bind(mod_id)
                        .push_bind(scope_kind)
                        .push_bind(scope_id)
                        .push_bind(*kind)
                        .push_bind(key.as_str())
                        .push_bind(value)
                        .push_bind(now);
                });
                qb.push(
                    " ON CONFLICT (mod_id, scope_kind, scope_id, kind, key) \
                     DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
                );
                execute_expecting_rows(&storage.executor, qb).await?;
            }
            let mut by_kind: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
            for (kind, key) in deletes {
                by_kind.entry(*kind).or_default().push(key.as_str());
            }
            for (kind, keys) in by_kind {
                let mut qb = delete_from!(TableName::MOD_STATE);
                qb.push(" WHERE mod_id = ")
                    .push_bind(mod_id)
                    .push(" AND scope_kind = ")
                    .push_bind(scope_kind)
                    .push(" AND scope_id = ")
                    .push_bind(scope_id)
                    .push(" AND kind = ")
                    .push_bind(kind)
                    .push(" AND key IN (");
                {
                    let mut sep = qb.separated(", ");
                    for key in keys {
                        sep.push_bind(key);
                    }
                }
                qb.push(")");
                execute_allowing_zero_rows(&storage.executor, qb).await?;
            }
            Ok(())
        })
        .await
    }

    /// 列出 mod 的全部状态，按作用域、类型与 key 排序
    pub async fn mod_list_state(&self, mod_id: i64) -> Result<Vec<ModStateEntry>> {
        self.executor
            .fetch_all(
                select_states()
                    .push(" WHERE mod_id = ")
                    .push_bind(mod_id)
                    .push(" ORDER BY scope_kind ASC, scope_id ASC, kind ASC, key ASC")
                    .build_query_as::<ModStateEntry>(),
            )
            .await
    }

    /// 按类型列出状态
    pub async fn mod_list_state_kind(&self, kind: &str) -> Result<Vec<ModStateEntry>> {
        self.executor
            .fetch_all(
                select_states()
                    .push(" WHERE kind = ")
                    .push_bind(kind)
                    .build_query_as::<ModStateEntry>(),
            )
            .await
    }

    /// 在单个事务内删除 mod 及其版本、启用关系与状态
    pub async fn mod_delete(&self, mod_id: i64) -> Result<()> {
        self.with_tx(|storage| async move {
            let mut state = delete_from!(TableName::MOD_STATE);
            state.push(" WHERE mod_id = ").push_bind(mod_id);
            execute_allowing_zero_rows(&storage.executor, state).await?;

            let mut versions = delete_from!(TableName::MOD_VERSIONS);
            versions.push(" WHERE mod_id = ").push_bind(mod_id);
            execute_allowing_zero_rows(&storage.executor, versions).await?;

            let mut enabled = delete_from!(TableName::TOPIC_MODS);
            enabled.push(" WHERE mod_id = ").push_bind(mod_id);
            execute_allowing_zero_rows(&storage.executor, enabled).await?;

            execute_expecting_rows(&storage.executor, delete_by_id!(TableName::MODS, mod_id)).await
        })
        .await
    }
}
