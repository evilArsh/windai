use crate::error::{Result, StorageError};
use crate::utils::{conflict, is_unique_violation, not_found, now_ms};
use crate::{Mod, Storage};
use serde::Deserialize;

/// 清单中与存储记录比对的字段
#[derive(Deserialize)]
struct ManifestIdentity {
    name: String,
    version: u32,
}

fn manifest_identity(manifest: &str) -> Result<(String, u32)> {
    let identity: ManifestIdentity = serde_json::from_str(manifest)?;
    Ok((identity.name, identity.version))
}

impl Storage {
    /// 返回全部 topic 与各自启用的 mod 名字
    pub async fn all_enabled_mods(&self) -> Result<Vec<(i64, String)>> {
        let rows = self
            .executor
            .fetch_all(sqlx::query_as::<_, (i64, String)>(
                "SELECT tm.topic_id, m.name
                   FROM topic_mods tm
                   JOIN mods m ON m.id = tm.mod_id
                  ORDER BY tm.topic_id, m.name",
            ))
            .await?;
        Ok(rows)
    }

    /// 按名称获取 mod
    pub async fn mod_by_name(&self, mod_name: &str) -> Result<Option<Mod>> {
        let module = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, Mod>(
                    "SELECT id, name, version, manifest, created_at, updated_at
                       FROM mods
                      WHERE name = ?",
                )
                .bind(mod_name),
            )
            .await?;
        Ok(module)
    }

    /// 全部 mod，按名称升序
    pub async fn list_mods(&self) -> Result<Vec<Mod>> {
        let modules = self
            .executor
            .fetch_all(sqlx::query_as::<_, Mod>(
                "SELECT id, name, version, manifest, created_at, updated_at
                   FROM mods
                  ORDER BY name",
            ))
            .await?;
        Ok(modules)
    }

    /// 建立 mod 并写入版本 1，返回新 id
    pub async fn install_mod(&self, name: &str, manifest: &str) -> Result<i64> {
        let (manifest_name, manifest_version) = manifest_identity(manifest)?;
        if manifest_name != name || manifest_version != 1 {
            return Err(conflict(
                "mod",
                name,
                "the initial manifest must name the mod and declare version 1",
            ));
        }
        let now = now_ms();
        let inserted = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO mods
                        (name, version, created_at, updated_at, manifest)
                     VALUES (?, 1, ?, ?, ?)
                     RETURNING id",
                )
                .bind(name)
                .bind(now)
                .bind(now)
                .bind(manifest),
            )
            .await;
        match inserted {
            Ok(id) => Ok(id),
            Err(error) if is_unique_violation(&error) => {
                Err(conflict("mod", name, "the name is already installed"))
            }
            Err(error) => Err(error),
        }
    }

    /// 用新版本替换当前记录
    pub async fn update_mod(
        &self,
        mod_id: i64,
        expected_version: u32,
        version: u32,
        manifest: &str,
    ) -> Result<()> {
        self.with_tx(async move |storage| {
            let current = storage
                .executor
                .fetch_optional(
                    sqlx::query_as::<_, (String, i64)>(
                        "SELECT name, version FROM mods WHERE id = ?",
                    )
                    .bind(mod_id),
                )
                .await?
                .ok_or_else(|| not_found("mod", mod_id))?;
            let (stored_name, stored_version) = current;
            let current =
                u32::try_from(stored_version).map_err(|_| StorageError::MalformedColumn {
                    column: "version".into(),
                    detail: format!("{stored_version} is outside the u32 range"),
                })?;
            if version == 0 {
                return Err(conflict("mod", mod_id, "a version must be at least 1"));
            }
            let (manifest_name, manifest_version) = manifest_identity(manifest)?;
            if manifest_version != version {
                return Err(conflict(
                    "mod",
                    mod_id,
                    format!("the manifest version {manifest_version} does not match {version}"),
                ));
            }
            if stored_name != manifest_name {
                return Err(conflict(
                    "mod",
                    mod_id,
                    format!("the manifest name {manifest_name} does not match {stored_name}"),
                ));
            }
            if current != expected_version {
                return Err(conflict(
                    "mod",
                    mod_id,
                    format!("the current version is {current}, not {expected_version}"),
                ));
            }
            if version <= current {
                return Err(conflict(
                    "mod",
                    mod_id,
                    format!("version {version} must be greater than the current {current}"),
                ));
            }
            let updated = storage
                .executor
                .execute(
                    sqlx::query(
                        "UPDATE mods
                            SET version = ?, manifest = ?, updated_at = ?
                          WHERE id = ? AND version = ?",
                    )
                    .bind(i64::from(version))
                    .bind(manifest)
                    .bind(now_ms())
                    .bind(mod_id)
                    .bind(i64::from(expected_version)),
                )
                .await?;
            if updated.rows_affected() != 1 {
                return Err(conflict(
                    "mod",
                    mod_id,
                    format!("the current version is no longer {expected_version}"),
                ));
            }
            Ok(())
        })
        .await
    }

    /// 话题启用的 mod，按 mod 名升序
    pub async fn enabled_mods(&self, topic_id: i64) -> Result<Vec<Mod>> {
        let rows = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, Mod>(
                    "SELECT m.id, m.name, m.version, m.manifest,
                            m.created_at, m.updated_at
                       FROM topic_mods tm
                       JOIN mods m ON m.id = tm.mod_id
                      WHERE tm.topic_id = ?
                      ORDER BY m.name",
                )
                .bind(topic_id),
            )
            .await?;
        Ok(rows)
    }

    /// 列出启用该 mod 的 topic，按 id 升序
    pub async fn topics_with_mod(&self, mod_id: i64) -> Result<Vec<i64>> {
        let rows = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, (i64,)>(
                    "SELECT topic_id
                       FROM topic_mods
                      WHERE mod_id = ?
                      ORDER BY topic_id",
                )
                .bind(mod_id),
            )
            .await?;
        Ok(rows.into_iter().map(|(topic_id,)| topic_id).collect())
    }

    /// 在话题中启用 mod，返回关系是否变化
    pub async fn enable_mod(&self, topic_id: i64, mod_id: i64) -> Result<bool> {
        self.with_tx(async move |storage| {
            storage.ensure_enablement_targets(topic_id, mod_id).await?;
            let inserted = storage
                .executor
                .execute(
                    sqlx::query(
                        "INSERT INTO topic_mods (topic_id, mod_id)
                         VALUES (?, ?)
                         ON CONFLICT (topic_id, mod_id) DO NOTHING",
                    )
                    .bind(topic_id)
                    .bind(mod_id),
                )
                .await?;
            Ok(inserted.rows_affected() == 1)
        })
        .await
    }

    /// 在话题中禁用 mod，返回关系是否变化
    pub async fn disable_mod(&self, topic_id: i64, mod_id: i64) -> Result<bool> {
        self.with_tx(async move |storage| {
            storage.ensure_enablement_targets(topic_id, mod_id).await?;
            let deleted = storage
                .executor
                .execute(
                    sqlx::query(
                        "DELETE FROM topic_mods
                          WHERE topic_id = ? AND mod_id = ?",
                    )
                    .bind(topic_id)
                    .bind(mod_id),
                )
                .await?;
            Ok(deleted.rows_affected() == 1)
        })
        .await
    }

    /// 删除 mod 及其启用关系，返回受影响的话题
    pub async fn delete_mod(&self, mod_id: i64) -> Result<Vec<i64>> {
        self.with_tx(async move |storage| {
            let topics: Vec<i64> = storage
                .executor
                .fetch_all(
                    sqlx::query_as::<_, (i64,)>(
                        "SELECT topic_id
                           FROM topic_mods
                          WHERE mod_id = ?
                          ORDER BY topic_id",
                    )
                    .bind(mod_id),
                )
                .await?
                .into_iter()
                .map(|(topic_id,)| topic_id)
                .collect();
            let deleted = storage
                .executor
                .execute(sqlx::query("DELETE FROM mods WHERE id = ?").bind(mod_id))
                .await?;
            if deleted.rows_affected() == 0 {
                return Err(not_found("mod", mod_id));
            }
            Ok(topics)
        })
        .await
    }

    async fn ensure_enablement_targets(&self, topic_id: i64, mod_id: i64) -> Result<()> {
        let exists = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, (bool, bool)>(
                    "SELECT EXISTS(SELECT 1 FROM topics WHERE id = ?),
                            EXISTS(SELECT 1 FROM mods WHERE id = ?)",
                )
                .bind(topic_id)
                .bind(mod_id),
            )
            .await?;
        let (topic_exists, mod_exists) = exists.unwrap_or((false, false));
        if !topic_exists {
            return Err(not_found("topic", topic_id));
        }
        if !mod_exists {
            return Err(not_found("mod", mod_id));
        }
        Ok(())
    }
}
