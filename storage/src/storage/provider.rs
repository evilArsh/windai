use crate::storage::utils::value_or_none;

use super::{Storage, StorageError, lock_db};
use windai_domain::provider::{Credentials, Provider};
fn row_to_provider(row: &rusqlite::Row<'_>) -> Result<Provider, rusqlite::Error> {
    Ok(Provider {
        id: row.get(0)?,
        name: row.get(1)?,
        alias: row.get(2)?,
        description: row.get(3)?,
        base_url: row.get(4)?,
        doc: row.get(5)?,
        active: row.get(6)?,
    })
}

fn row_to_credentials(row: &rusqlite::Row<'_>) -> Result<Credentials, rusqlite::Error> {
    Ok(Credentials {
        id: row.get(0)?,
        provider_id: row.get(1)?,
        key: row.get(2)?,
    })
}

impl Storage {
    /// 创建提供商
    /// - 创建成功后将 id 设置到 credentials 中
    pub fn create_provider(&self, provider: &mut Provider) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let row_count = conn.execute(
            "INSERT INTO providers (name, alias, description, base_url, doc, active)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &provider.name,
                &provider.alias,
                &provider.description,
                &provider.base_url,
                &provider.doc,
                provider.active,
            ),
        )?;
        provider.id = conn.last_insert_rowid();
        Ok(row_count)
    }

    /// 根据 id 查询提供商
    pub fn get_provider(&self, id: i64) -> Result<Option<Provider>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, description, base_url, doc, active
            FROM providers WHERE id = ?1",
        )?;
        value_or_none(stmt.query_row([id], row_to_provider))
    }

    /// 根据 name 查询提供商
    pub fn get_provider_by_name(&self, name: &str) -> Result<Option<Provider>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, description, base_url, doc, active
            FROM providers WHERE name = ?1",
        )?;
        value_or_none(stmt.query_row([name], row_to_provider))
    }

    /// 查询所有提供商
    pub fn list_all_providers(&self) -> Result<Vec<Provider>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, description, base_url, doc, active
            FROM providers ORDER BY created_at DESC",
        )?;
        let providers = stmt
            .query_map([], row_to_provider)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(providers)
    }

    /// 更新提供商
    pub fn update_provider(&self, provider: &Provider) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute(
            "UPDATE providers SET name = ?1, alias = ?2, description = ?3, base_url = ?4,
            doc = ?5, active = ?6, updated_at = strftime('%s', 'now') WHERE id = ?7",
            (
                &provider.name,
                &provider.alias,
                &provider.description,
                &provider.base_url,
                &provider.doc,
                provider.active,
                provider.id,
            ),
        )?)
    }

    /// 根据 id 删除提供商
    pub fn delete_provider(&self, id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);

        Ok(conn.execute("DELETE FROM providers WHERE id = ?1", [id])?)
    }

    // ========== Credentials ==========

    /// 创建凭证
    /// - 创建成功后将 id 设置到 credentials 中
    pub fn create_credentials(&self, credentials: &mut Credentials) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let row_count = conn.execute(
            "INSERT INTO credentials (provider_id, api_key, active)
            VALUES (?1, ?2, ?3)",
            (credentials.provider_id, &credentials.key, 1),
        )?;
        credentials.id = conn.last_insert_rowid();
        Ok(row_count)
    }

    /// 根据 id 查询凭证
    pub fn get_credentials(&self, id: i64) -> Result<Option<Credentials>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, provider_id, api_key, active, created_at, updated_at
            FROM credentials WHERE id = ?1",
        )?;
        value_or_none(stmt.query_row([id], row_to_credentials))
    }

    /// 根据 provider_id 查询该提供商下的所有凭证
    pub fn get_credentials_by_provider(
        &self,
        provider_id: i64,
    ) -> Result<Vec<Credentials>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, provider_id, api_key, active, created_at, updated_at
            FROM credentials WHERE provider_id = ?1 ORDER BY active DESC, created_at DESC",
        )?;
        let credentials = stmt
            .query_map([provider_id], row_to_credentials)?
            .collect::<Result<Vec<Credentials>, rusqlite::Error>>()?;
        Ok(credentials)
    }

    /// 更新凭证
    pub fn update_credentials(&self, credentials: &Credentials) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute(
            "UPDATE credentials SET provider_id = ?1, api_key = ?2, active = ?3,
            updated_at = strftime('%s', 'now') WHERE id = ?4",
            (credentials.provider_id, &credentials.key, 1, credentials.id),
        )?)
    }

    /// 根据 id 删除凭证
    pub fn delete_credentials(&self, id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute("DELETE FROM credentials WHERE id = ?1", [id])?)
    }
}
