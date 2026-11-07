use crate::db::DbRow;
use crate::error::Result;
use crate::{Mod, Storage};
use sqlx::{FromRow, Row};
use wind_wire::CompiledScripts;

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
    /// TODO: 返回全部 topic 与各自启用的 mod 名字
    pub async fn all_enabled_mods(&self) -> Result<Vec<(i64, String)>> {
        todo!()
    }

    /// TODO: 获取mod信息，
    pub async fn mod_by_name(&self, mod_name: &str) -> Result<Option<Mod>> {
        todo!()
    }

    /// TODO: 建立 mod 并发布初始版本，返回新 id
    pub async fn install_mod(
        &self,
        name: &str,
        manifest: &str,
        compiled: Option<&CompiledScripts>,
    ) -> Result<i64> {
        todo!()
    }

    /// TODO: 写入指定版本，编译产物随版本保存
    pub async fn publish_mod(
        &self,
        mod_id: i64,
        version: u32,
        manifest: &str,
        compiled: Option<&CompiledScripts>,
    ) -> Result<()> {
        todo!()
    }

    /// TODO: 发布新版本并把它设为当前版本
    pub async fn publish_current_mod(
        &self,
        mod_id: i64,
        version: u32,
        manifest: &str,
        compiled: Option<&CompiledScripts>,
    ) -> Result<()> {
        todo!()
        // self.with_tx(|storage| async move {
        //     storage
        //         .mod_publish(mod_id, version, manifest, compiled)
        //         .await?;
        //     storage.mod_set_current_version(mod_id, version).await
        // })
        // .await
    }

    /// TODO: 返回每个 mod 当前版本的清单
    pub async fn current_mod_manifests(&self) -> Result<Vec<(i64, String)>> {
        todo!()
    }

    /// TODO: 列出启用该 mod 的 topic，按 id 升序
    pub async fn topics_with_mod(&self, mod_id: i64) -> Result<Vec<i64>> {
        todo!()
    }
}
