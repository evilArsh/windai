use sqlx::QueryBuilder;

use super::utils::{self, ensure_affected};
use crate::{
    db::{DbDriver, DbPool},
    delete_by_id,
    error::{CoreError, Result},
    insert,
    models::{CreateMcpServer, McpServerParam, UpdateMcpServer},
    select_fields,
    storage::next_id,
    update,
};
pub struct McpStorage {
    db: DbPool,
}

impl McpStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub async fn create(&self, data: CreateMcpServer) -> Result<i64> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "mcp server name cannot be empty".into(),
            ));
        }
        let id = next_id();
        let mut qb = insert!(
            "mcp_servers",
            ("id", id),
            ("type", data.r#type.to_string()),
            ("name", data.name),
            ("url", data.url),
            ("description", data.description),
            ("command", data.command),
            ("args", utils::vec_to_str_default(data.args.as_deref())?),
            ("env", utils::map_to_str_default(data.env.as_ref())?),
        );
        qb.build().execute(&self.db).await?;
        Ok(id)
    }

    pub async fn update(&self, id: i64, data: UpdateMcpServer) -> Result<()> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "mcp server name cannot be empty".into(),
            ));
        }
        let mut qb = update!(
            "mcp_servers",
            id,
            ("type", data.r#type.map(|t| t.to_string())),
            ("name", Some(data.name)),
            ("url", data.url),
            ("description", data.description),
            ("command", data.command),
            ("args", utils::vec_to_str_optional(data.args.as_deref())?),
            ("env", utils::map_to_str_optional(data.env.as_ref())?)
        );
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("mcp_servers", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    fn common_select<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            "mcp_servers",
            (
                "id",
                "type",
                "name",
                "url",
                "description",
                "command",
                "args",
                "env",
                "created_at"
            )
        )
    }

    pub async fn get(&self, id: i64) -> Result<Option<McpServerParam>> {
        let mut qb = Self::common_select();
        let row = qb
            .push(" WHERE id = ")
            .push_bind(id)
            .build_query_as::<McpServerParam>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    /// 通过服务名字查询
    pub async fn get_by_name(&self, name: &str) -> Result<Option<McpServerParam>> {
        let mut qb = Self::common_select();
        let row = qb
            .push(" WHERE name = ")
            .push_bind(name)
            .build_query_as::<McpServerParam>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    /// 通过MCP服务名字批量查询
    pub async fn batch_get_by_names(&self, names: &[String]) -> Result<Vec<McpServerParam>> {
        if names.is_empty() {
            return Err(CoreError::Validation("names are empty".into()));
        }
        let mut qb = Self::common_select();
        qb.push(" WHERE name IN ( ");
        let mut separated = qb.separated(", ");
        for name in names {
            separated.push_bind(name);
        }
        separated.push_unseparated(") ");

        let rows = qb
            .build_query_as::<McpServerParam>() // 使用 build_query_as 触发 FromRow
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    /// 通过MCP服务ID批量查询
    pub async fn batch_get_by_ids(&self, ids: &[i64]) -> Result<Vec<McpServerParam>> {
        if ids.is_empty() {
            return Err(CoreError::Validation("mcp ids are empty".into()));
        }
        let mut qb = Self::common_select();
        qb.push(" WHERE id IN ( ");
        // TODO: 使用宏优化
        let mut separated = qb.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") ");

        let rows = qb
            .build_query_as::<McpServerParam>() // 使用 build_query_as 触发 FromRow
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    pub async fn list(&self) -> Result<Vec<McpServerParam>> {
        let mut qb = Self::common_select();
        qb.push(" ORDER BY id ASC ");
        let rows = qb
            .build_query_as::<McpServerParam>()
            .fetch_all(&self.db)
            .await?;

        Ok(rows)
    }
}
