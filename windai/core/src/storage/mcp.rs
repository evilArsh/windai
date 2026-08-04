use sqlx::QueryBuilder;

use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected},
};
use crate::{
    db::DbDriver,
    delete_by_id,
    error::{CoreError, Result},
    insert,
    models::{CreateMcpServer, McpServerParam, UpdateMcpServer},
    select_fields,
    storage::{TableName, next_id},
    update,
};
#[derive(Clone)]
pub struct McpStorage {
    executor: StorageExecutor,
}

impl McpStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    pub async fn create(&self, data: CreateMcpServer) -> Result<McpServerParam> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "mcp server name cannot be empty".into(),
            ));
        }
        let id = next_id();
        let now = crate::storage::now_ts();
        let args = utils::vec_to_str_default(data.args.as_deref())?;
        let env = utils::map_to_str_default(data.env.as_ref())?;
        let mut qb = insert!(
            TableName::MCP_SERVERS,
            ("id", id),
            ("type", data.r#type.to_string()),
            ("name", data.name.clone()),
            ("url", data.url.clone()),
            ("description", data.description.clone()),
            ("command", data.command.clone()),
            ("args", args),
            ("env", env),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;
        Ok(McpServerParam {
            id,
            r#type: data.r#type,
            name: data.name,
            url: data.url,
            description: data.description,
            command: data.command,
            args: data.args,
            env: data.env,
            created_at: now,
        })
    }

    pub async fn update(&self, id: i64, data: UpdateMcpServer) -> Result<()> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "mcp server name cannot be empty".into(),
            ));
        }
        let mut qb = update!(
            TableName::MCP_SERVERS,
            id,
            ("type", data.r#type.map(|t| t.to_string())),
            ("name", Some(data.name)),
            ("url", data.url),
            ("description", data.description),
            ("command", data.command),
            ("args", utils::vec_to_str_optional(data.args.as_deref())?),
            ("env", utils::map_to_str_optional(data.env.as_ref())?)
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!(TableName::MCP_SERVERS, id);
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    fn common_select<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::MCP_SERVERS,
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
            .build_query_as::<McpServerParam>();
        let row = self.executor.fetch_optional(row).await?;
        Ok(row)
    }

    /// 通过服务名字查询
    pub async fn get_by_name(&self, name: &str) -> Result<Option<McpServerParam>> {
        let mut qb = Self::common_select();
        let row = qb
            .push(" WHERE name = ")
            .push_bind(name)
            .build_query_as::<McpServerParam>();
        let row = self.executor.fetch_optional(row).await?;
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

        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<McpServerParam>())
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
        let mut separated = qb.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") ");

        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<McpServerParam>())
            .await?;
        Ok(rows)
    }

    pub async fn list(&self) -> Result<Vec<McpServerParam>> {
        let mut qb = Self::common_select();
        qb.push(" ORDER BY id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<McpServerParam>())
            .await?;

        Ok(rows)
    }
}
