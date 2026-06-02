use super::utils;
use crate::{
    db::DbPool,
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
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
        let args_json = utils::vec_to_str(data.args.as_deref())?;
        let env_json = utils::map_to_str(data.env.as_ref())?;
        let auto_approves_json = utils::vec_to_str(data.auto_approves.as_deref())?;

        let id = next_id();
        let mut qb = insert!(
            "mcp_servers",
            ("id", id),
            ("type", data.r#type.to_string()),
            ("name", data.name),
            ("url", data.url),
            ("description", data.description),
            ("command", data.command),
            ("args", args_json),
            ("env", env_json),
            ("auto_approves", auto_approves_json),
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
            ("args", Some(utils::vec_to_str(data.args.as_deref())?)),
            ("env", Some(utils::map_to_str(data.env.as_ref())?)),
            (
                "auto_approves",
                Some(utils::vec_to_str(data.auto_approves.as_deref())?)
            ),
        );
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("mcp_servers", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<McpServerParam>> {
        let mut qb = get_by_id!(
            "mcp_servers",
            id,
            (
                "id",
                "type",
                "name",
                "url",
                "description",
                "command",
                "args",
                "env",
                "auto_approves",
                "created_at"
            )
        );
        let row = qb
            .build_query_as::<McpServerParam>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    pub async fn list(&self) -> Result<Vec<McpServerParam>> {
        let mut qb = select_fields!(
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
                "auto_approves",
                "created_at"
            )
        );
        qb.push(" ORDER BY id ASC ");
        let rows = qb
            .build_query_as::<McpServerParam>()
            .fetch_all(&self.db)
            .await?;

        Ok(rows)
    }
}
