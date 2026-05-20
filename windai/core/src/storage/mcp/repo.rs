use crate::{
    error::{CoreError, Result},
    models::{CreateMcpServer, McpServerParam},
};
use sqlx::{Row, SqlitePool, Transaction};
use wind_mcp::client::TransportType;

pub struct McpRepo {
    pub(crate) db: SqlitePool,
}

impl McpRepo {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    fn row_to_mcp_server(row: sqlx::sqlite::SqliteRow) -> Result<McpServerParam> {
        let type_str: String = row.get("type");
        let r#type: TransportType = type_str.parse().map_err(|e| CoreError::StrumParse(e))?;
        let args_str: String = row.get("args");
        let env_str: String = row.get("env");
        Ok(McpServerParam {
            id: row.get("id"),
            r#type,
            name: row.get("name"),
            url: row.get("url"),
            description: row.get("description"),
            command: row.get("command"),
            args: serde_json::from_str(&args_str).ok(),
            env: serde_json::from_str(&env_str).ok(),
            created_at: row.get("created_at"),
        })
    }

    pub async fn create(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        data: CreateMcpServer,
    ) -> Result<i64> {
        let args_json = serde_json::to_string(&data.args.unwrap_or_default())?;
        let env_json = serde_json::to_string(&data.env.unwrap_or_default())?;
        let row = sqlx::query(
            "INSERT INTO mcp_servers
            (type, name, url, description, command, args, env)
            VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(data.r#type.to_string())
        .bind(data.name)
        .bind(data.url)
        .bind(data.description)
        .bind(data.command)
        .bind(args_json)
        .bind(env_json)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }

    pub async fn update(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
        r#type: &str,
        name: &str,
        url: Option<&str>,
        description: Option<&str>,
        command: Option<&str>,
        args_json: &str,
        env_json: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE mcp_servers SET
            type = ?, name = ?, url = ?, description = ?, command = ?, args = ?, env = ?,
            updated_at = strftime('%s', 'now')
            WHERE id = ?"#,
        )
        .bind(r#type)
        .bind(name)
        .bind(url)
        .bind(description)
        .bind(command)
        .bind(args_json)
        .bind(env_json)
        .bind(id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<McpServerParam>> {
        let row = sqlx::query(
            "SELECT
            id, type, name, url, description, command, args, env,
            created_at
            FROM mcp_servers WHERE id = ?",
        )
        .bind(id)
        .map(Self::row_to_mcp_server)
        .fetch_optional(&self.db)
        .await?;

        row.map(|r| r.map_err(Into::into)).transpose()
    }

    pub async fn delete(&self, tx: &mut Transaction<'_, sqlx::Sqlite>, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM mcp_servers WHERE id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<McpServerParam>> {
        let rows = sqlx::query(
            "SELECT
            id, type, name, url, description, command, args, env,
            created_at
            FROM mcp_servers ORDER BY id ASC",
        )
        .map(Self::row_to_mcp_server)
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .flat_map(|r| r.ok())
        .collect();

        Ok(rows)
    }
}
