use super::repo::McpRepo;
use crate::{
    db,
    error::{CoreError, Result},
    models::{CreateMcpServer, McpServerParam, UpdateMcpServer},
};
use sqlx::SqlitePool;

pub struct McpService {
    repo: McpRepo,
}

impl McpService {
    pub fn new(db: SqlitePool) -> Self {
        Self {
            repo: McpRepo::new(db),
        }
    }

    pub async fn create(&self, data: CreateMcpServer) -> Result<i64> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "mcp server name cannot be empty".into(),
            ));
        }

        let mut tx = db::begin_tx(&self.repo.db).await?;
        let id = self.repo.create(&mut tx, data).await?;
        tx.commit().await?;

        Ok(id)
    }

    pub async fn update(&self, id: i64, data: UpdateMcpServer) -> Result<()> {
        let current = self
            .repo
            .get(id)
            .await?
            .ok_or(CoreError::NotFound(format!("mcp server {id}")))?;

        let r#type = data
            .r#type
            .map(|t| t.to_string())
            .unwrap_or_else(|| current.r#type.to_string());
        let name = data.name.as_deref().unwrap_or(&current.name);
        let url = data.url.as_deref().or(current.url.as_deref());
        let description = data
            .description
            .as_deref()
            .or(current.description.as_deref());
        let command = data.command.as_deref().or(current.command.as_deref());
        let args_json = match &data.args {
            Some(a) => serde_json::to_string(a).unwrap_or_default(),
            None => serde_json::to_string(&current.args).unwrap_or_default(),
        };
        let env_json = match &data.env {
            Some(e) => serde_json::to_string(e).unwrap_or_default(),
            None => serde_json::to_string(&current.env).unwrap_or_default(),
        };

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update(
                &mut tx,
                id,
                &r#type,
                name,
                url,
                description,
                command,
                &args_json,
                &env_json,
            )
            .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete(&mut tx, id).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<McpServerParam> {
        self.repo
            .get(id)
            .await?
            .ok_or(CoreError::NotFound(format!("mcp server {id}")))
    }

    pub async fn list(&self) -> Result<Vec<McpServerParam>> {
        self.repo.list().await
    }
}
