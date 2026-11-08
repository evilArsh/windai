use std::sync::Arc;

use wind_storage::{McpServerRecord, Storage};
use wind_wire::McpDefinition;

use crate::config;
use crate::error::Result;
use crate::registry::{BindingStatus, Registry};

/// 一次配置修改的结果
#[derive(Debug, Clone, PartialEq)]
pub struct McpChange {
    /// 修改后的配置，删除时为空
    pub record: Option<McpServerRecord>,
    /// 需要重新同步的话题
    pub affected_topics: Vec<i64>,
}

/// 独立 MCP 配置的管理入口
pub struct McpService {
    storage: Storage,
    registry: Arc<Registry>,
}

impl McpService {
    pub fn new(storage: Storage, registry: Arc<Registry>) -> Self {
        Self { storage, registry }
    }

    /// 运行中的绑定状态
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// 一个话题的绑定状态，含连接失败的原因
    pub fn status(&self, topic_id: i64) -> Vec<BindingStatus> {
        self.registry.status(topic_id)
    }

    /// 新建配置，不自动启用
    pub async fn create(&self, definition: &McpDefinition) -> Result<McpChange> {
        config::validate_definition(definition)?;
        let record = self.storage.create_mcp_server(definition).await?;
        Ok(McpChange {
            record: Some(record),
            affected_topics: Vec::new(),
        })
    }

    /// 按 revision 更新配置并返回需要同步的话题
    pub async fn update(
        &self,
        id: i64,
        expected_revision: u32,
        definition: &McpDefinition,
    ) -> Result<McpChange> {
        config::validate_definition(definition)?;
        let affected_topics = self.storage.topics_with_mcp_server(id).await?;
        let record = self
            .storage
            .update_mcp_server(id, expected_revision, definition)
            .await?;
        Ok(McpChange {
            record: Some(record),
            affected_topics,
        })
    }

    /// 删除配置与启用关系
    pub async fn delete(&self, id: i64) -> Result<McpChange> {
        let affected_topics = self.storage.delete_mcp_server(id).await?;
        Ok(McpChange {
            record: None,
            affected_topics,
        })
    }

    /// 在话题中启用配置
    pub async fn enable(&self, topic_id: i64, id: i64) -> Result<McpChange> {
        self.storage.enable_mcp_server(topic_id, id).await?;
        let record = self.storage.mcp_server(id).await?;
        Ok(McpChange {
            record,
            affected_topics: vec![topic_id],
        })
    }

    /// 在话题中禁用配置
    pub async fn disable(&self, topic_id: i64, id: i64) -> Result<McpChange> {
        self.storage.disable_mcp_server(topic_id, id).await?;
        let record = self.storage.mcp_server(id).await?;
        Ok(McpChange {
            record,
            affected_topics: vec![topic_id],
        })
    }

    /// 读取一条配置
    pub async fn get(&self, id: i64) -> Result<Option<McpServerRecord>> {
        Ok(self.storage.mcp_server(id).await?)
    }

    /// 全部独立配置，按 id 升序
    pub async fn list(&self) -> Result<Vec<McpServerRecord>> {
        Ok(self.storage.list_mcp_servers().await?)
    }

    /// 话题启用的独立配置，按 id 升序
    pub async fn enabled(&self, topic_id: i64) -> Result<Vec<McpServerRecord>> {
        Ok(self.storage.enabled_mcp_servers(topic_id).await?)
    }
}
