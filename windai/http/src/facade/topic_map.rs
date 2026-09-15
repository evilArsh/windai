use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{CreateTopicAgentMap, TopicAgentMap};

use crate::dto::{ApiResponse, map_core_error};

/// 话题能力映射的门面
pub struct TopicMapFacade {
    core: Arc<WindCore>,
}

impl TopicMapFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    /// 列出话题的 Agent 能力映射
    pub async fn list(&self, topic_id: i64) -> ApiResponse<Vec<TopicAgentMap>> {
        // 先确认话题存在，避免「话题不存在」与「话题没有映射」都返回空列表
        if let Err(err) = self.require_topic(topic_id).await {
            return err.without_data();
        }
        match self
            .core
            .storage()
            .agent()
            .list_agent_maps_by_topic(topic_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(err) => map_core_error(err),
        }
    }

    /// 为话题添加一个 Agent 能力
    pub async fn create(&self, data: CreateTopicAgentMap) -> ApiResponse<TopicAgentMap> {
        // 先确认话题存在，避免为不存在的话题写入孤儿映射
        if let Err(err) = self.require_topic(data.topic_id).await {
            return err.without_data();
        }

        let created = match self
            .core
            .storage()
            .agent()
            .create_topic_agent_map(data)
            .await
        {
            Ok(map) => map,
            Err(err) => return map_core_error(err),
        };
        ApiResponse::ok(created)
    }

    /// 删除能力映射
    pub async fn delete(&self, map_id: i64) -> ApiResponse<()> {
        match self
            .core
            .storage()
            .agent()
            .delete_topic_agent_map(map_id)
            .await
        {
            Ok(()) => ApiResponse::ok(()),
            Err(err) => map_core_error(err),
        }
    }

    /// 校验话题存在，错误载体用 `ApiResponse<()>`，调用方以 `without_data()` 转成自己的 data 类型
    async fn require_topic(&self, topic_id: i64) -> Result<(), ApiResponse<()>> {
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(ApiResponse::not_found("topic not found")),
            Err(err) => Err(map_core_error(err)),
        }
    }
}
