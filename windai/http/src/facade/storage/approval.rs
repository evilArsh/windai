use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::ToolApprovalRequest;

use crate::dto::envelope::{ApiResponse, map_core_error};

pub struct ToolApprovalFacade {
    core: Arc<WindCore>,
}

impl ToolApprovalFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_by_message(&self, message_id: i64) -> ApiResponse<Vec<ToolApprovalRequest>> {
        match self
            .core
            .storage()
            .approval()
            .list_by_message(message_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_pending_by_topic(
        &self,
        topic_id: i64,
    ) -> ApiResponse<Vec<ToolApprovalRequest>> {
        match self
            .core
            .storage()
            .approval()
            .list_pending_by_topic(topic_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_pending_by_binding(
        &self,
        binding_id: i64,
    ) -> ApiResponse<Vec<ToolApprovalRequest>> {
        match self
            .core
            .storage()
            .approval()
            .list_pending_by_binding(binding_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }
}
