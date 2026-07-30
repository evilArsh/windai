use crate::{
    db::DbDriver,
    error::Result,
    insert_fields,
    models::agent::{CreateToolApprovalRequests, ToolApprovalRequest, ToolApprovalStatus},
    select_fields,
    storage::{TableName, next_id, now_ts},
    update,
};

use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected},
};

#[derive(Clone)]
pub struct ToolApprovalStorage {
    executor: StorageExecutor,
}
impl ToolApprovalStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    /// 创建新的审批请求，
    /// 参考batch_create_assistant
    pub async fn create_requests(&self, input: CreateToolApprovalRequests) -> Result<Vec<i64>> {
        if input.calls.is_empty() {
            return Ok(Vec::new());
        }

        struct PreparedApproval {
            id: i64,
            tool_call_id: String,
            tool_name: String,
            arguments: String,
        }

        let now = now_ts();
        let mut rows = Vec::with_capacity(input.calls.len());
        for call in input.calls {
            rows.push(PreparedApproval {
                id: next_id(),
                tool_call_id: call.tool_call_id,
                tool_name: call.tool_name,
                arguments: utils::map_to_str_default(Some(&call.arguments))?,
            });
        }

        let mut qb = insert_fields!(
            TableName::TOOL_APPROVAL_REQUESTS,
            (
                "id",
                "parent_topic_id",
                "topic_id",
                "message_id",
                "binding_id",
                "tool_call_id",
                "tool_name",
                "arguments",
                "status",
                "created_at",
                "updated_at"
            )
        );
        qb.push_values(rows.iter(), |mut b, item| {
            b.push_bind(item.id);
            b.push_bind(input.parent_topic_id);
            b.push_bind(input.topic_id);
            b.push_bind(input.message_id);
            b.push_bind(input.binding_id);
            b.push_bind(&item.tool_call_id);
            b.push_bind(&item.tool_name);
            b.push_bind(&item.arguments);
            b.push_bind(ToolApprovalStatus::Pending.to_string());
            b.push_bind(now);
            b.push_bind(now);
        });
        self.executor.execute(qb.build()).await?;

        Ok(rows.into_iter().map(|row| row.id).collect())
    }

    /// 设置审批状态
    pub async fn set_status(&self, id: i64, status: ToolApprovalStatus) -> Result<()> {
        let mut qb = update!(
            TableName::TOOL_APPROVAL_REQUESTS,
            id,
            ("status", Some(status.to_string()))
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn list_pending_by_message(
        &self,
        message_id: i64,
    ) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("message_id", message_id).await
    }

    pub async fn list_by_message(&self, message_id: i64) -> Result<Vec<ToolApprovalRequest>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_common()
                    .push(" WHERE message_id = ")
                    .push_bind(message_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<ToolApprovalRequest>(),
            )
            .await?;
        Ok(rows)
    }

    pub async fn list_pending_by_topic(&self, topic_id: i64) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("topic_id", topic_id).await
    }

    pub async fn list_pending_by_binding(
        &self,
        binding_id: i64,
    ) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("binding_id", binding_id).await
    }

    async fn list_pending_by(&self, column: &str, id: i64) -> Result<Vec<ToolApprovalRequest>> {
        let mut qb = Self::select_common();
        qb.push(" WHERE ")
            .push(column)
            .push(" = ")
            .push_bind(id)
            .push(" AND status = ")
            .push_bind(ToolApprovalStatus::Pending.to_string())
            .push(" ORDER BY id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<ToolApprovalRequest>())
            .await?;
        Ok(rows)
    }

    fn select_common<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::TOOL_APPROVAL_REQUESTS,
            (
                "id",
                "parent_topic_id",
                "topic_id",
                "message_id",
                "binding_id",
                "tool_call_id",
                "tool_name",
                "arguments",
                "status",
                "created_at",
                "updated_at"
            )
        )
    }
}
