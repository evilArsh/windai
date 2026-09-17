use crate::{
    db::DbDriver,
    error::{CoreError, Result},
    insert_fields,
    models::{
        ApprovalRecord,
        agent::{CreateToolApprovalRequests, ToolApprovalRequest, ToolApprovalStatus},
    },
    select_fields,
    storage::TableName,
};
use sqlx::Row;
use std::collections::HashMap;

use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected, now_ts},
};

#[derive(Clone)]
pub struct ToolApprovalStorage {
    executor: StorageExecutor,
}
impl ToolApprovalStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    /// 创建新的审批请求
    pub async fn create_requests(
        &self,
        input: CreateToolApprovalRequests,
    ) -> Result<Vec<ToolApprovalRequest>> {
        if input.calls.is_empty() {
            return Ok(Vec::new());
        }

        let CreateToolApprovalRequests {
            instance_id,
            topic_id,
            message_id,
            calls,
        } = input;
        let now = now_ts();
        let status = ToolApprovalStatus::Pending;

        struct PreparedApproval {
            tool_call_id: String,
            tool_name: String,
            arguments: String,
        }

        let rows = calls
            .into_iter()
            .map(|call| {
                Ok(PreparedApproval {
                    tool_call_id: call.tool_call_id,
                    tool_name: call.tool_name,
                    arguments: utils::map_to_str_default(Some(&call.arguments))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        const CHUNK: usize = 3000;
        let rows_ref = &rows;

        let id_by_call: HashMap<String, i64> = self
            .executor
            .with_tx(|executor| async move {
                let mut map = HashMap::with_capacity(rows_ref.len());
                for chunk in rows_ref.chunks(CHUNK) {
                    let mut qb = insert_fields!(
                        TableName::TOOL_APPROVAL_REQUESTS,
                        (
                            "topic_id",
                            "message_id",
                            "instance_id",
                            "tool_call_id",
                            "tool_name",
                            "arguments",
                            "status",
                            "created_at",
                            "updated_at"
                        )
                    );
                    qb.push_values(chunk.iter(), |mut b, item| {
                        b.push_bind(topic_id);
                        b.push_bind(message_id);
                        b.push_bind(instance_id);
                        b.push_bind(&item.tool_call_id);
                        b.push_bind(&item.tool_name);
                        b.push_bind(&item.arguments);
                        b.push_bind(status.to_string());
                        b.push_bind(now);
                        b.push_bind(now);
                    });
                    qb.push(" RETURNING id, tool_call_id");

                    for row in executor.fetch_all_rows(qb.build()).await? {
                        map.insert(row.get("tool_call_id"), row.get("id"));
                    }
                }
                Ok(map)
            })
            .await?;

        if id_by_call.len() != rows.len() {
            return Err(CoreError::Validation(
                "duplicated tool_call_id in approval requests".into(),
            ));
        }

        Ok(rows
            .into_iter()
            .map(|row| ToolApprovalRequest {
                id: id_by_call[&row.tool_call_id],
                instance_id,
                topic_id,
                message_id,
                tool_call_id: row.tool_call_id,
                tool_name: row.tool_name,
                arguments: serde_json::Value::String(row.arguments),
                status,
                created_at: now,
                updated_at: now,
            })
            .collect())
    }

    /// 批量设置审批状态
    pub async fn batch_set_status(&self, records: Vec<ApprovalRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut qb = sqlx::QueryBuilder::new("");
        qb.push("UPDATE ")
            .push(TableName::TOOL_APPROVAL_REQUESTS)
            .push(" SET status = CASE id ");
        for r in &records {
            qb.push("WHEN ")
                .push_bind(r.id)
                .push(" THEN ")
                .push_bind(r.status.to_string())
                .push(" ");
        }
        qb.push("ELSE status END, updated_at = ")
            .push_bind(now_ts())
            .push(" WHERE id IN (");
        let mut sep = qb.separated(", ");
        for r in &records {
            sep.push_bind(r.id);
        }
        qb.push(")");

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

    pub async fn list_pending_by_instance(
        &self,
        instance_id: i64,
    ) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("instance_id", instance_id).await
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
                "topic_id",
                "message_id",
                "instance_id",
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
