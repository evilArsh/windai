use std::future::Future;
use std::pin::Pin;

use crate::{
    agent::ToolApprovalService,
    db::DbPool,
    error::Result,
    insert_fields,
    models::agent::{
        CreateToolApprovalRequests, ToolApprovalDecision, ToolApprovalRequest, ToolApprovalStatus,
    },
    select_fields,
    storage::{next_id, now_ts},
    update,
};

use super::utils::{self, ensure_affected};

pub struct ToolApprovalStorage {
    db: DbPool,
}

impl ToolApprovalService for ToolApprovalStorage {
    fn create_requests<'a>(
        &'a self,
        input: CreateToolApprovalRequests,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ToolApprovalRequest>>> + Send + 'a>> {
        Box::pin(async move { ToolApprovalStorage::create_requests(self, input).await })
    }

    fn approve<'a>(
        &'a self,
        id: i64,
        decision: ToolApprovalDecision,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { ToolApprovalStorage::approve(self, id, decision).await })
    }

    fn deny<'a>(
        &'a self,
        id: i64,
        decision: ToolApprovalDecision,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { ToolApprovalStorage::deny(self, id, decision).await })
    }

    fn list_pending_by_message<'a>(
        &'a self,
        message_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ToolApprovalRequest>>> + Send + 'a>> {
        Box::pin(
            async move { ToolApprovalStorage::list_pending_by_message(self, message_id).await },
        )
    }
}

impl ToolApprovalStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub async fn create_requests(
        &self,
        input: CreateToolApprovalRequests,
    ) -> Result<Vec<ToolApprovalRequest>> {
        if input.calls.is_empty() {
            return Ok(Vec::new());
        }

        let now = now_ts();
        let rows = input
            .calls
            .into_iter()
            .map(|call| {
                Ok((
                    next_id(),
                    input.root_topic_id,
                    input.topic_id,
                    input.message_id,
                    input.agent_instance_id,
                    call.tool_call_id,
                    call.tool_name,
                    serde_json::to_string(&call.arguments)?,
                    ToolApprovalStatus::Pending.to_string(),
                    serde_json::to_string(&Option::<ToolApprovalDecision>::None)?,
                    now,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        insert_fields!(
            "tool_approval_requests",
            (
                "id",
                "root_topic_id",
                "topic_id",
                "message_id",
                "agent_instance_id",
                "tool_call_id",
                "tool_name",
                "arguments",
                "status",
                "decision",
                "created_at",
                "updated_at"
            )
        )
        .push_values(rows.iter(), |mut b, row| {
            b.push_bind(row.0);
            b.push_bind(row.1);
            b.push_bind(row.2);
            b.push_bind(row.3);
            b.push_bind(row.4);
            b.push_bind(&row.5);
            b.push_bind(&row.6);
            b.push_bind(&row.7);
            b.push_bind(&row.8);
            b.push_bind(&row.9);
            b.push_bind(row.10);
            b.push_bind(row.10);
        })
        .build()
        .execute(&self.db)
        .await?;

        self.list_by_ids(rows.iter().map(|row| row.0).collect::<Vec<_>>().as_slice())
            .await
    }

    pub async fn approve(&self, id: i64, decision: ToolApprovalDecision) -> Result<()> {
        self.set_status(id, ToolApprovalStatus::Approved, decision)
            .await
    }

    pub async fn deny(&self, id: i64, decision: ToolApprovalDecision) -> Result<()> {
        self.set_status(id, ToolApprovalStatus::Denied, decision)
            .await
    }

    async fn set_status(
        &self,
        id: i64,
        status: ToolApprovalStatus,
        decision: ToolApprovalDecision,
    ) -> Result<()> {
        let mut qb = update!(
            "tool_approval_requests",
            id,
            ("status", Some(status.to_string())),
            (
                "decision",
                Some(utils::map_to_str_default(Some(&Some(decision)))?)
            )
        );
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
    }

    pub async fn list_pending_by_message(
        &self,
        message_id: i64,
    ) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("message_id", message_id).await
    }

    pub async fn list_pending_by_topic(&self, topic_id: i64) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("topic_id", topic_id).await
    }

    pub async fn list_pending_by_instance(
        &self,
        agent_instance_id: i64,
    ) -> Result<Vec<ToolApprovalRequest>> {
        self.list_pending_by("agent_instance_id", agent_instance_id)
            .await
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
        let rows = qb
            .build_query_as::<ToolApprovalRequest>()
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    async fn list_by_ids(&self, ids: &[i64]) -> Result<Vec<ToolApprovalRequest>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = Self::select_common();
        qb.push(" WHERE id IN ( ");
        let mut separated = qb.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ORDER BY id ASC ");
        let rows = qb
            .build_query_as::<ToolApprovalRequest>()
            .fetch_all(&self.db)
            .await?;
        Ok(rows)
    }

    fn select_common<'a>() -> sqlx::QueryBuilder<'a, crate::db::DbDriver> {
        select_fields!(
            "tool_approval_requests",
            (
                "id",
                "root_topic_id",
                "topic_id",
                "message_id",
                "agent_instance_id",
                "tool_call_id",
                "tool_name",
                "arguments",
                "status",
                "decision",
                "created_at",
                "updated_at"
            )
        )
    }
}
