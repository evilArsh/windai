use crate::db::{DbDriver, DbRow};
use crate::error::Result;
use crate::models::BlockColumns;
use crate::utils::{ensure_affected, now_ts};
use crate::{AgentPending, ContextRow, Storage, Turn};
use serde_json::{Value, json};
use sqlx::{FromRow, QueryBuilder, Row};
use wind_wire::{Block, Content, PendingKind, Role};

/// 一条还没有对应 tool_result 的工具调用
#[derive(Debug, Clone, PartialEq)]
pub struct UnpairedToolCall {
    pub turn_id: i64,
    pub message_id: i64,
    pub call_id: String,
    pub data: Option<serde_json::Value>,
}

impl Storage {
    /// 创建新的 Turn
    pub async fn create_turn(
        &self,
        topic_id: i64,
        instance_id: i64,
        from_id: Option<i64>,
    ) -> Result<i64> {
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO turn
                        (instance_id, topic_id, from_id, created_at)
                    VALUES (?, ?, ?, ?)
                    RETURNING id",
                )
                .bind(instance_id)
                .bind(topic_id)
                .bind(from_id)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
    }

    /// 创建一条 Message
    pub async fn create_message(
        &self,
        turn_id: i64,
        role: Role,
        model_id: Option<i64>,
    ) -> Result<i64> {
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO message
                        (turn_id, role, model_id, created_at)
                    VALUES (?, ?, ?, ?)
                    RETURNING id",
                )
                .bind(turn_id)
                .bind(role.as_ref())
                .bind(model_id)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
    }

    /// 读取实例全部历史 turn 已提交的块
    pub async fn list_instance_context(&self, instance_id: i64) -> Result<Vec<ContextRow>> {
        let rows = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, ContextRow>(
                    "SELECT m.input_tokens,
                            m.output_tokens,
                            m.created_at,
                            b.id,
                            b.message_id,
                            b.seq,
                            b.kind,
                            b.role,
                            b.revision,
                            b.content,
                            b.format,
                            b.item_id,
                            b.call_id,
                            b.call_name,
                            b.call_arguments,
                            b.call_res
                    FROM turn t
                    JOIN message m ON m.turn_id = t.id
                    JOIN block b ON b.message_id = m.id
                    WHERE t.instance_id = ?
                    ORDER BY m.id, b.seq",
                )
                .bind(instance_id),
            )
            .await?;
        Ok(rows)
    }

    pub async fn create_pending(
        &self,
        instance_id: i64,
        kind: PendingKind,
        payload: Vec<Content>,
        source_id: Option<i64>,
    ) -> Result<i64> {
        let payload = serde_json::to_string(&payload)?;
        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO agent_pending
                        (instance_id, kind, payload_json, source_id, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    RETURNING id",
                )
                .bind(instance_id)
                .bind(kind.as_ref())
                .bind(payload)
                .bind(source_id)
                .bind(now_ts()),
            )
            .await?;
        Ok(id)
    }

    /// 列出实例内还没有对应 tool_result 的工具调用
    pub async fn tool_calls_without_results(
        &self,
        instance_id: i64,
    ) -> Result<Vec<UnpairedToolCall>> {
        let rows: Vec<UnpairedToolCall> = self
            .executor
            .fetch_all(
                sqlx::query_as::<_, UnpairedToolCall>(
                    "SELECT t.id AS turn_id,
                            m.id AS message_id,
                            b.call_id,
                            b.call_name,
                            b.call_arguments
                    FROM block b
                    JOIN message m ON m.id = b.message_id
                    JOIN turn t ON t.id = m.turn_id
                    WHERE t.instance_id = ?
                        AND b.kind = 'tool_call'
                        AND b.call_id IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1
                            FROM block r
                            WHERE r.kind = 'tool_result'
                                AND r.call_id = b.call_id
                        )
                    ORDER BY b.id",
                )
                .bind(instance_id),
            )
            .await?;
        Ok(rows)
    }

    /// 写入消息的结束原因与用量
    pub async fn seal_message(
        &self,
        message_id: i64,
        finish_reason: &str,
        input_tokens: i32,
        output_tokens: i32,
    ) -> Result<()> {
        let result = self
            .executor
            .execute(
                sqlx::query(
                    "UPDATE message
                    SET finish_reason = ?, input_tokens = ?, output_tokens = ?
                    WHERE id = ?",
                )
                .bind(finish_reason)
                .bind(i64::from(input_tokens))
                .bind(i64::from(output_tokens))
                .bind(message_id),
            )
            .await?;
        ensure_affected(result)
    }

    /// 批量获取待发送的 pending 数据
    ///
    /// 用户输入的任务会先入库，pending 数据可能包含用户输入和子agent任务结果
    pub async fn get_pending_inputs_by_ids(&self, ids: &[i64]) -> Result<Vec<AgentPending>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<DbDriver>::new(
            "SELECT id, instance_id, kind, payload_json, source_id, created_at
            FROM agent_pending
            WHERE id IN (",
        );
        {
            let mut separated = query.separated(", ");
            for id in ids {
                separated.push_bind(*id);
            }
        }
        query.push(") ORDER BY id");

        let rows: Vec<AgentPending> = self.executor.fetch_all(query.build_query_as()).await?;
        Ok(rows)
    }

    /// 批量删除 pending 数据
    pub async fn delete_pending_by_ids(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut query = QueryBuilder::<DbDriver>::new("DELETE FROM agent_pending WHERE id IN (");
        {
            let mut separated = query.separated(", ");
            for id in ids {
                separated.push_bind(*id);
            }
        }
        query.push(")");
        self.executor.execute(query.build()).await?;
        Ok(())
    }

    /// 读取该消息所在的 turn
    pub async fn get_turn_of(&self, message_id: i64) -> Result<Option<Turn>> {
        let turn = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, Turn>(
                    "SELECT t.id, t.instance_id, t.topic_id, t.from_id, t.created_at
                    FROM turn t
                    JOIN message m ON m.turn_id = t.id
                    WHERE m.id = ?",
                )
                .bind(message_id),
            )
            .await?;
        Ok(turn)
    }

    /// 批量插入块，根据 seq 确保返回的 id 顺序与 blocks 顺序一致
    pub async fn create_blocks(&self, message_id: i64, blocks: &[Block]) -> Result<Vec<i64>> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<DbDriver>::new(
            "INSERT INTO block
                (message_id, seq, kind, role, revision, content, format,
                item_id, call_id, call_name, call_arguments, call_res)",
        );
        query.push_values(blocks, |mut separated, block| {
            let columns = BlockColumns::of(&block.body);
            separated
                .push_bind(message_id)
                .push_bind(block.seq)
                .push_bind(block.tag())
                .push_bind(block.role.as_ref())
                .push_bind(block.revision)
                .push_bind(columns.content)
                .push_bind(columns.format)
                .push_bind(columns.item_id)
                .push_bind(columns.call_id)
                .push_bind(columns.call_name)
                .push_bind(columns.call_arguments)
                .push_bind(columns.call_res);
        });
        query.push(" RETURNING seq, id");

        let mut inserted: Vec<(i64, i64)> = self
            .executor
            .fetch_all(query.build_query_as::<(i64, i64)>())
            .await?;

        inserted.sort_unstable_by_key(|(seq, _)| *seq);
        Ok(inserted.into_iter().map(|(_, id)| id).collect())
    }
}

impl<'r> FromRow<'r, DbRow> for UnpairedToolCall {
    fn from_row(row: &'r DbRow) -> sqlx::Result<Self> {
        let arguments = row
            .try_get::<Option<String>, _>("call_arguments")?
            .as_deref()
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .unwrap_or(Value::Null);
        Ok(Self {
            turn_id: row.try_get("turn_id")?,
            message_id: row.try_get("message_id")?,
            call_id: row.try_get("call_id")?,
            data: Some(json!({
                "name": row.try_get::<Option<String>, _>("call_name")?.unwrap_or_default(),
                "arguments": arguments,
            })),
        })
    }
}

impl Storage {
    pub async fn list_pending_by_topic(&self, topic_id: i64) -> Result<Vec<AgentPending>> {
        self.executor
            .fetch_all(
                sqlx::query_as::<_, AgentPending>(
                    "SELECT p.id, p.instance_id, p.kind, p.payload_json, p.source_id, p.created_at
             FROM agent_pending p JOIN agent_instances i ON i.id = p.instance_id
             WHERE i.topic_id = ? ORDER BY p.id",
                )
                .bind(topic_id),
            )
            .await
    }

    pub async fn close_pending(&self, instance_id: i64) -> Result<()> {
        self.executor
            .execute(
                sqlx::query(
                    "DELETE FROM agent_pending WHERE instance_id IN (
                WITH RECURSIVE sub(id) AS (
                    SELECT id FROM agent_instances WHERE id = ?
                    UNION ALL SELECT i.id FROM agent_instances i JOIN sub ON i.parent_id = sub.id
                ) SELECT id FROM sub
             )",
                )
                .bind(instance_id),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_background_result(
        &self,
        parent_id: i64,
        payload: Vec<Content>,
        source_id: i64,
    ) -> Result<Option<i64>> {
        let row: Option<(i64,)> = self.executor.fetch_optional(sqlx::query_as(
            "INSERT INTO agent_pending (instance_id, kind, payload_json, source_id, created_at)
             SELECT ?, 'background_result', ?, ?, ? FROM agent_instances
             WHERE id = ? AND phase IN ('running', 'waiting_children', 'suspended', 'done')
             ON CONFLICT (instance_id, kind, source_id) WHERE source_id IS NOT NULL DO NOTHING
             RETURNING id"
        ).bind(parent_id).bind(serde_json::to_string(&payload)?).bind(source_id).bind(now_ts()).bind(parent_id)).await?;
        Ok(row.map(|(id,)| id))
    }
}
