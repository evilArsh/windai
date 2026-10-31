use crate::db::DbDriver;
use crate::error::Result;
use crate::{AgentPending, Storage, Turn};
use sqlx::QueryBuilder;
use sqlx::types::Json;
use wind_wire::{Block, Role};

/// 一条还没有对应 tool_result 的工具调用
#[derive(Debug, Clone, PartialEq)]
pub struct UnpairedToolCall {
    pub turn_id: i64,
    pub message_id: i64,
    pub call_id: String,
    pub data: Option<serde_json::Value>,
}

impl Storage {
    /// TODO: 创建新的 Turn
    pub async fn create_turn(
        &self,
        _topic_id: i64,
        _instance_id: i64,
        _from_id: Option<i64>,
    ) -> Result<i64> {
        todo!()
    }

    /// TODO: 创建一条 Message
    pub async fn create_message(
        &self,
        _turn_id: i64,
        _role: Role,
        _model_id: Option<i64>,
    ) -> Result<i64> {
        todo!()
    }

    /// TODO: 列出实例内还没有对应 tool_result 的工具调用
    pub async fn tool_calls_without_results(
        &self,
        _instance_id: i64,
    ) -> Result<Vec<UnpairedToolCall>> {
        todo!()
    }

    /// TODO: 批量获取待发送的 pending 数据
    ///
    /// 用户输入的任务会先入库，pending 数据可能包含用户输入和子agent任务结果
    pub async fn get_pending_inputs_by_ids(&self, _ids: &[i64]) -> Result<Vec<AgentPending>> {
        todo!()
    }

    /// TODO: 批量删除 pending 数据
    pub async fn delete_pending_by_ids(&self, _ids: &[i64]) -> Result<()> {
        todo!()
    }

    /// TODO: 读取该消息所在的turn
    pub async fn get_turn_of(&self, _message_id: i64) -> Result<Option<Turn>> {
        todo!()
    }

    /// TODO: 批量插入块，根据 seq 确保返回的id顺序 blocks顺序一致
    pub async fn create_blocks(&self, message_id: i64, blocks: &[Block]) -> Result<Vec<i64>> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<DbDriver>::new(
                "INSERT INTO block
                (message_id, seq, kind, role, revision, content, format, call_id, call_name, call_arguments, call_res)",
            );
        query.push_values(blocks, |mut separated, block| {
            let call = block.tool_call();
            let audio = block.audio();
            let output = block.call_output();
            separated
                .push_bind(message_id)
                .push_bind(block.seq())
                .push_bind(block.kind().to_string())
                .push_bind(block.role().to_string())
                .push_bind(block.revision())
                .push_bind(
                    block
                        .text()
                        .or_else(|| block.reasoning())
                        .or_else(|| block.media())
                        .map(str::to_string),
                )
                .push_bind(audio.map(|audio| audio.format))
                .push_bind(block.call_id().map(str::to_string))
                .push_bind(call.as_ref().map(|call| call.name.clone()))
                .push_bind(call.map(|call| call.arguments))
                .push_bind(output.map(|output| Json(output.content)));
        });
        query.push(" RETURNING seq, id");

        let mut inserted: Vec<InsertedBlock> = self
            .executor
            .fetch_all(query.build_query_as::<InsertedBlock>())
            .await?;

        inserted.sort_unstable_by_key(|block| block.seq);
        Ok(inserted.into_iter().map(|block| block.id).collect())
    }
}

/// 插入后读回的块标识
#[derive(sqlx::FromRow)]
struct InsertedBlock {
    seq: i64,
    id: i64,
}
