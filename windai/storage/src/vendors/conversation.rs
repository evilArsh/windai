use wind_wire::{Block, Role};

use crate::error::Result;
use crate::{AgentPending, Storage, Turn};

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
    pub async fn create_new_turn(
        &self,
        _instance_id: i64,
        _topic_id: i64,
        _from_id: Option<i64>,
    ) -> Result<i64> {
        todo!()
    }

    /// TODO: 创建一条 Message
    pub async fn create_new_message(
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
    pub async fn create_blocks(&self, message_id: i64, blocks: &[Block]) -> Result<Vec<i64>> {}
}
