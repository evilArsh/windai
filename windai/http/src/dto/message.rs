use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use wind_ai::message::Content;

/// 提交对话输入：向 TopicRuntime 提交用户消息，不创建 Message 记录
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct CreateChatRequest {
    /// 对话内容列表，支持文本/图片/文件/音频/函数调用结果
    pub content: Vec<Content>,
}

/// 对话提交回执（异步，不等待最终回答）。
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct SubmitChatResponse {
    /// 对话是否已受理
    pub accepted: bool,
}
