use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone, schemars::JsonSchema)]
#[serde(untagged)]
pub enum Tools {
    Function(FunctionTool),
}

#[derive(Debug, Serialize, Deserialize, Clone, schemars::JsonSchema)]
pub struct FunctionTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct FunctionCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 用于流式消息中的 delta 配对
    pub item_id: Option<String>,
    /// 工具调用 id
    ///
    /// 在流式调用中可能为空，delta 数据使用 item_id 定位所属 call_id 调用
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct FunctionCallOutput {
    pub id: String,
    pub content: Value,
}
