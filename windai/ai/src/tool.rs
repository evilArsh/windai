use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 统一工具类型
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum Tools {
    /// 函数调用
    Function(FunctionTool),
}

/// 函数调用详细描述信息
#[derive(Debug, Serialize, Clone)]
pub struct FunctionTool {
    /// 要调用的函数名称
    pub name: String,
    /// 函数描述。模型根据此描述决定是否调用该函数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 描述函数参数的 JSON schema 对象
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// 是否强制执行严格的参数验证
    /// - 一些中间厂商可能不支持该参数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

/// 模型返回的函数调用信息
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionCall {
    /// 函数调用ID，由模型生成
    pub id: String,
    /// 函数工具名称
    pub name: String,
    /// 模型生成的函数调用参数
    pub arguments: String,
}

/// 函数调用结果
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionCallOutput {
    /// 函数调用ID，由模型生成
    pub id: String,
    /// 本地函数调用结果
    pub content: Value,
}
