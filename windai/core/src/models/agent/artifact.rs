use serde::{Deserialize, Serialize};

/// Agent 输出的持久化结果。主 Agent 通过 artifact id 按需读取细节。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentArtifact {
    /// Artifact 主键。
    pub id: i64,
    /// Artifact 所属 root Topic id。
    pub root_topic_id: i64,
    /// 创建 Artifact 的 Topic id。
    pub topic_id: i64,
    /// 创建 Artifact 的 AgentInstance id。
    pub agent_instance_id: i64,
    /// Artifact 内容类型。
    pub artifact_type: ArtifactType,
    /// Artifact 标题。
    pub title: String,
    /// Artifact 摘要。
    pub summary: Option<String>,
    /// Artifact 内容载荷。
    pub content: ArtifactContent,
    /// Artifact 标签。
    pub tags: Vec<String>,
    /// 创建时间戳。
    pub created_at: i64,
}

/// Artifact 的内容类型。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ArtifactType {
    /// 报告。
    Report,
    /// JSON 结构化结果。
    Json,
    /// Patch 或 diff。
    Patch,
    /// 文件路径引用。
    FileRef,
    /// 日志。
    Log,
    /// 测试结果。
    TestResult,
    /// 研究笔记。
    ResearchNote,
    /// 计划。
    Plan,
}

/// Artifact 的具体内容载荷。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactContent {
    /// 文本内容。
    Text {
        /// 文本正文。
        text: String,
    },
    /// JSON 内容。
    Json {
        /// JSON 值。
        value: serde_json::Value,
    },
    /// 本地文件引用。
    FileRef {
        /// 文件路径。
        path: String,
    },
    /// 二进制资源引用。
    BytesRef {
        /// 二进制资源 URI。
        uri: String,
        /// 可选 SHA-256 校验值。
        sha256: Option<String>,
    },
}
