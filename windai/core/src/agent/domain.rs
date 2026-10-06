use super::tree::Domain;
use crate::models::{AgentInstance, Message};
use serde::{Deserialize, Serialize};
use wind_ai::message::Message as AiMessage;

/// Agent 任务树的领域标签
#[derive(Debug, Clone)]
pub struct AgentDomain;

impl Domain for AgentDomain {
    type Spec = AgentSpec;
    type Suspend = PendingApproval;
    type Resume = ApprovalAnswer;
    type Output = AgentOutcome;
    type Error = AgentFailure;
    type Progress = AgentProgress;
}

/// 一个节点跑一段所需的全部信息
///
/// 只放能从库重建的指针：上下文每次从 store 重载
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSpec {
    /// 实例 id
    pub instance_id: i64,
    /// 本轮写入的 assistant 消息
    pub message_id: Option<i64>,
}

/// 挂起标记：等用户审批
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingApproval {
    pub instance_id: i64,
    pub message_id: i64,
}

/// 用户对审批的应答
#[derive(Debug, Clone)]
pub struct ApprovalAnswer {
    /// 放行的审批 id
    pub allow_ids: Vec<i64>,
    /// 拒绝的审批 id
    pub deny_ids: Vec<i64>,
}

/// 节点产出
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutcome {
    pub message_id: i64,
}

/// 节点失败
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentFailure {
    pub message_id: Option<i64>,
    pub error: String,
}

/// 对外可见但不构成树状态的进度
#[derive(Debug, Clone)]
pub enum AgentProgress {
    /// 实例已被采用
    InstanceCreated(AgentInstance),
    /// 消息已落库
    MessageCreated(Message),
    /// 内容块分片
    ///
    /// `message_id` 是这一块归属的消息：用户输入块属于用户消息，
    /// 助手产出与工具结果属于本轮运行写入的那条 assistant 消息
    ///
    /// `index` 是本次运行内的块序号
    Block {
        message_id: i64,
        index: i64,
        data: AiMessage,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::Domain;

    /// 编译期契约：Domain 的关联类型必须满足引擎要求的全部 bound
    fn assert_domain<D: Domain>() {}

    #[test]
    fn agent_domain_satisfies_the_engine_bounds() {
        assert_domain::<AgentDomain>();
    }

    #[test]
    fn spec_suspension_output_and_error_round_trip_through_json() {
        let spec = AgentSpec {
            instance_id: 7,
            message_id: Some(11),
        };
        let back: AgentSpec = serde_json::from_str(&serde_json::to_string(&spec).unwrap()).unwrap();
        assert_eq!(back, spec);

        let opening = AgentSpec {
            instance_id: 7,
            message_id: None,
        };
        let back: AgentSpec =
            serde_json::from_str(&serde_json::to_string(&opening).unwrap()).unwrap();
        assert_eq!(back, opening, "开新一轮的 spec 要能原样往返");

        let suspend = PendingApproval {
            instance_id: 7,
            message_id: 11,
        };
        let back: PendingApproval =
            serde_json::from_str(&serde_json::to_string(&suspend).unwrap()).unwrap();
        assert_eq!(back, suspend, "挂起标记要能回显比对");

        let out = AgentOutcome { message_id: 11 };
        let back: AgentOutcome =
            serde_json::from_str(&serde_json::to_string(&out).unwrap()).unwrap();
        assert_eq!(back, out);

        let err = AgentFailure {
            message_id: Some(11),
            error: "boom".into(),
        };
        let back: AgentFailure =
            serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(back, err);
    }
}
