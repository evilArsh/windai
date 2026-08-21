use super::task_fsm::TaskEvent;
use crate::agent::{
    event::{TopicEvent, TopicMsg},
    runtime::AgentRunConfig,
    task::TaskSpec,
};

pub enum FsmEvent {
    Topic(TopicMsg),
    /// 主 Agent 开始运行
    Start {
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 子 Agent 开始运行
    StartChild {
        parent_binding_id: i64,
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    ChildResolved {
        parent_binding_id: i64,
    },
    /// 统一规约业务事件
    Emit(TopicEvent),
    Signal {
        binding_id: i64,
        event: TaskEvent,
    },
}
