use super::super::{
    event::{TopicEvent, TopicMsg},
    task::TaskSpec,
};
use super::task_fsm::TaskEvent;

pub enum FsmEvent {
    Topic(TopicMsg),
    /// Agent 任务开始
    Start {
        spec: TaskSpec,
    },
    /// 全部子任务完成信号
    ChildResolved {
        /// 父实例 id
        instance_id: i64,
    },
    /// 统一规约业务事件
    Emit(TopicEvent),
    Signal {
        instance_id: i64,
        event: TaskEvent,
    },
}
