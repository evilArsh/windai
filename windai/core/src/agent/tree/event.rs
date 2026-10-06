use super::{JobId, NodeId, domain::Domain, phase::CancelOrigin, view::TreeEvent};
use crate::agent::behavior::{Input, SpawnRequest};

/// 归约引擎的输入
#[derive(Debug)]
pub enum Event<D: Domain> {
    /// 派生一个新节点，id 由领域提供
    Spawn {
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        /// 不进入父节点的等待屏障
        detached: bool,
    },
    /// 登记待消费输入：节点空闲则立刻开一轮，忙则只置位
    Enqueue { node: NodeId, spec: D::Spec },
    /// 领域已把该节点的产出投递回去，释放它的投递义务
    Deliver { node: NodeId },
    /// 外部输入到达
    Resume {
        node: NodeId,
        expect: D::Suspend,
        input: D::Resume,
    },
    /// 请求取消一棵子树
    CancelRequest { root: NodeId, origin: CancelOrigin },
    /// 进度增量
    Progress {
        node: NodeId,
        job: JobId,
        chunk: D::Progress,
    },
    /// worker 挂起
    Suspended {
        node: NodeId,
        job: JobId,
        payload: D::Suspend,
    },
    /// worker 派生一批子节点
    Spawned {
        node: NodeId,
        job: JobId,
        spawn: Vec<SpawnRequest<D>>,
    },
    /// worker 完成
    Completed {
        node: NodeId,
        job: JobId,
        output: D::Output,
    },
    /// worker 失败
    Failed {
        node: NodeId,
        job: JobId,
        error: D::Error,
    },
    /// worker 已停止
    Stopped { node: NodeId, job: JobId },
    /// worker 没有交出判定就死了
    Aborted { node: NodeId, job: JobId },
    /// 恢复时把一个重建出来的节点收敛为终态
    Recovered { node: NodeId },
}

impl<D: Domain> Event<D> {
    /// 事件作用的主体节点，Spawn 返回其父节点
    pub fn node(&self) -> Option<NodeId> {
        match self {
            Event::Spawn { parent, .. } => *parent,
            Event::Enqueue { node, .. }
            | Event::Deliver { node }
            | Event::Resume { node, .. }
            | Event::CancelRequest { root: node, .. }
            | Event::Progress { node, .. }
            | Event::Suspended { node, .. }
            | Event::Spawned { node, .. }
            | Event::Completed { node, .. }
            | Event::Failed { node, .. }
            | Event::Stopped { node, .. }
            | Event::Aborted { node, .. }
            | Event::Recovered { node } => Some(*node),
        }
    }

    /// 事件是否让终态节点重新活跃
    pub fn reopens(&self) -> bool {
        matches!(self, Event::Spawn { .. } | Event::Enqueue { .. })
    }

    /// 事件是否属于可绕过的终态吸收事件，吸收针对的是回报；
    /// 
    /// `CancelRequest` 作用于整棵子树
    /// 
    /// `Recovered` 是重建后的收敛指令，要挨个走过已终态的子节点
    /// 
    /// `Deliver` 释放的正是已终态节点的投递义务
    pub fn bypasses_entry_absorption(&self) -> bool {
        matches!(
            self,
            Event::CancelRequest { .. } | Event::Recovered { .. } | Event::Deliver { .. }
        )
    }
}

/// reduce 的产出
#[derive(Debug)]
pub enum Effect<D: Domain> {
    /// 启动一个 worker
    Launch {
        node: NodeId,
        job: JobId,
        input: Input<D>,
    },
    /// 请求停止一个 worker
    ///
    /// 幂等：同一个 `(node, job)` 重复下发只等价于一次请求，driver 重复收到必须无副作用
    Stop { node: NodeId, job: JobId },
    /// 持久化节点状态
    Persist { node: NodeId },
    /// 投放对外事件
    Publish(TreeEvent<D>),
}
