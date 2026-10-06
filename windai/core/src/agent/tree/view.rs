use super::{NodeId, domain::Domain, phase::Phase, phase::Terminal};

/// 引擎对外事件
#[derive(Debug, Clone)]
pub enum TreeEvent<D: Domain> {
    /// 节点已创建
    NodeCreated {
        node: NodeId,
        parent: Option<NodeId>,
    },
    /// 非终态迁移
    PhaseChanged { node: NodeId, to: Box<Phase<D>> },
    /// 进度增量
    Progress { node: NodeId, chunk: D::Progress },
    /// 终态迁移
    Terminated { node: NodeId, terminal: Terminal<D> },
    /// 整棵树安静
    Sealed,
}
