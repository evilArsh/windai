pub(crate) mod tree;

use self::tree::{Domain, JobId, NodeId, Terminal};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ChildResult<D: Domain> {
    pub node: NodeId,
    pub terminal: Terminal<D>,
}

/// 输入的任务
#[derive(Debug)]
pub enum Input<D: Domain> {
    Fresh(D::Spec),
    Children(Vec<ChildResult<D>>),
    External(D::Resume),
    /// 节点空闲时一次取走的全部待消费输入
    Idle(Vec<D::Spec>),
}

#[derive(Debug)]
pub struct SpawnRequest<D: Domain> {
    pub id: NodeId,
    pub spec: D::Spec,
    pub detached: bool,
}

#[derive(Debug)]
pub enum Outcome<D: Domain> {
    Completed(D::Output),
    Failed(D::Error),
    Suspend(D::Suspend),
    Spawn { spawn: Vec<SpawnRequest<D>> },
}

pub struct Ctx {
    // 节点id(instance id)
    pub node: NodeId,
    pub job: JobId,
    pub cancel: CancellationToken,
}

impl Ctx {
    pub fn new(node: NodeId, job: JobId, cancel: CancellationToken) -> Self {
        Self { node, job, cancel }
    }
}

#[async_trait::async_trait]
pub trait Runner<D: Domain>: Send + Sync + 'static {
    async fn run(&self, ctx: Ctx, input: Input<D>) -> Outcome<D>;
}
