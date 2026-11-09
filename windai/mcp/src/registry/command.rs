use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::{Catalog, LaunchRequest, Started};
use crate::connector::{Connection, ToolDescriptor};
use crate::error::Result;
use crate::roots::{ScopeId, ScopeRoots};

pub(crate) enum Command {
    Acquire {
        ref_id: String,
        request: LaunchRequest,
    },
    Release {
        ref_id: String,
    },
    SetRoots {
        scope: ScopeId,
        roots: ScopeRoots,
    },
    ListTools {
        reply: oneshot::Sender<Catalog>,
    },
    EntryGeneration {
        name: String,
        reply: oneshot::Sender<Option<u64>>,
    },
    Refresh {
        name: String,
    },
    Report {
        name: String,
        generation: u64,
        phase: ReportPhase,
    },
    RefreshDone {
        name: String,
        generation: u64,
        result: Result<Vec<ToolDescriptor>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    EffectDone,
}

pub(crate) enum ReportPhase {
    Launch(Result<Started>),
    Stop(Result<()>),
}

pub(crate) enum Effect {
    Start {
        name: String,
        generation: u64,
        request: LaunchRequest,
    },
    Stop {
        name: String,
        generation: u64,
        connection: Arc<Connection>,
        server_task: Option<JoinHandle<()>>,
    },
    /// 丢弃过期的启动结果
    Discard(Started),
    Refresh {
        name: String,
        generation: u64,
        connection: Arc<Connection>,
    },
}
