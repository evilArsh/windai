use super::{BuiltinScope, replace};
use crate::RegistryHandle;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Json;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};

mod snapshot;

pub use snapshot::{ModDir, SkillDir, WorkspaceInfo, WorkspaceSnapshot};

pub struct WorkspaceServer {
    name: String,
    info: WorkspaceInfo,
    tool_router: ToolRouter<Self>,
}

impl WorkspaceServer {
    pub fn new(name: impl Into<String>, info: WorkspaceInfo) -> Self {
        Self {
            name: name.into(),
            info,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl WorkspaceServer {
    #[tool(
        name = "workspace_info",
        description = "Return this topic's runtime paths. Call it before working with files or processes instead of guessing paths: it reports the topic workspace, the public skills root, and the directories of the mods enabled for this topic."
    )]
    async fn workspace_info(&self) -> Json<WorkspaceSnapshot> {
        Json(self.info.snapshot())
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for WorkspaceServer {
    fn get_info(&self) -> ServerConfig {
        let mut server_info = Implementation::new(self.name.clone(), env!("CARGO_PKG_VERSION"));
        server_info.description = Some("runtime path inventory for this topic".to_owned());
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(server_info)
    }
}

pub(super) const SLOT: &str = "workspace";

pub(super) async fn sync(registry: &RegistryHandle, scope: BuiltinScope, info: WorkspaceInfo) {
    let name = scope.service_name(SLOT);
    let server = WorkspaceServer::new(name.clone(), info);
    replace(registry, name, Some(server)).await;
}
