use super::identity::AssetRoot;
use super::params::{ServerParams, StdioParams};
use super::process::build_command;
use rmcp::model::{CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Prompt};
use rmcp::model::{Resource, TextContent, Tool};
use rmcp::service::{ClientInitializeError, DynService, RunningService};
use rmcp::transport::{StreamableHttpClientTransport, TokioChildProcess};
use rmcp::{RoleClient, ServerHandler, ServiceError, ServiceExt};
use serde_json::{Map, Value};
use std::process::Stdio;

type ClientService = RunningService<RoleClient, Box<dyn DynService<RoleClient>>>;

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error(transparent)]
    Transport(#[from] std::io::Error),
    #[error(transparent)]
    Initialize(Box<ClientInitializeError>),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error("relative command {command} does not resolve inside the asset root")]
    UnresolvedCommand { command: String },
}

impl From<ClientInitializeError> for ConnectorError {
    fn from(error: ClientInitializeError) -> Self {
        Self::Initialize(Box::new(error))
    }
}

impl ConnectorError {
    pub fn disconnected(&self) -> bool {
        matches!(
            self,
            Self::Transport(_)
                | Self::Service(ServiceError::TransportSend(_) | ServiceError::TransportClosed)
        )
    }
}

pub struct ServerHandle {
    service: ClientService,
    builtin_task: Option<tokio::task::JoinHandle<()>>,
}

impl ServerHandle {
    pub async fn connect(params: &ServerParams, root: &AssetRoot) -> Result<Self, ConnectorError> {
        let service = match params {
            ServerParams::Stdio(stdio) => {
                let command = build_command(&resolved_against(stdio, root)?);
                let (transport, _stderr) = TokioChildProcess::builder(command)
                    .stderr(Stdio::null())
                    .spawn()?;
                ().into_dyn().serve(transport).await?
            }
            ServerParams::Streamable(remote) => {
                let transport = StreamableHttpClientTransport::from_uri(remote.url.as_str());
                ().into_dyn().serve(transport).await?
            }
        };
        Ok(Self {
            service,
            builtin_task: None,
        })
    }

    pub async fn connect_builtin<H>(handler: H) -> Result<Self, ConnectorError>
    where
        H: ServerHandler + Send + Sync + 'static,
    {
        let (server_tx, client_rx) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            match handler.serve(server_tx).await {
                Ok(service) => {
                    let _ = service.waiting().await;
                }
                Err(error) => log::error!("builtin server serve failed: {}", error),
            }
        });
        let service = ().into_dyn().serve(client_rx).await?;
        Ok(Self {
            service,
            builtin_task: Some(server_task),
        })
    }

    pub async fn disconnect(mut self) {
        let _ = self.service.close().await;
        if let Some(task) = self.builtin_task.take() {
            let _ = task.await;
        }
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Option<&Map<String, Value>>,
    ) -> Result<CallToolResult, ConnectorError> {
        let params = CallToolRequestParams::new(name.to_owned());
        let params = if let Some(args) = arguments {
            params.with_arguments(args.clone())
        } else {
            params
        };
        match self.service.call_tool_once(params).await? {
            CallToolResponse::Complete(result) => Ok(result),
            CallToolResponse::InputRequired(request) => {
                let request =
                    serde_json::to_string(&request).unwrap_or_else(|_| String::from("{}"));
                Ok(CallToolResult::success(vec![ContentBlock::Text(
                    TextContent::new(request),
                )]))
            }
            CallToolResponse::Task(_) => {
                Err(ConnectorError::Service(ServiceError::UnexpectedResponse))
            }
            _ => Err(ConnectorError::Service(ServiceError::UnexpectedResponse)),
        }
    }

    pub async fn list_tools(&self) -> Result<Vec<Tool>, ConnectorError> {
        self.service
            .list_all_tools()
            .await
            .map_err(ConnectorError::Service)
    }

    pub async fn list_prompts(&self) -> Result<Vec<Prompt>, ConnectorError> {
        self.service
            .list_all_prompts()
            .await
            .map_err(ConnectorError::Service)
    }

    pub async fn list_resources(&self) -> Result<Vec<Resource>, ConnectorError> {
        self.service
            .list_all_resources()
            .await
            .map_err(ConnectorError::Service)
    }
}

fn resolved_against(params: &StdioParams, root: &AssetRoot) -> Result<StdioParams, ConnectorError> {
    let Some(path) = root.resolve(&params.command) else {
        // TODO: 本地MCP只能是可执行文件；目标是`command: "./index.js"` 执行
        if params.command.starts_with("./") {
            return Err(ConnectorError::UnresolvedCommand {
                command: params.command.clone(),
            });
        }
        return Ok(params.clone());
    };
    Ok(StdioParams {
        command: path.to_string_lossy().into_owned(),
        ..params.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// 只关心命令行的连接描述
    fn params(command: &str) -> StdioParams {
        StdioParams {
            name: "fetch".to_string(),
            description: None,
            command: command.to_string(),
            args: vec!["--serve".to_string()],
            env: None,
            cwd: None,
        }
    }

    #[test]
    fn a_relative_command_is_resolved_against_the_asset_root() {
        let root = AssetRoot::new("assets");
        let resolved = resolved_against(&params("./mcp/fetch/serve"), &root).expect("parse");
        let expected = root.root().join("mcp").join("fetch").join("serve");
        assert_eq!(Path::new(&resolved.command), expected.as_path());
        assert_eq!(resolved.args, vec!["--serve".to_string()]);
    }

    #[test]
    fn a_command_that_names_no_asset_is_left_alone() {
        let root = AssetRoot::new("assets");
        let resolved = resolved_against(&params("uvx"), &root).expect("leave path commands alone");
        assert_eq!(resolved.command, "uvx");
    }

    #[test]
    fn a_command_looked_up_on_the_path_keeps_its_arguments() {
        let root = AssetRoot::new("assets");
        let resolved = resolved_against(&params("node"), &root).expect("leave path commands alone");
        assert_eq!(resolved.command, "node");
        assert_eq!(
            resolved.args,
            vec!["--serve".to_string()],
            "a command passed through unchanged keeps its arguments too"
        );
    }

    #[test]
    fn a_relative_command_that_leaves_the_asset_root_is_refused() {
        let root = AssetRoot::new("assets");
        let error = resolved_against(&params("./../serve"), &root)
            .expect_err("refuse a command outside the root");
        match error {
            ConnectorError::UnresolvedCommand { command } => assert_eq!(command, "./../serve"),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}
