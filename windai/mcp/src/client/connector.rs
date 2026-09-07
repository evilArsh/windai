use super::McpError;
use super::cmd_normalizer;
use super::{ServerParams, StdioParams};
use rmcp::ServiceError;
use rmcp::model::ContentBlock;
use rmcp::model::TextContent;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Prompt, Resource, Tool,
};
use rmcp::service::RunningService;
use rmcp::transport::{ConfigureCommandExt, StreamableHttpClientTransport, TokioChildProcess};
use rmcp::{RoleClient, ServerHandler, ServiceExt, service::DynService};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tokio::sync::Notify;

type ClientService = RunningService<RoleClient, Box<dyn DynService<RoleClient>>>;

enum DedupState {
    InFlight { notify: Arc<Notify> },
}

static DEDUP_MAP: LazyLock<Mutex<HashMap<String, DedupState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub struct ServerHandle {
    service: ClientService,
}

impl ServerHandle {
    pub async fn connect(params: &ServerParams) -> Result<Self, McpError> {
        match params {
            ServerParams::Stdio(stdio) => {
                if let Some(normalized) = cmd_normalizer::normalize(stdio) {
                    Self::connect_with_dedup(stdio, &normalized).await
                } else {
                    Self::connect_direct(params).await
                }
            }
            ServerParams::Streamable(_) => Self::connect_direct(params).await,
        }
    }

    /// 连接内建内存服务。
    pub async fn connect_builtin<H>(handler: H) -> Result<Self, McpError>
    where
        H: ServerHandler + Send + Sync + 'static,
    {
        let (server_tx, client_rx) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            match handler.serve(server_tx).await {
                Ok(service) => {
                    let _ = service.waiting().await;
                }
                Err(e) => log::error!("builtin server serve failed: {}", e),
            }
        });
        let service = ().into_dyn().serve(client_rx).await?;
        Ok(Self { service })
    }

    async fn connect_direct(params: &ServerParams) -> Result<Self, McpError> {
        let service = match params {
            ServerParams::Stdio(params) => {
                let cmd: Command = Command::new(&params.command).configure(|cmd| {
                    cmd.args(&params.args);
                    if let Some(env) = &params.env {
                        for (k, v) in env {
                            cmd.env(k, v);
                        }
                    }
                });
                let transport = TokioChildProcess::new(cmd)?;
                // TODO: MRTR
                ().into_dyn().serve(transport).await?
            }
            ServerParams::Streamable(params) => {
                let transport = StreamableHttpClientTransport::from_uri(&*params.url);
                ().into_dyn().serve(transport).await?
            }
        };
        Ok(Self { service })
    }

    async fn connect_with_dedup(
        params: &StdioParams,
        normalized: &cmd_normalizer::NormalizedCommand,
    ) -> Result<Self, McpError> {
        let key = &normalized.dedup_key;

        loop {
            let notify;
            let is_connector;
            {
                let mut map = DEDUP_MAP.lock().unwrap();
                match map.get(key.as_str()) {
                    Some(DedupState::InFlight { notify: n }) => {
                        notify = n.clone();
                        is_connector = false;
                    }
                    None => {
                        notify = Arc::new(Notify::new());
                        map.insert(
                            key.clone(),
                            DedupState::InFlight {
                                notify: notify.clone(),
                            },
                        );
                        is_connector = true;
                    }
                }
            }

            if is_connector {
                let stdio = StdioParams {
                    command: normalized.command.clone(),
                    args: normalized.args.clone(),
                    ..params.clone()
                };
                let result = Self::connect_direct(&ServerParams::Stdio(stdio)).await;
                {
                    let mut map = DEDUP_MAP.lock().unwrap();
                    map.remove(key.as_str());
                }
                notify.notify_waiters();
                return result;
            } else {
                notify.notified().await;
            }
        }
    }

    pub async fn disconnect(mut self) {
        let _ = self.service.close().await;
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Option<&Map<String, Value>>,
    ) -> Result<CallToolResult, McpError> {
        let params = CallToolRequestParams::new(name.to_owned());
        let params = if let Some(args) = arguments {
            params.with_arguments(args.clone())
        } else {
            params
        };
        // https://modelcontextprotocol.io/seps/2322-MRTR
        // 迁移日志 https://github.com/modelcontextprotocol/rust-sdk/discussions/969
        match self.service.call_tool_once(params).await? {
            CallToolResponse::Complete(result) => Ok(result),
            // TODO: MRTR
            CallToolResponse::InputRequired(res) => {
                let res_str = match serde_json::to_string(&res) {
                    Ok(json) => json,
                    Err(_) => String::from("{}"),
                };
                Ok(CallToolResult::success(vec![ContentBlock::Text(
                    TextContent::new(res_str),
                )]))
            }
            CallToolResponse::Task(_) => Err(McpError::Service(ServiceError::UnexpectedResponse)),
            _ => Err(McpError::Service(ServiceError::UnexpectedResponse)),
        }
    }

    pub async fn list_tools(&self) -> Result<Vec<Tool>, McpError> {
        self.service
            .list_all_tools()
            .await
            .map_err(McpError::Service)
    }

    pub async fn list_prompts(&self) -> Result<Vec<Prompt>, McpError> {
        self.service
            .list_all_prompts()
            .await
            .map_err(McpError::Service)
    }

    pub async fn list_resources(&self) -> Result<Vec<Resource>, McpError> {
        self.service
            .list_all_resources()
            .await
            .map_err(McpError::Service)
    }
}
