use std::process::Stdio;
use std::time::{Duration, Instant};

use rmcp::model::{CallToolRequestParams, CallToolResponse, ProtocolVersion, Tool};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt, DynService, Peer, RunningService};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransport;
use rmcp::transport::{IntoTransport, TokioChildProcess};
use rmcp::{RoleClient, ServiceExt};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wind_wire::McpConfig;

use crate::config::{self, Environment, McpOptions};
use crate::error::{McpError, Result};
use crate::identity::{self, ConnectionKey};
use crate::process;

type Client = RunningService<RoleClient, Box<dyn DynService<RoleClient>>>;

/// 远端工具的定义
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteTool {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

impl From<Tool> for RemoteTool {
    fn from(tool: Tool) -> Self {
        Self {
            name: tool.name.into_owned(),
            description: tool.description.map(|description| description.into_owned()),
            input_schema: Value::Object((*tool.input_schema).clone()),
        }
    }
}

/// 按配置建立 MCP 客户端连接
pub struct Connector {
    environment: Environment,
    options: McpOptions,
}

impl Connector {
    pub fn new(environment: Environment, options: McpOptions) -> Self {
        Self {
            environment,
            options,
        }
    }

    /// 使用宿主进程当前的环境变量
    pub fn from_host(options: McpOptions) -> Self {
        Self::new(config::environment_snapshot(), options)
    }

    pub fn options(&self) -> &McpOptions {
        &self.options
    }

    /// 计算配置的连接身份，同时解析可执行文件
    pub fn key(&self, config: &McpConfig) -> Result<ConnectionKey> {
        identity::connection_key(config, &self.environment)
    }

    pub async fn connect(&self, config: &McpConfig) -> Result<Connection> {
        let key = self.key(config)?;
        let client = match &key {
            ConnectionKey::Stdio {
                program,
                args,
                environment,
                cwd,
            } => {
                let command = process::command(program, args, cwd, environment);
                let (transport, _stderr) = TokioChildProcess::builder(command)
                    .stderr(Stdio::null())
                    .spawn()
                    .map_err(|error| McpError::Connect(error.to_string()))?;
                self.handshake(transport).await?
            }
            ConnectionKey::StreamableHttp { url } => {
                let transport = StreamableHttpClientTransport::from_uri(url.as_str());
                self.handshake(transport).await?
            }
        };
        Ok(Connection::new(
            client,
            self.options.list_timeout,
            self.options.call_timeout,
            self.options.shutdown_grace,
        ))
    }

    /// 在一个已有的双向流上建立客户端，供进程内管道使用
    pub async fn connect_duplex<R, W>(&self, reader: R, writer: W) -> Result<Connection>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let client = self.handshake((reader, writer)).await?;
        Ok(Connection::new(
            client,
            self.options.list_timeout,
            self.options.call_timeout,
            self.options.shutdown_grace,
        ))
    }

    async fn handshake<T, E, A>(&self, transport: T) -> Result<Client>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let started = tokio::time::timeout(
            self.options.connect_timeout,
            ().into_dyn()
                .serve_with_lifecycle(transport, Self::lifecycle()),
        );
        match started.await {
            Ok(Ok(client)) => Ok(client),
            Ok(Err(error)) => Err(McpError::Connect(error.to_string())),
            Err(_) => Err(McpError::Timeout),
        }
    }

    /// 优先协商最新协议，对旧服务端回退到 initialize
    fn lifecycle() -> ClientLifecycleMode {
        ClientLifecycleMode::Auto {
            preferred_versions: vec![ProtocolVersion::LATEST],
            legacy_version: Some(ProtocolVersion::LATEST_WITH_INITIALIZE),
        }
    }
}

/// 一条已握手的连接
pub struct Connection {
    peer: Peer<RoleClient>,
    service: Mutex<Option<Client>>,
    list_timeout: Duration,
    call_timeout: Duration,
    shutdown_grace: Duration,
}

impl Connection {
    fn new(
        client: Client,
        list_timeout: Duration,
        call_timeout: Duration,
        shutdown_grace: Duration,
    ) -> Self {
        Self {
            peer: client.peer().clone(),
            service: Mutex::new(Some(client)),
            list_timeout,
            call_timeout,
            shutdown_grace,
        }
    }

    /// 读取全部工具，自动翻页
    pub async fn list_tools(&self, cancel: CancellationToken) -> Result<Vec<RemoteTool>> {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(McpError::Cancelled),
            result = tokio::time::timeout(self.list_timeout, self.peer.list_all_tools()) => result,
        };
        match result {
            Ok(Ok(tools)) => Ok(tools.into_iter().map(RemoteTool::from).collect()),
            Ok(Err(error)) => Err(McpError::Service(error)),
            Err(_) => Err(McpError::Timeout),
        }
    }

    /// 调用工具并返回完整的调用结果
    pub async fn call(
        &self,
        tool: &str,
        arguments: Value,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> Result<Value> {
        let arguments = match arguments {
            Value::Object(arguments) => arguments,
            _ => return Err(McpError::ArgumentsNotObject),
        };
        let budget = self.budget(deadline)?;
        let params = CallToolRequestParams::new(tool.to_string()).with_arguments(arguments);
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(McpError::Cancelled),
            result = tokio::time::timeout(budget, self.peer.call_tool_once(params)) => result,
        };
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return Err(McpError::Service(error)),
            Err(_) => return Err(McpError::Timeout),
        };
        match response {
            CallToolResponse::Complete(result) => {
                serde_json::to_value(&result).map_err(|error| McpError::Encode(error.to_string()))
            }
            _ => Err(McpError::UnsupportedResponse),
        }
    }

    /// 关闭连接并等待清理
    pub async fn close(&self) -> Result<()> {
        let mut service = self.service.lock().await;
        let Some(mut client) = service.take() else {
            return Ok(());
        };
        client
            .close_with_timeout(self.shutdown_grace)
            .await
            .map_err(|error| McpError::Shutdown(error.to_string()))?;
        Ok(())
    }

    fn budget(&self, deadline: Instant) -> Result<Duration> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(McpError::Timeout)?;
        Ok(remaining.min(self.call_timeout))
    }
}
