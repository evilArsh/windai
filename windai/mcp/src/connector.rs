use crate::McpOptions;
use crate::error::{McpError, Result};
use crate::runner::LaunchPlan;
use process_wrap::tokio::CommandWrap;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use rmcp::model::{CallToolRequestParams, CallToolResponse, ProtocolVersion, Tool};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt, DynService, Peer, RunningService};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransport;
use rmcp::transport::{IntoTransport, TokioChildProcess};
use rmcp::{RoleClient, ServiceExt};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::process::{ChildStderr, Command};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use wind_wire::mcp::{McpConfig, McpDefinition};

type Client = RunningService<RoleClient, Box<dyn DynService<RoleClient>>>;

/// stderr 单行保留的字节数
const STDERR_LINE_LIMIT: usize = 64 * 1024;

/// 接收服务端 stderr 的一行
pub(crate) type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone, Debug, PartialEq)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

impl From<Tool> for ToolDescriptor {
    fn from(tool: Tool) -> Self {
        Self {
            name: tool.name.into_owned(),
            description: tool.description.map(|description| description.into_owned()),
            input_schema: Value::Object((*tool.input_schema).clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CallOutcome {
    Ok(Value),
    Failed(String),
}

pub struct Connection {
    peer: Peer<RoleClient>,
    service: Mutex<Option<Client>>,
    options: McpOptions,
    closed: AtomicBool,
    inflight: AtomicUsize,
    idle: Notify,
    /// 进程组 id，0 表示没有子进程
    #[cfg(unix)]
    pgid: AtomicU32,
    #[cfg(unix)]
    group_killed: AtomicBool,
}

struct Inflight<'a>(&'a Connection);

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        if self.0.inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

impl Connection {
    pub async fn open(
        definition: &McpDefinition,
        plan: &LaunchPlan,
        options: &McpOptions,
        log: Option<LogSink>,
    ) -> Result<Arc<Connection>> {
        match &definition.config {
            McpConfig::Stdio { env, .. } => {
                let command = command(plan, env);
                let (transport, stderr) = TokioChildProcess::builder(command)
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|error| McpError::Connect(error.to_string()))?;
                #[cfg(unix)]
                let pgid = transport.id();
                if let Some(stderr) = stderr {
                    drain_stderr(stderr, log);
                }
                let connection = handshake(transport, options).await?;
                #[cfg(unix)]
                if let Some(pgid) = pgid {
                    connection.pgid.store(pgid, Ordering::SeqCst);
                }
                Ok(connection)
            }
            McpConfig::StreamableHttp { url } => {
                let transport = StreamableHttpClientTransport::from_uri(url.as_str());
                handshake(transport, options).await
            }
        }
    }

    pub async fn attach<R, W>(reader: R, writer: W, options: &McpOptions) -> Result<Arc<Connection>>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        handshake((reader, writer), options).await
    }

    pub async fn list_tools(&self) -> Result<Vec<ToolDescriptor>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(McpError::ShuttingDown);
        }
        match tokio::time::timeout(self.options.list_timeout, self.peer.list_all_tools()).await {
            Ok(Ok(tools)) => Ok(tools.into_iter().map(ToolDescriptor::from).collect()),
            Ok(Err(error)) => Err(McpError::Service(error.to_string())),
            Err(_) => Err(McpError::Timeout),
        }
    }

    pub async fn call(
        &self,
        tool: &str,
        arguments: Value,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> CallOutcome {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let _inflight = Inflight(self);
        self.call_inner(tool, arguments, cancel, deadline).await
    }

    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(self.options.shutdown_grace, self.wait_idle()).await;
        let mut service = self.service.lock().await;
        let Some(mut client) = service.take() else {
            #[cfg(unix)]
            self.kill_group();
            return;
        };
        match client.close_with_timeout(self.options.shutdown_grace).await {
            Ok(Some(_)) => {}
            Ok(None) => log::warn!("the mcp connection did not close within the grace period"),
            Err(error) => log::warn!("cannot close the mcp connection: {error}"),
        }
        #[cfg(unix)]
        self.kill_group();
    }

    #[cfg(unix)]
    fn kill_group(&self) {
        let pgid = self.pgid.load(Ordering::SeqCst);
        if pgid == 0 || self.group_killed.swap(true, Ordering::SeqCst) {
            return;
        }
        // SAFETY: kill 只接收整数进程组 id，不涉及指针
        unsafe {
            libc::kill(-(pgid as i32), libc::SIGKILL);
        }
    }

    async fn call_inner(
        &self,
        tool: &str,
        arguments: Value,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> CallOutcome {
        if self.closed.load(Ordering::SeqCst) {
            return CallOutcome::Failed("the connection is closed".to_owned());
        }
        let Value::Object(arguments) = arguments else {
            return CallOutcome::Failed("the tool arguments must be a json object".to_owned());
        };
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return CallOutcome::Failed("the operation timed out".to_owned());
        };
        let budget = remaining.min(self.options.call_timeout);
        let params = CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments);
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return CallOutcome::Failed("the call was cancelled".to_owned()),
            result = tokio::time::timeout(budget, self.peer.call_tool_once(params)) => result,
        };
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return CallOutcome::Failed(error.to_string()),
            Err(_) => return CallOutcome::Failed("the operation timed out".to_owned()),
        };
        match response {
            CallToolResponse::Complete(result) => match serde_json::to_value(&result) {
                Ok(value) => CallOutcome::Ok(value),
                Err(error) => {
                    CallOutcome::Failed(format!("cannot serialize the tool result: {error}"))
                }
            },
            _ => CallOutcome::Failed("the server returned an unsupported tool response".to_owned()),
        }
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inflight.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn new(client: Client, options: McpOptions) -> Self {
        Self {
            peer: client.peer().clone(),
            service: Mutex::new(Some(client)),
            options,
            closed: AtomicBool::new(false),
            inflight: AtomicUsize::new(0),
            idle: Notify::new(),
            #[cfg(unix)]
            pgid: AtomicU32::new(0),
            #[cfg(unix)]
            group_killed: AtomicBool::new(false),
        }
    }
}

#[cfg(unix)]
impl Drop for Connection {
    fn drop(&mut self) {
        self.kill_group();
    }
}

async fn handshake<T, E, A>(transport: T, options: &McpOptions) -> Result<Arc<Connection>>
where
    T: IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let started = tokio::time::timeout(
        options.connect_timeout,
        ().into_dyn().serve_with_lifecycle(transport, lifecycle()),
    );
    match started.await {
        Ok(Ok(client)) => Ok(Arc::new(Connection::new(client, options.clone()))),
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

fn command(plan: &LaunchPlan, env: &BTreeMap<String, String>) -> CommandWrap {
    let mut command = Command::new(&plan.program);
    command
        .args(&plan.args)
        .current_dir(&plan.cwd)
        .env_clear()
        .kill_on_drop(true);
    for (key, value) in env {
        command.env(key, value);
    }
    #[cfg(unix)]
    {
        let mut wrapped = CommandWrap::from(command);
        wrapped.wrap(ProcessGroup::leader());
        wrapped
    }
    #[cfg(not(unix))]
    {
        CommandWrap::from(command)
    }
}

fn drain_stderr(stderr: ChildStderr, log: Option<LogSink>) {
    tokio::spawn(drain_reader(stderr, log));
}

async fn drain_reader<R>(reader: R, log: Option<LogSink>)
where
    R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    let mut truncated = false;
    loop {
        let available = match reader.fill_buf().await {
            Ok(available) => available,
            Err(error) => {
                log::warn!("cannot read the server stderr: {error}");
                break;
            }
        };
        if available.is_empty() {
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let (chunk, consumed) = match newline {
            Some(index) => (&available[..index], index + 1),
            None => (available, available.len()),
        };
        if !truncated {
            let room = STDERR_LINE_LIMIT - line.len();
            let take = chunk.len().min(room);
            line.extend_from_slice(&chunk[..take]);
            truncated = take < chunk.len();
        }
        reader.consume(consumed);
        if newline.is_some() {
            emit_line(&mut line, true, &log);
            truncated = false;
        }
    }
    if !line.is_empty() || truncated {
        emit_line(&mut line, false, &log);
    }
}

fn emit_line(line: &mut Vec<u8>, terminated: bool, log: &Option<LogSink>) {
    if terminated && line.ends_with(b"\r") {
        line.pop();
    }
    if let Some(log) = log {
        log(&String::from_utf8_lossy(line));
    }
    line.clear();
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rmcp::model::Tool;
    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    use super::{LogSink, STDERR_LINE_LIMIT, ToolDescriptor, drain_reader};

    #[test]
    fn tool_descriptor_keeps_the_schema() {
        let tool = Tool::new(
            "echo",
            "returns the arguments",
            json!({"type": "object"}).as_object().unwrap().clone(),
        );
        let descriptor = ToolDescriptor::from(tool);
        assert_eq!(descriptor.name, "echo");
        assert_eq!(
            descriptor.description.as_deref(),
            Some("returns the arguments")
        );
        assert_eq!(descriptor.input_schema, json!({"type": "object"}));
    }

    #[tokio::test]
    async fn stderr_reader_keeps_lines_after_invalid_utf8() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&captured);
        let log: LogSink = Arc::new(move |line| {
            sink.lock()
                .expect("lock the captured lines")
                .push(line.to_owned());
        });
        let task = tokio::spawn(drain_reader(reader, Some(log)));
        writer
            .write_all(b"ok\n\xFFbad\n")
            .await
            .expect("write the stderr bytes");
        drop(writer);
        task.await.expect("the stderr reader must finish");
        let captured = captured.lock().expect("lock the captured lines");
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0], "ok");
        assert_eq!(captured[1], "\u{FFFD}bad");
    }

    #[tokio::test]
    async fn stderr_reader_caps_a_long_line() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&captured);
        let log: LogSink = Arc::new(move |line| {
            sink.lock()
                .expect("lock the captured lines")
                .push(line.to_owned());
        });
        let task = tokio::spawn(drain_reader(reader, Some(log)));
        let mut payload = vec![b'x'; 100 * 1024];
        payload.extend_from_slice(b"\nok\n");
        writer
            .write_all(&payload)
            .await
            .expect("write the stderr bytes");
        drop(writer);
        task.await.expect("the stderr reader must finish");
        let captured = captured.lock().expect("lock the captured lines");
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].len(), STDERR_LINE_LIMIT);
        assert_eq!(captured[1], "ok");
    }
}
