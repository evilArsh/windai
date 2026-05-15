use super::McpError;
use super::connector::ServerHandle;
use super::{
    CallToolParam, CallToolResult, ClientEvent, ClientSnapshot, ClientStatus, Prompt, Resource,
    ServerParams, Tool,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc, oneshot};

// static REGISTRY: OnceLock<RegistryHandle> = OnceLock::new();
enum ServerState {
    Connecting { waiter: Arc<ConnectingWaiter> },
    Connected,
    Disconnecting,
}

struct ConnectingWaiter {
    notify: Arc<tokio::sync::Notify>,
    result: Arc<Mutex<Option<WaiterResult>>>,
}

enum WaiterResult {
    Connected(ClientSnapshot),
    Failed { error: String },
}

struct ServerEntry {
    state: ServerState,
    ref_sessions: HashSet<String>,
    params: ServerParams,
    handle: Option<ServerHandle>,
}

impl ServerEntry {
    fn status(&self) -> ClientStatus {
        match self.state {
            ServerState::Connecting { .. } => ClientStatus::Connecting,
            ServerState::Connected => ClientStatus::Connected,
            ServerState::Disconnecting => ClientStatus::Disconnected,
        }
    }

    fn snapshot(&self) -> ClientSnapshot {
        ClientSnapshot {
            name: self.params.get_name().into_owned(),
            transport: self.params.get_transport(),
            status: self.status(),
            ref_sessions: self.ref_sessions.clone(),
        }
    }
}

enum RegistryRequest {
    Acquire {
        session_id: String,
        params: ServerParams,
        reply: oneshot::Sender<Result<ClientSnapshot, McpError>>,
    },
    Release {
        session_id: String,
        name: String,
        reply: oneshot::Sender<Result<ClientSnapshot, McpError>>,
    },
    ListClients {
        reply: oneshot::Sender<Vec<ClientSnapshot>>,
    },
    GetClient {
        name: String,
        reply: oneshot::Sender<Option<ClientSnapshot>>,
    },
    ListTools {
        name: String,
        reply: oneshot::Sender<Result<Vec<Tool>, McpError>>,
    },
    ListAllTools {
        reply: oneshot::Sender<Result<Vec<Tool>, McpError>>,
    },
    ListPrompts {
        name: String,
        reply: oneshot::Sender<Result<Vec<Prompt>, McpError>>,
    },
    ListResources {
        name: String,
        reply: oneshot::Sender<Result<Vec<Resource>, McpError>>,
    },
    CallTool {
        param: CallToolParam,
        reply: oneshot::Sender<Result<CallToolResult, McpError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct RegistryHandle {
    tx: mpsc::Sender<RegistryRequest>,
    event_tx: broadcast::Sender<ClientEvent>,
}

impl RegistryHandle {
    /// 订阅 MCP 服务事件
    pub fn subscribe(&self) -> broadcast::Receiver<ClientEvent> {
        self.event_tx.subscribe()
    }

    /// 启动一个 MCP 服务
    /// - 不同的 session 共享同一个 Stdio 服务
    /// - 多个 stdio MCP 服务同时启动`同一个` node/python 包时，必须按序等待执行完毕，以避免其 cli 工具发生文件写冲突
    pub async fn acquire(
        &self,
        session_id: &str,
        params: ServerParams,
    ) -> Result<ClientSnapshot, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::Acquire {
                session_id: session_id.to_string(),
                params,
                reply,
            })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    /// 停止一个 MCP 服务
    /// - 如果 MCP 服务正在被多个 session 共享，则只删除该 session 的引用
    /// - 没有 session 使用该服务时，服务将停止
    pub async fn release(&self, session_id: &str, name: &str) -> Result<ClientSnapshot, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::Release {
                session_id: session_id.to_string(),
                name: name.to_string(),
                reply,
            })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    /// 列出所有 MCP 客户端
    pub async fn list_clients(&self) -> Vec<ClientSnapshot> {
        let (reply, rx) = oneshot::channel();
        let _ = self.tx.send(RegistryRequest::ListClients { reply }).await;
        rx.await.unwrap_or_default()
    }

    /// 根据 server name 获取对应 MCP 客户端
    pub async fn get_client(&self, name: &str) -> Option<ClientSnapshot> {
        let (reply, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(RegistryRequest::GetClient {
                name: name.to_string(),
                reply,
            })
            .await;
        rx.await.ok()?
    }

    /// 列出指定 server name 的工具
    pub async fn list_tools(&self, name: &str) -> Result<Vec<Tool>, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::ListTools {
                name: name.to_string(),
                reply,
            })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    /// 列出所有工具
    pub async fn list_all_tools(&self) -> Result<Vec<Tool>, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::ListAllTools { reply })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    /// 列出指定 server id 的提示
    pub async fn list_prompts(&self, name: &str) -> Result<Vec<Prompt>, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::ListPrompts {
                name: name.to_string(),
                reply,
            })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    /// 列出指定 server name 的资源
    pub async fn list_resources(&self, name: &str) -> Result<Vec<Resource>, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::ListResources {
                name: name.to_string(),
                reply,
            })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    /// 调用工具
    pub async fn call_tool(&self, param: &CallToolParam) -> Result<CallToolResult, McpError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryRequest::CallTool {
                param: param.clone(),
                reply,
            })
            .await
            .map_err(|_| McpError::ManagerShutdown)?;
        rx.await.map_err(|_| McpError::ManagerShutdown)?
    }

    pub async fn shutdown(&self) {
        let (reply, rx) = oneshot::channel();
        let _ = self.tx.send(RegistryRequest::Shutdown { reply }).await;
        let _ = rx.await;
    }
}

pub struct Registry {
    rx: mpsc::Receiver<RegistryRequest>,
    event_tx: broadcast::Sender<ClientEvent>,
    servers: HashMap<String, ServerEntry>,
}

impl Registry {
    const CMD_CHANNEL_SIZE: usize = 64;
    const EVENT_CHANNEL_SIZE: usize = 256;

    // pub fn global() -> &'static RegistryHandle {
    //     REGISTRY.get_or_init(|| Registry::new())
    // }
    pub fn new() -> RegistryHandle {
        let (tx, rx) = mpsc::channel(Self::CMD_CHANNEL_SIZE);
        let (event_tx, _) = broadcast::channel(Self::EVENT_CHANNEL_SIZE);

        let registry = Self {
            rx,
            event_tx: event_tx.clone(),
            servers: HashMap::new(),
        };
        tokio::spawn(registry.run());

        RegistryHandle { tx, event_tx }
    }

    fn broadcast(&self, event: ClientEvent) {
        let _ = self.event_tx.send(event);
    }

    #[inline]
    fn get_handle(&self, server_name: &str) -> Result<&ServerHandle, McpError> {
        let entry = self.get_entry(server_name)?;
        let handle = entry
            .handle
            .as_ref()
            .ok_or(McpError::ServerNotFound(server_name.to_string()))?;

        Ok(handle)
    }

    #[inline]
    fn get_entry(&self, server_name: &str) -> Result<&ServerEntry, McpError> {
        let entry = self
            .servers
            .get(server_name)
            .ok_or(McpError::ServerNotFound(server_name.to_string()))?;

        Ok(entry)
    }

    #[inline]
    fn get_entry_mut(&mut self, server_name: &str) -> Result<&mut ServerEntry, McpError> {
        let entry = self
            .servers
            .get_mut(server_name)
            .ok_or(McpError::ServerNotFound(server_name.to_string()))?;

        Ok(entry)
    }

    async fn run(mut self) {
        loop {
            let Some(request) = self.rx.recv().await else {
                break;
            };

            match request {
                RegistryRequest::Acquire {
                    session_id,
                    params,
                    reply,
                } => {
                    let result = self.acquire(&session_id, params).await;
                    let _ = reply.send(result);
                }
                RegistryRequest::Release {
                    session_id,
                    name,
                    reply,
                } => {
                    let result = self.release(&session_id, &name).await;
                    let _ = reply.send(result);
                }
                RegistryRequest::ListClients { reply } => {
                    let _ = reply.send(self.list_clients());
                }
                RegistryRequest::GetClient { name, reply } => {
                    let _ = reply.send(self.get_client(&name));
                }
                RegistryRequest::ListTools { name, reply } => {
                    let _ = reply.send(self.list_tools(&name).await);
                }
                RegistryRequest::ListAllTools { reply } => {
                    let _ = reply.send(self.list_all_tools().await);
                }
                RegistryRequest::ListPrompts { name, reply } => {
                    let _ = reply.send(self.list_prompts(&name).await);
                }
                RegistryRequest::ListResources { name, reply } => {
                    let _ = reply.send(self.list_resources(&name).await);
                }
                RegistryRequest::CallTool { param, reply } => {
                    let _ = reply.send(self.call_tool(&param).await);
                }
                RegistryRequest::Shutdown { reply } => {
                    self.shutdown().await;
                    let _ = reply.send(());
                    return;
                }
            }
        }
    }

    async fn acquire(
        &mut self,
        session_id: &str,
        params: ServerParams,
    ) -> Result<ClientSnapshot, McpError> {
        let name = params.get_name().into_owned();
        if let Some(entry) = self.servers.get_mut(&name) {
            match &entry.state {
                ServerState::Connected => {
                    entry.ref_sessions.insert(session_id.to_string());
                    return Ok(entry.snapshot());
                }
                ServerState::Connecting { waiter } => {
                    let waiter = waiter.clone();
                    return self.wait_for_connecting(waiter).await;
                }
                ServerState::Disconnecting => {
                    self.servers.remove(&name);
                }
            }
        }

        let waiter = Arc::new(ConnectingWaiter {
            notify: Arc::new(tokio::sync::Notify::new()),
            result: Arc::new(std::sync::Mutex::new(None)),
        });

        self.servers.insert(
            name.clone(),
            ServerEntry {
                state: ServerState::Connecting { waiter },
                ref_sessions: HashSet::from([session_id.to_string()]),
                params: params.clone(),
                handle: None,
            },
        );

        self.broadcast(ClientEvent::Connecting { name: name.clone() });

        let connect_result = ServerHandle::connect(&params).await;

        let entry = match self.servers.get_mut(&name) {
            Some(entry) => entry,
            None => {
                log::error!("Error after connection, server not found,  name: {}", &name);
                return Err(McpError::ServerNotFound(name));
            }
        };
        let name = entry.params.get_name().into_owned();
        match connect_result {
            Ok(handle) => {
                entry.handle = Some(handle);
                if let ServerState::Connecting { waiter: w } =
                    std::mem::replace(&mut entry.state, ServerState::Connected)
                {
                    let snapshot = entry.snapshot();
                    *w.result.lock().unwrap() = Some(WaiterResult::Connected(snapshot.clone()));
                    w.notify.notify_waiters();

                    self.broadcast(ClientEvent::Connected { name });
                    return Ok(snapshot);
                } else {
                    log::warn!(
                        "server's previous state was not 'Connecting' after connected, name: {}",
                        &name
                    );
                }
                Ok(entry.snapshot())
            }
            Err(e) => {
                let error_msg = e.to_string();

                if let ServerState::Connecting { waiter: w } =
                    std::mem::replace(&mut entry.state, ServerState::Disconnecting)
                {
                    *w.result.lock().unwrap() = Some(WaiterResult::Failed {
                        error: error_msg.clone(),
                    });
                    w.notify.notify_waiters();
                }

                self.servers.remove(&name);

                self.broadcast(ClientEvent::Error {
                    name,
                    error: error_msg.clone(),
                });

                Err(McpError::Other(error_msg))
            }
        }
    }

    async fn wait_for_connecting(
        &self,
        waiter: Arc<ConnectingWaiter>,
    ) -> Result<ClientSnapshot, McpError> {
        waiter.notify.notified().await;
        let result = waiter
            .result
            .lock()
            .unwrap()
            .take()
            .expect("connecting waiter result was not set");

        match result {
            WaiterResult::Connected(snapshot) => Ok(snapshot),
            WaiterResult::Failed { error } => Err(McpError::Other(error)),
        }
    }

    async fn release(&mut self, session_id: &str, name: &str) -> Result<ClientSnapshot, McpError> {
        let entry = self.get_entry_mut(name)?;
        entry.ref_sessions.remove(session_id);
        if !entry.ref_sessions.is_empty() {
            return Ok(entry.snapshot());
        }
        entry.state = ServerState::Disconnecting;
        if let Some(handle) = entry.handle.take() {
            handle.disconnect().await;
        }
        let snapshot = entry.snapshot();
        let name = entry.params.get_name().into_owned();
        self.servers.remove(&name);

        self.broadcast(ClientEvent::Disconnected {
            name,
            reason: "normal shutdown".to_string(),
        });

        Ok(snapshot)
    }

    fn list_clients(&self) -> Vec<ClientSnapshot> {
        self.servers.values().map(|e| e.snapshot()).collect()
    }

    fn get_client(&self, name: &str) -> Option<ClientSnapshot> {
        self.servers.get(name).map(|e| e.snapshot())
    }

    async fn list_tools(&self, name: &str) -> Result<Vec<Tool>, McpError> {
        let handle = self.get_handle(name)?;
        Ok(handle
            .list_tools()
            .await?
            .into_iter()
            .map(|t| Tool::new(name, &t))
            .collect())
    }

    async fn list_all_tools(&self) -> Result<Vec<Tool>, McpError> {
        let handles: Vec<(String, &ServerHandle)> = self
            .servers
            .values()
            .filter_map(|entry| {
                entry
                    .handle
                    .as_ref()
                    .map(|h| (entry.params.get_name().into_owned(), h))
            })
            .collect();

        let futures = handles.into_iter().map(|(name, handle)| async move {
            let tools = handle
                .list_tools()
                .await?
                .into_iter()
                .map(|t| Tool::new(&name, &t))
                .collect::<Vec<Tool>>();
            Ok::<Vec<Tool>, McpError>(tools)
        });

        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().flat_map(|v| v).collect())
    }
    async fn list_prompts(&self, name: &str) -> Result<Vec<Prompt>, McpError> {
        let handle = self.get_handle(name)?;
        Ok(handle
            .list_prompts()
            .await?
            .into_iter()
            .map(|t| t.into())
            .collect())
    }

    async fn list_resources(&self, name: &str) -> Result<Vec<Resource>, McpError> {
        let handle = self.get_handle(name)?;
        Ok(handle
            .list_resources()
            .await?
            .into_iter()
            .map(|t| t.into())
            .collect())
    }

    async fn call_tool(&self, param: &CallToolParam) -> Result<CallToolResult, McpError> {
        let handle = self.get_handle(&param.server_name)?;
        handle
            .call_tool(&param.tool_name, param.arguments.as_ref())
            .await
            .map(CallToolResult::from)
    }

    async fn shutdown(&mut self) {
        let ids: Vec<String> = self.servers.keys().cloned().collect();
        for id in ids {
            if let Some(entry) = self.servers.remove(&id) {
                if let Some(handle) = entry.handle {
                    handle.disconnect().await;
                }
                self.broadcast(ClientEvent::Disconnected {
                    name: entry.params.get_name().into_owned(),
                    reason: "shutdown".to_string(),
                });
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::StdioParams;

    // use std::sync::OnceLock;
    // static REGISTRY: OnceLock<RegistryHandle> = OnceLock::new();

    fn everything_params() -> ServerParams {
        ServerParams::Stdio(StdioParams {
            name: "test-everything".to_string(),
            description: None,
            command: "npx".to_string(),
            args: vec![
                "-y".to_string(),
                "@modelcontextprotocol/server-everything".to_string(),
            ],
            env: None,
        })
    }

    // fn fetch_params() -> ServerParams {
    //     ServerParams::Stdio(StdioParams {
    //         id: "test-fetch".to_string(),
    //         name: "mcp-server-fetch".to_string(),
    //         description: None,
    //         command: "uvx".to_string(),
    //         args: vec!["mcp-server-fetch".to_string()],
    //         env: None,
    //     })
    // }

    // fn setup_registry() -> &'static RegistryHandle {
    //     REGISTRY.get_or_init(|| {
    //         let _ = env_logger::builder().is_test(true).try_init();
    //         Registry::new()
    //     })
    // }
    fn setup_registry() -> RegistryHandle {
        let _ = env_logger::builder().is_test(true).try_init();
        Registry::new()
    }

    #[tokio::test]
    #[ignore = "need install uv,bun"]
    async fn test_acquire_emits_connecting_and_connected_events() {
        let handle = setup_registry();
        let params = everything_params();

        let mut rx = handle.subscribe();

        let handle_clone = handle.clone();
        let params_clone = params.clone();
        let acquire_task =
            tokio::spawn(async move { handle_clone.acquire("session-1", params_clone).await });

        let mut got_connecting = false;
        let mut got_connected = false;

        loop {
            tokio::select! {
                event = rx.recv() => {
                    if let Ok(event) = event {
                        match &event {
                            ClientEvent::Connecting { name, .. } if name == "test-everything" => {
                                log::debug!("Got Connecting event for {}", name);
                                got_connecting = true;
                            }
                            ClientEvent::Connected { name, .. } if name == "test-everything" => {
                                log::debug!("Got Connected event for {}", name);
                                got_connected = true;
                                break;
                            }
                            ClientEvent::Disconnected { name, .. } if name == "test-everything" => {
                                log::debug!("Got Disconnected event for {}", name);
                                break;
                            }
                            _ => {
                                panic!()
                            }
                        }
                    } else {
                        panic!()
                    }
                }
            }
        }

        let result = acquire_task.await.expect("acquire task");
        assert!(result.is_ok());

        assert!(got_connecting, "should have received Connecting event");
        assert!(got_connected, "should have received Connected event");

        handle.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "need install uv,bun"]
    async fn test_duplicate_acquire_same_id() {
        let handle = setup_registry();
        let params = everything_params();
        let name = params.get_name();

        let snapshot1 = handle
            .acquire("session-1", params.clone())
            .await
            .expect("first acquire");

        let snapshot2 = handle
            .acquire("session-2", params.clone())
            .await
            .expect("second acquire");
        assert_eq!(snapshot1.name, snapshot2.name);

        let client = handle.get_client(&name).await;
        assert!(client.is_some());
        assert_eq!(client.unwrap().ref_sessions.len(), 2);

        handle.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "need install uv,bun"]
    async fn test_release_single_session() {
        let handle = setup_registry();
        let params = everything_params();
        let id = params.get_name();

        handle
            .acquire("session-1", params.clone())
            .await
            .expect("acquire");

        let snapshot = handle.release("session-1", &id).await.expect("release");
        assert_eq!(snapshot.status, ClientStatus::Disconnected);

        // Server should be gone after last reference releases
        let client = handle.get_client(&id).await;
        assert!(client.is_none());

        handle.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "need install uv,bun"]
    async fn test_release_shared_session_keeps_server() {
        let handle = setup_registry();
        let params = everything_params();
        let id = params.get_name();

        handle
            .acquire("session-1", params.clone())
            .await
            .expect("first");
        handle
            .acquire("session-2", params.clone())
            .await
            .expect("second");

        let snapshot = handle.release("session-1", &id).await.expect("release s1");
        assert_eq!(snapshot.status, ClientStatus::Connected);

        let client = handle.get_client(&id).await;
        assert!(client.is_some());
        assert_eq!(client.unwrap().ref_sessions.len(), 1);

        handle.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "need install uv,bun"]
    async fn test_release_emits_disconnected_event() {
        let handle = setup_registry();
        let params = everything_params();
        let p_name = params.get_name();

        handle
            .acquire("session-1", params.clone())
            .await
            .expect("acquire");

        let mut rx = handle.subscribe();

        handle
            .release("session-1", "test-everything")
            .await
            .expect("release");

        let mut got_disconnected = false;
        loop {
            tokio::select! {
                event = rx.recv() => {
                    if let Ok(event) = event {
                        if let ClientEvent::Disconnected { name, .. } = event {
                            if name == p_name {
                                got_disconnected = true;
                                break;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    break;
                }
            }
        }

        assert!(got_disconnected, "should have received Disconnected event");

        handle.shutdown().await;
    }

    #[tokio::test]
    #[ignore = "need install uv,bun"]
    async fn test_full_lifecycle_acquire_use_release() {
        let handle = setup_registry();
        let params = everything_params();
        let name = params.get_name();

        // 1. Acquire
        let snapshot = handle
            .acquire("session-1", params.clone())
            .await
            .expect("acquire");
        assert_eq!(snapshot.status, ClientStatus::Connected);

        // 2. List tools
        let tools = handle.list_tools(&name).await.expect("list tools");
        assert!(!tools.is_empty());

        // 3. Call a tool
        let result = handle
            .call_tool(&CallToolParam {
                server_name: name.into_owned(),
                tool_name: "echo".to_string(),
                arguments: Some(
                    serde_json::from_value(serde_json::json!({ "message": "lifecycle test" }))
                        .unwrap(),
                ),
            })
            .await
            .expect("echo call");
        assert!(!result.is_error.is_some_and(|f| f == true));

        // 4. Release
        let released = handle
            .release("session-1", "test-everything")
            .await
            .expect("release");
        assert_eq!(released.status, ClientStatus::Disconnected);

        // 5. Verify gone
        assert!(handle.get_client("test-everything").await.is_none());

        handle.shutdown().await;
    }
}
