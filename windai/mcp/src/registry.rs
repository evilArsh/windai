use super::builtin::NAMES;
use super::connector::ServerHandle;
use super::events::{
    CORE_MCP_ATTACH, CORE_MCP_CRASH, CORE_MCP_DETACH, CORE_MCP_SERVER_LIST, CORE_MCP_START,
    CORE_MCP_STOP, CORE_MCP_TOOL_CALL, CORE_MCP_TOOL_LIST, CORE_MCP_TOOL_RESULT, McpAttach,
    McpCrash, McpDetach, McpServerInfo, McpServerList, McpStart, McpStop, McpToolCall, McpToolInfo,
    McpToolList, McpToolResult, Transport,
};
use super::identity::{AssetRoot, Identity, IdentityError, identity_of};
use super::params::ServerParams;
use rmcp::ServerHandler;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;
use wind_event::context::DispatchCx;
use wind_event::def::EventDef;
use wind_event::dispatch::Dispatcher;

const BUILTIN_DESCRIPTION: &str = "provided by the host";

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("server {name} is not connected")]
    NotConnected { name: String },
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error("cannot reach {name}: {reason}")]
    Connect { name: String, reason: String },
    #[error("cannot create the directory {path}: {reason}")]
    Directory { path: String, reason: String },
    #[error("cannot derive the servers of {context}: {reason}")]
    Servers {
        /// 话题、mod 名或服务名
        context: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallOutcome {
    /// 实际发送的参数，可能被拦截链改写
    pub arguments: serde_json::Value,
    /// 服务返回的结果
    pub result: serde_json::Value,
}

#[derive(Debug, Clone)]
struct Origin {
    name: String,
    description: Option<String>,
    transport: Transport,
}

impl Origin {
    fn declared(params: &ServerParams) -> Self {
        Self {
            name: params.name().to_string(),
            description: params.description().map(str::to_string),
            transport: transport_of(params),
        }
    }

    fn builtin(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: Some(BUILTIN_DESCRIPTION.to_string()),
            transport: Transport::Builtin,
        }
    }
}

struct Connection {
    origin: Origin,
    handle: Arc<ServerHandle>,
}

struct Pending {
    origin: Origin,
    notify: Arc<Notify>,
}

enum Slot {
    Ready(Box<Connection>),
    Connecting(Pending),
}

#[derive(Default)]
struct State {
    connections: HashMap<Identity, Slot>,
    bindings: HashMap<i64, BTreeMap<String, Identity>>,
}

enum Step {
    Joined { severed: Option<Box<Connection>> },
    Waiting(Arc<Notify>),
    Leading(Arc<Notify>),
}

struct Standing {
    transport: Transport,
    running: bool,
    description: Option<String>,
}

struct Claim<'a> {
    registry: &'a Registry,
    identity: Identity,
    notify: Arc<Notify>,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.registry.discard(&self.identity, &self.notify);
    }
}

pub struct Registry {
    state: Mutex<State>,
    bus: Arc<Dispatcher>,
}

impl Registry {
    pub fn new(bus: Arc<Dispatcher>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            bus,
        }
    }

    pub fn connection_count(&self) -> usize {
        self.lock()
            .connections
            .values()
            .filter(|slot| matches!(slot, Slot::Ready(_)))
            .count()
    }

    pub fn reference_count(&self, name: &str) -> usize {
        self.lock()
            .bindings
            .values()
            .filter(|names| names.contains_key(name))
            .count()
    }

    pub async fn attach(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        name: &str,
        params: &ServerParams,
        root: &AssetRoot,
    ) -> Result<(), McpError> {
        self.decide(
            cx,
            &CORE_MCP_ATTACH,
            McpAttach {
                topic_id,
                server_name: name.to_string(),
            },
        )
        .await;
        let identity = self.identity(params, root)?;
        let origin = Origin::declared(params);
        let notify = loop {
            let step = {
                let mut state = self.lock();
                state.join(topic_id, name, &identity, &origin)
            };
            match step {
                Step::Joined { severed } => {
                    self.sever(
                        cx,
                        severed,
                        format!("topic {topic_id} bound {name} to another connection"),
                    )
                    .await;
                    return Ok(());
                }
                Step::Waiting(notify) => self.await_landing(&identity, &notify).await,
                Step::Leading(notify) => break notify,
            }
        };
        let _claim = Claim {
            registry: self,
            identity: identity.clone(),
            notify: Arc::clone(&notify),
        };
        self.decide(
            cx,
            &CORE_MCP_START,
            McpStart {
                server_name: name.to_string(),
                reason: format!("attached to topic {topic_id}"),
            },
        )
        .await;
        let handle = self.open(params, root).await?;
        let severed = {
            let mut state = self.lock();
            state.settle(
                topic_id,
                name,
                &identity,
                Connection {
                    origin,
                    handle: Arc::new(handle),
                },
            )
        };
        notify.notify_waiters();
        self.sever(
            cx,
            severed,
            format!("topic {topic_id} bound {name} to another connection"),
        )
        .await;
        Ok(())
    }

    /// 同一身份替换旧连接
    pub async fn connect_builtin<H>(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        name: &str,
        scope: &str,
        handler: H,
    ) -> Result<(), McpError>
    where
        H: ServerHandler + Send + Sync + 'static,
    {
        self.decide(
            cx,
            &CORE_MCP_ATTACH,
            McpAttach {
                topic_id,
                server_name: name.to_string(),
            },
        )
        .await;
        let identity = Identity::Builtin {
            name: name.to_string(),
            scope: scope.to_string(),
        };
        let origin = Origin::builtin(name);
        let replaced = self.lock().take(&identity);
        self.sever(
            cx,
            replaced,
            format!("topic {topic_id} registered {name} again"),
        )
        .await;
        let notify = loop {
            let step = {
                let mut state = self.lock();
                state.join(topic_id, name, &identity, &origin)
            };
            match step {
                Step::Joined { severed } => {
                    self.sever(
                        cx,
                        severed,
                        format!("topic {topic_id} bound {name} to another connection"),
                    )
                    .await;
                    return Ok(());
                }
                Step::Waiting(notify) => self.await_landing(&identity, &notify).await,
                Step::Leading(notify) => break notify,
            }
        };
        let _claim = Claim {
            registry: self,
            identity: identity.clone(),
            notify: Arc::clone(&notify),
        };
        self.decide(
            cx,
            &CORE_MCP_START,
            McpStart {
                server_name: name.to_string(),
                reason: format!("registered for topic {topic_id}"),
            },
        )
        .await;
        let handle = self.open_builtin(name, handler).await?;
        let severed = {
            let mut state = self.lock();
            state.settle(
                topic_id,
                name,
                &identity,
                Connection {
                    origin,
                    handle: Arc::new(handle),
                },
            )
        };
        notify.notify_waiters();
        self.sever(
            cx,
            severed,
            format!("topic {topic_id} bound {name} to another connection"),
        )
        .await;
        Ok(())
    }

    pub fn server_names(&self, topic_id: i64) -> Vec<String> {
        self.lock()
            .bindings
            .get(&topic_id)
            .map(|names| names.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 最后一个引用释放时才断开
    pub async fn detach(&self, cx: &DispatchCx, topic_id: i64, name: &str) {
        self.decide(
            cx,
            &CORE_MCP_DETACH,
            McpDetach {
                topic_id,
                server_name: name.to_string(),
            },
        )
        .await;
        let severed = {
            let mut state = self.lock();
            state.release(topic_id, name)
        };
        let Some(severed) = severed else {
            return;
        };
        self.stop(
            cx,
            severed,
            format!("the last reference from topic {topic_id} was released"),
        )
        .await;
    }

    pub async fn list_servers(&self, cx: &DispatchCx, topic_id: i64) -> Vec<McpServerInfo> {
        let named: BTreeMap<String, Identity> = self
            .lock()
            .bindings
            .get(&topic_id)
            .cloned()
            .unwrap_or_default();
        let mut servers: Vec<McpServerInfo> = {
            let state = self.lock();
            named
                .iter()
                .map(|(name, identity)| {
                    let standing = state.standing(identity);
                    McpServerInfo {
                        name: name.clone(),
                        description: standing.description,
                        transport: standing.transport,
                        running: standing.running,
                    }
                })
                .collect()
        };
        for name in NAMES {
            if servers.iter().any(|server| server.name == *name) {
                continue;
            }
            servers.push(McpServerInfo {
                name: (*name).to_string(),
                description: Some(BUILTIN_DESCRIPTION.to_string()),
                transport: Transport::Builtin,
                running: false,
            });
        }
        servers.sort_by(|left, right| left.name.cmp(&right.name));
        let payload = McpServerList { servers };
        self.decide(cx, &CORE_MCP_SERVER_LIST, payload)
            .await
            .servers
    }

    pub async fn list_tools(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
    ) -> Result<Vec<McpToolInfo>, McpError> {
        let mut tools = Vec::new();
        for server in self.list_servers(cx, topic_id).await {
            tools.extend(self.tools_of(cx, topic_id, &server.name).await?);
        }
        let payload = McpToolList { tools };
        Ok(self.decide(cx, &CORE_MCP_TOOL_LIST, payload).await.tools)
    }

    pub async fn call_tool(
        &self,
        cx: &DispatchCx,
        call_id: &str,
        server_name: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<CallOutcome, McpError> {
        let call = McpToolCall {
            server_name: server_name.to_string(),
            tool: tool.to_string(),
            arguments,
            scope: cx.scope.clone(),
        };
        let call = self.bus.decide(&CORE_MCP_TOOL_CALL, cx, call).await;

        let result = self.execute(cx, cx.scope.topic_id(), &call).await?;
        let payload = McpToolResult {
            server_name: call.server_name.clone(),
            tool: call.tool.clone(),
            call_id: call_id.to_string(),
            result,
        };
        let payload = self.bus.decide(&CORE_MCP_TOOL_RESULT, cx, payload).await;
        Ok(CallOutcome {
            arguments: call.arguments,
            result: payload.result,
        })
    }

    async fn execute(
        &self,
        cx: &DispatchCx,
        topic_id: Option<i64>,
        call: &McpToolCall,
    ) -> Result<serde_json::Value, McpError> {
        let handle = self
            .lock()
            .handle_of(topic_id, &call.server_name)
            .ok_or_else(|| McpError::NotConnected {
                name: call.server_name.clone(),
            })?;
        let arguments = match &call.arguments {
            serde_json::Value::Object(map) => Some(map.clone()),
            _ => None,
        };
        match handle.call_tool(&call.tool, arguments.as_ref()).await {
            Ok(result) => Ok(serde_json::to_value(&result).unwrap_or(serde_json::Value::Null)),
            Err(error) => {
                let reason = error.to_string();
                if error.disconnected() {
                    self.report_crash(cx, &call.server_name, &reason).await;
                }
                Err(McpError::Connect {
                    name: call.server_name.clone(),
                    reason,
                })
            }
        }
    }

    async fn report_crash(&self, cx: &DispatchCx, server_name: &str, error: &str) {
        self.bus
            .notify(
                &CORE_MCP_CRASH,
                cx,
                McpCrash {
                    server_name: server_name.to_string(),
                    error: error.to_string(),
                },
            )
            .await;
    }

    pub async fn shutdown(&self) {
        let drained: Vec<Slot> = self
            .lock()
            .connections
            .drain()
            .map(|(_, slot)| slot)
            .collect();
        for slot in drained {
            match slot {
                Slot::Ready(connection) => {
                    let Connection { handle, .. } = *connection;
                    if let Ok(handle) = Arc::try_unwrap(handle) {
                        handle.disconnect().await;
                    }
                }
                Slot::Connecting(pending) => pending.notify.notify_waiters(),
            }
        }
    }

    async fn await_landing(&self, identity: &Identity, notify: &Arc<Notify>) {
        let mut waiter = Box::pin(notify.notified());
        waiter.as_mut().enable();
        let ours = {
            let state = self.lock();
            matches!(
                state.connections.get(identity),
                Some(Slot::Connecting(pending)) if Arc::ptr_eq(&pending.notify, notify)
            )
        };
        if ours {
            waiter.await;
        }
    }

    fn discard(&self, identity: &Identity, notify: &Arc<Notify>) {
        if self.lock().discard(identity, notify) {
            notify.notify_waiters();
        }
    }

    async fn sever(&self, cx: &DispatchCx, severed: Option<Box<Connection>>, reason: String) {
        let Some(severed) = severed else {
            return;
        };
        self.stop(cx, severed, reason).await;
    }

    async fn stop(&self, cx: &DispatchCx, connection: Box<Connection>, reason: String) {
        let Connection { origin, handle } = *connection;
        self.decide(
            cx,
            &CORE_MCP_STOP,
            McpStop {
                server_name: origin.name,
                reason,
            },
        )
        .await;
        if let Ok(handle) = Arc::try_unwrap(handle) {
            handle.disconnect().await;
        }
    }

    async fn open(
        &self,
        params: &ServerParams,
        root: &AssetRoot,
    ) -> Result<ServerHandle, McpError> {
        ServerHandle::connect(params, root)
            .await
            .map_err(|error| McpError::Connect {
                name: params.name().to_string(),
                reason: error.to_string(),
            })
    }

    async fn open_builtin<H>(&self, name: &str, handler: H) -> Result<ServerHandle, McpError>
    where
        H: ServerHandler + Send + Sync + 'static,
    {
        ServerHandle::connect_builtin(handler)
            .await
            .map_err(|error| McpError::Connect {
                name: name.to_string(),
                reason: error.to_string(),
            })
    }

    fn identity(&self, params: &ServerParams, root: &AssetRoot) -> Result<Identity, McpError> {
        Ok(identity_of(params, root)?)
    }

    async fn tools_of(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        server_name: &str,
    ) -> Result<Vec<McpToolInfo>, McpError> {
        let Some(handle) = self.lock().handle_of(Some(topic_id), server_name) else {
            return Ok(Vec::new());
        };
        let tools = match handle.list_tools().await {
            Ok(tools) => tools,
            Err(error) => {
                let reason = error.to_string();
                if error.disconnected() {
                    self.report_crash(cx, server_name, &reason).await;
                }
                return Err(McpError::Connect {
                    name: server_name.to_string(),
                    reason,
                });
            }
        };
        Ok(tools
            .into_iter()
            .map(|tool| McpToolInfo {
                server_name: server_name.to_string(),
                name: tool.name.to_string(),
                description: tool.description.as_ref().map(|value| value.to_string()),
                input_schema: serde_json::to_value(&tool.input_schema)
                    .unwrap_or(serde_json::json!({})),
            })
            .collect())
    }

    async fn decide<P>(&self, cx: &DispatchCx, def: &EventDef<P>, payload: P) -> P
    where
        P: Serialize + DeserializeOwned + Send + Sync,
    {
        self.bus.decide(def, cx, payload).await
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl State {
    fn join(&mut self, topic_id: i64, name: &str, identity: &Identity, origin: &Origin) -> Step {
        if matches!(self.connections.get(identity), Some(Slot::Ready(_))) {
            return Step::Joined {
                severed: self.bind(topic_id, name, identity),
            };
        }
        let waiting = match self.connections.get(identity) {
            Some(Slot::Connecting(pending)) => Some(Arc::clone(&pending.notify)),
            _ => None,
        };
        if let Some(notify) = waiting {
            return Step::Waiting(notify);
        }
        let notify = Arc::new(Notify::new());
        self.connections.insert(
            identity.clone(),
            Slot::Connecting(Pending {
                origin: origin.clone(),
                notify: Arc::clone(&notify),
            }),
        );
        Step::Leading(notify)
    }

    fn bind(&mut self, topic_id: i64, name: &str, identity: &Identity) -> Option<Box<Connection>> {
        let replaced = self
            .bindings
            .entry(topic_id)
            .or_default()
            .insert(name.to_string(), identity.clone())?;
        if &replaced == identity || self.referenced(&replaced) {
            return None;
        }
        self.take(&replaced)
    }

    fn settle(
        &mut self,
        topic_id: i64,
        name: &str,
        identity: &Identity,
        connection: Connection,
    ) -> Option<Box<Connection>> {
        self.connections
            .insert(identity.clone(), Slot::Ready(Box::new(connection)));
        self.bind(topic_id, name, identity)
    }

    fn release(&mut self, topic_id: i64, name: &str) -> Option<Box<Connection>> {
        let gone = self.bindings.get_mut(&topic_id)?.remove(name)?;
        if self.bindings.get(&topic_id).is_some_and(BTreeMap::is_empty) {
            self.bindings.remove(&topic_id);
        }
        if self.referenced(&gone) {
            return None;
        }
        self.take(&gone)
    }

    fn take(&mut self, identity: &Identity) -> Option<Box<Connection>> {
        match self.connections.remove(identity) {
            Some(Slot::Ready(connection)) => Some(connection),
            Some(slot @ Slot::Connecting(_)) => {
                self.connections.insert(identity.clone(), slot);
                None
            }
            None => None,
        }
    }

    fn referenced(&self, identity: &Identity) -> bool {
        self.bindings
            .values()
            .any(|names| names.values().any(|bound| bound == identity))
    }

    fn handle_of(&self, topic_id: Option<i64>, name: &str) -> Option<Arc<ServerHandle>> {
        let topic_id = topic_id?;
        let identity = self.bindings.get(&topic_id)?.get(name)?;
        match self.connections.get(identity) {
            Some(Slot::Ready(connection)) => Some(Arc::clone(&connection.handle)),
            _ => None,
        }
    }

    fn discard(&mut self, identity: &Identity, notify: &Arc<Notify>) -> bool {
        let mine = matches!(
            self.connections.get(identity),
            Some(Slot::Connecting(pending)) if Arc::ptr_eq(&pending.notify, notify)
        );
        if mine {
            self.connections.remove(identity);
        }
        mine
    }

    fn standing(&self, identity: &Identity) -> Standing {
        let (origin, running) = match self.connections.get(identity) {
            Some(Slot::Ready(connection)) => (&connection.origin, true),
            Some(Slot::Connecting(pending)) => (&pending.origin, false),
            None => {
                return Standing {
                    transport: transport_of_identity(identity),
                    running: false,
                    description: None,
                };
            }
        };
        Standing {
            transport: origin.transport,
            running,
            description: origin.description.clone(),
        }
    }
}

fn transport_of(params: &ServerParams) -> Transport {
    match params {
        ServerParams::Stdio(_) => Transport::Stdio,
        ServerParams::Streamable(_) => Transport::Streamable,
    }
}

fn transport_of_identity(identity: &Identity) -> Transport {
    match identity {
        Identity::Package { .. } | Identity::Asset { .. } | Identity::Command { .. } => {
            Transport::Stdio
        }
        Identity::Remote { .. } => Transport::Streamable,
        Identity::Builtin { .. } => Transport::Builtin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{StdioParams, StreamableParams};

    /// 远端连接描述
    fn remote() -> ServerParams {
        ServerParams::Streamable(StreamableParams {
            name: "remote".into(),
            description: "remote knowledge base".into(),
            url: "https://mcp.example.com/v1".into(),
        })
    }

    /// 子进程连接描述
    fn local() -> ServerParams {
        ServerParams::Stdio(StdioParams {
            name: "fetch".into(),
            description: Some("fetch web pages".into()),
            command: "uvx".into(),
            args: vec!["mcp-server-fetch".into()],
            env: None,
            cwd: None,
        })
    }

    #[test]
    fn a_remote_description_lists_as_streamable() {
        assert_eq!(transport_of(&remote()), Transport::Streamable);
    }

    #[test]
    fn a_stdio_description_lists_as_stdio() {
        assert_eq!(transport_of(&local()), Transport::Stdio);
    }

    #[test]
    fn a_connecting_identity_is_listed_from_its_pending_description() {
        let identity = Identity::Package {
            package: "mcp-server-fetch".into(),
        };
        let mut state = State::default();
        state.connections.insert(
            identity.clone(),
            Slot::Connecting(Pending {
                origin: Origin::declared(&remote()),
                notify: Arc::new(Notify::new()),
            }),
        );
        let standing = state.standing(&identity);
        assert_eq!(
            standing.transport,
            Transport::Streamable,
            "a remote must not be reported as a subprocess"
        );
        assert!(!standing.running, "not connected yet");
        assert_eq!(
            standing.description.as_deref(),
            Some("remote knowledge base"),
            "the description comes from the origin in the in-flight registration"
        );
    }

    #[test]
    fn a_builtin_identity_is_listed_as_in_process() {
        let identity = Identity::Builtin {
            name: "fs".into(),
            scope: "topic:1".into(),
        };
        let mut state = State::default();
        state.connections.insert(
            identity.clone(),
            Slot::Connecting(Pending {
                origin: Origin::builtin("fs"),
                notify: Arc::new(Notify::new()),
            }),
        );
        let standing = state.standing(&identity);
        assert_eq!(
            standing.transport,
            Transport::Builtin,
            "builtin services are in-process connections"
        );
        assert_eq!(
            standing.description.as_deref(),
            Some("provided by the host"),
            "the description is provided by the host"
        );
    }

    /// 表里没有该槽位时按身份本身报告连接方式
    ///
    /// 换绑窗口里连接已经移除而绑定还在，槽位缺失描述的就是这种时刻
    #[test]
    fn an_identity_with_no_slot_is_listed_from_its_own_shape() {
        let state = State::default();
        let cases = [
            (
                Identity::Remote {
                    url: "https://mcp.example.com/v1".into(),
                },
                Transport::Streamable,
            ),
            (
                Identity::Builtin {
                    name: "fs".into(),
                    scope: "topic:1".into(),
                },
                Transport::Builtin,
            ),
            (
                Identity::Command {
                    program: "node".into(),
                    args: vec!["server.js".into()],
                },
                Transport::Stdio,
            ),
        ];
        for (identity, transport) in cases {
            let standing = state.standing(&identity);
            assert_eq!(standing.transport, transport, "identity {identity:?}");
            assert!(
                !standing.running,
                "a slot absent from the table is not running"
            );
            assert_eq!(standing.description, None, "no origin to report");
        }
    }

    #[test]
    fn two_names_on_one_topic_share_one_identity() {
        let identity = Identity::Builtin {
            name: "fs".into(),
            scope: "topic:1".into(),
        };
        let origin = Origin::builtin("fs");
        let mut state = State::default();

        assert!(
            matches!(state.join(1, "fs", &identity, &origin), Step::Leading(_)),
            "the first registration is responsible for establishing"
        );
        assert!(
            matches!(state.join(1, "alias", &identity, &origin), Step::Waiting(_)),
            "a name is only a reference key; the same identity need not connect again"
        );
        // 连接建立之后才把两个名字都登记到该身份上
        assert!(state.bind(1, "fs", &identity).is_none());
        assert!(state.bind(1, "alias", &identity).is_none());
        assert!(state.referenced(&identity), "both names point at it");
        assert!(
            state.release(1, "fs").is_none(),
            "another name still points at it, so it cannot be detached"
        );
        assert!(state.referenced(&identity));
        assert!(
            state.release(1, "alias").is_none(),
            "this slot is not fully established; cleanup is the responsibility of the call that started it"
        );
        assert!(
            !state.referenced(&identity),
            "both names have been released"
        );
    }

    /// 同一个身份同时只有一条在途登记
    ///
    /// 进程内连接的握手不产生可观察的在途窗口，这条规则只能在状态层约束
    #[test]
    fn one_identity_is_led_once_at_a_time() {
        let identity = Identity::Builtin {
            name: "fs".into(),
            scope: "topic:1".into(),
        };
        let origin = Origin::builtin("fs");
        let mut state = State::default();

        assert!(
            matches!(state.join(1, "fs", &identity, &origin), Step::Leading(_)),
            "the first registration is responsible for establishing"
        );
        assert!(
            matches!(state.join(2, "fs", &identity, &origin), Step::Waiting(_)),
            "later callers wait for it to land instead of opening another one"
        );
        assert!(
            matches!(state.join(2, "fs", &identity, &origin), Step::Waiting(_)),
            "registering again still means waiting"
        );
    }

    /// 丢弃在途登记会释放对应槽位
    ///
    /// 移除之后，后续调用自行建立连接，不会等待一个不会再触发的通知
    #[test]
    fn a_dropped_registration_frees_its_slot() {
        let identity = Identity::Builtin {
            name: "fs".into(),
            scope: "topic:1".into(),
        };
        let origin = Origin::builtin("fs");
        let mut state = State::default();

        let notify = match state.join(1, "fs", &identity, &origin) {
            Step::Leading(notify) => notify,
            _ => panic!("expected the leading step"),
        };
        assert!(
            state.discard(&identity, &notify),
            "the slot of this registration is removed"
        );
        assert!(
            !state.discard(&identity, &notify),
            "the slot of this registration is already removed, so it no longer matches"
        );
        assert!(
            matches!(state.join(2, "fs", &identity, &origin), Step::Leading(_)),
            "after it is dropped, later callers establish it themselves"
        );
    }
}
