//! mod 与内建 MCP 的资源适配

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use wind_event::{DispatchCx, Scope};
use wind_mcp::builtin::fs::FsServer;
use wind_mcp::builtin::skills::{SkillRoot, SkillsServer};
use wind_mcp::{BoundTool, CallOutcome, RegistryHandle};
use wind_mod::installer::{Installer, UnavailableMod};
use wind_mod::resources::{ModResources, Resources};
use wind_storage::Storage;
use wind_wire::McpDefinition;
use wind_wire::tool::{FunctionTool, Tools};

use crate::error::{CoreError, Result, panic_text};
use crate::llm::tools::{DeclaredTool, McpTarget, ToolOrigin};
use crate::llm::{McpService as LlmMcpService, ModService};

/// 一次资源修改的结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    /// 持久化变更影响的话题
    pub affected_topics: Vec<i64>,
}

/// 资源发布、工具目录与调用的适配点
#[derive(Clone)]
pub struct ResourceServices {
    installer: Installer,
    resources: Resources,
    storage: Storage,
    registry: RegistryHandle,
    /// 历史 NULL workspace 的兜底根
    topics_root: PathBuf,
    gate: Arc<AsyncMutex<()>>,
    topics: Arc<Mutex<HashMap<i64, TopicRefs>>>,
    closed: Arc<AtomicBool>,
}

impl ResourceServices {
    pub fn new(
        installer: Installer,
        resources: Resources,
        storage: Storage,
        registry: RegistryHandle,
        topics_root: PathBuf,
    ) -> Self {
        Self {
            installer,
            resources,
            storage,
            registry,
            topics_root,
            gate: Arc::new(AsyncMutex::new(())),
            topics: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 从数据库恢复启用集合与订阅
    pub async fn restore(&self) -> Result<Vec<UnavailableMod>> {
        self.mutate(move |this| async move {
            let _gate = this.gate.lock().await;
            this.ensure_open()?;
            Ok(this.installer.restore().await?)
        })
        .await
    }

    pub async fn install(&self, cx: &DispatchCx, package: &Path) -> Result<ApplyReport> {
        let cx = cx.clone();
        let package = package.to_path_buf();
        self.mutate(move |this| async move {
            {
                let _gate = this.gate.lock().await;
                this.ensure_open()?;
                this.installer.install(&cx, &package).await?;
            }
            Ok(ApplyReport {
                affected_topics: Vec::new(),
            })
        })
        .await
    }

    pub async fn update(&self, cx: &DispatchCx, package: &Path) -> Result<ApplyReport> {
        let cx = cx.clone();
        let package = package.to_path_buf();
        self.mutate(move |this| async move {
            let change = {
                let _gate = this.gate.lock().await;
                this.ensure_open()?;
                this.installer.update(&cx, &package).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn delete(&self, cx: &DispatchCx, name: &str) -> Result<ApplyReport> {
        let cx = cx.clone();
        let name = name.to_string();
        self.mutate(move |this| async move {
            let change = {
                let _gate = this.gate.lock().await;
                this.ensure_open()?;
                this.installer.delete(&cx, &name).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn enable(&self, cx: &DispatchCx, topic_id: i64, name: &str) -> Result<ApplyReport> {
        let cx = cx.clone();
        let name = name.to_string();
        self.mutate(move |this| async move {
            let change = {
                let _gate = this.gate.lock().await;
                this.ensure_open()?;
                this.installer.enable(&cx, topic_id, &name).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn disable(&self, cx: &DispatchCx, topic_id: i64, name: &str) -> Result<ApplyReport> {
        let cx = cx.clone();
        let name = name.to_string();
        self.mutate(move |this| async move {
            let change = {
                let _gate = this.gate.lock().await;
                this.ensure_open()?;
                this.installer.disable(&cx, topic_id, &name).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    /// 释放一个话题的运行绑定，保留持久启用关系
    pub async fn release_topic(&self, topic_id: i64) -> Result<()> {
        let _gate = self.gate.lock().await;
        self.ensure_open()?;
        let refs = {
            let mut topics = self.lock_topics();
            topics.remove(&topic_id)
        };
        if let Some(refs) = refs {
            if !refs.main.is_empty() {
                self.registry.release(refs.main);
            }
            for kind in BuiltinKind::ALL {
                self.registry.release(kind.service_name(topic_id));
            }
        }
        Ok(())
    }

    /// 停止接纳新操作并关闭全部连接
    pub async fn shutdown(&self) -> Result<()> {
        self.mutate(move |this| async move {
            let _gate = this.gate.lock().await;
            this.closed.store(true, Ordering::SeqCst);
            this.registry.shutdown().await;
            Ok(())
        })
        .await
    }

    // 调用方取消只放弃等待，操作在独立任务里走完
    async fn mutate<T, F, Fut>(&self, op: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(ResourceServices) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let this = self.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let caught = AssertUnwindSafe(op(this)).catch_unwind().await;
            let result = caught.unwrap_or_else(|payload| {
                log::error!(
                    "[resources] the resource operation panicked: {}",
                    panic_text(payload.as_ref())
                );
                Err(CoreError::Internal(
                    "the resource operation panicked".to_string(),
                ))
            });
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| {
            CoreError::Internal("the resource operation ended unexpectedly".to_string())
        })?
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(CoreError::ResourcesClosed);
        }
        Ok(())
    }

    fn lock_topics(&self) -> std::sync::MutexGuard<'_, HashMap<i64, TopicRefs>> {
        self.topics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn apply(&self, topics: Vec<i64>) -> Result<ApplyReport> {
        let mut seen = HashSet::new();
        let affected_topics: Vec<i64> = topics
            .into_iter()
            .filter(|topic_id| seen.insert(*topic_id))
            .collect();
        let mut first_error = None;
        for &topic_id in &affected_topics {
            let _gate = self.gate.lock().await;
            self.ensure_open()?;
            if let Err(error) = self.publish(topic_id).await {
                log::warn!("the topic {topic_id} cannot be republished: {error}");
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(ApplyReport { affected_topics }),
        }
    }

    /// 首次声明时按需发布；同一个话题只发布一次
    async fn ensure_topic(&self, topic_id: i64) -> Result<()> {
        if self.lock_topics().contains_key(&topic_id) {
            return Ok(());
        }
        let _gate = self.gate.lock().await;
        self.ensure_open()?;
        if self.lock_topics().contains_key(&topic_id) {
            return Ok(());
        }
        self.publish(topic_id).await
    }

    /// 重新发布一个话题的 mod 服务与内建服务；调用方已持 gate
    async fn publish(&self, topic_id: i64) -> Result<()> {
        self.ensure_open()?;
        let mods = self.resources.topic(topic_id).await?;
        let definitions = mod_definitions(&mods);
        let workspace = self.workspace_of(topic_id).await?;
        let skill_roots = skill_roots(&mods);

        let (new_main, old_main) = {
            let mut topics = self.lock_topics();
            let refs = topics.entry(topic_id).or_default();
            refs.generation += 1;
            let new_main = format!("topic_{topic_id}@{}", refs.generation);
            let old_main = std::mem::replace(&mut refs.main, new_main.clone());
            (new_main, old_main)
        };
        for definition in definitions {
            self.registry.acquire(new_main.clone(), definition);
        }
        if !old_main.is_empty() {
            self.registry.release(old_main);
        }

        self.sync_builtin(
            topic_id,
            BuiltinKind::Fs,
            BuiltinSnapshot::Fs(vec![workspace]),
        )
        .await;
        self.sync_builtin(
            topic_id,
            BuiltinKind::Skills,
            BuiltinSnapshot::Skills(skill_roots),
        )
        .await;
        Ok(())
    }

    /// 根快照变化时先停旧实例再启动新实例
    async fn sync_builtin(&self, topic_id: i64, kind: BuiltinKind, snapshot: BuiltinSnapshot) {
        let current = {
            let topics = self.lock_topics();
            topics
                .get(&topic_id)
                .and_then(|refs| refs.builtins.get(&kind).cloned())
        };
        if current.as_ref() == Some(&snapshot) {
            return;
        }
        let service = kind.service_name(topic_id);
        if current.is_some() {
            self.registry.release(service.clone());
        }
        let started = if snapshot.is_empty() {
            Ok(())
        } else {
            match &snapshot {
                BuiltinSnapshot::Fs(roots) => {
                    let server = FsServer::new(service.clone(), roots.clone());
                    self.registry.acquire_builtin(service.clone(), server).await
                }
                BuiltinSnapshot::Skills(roots) => {
                    let server = SkillsServer::new(service.clone(), roots.clone());
                    self.registry.acquire_builtin(service.clone(), server).await
                }
            }
        };
        match started {
            Ok(()) => {
                let mut topics = self.lock_topics();
                if let Some(refs) = topics.get_mut(&topic_id) {
                    refs.builtins.insert(kind, snapshot);
                }
            }
            Err(error) => {
                log::warn!("the builtin mcp service {service} failed to start: {error}");
            }
        }
    }

    async fn workspace_of(&self, topic_id: i64) -> Result<PathBuf> {
        let topic = self.storage.get_topic_by_id(topic_id).await?;
        let workspace = match topic.and_then(|topic| topic.workspace) {
            Some(workspace) => PathBuf::from(workspace),
            None => self.topics_root.join(topic_id.to_string()),
        };
        if let Err(error) = std::fs::create_dir_all(&workspace) {
            log::warn!(
                "the workspace {} cannot be created: {error}",
                workspace.display()
            );
        }
        Ok(workspace)
    }
}

#[async_trait::async_trait]
impl ModService for ResourceServices {
    async fn prompt_fragments(&self, topic_id: i64) -> Result<Vec<String>> {
        let mut fragments = Vec::new();
        for module in self.resources.topic(topic_id).await? {
            fragments.extend(module.prompts);
        }
        Ok(fragments)
    }

    async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>> {
        Ok(Vec::new())
    }

    async fn version_of(&self, mod_name: &str) -> Result<i64> {
        self.installer
            .get(mod_name)
            .await?
            .map(|module| i64::from(module.version))
            .ok_or_else(|| CoreError::Denied(format!("mod {mod_name} is not installed")))
    }

    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        _input: Value,
    ) -> Result<Value> {
        Err(CoreError::Denied(
            "mod requests are not executable without a script engine".to_string(),
        ))
    }
}

#[async_trait::async_trait]
impl LlmMcpService for ResourceServices {
    async fn declarations(&self, _cx: &DispatchCx, topic_id: i64) -> Result<Vec<DeclaredTool>> {
        self.ensure_open()?;
        self.ensure_topic(topic_id).await?;
        let refs = {
            let topics = self.lock_topics();
            topics
                .get(&topic_id)
                .map(|refs| refs.tool_refs(topic_id))
                .unwrap_or_default()
        };
        let mut seen = HashSet::new();
        let mut tools = Vec::new();
        for ref_id in refs {
            for tool in self.registry.list_tools_by_ref(&ref_id).await.iter() {
                let key = (tool.server.clone(), tool.tool.name.clone());
                if !seen.insert(key) {
                    continue;
                }
                tools.push(declared(tool));
            }
        }
        Ok(tools)
    }

    async fn call(
        &self,
        cx: &DispatchCx,
        _call_id: &str,
        target: &McpTarget,
        arguments: Value,
    ) -> Result<Value> {
        self.ensure_open()?;
        match self
            .registry
            .call(
                &target.server,
                &target.tool,
                arguments,
                cx.cancel().clone(),
                cx.deadline(),
            )
            .await
        {
            CallOutcome::Ok(value) => Ok(value),
            CallOutcome::Failed(reason) => Err(CoreError::Denied(reason)),
        }
    }
}

/// 一个话题当前的引用名
#[derive(Default)]
struct TopicRefs {
    main: String,
    generation: u64,
    builtins: BTreeMap<BuiltinKind, BuiltinSnapshot>,
}

impl TopicRefs {
    fn tool_refs(&self, topic_id: i64) -> Vec<String> {
        let mut refs = Vec::with_capacity(1 + BuiltinKind::ALL.len());
        refs.push(self.main.clone());
        for kind in BuiltinKind::ALL {
            refs.push(kind.service_name(topic_id));
        }
        refs
    }
}

/// 内建服务的种类
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BuiltinKind {
    Fs,
    Skills,
}

impl BuiltinKind {
    const ALL: [BuiltinKind; 2] = [BuiltinKind::Fs, BuiltinKind::Skills];

    fn name(self) -> &'static str {
        match self {
            BuiltinKind::Fs => "fs",
            BuiltinKind::Skills => "skills",
        }
    }

    fn service_name(self, topic_id: i64) -> String {
        format!("wd_{}_{topic_id}", self.name())
    }
}

/// 上次成功发布的内建根，用于判断是否需要重建
#[derive(Clone, PartialEq, Eq)]
enum BuiltinSnapshot {
    Fs(Vec<PathBuf>),
    Skills(Vec<SkillRoot>),
}

impl BuiltinSnapshot {
    fn is_empty(&self) -> bool {
        match self {
            BuiltinSnapshot::Fs(roots) => roots.is_empty(),
            BuiltinSnapshot::Skills(roots) => roots.is_empty(),
        }
    }
}

fn mod_definitions(mods: &[ModResources]) -> Vec<McpDefinition> {
    let mut seen = BTreeSet::new();
    let mut definitions = Vec::new();
    for module in mods {
        for (local, definition) in &module.mcp {
            let name = format!("mod_{}_{local}", module.name);
            if !seen.insert(name.clone()) {
                log::warn!(
                    "the mcp service name {name} is constructed by more than one mod; keeping the first"
                );
                continue;
            }
            let mut definition = definition.clone();
            definition.name = name;
            definitions.push(definition);
        }
    }
    definitions
}

fn skill_roots(mods: &[ModResources]) -> Vec<SkillRoot> {
    mods.iter()
        .filter_map(|module| {
            module.skills.as_ref().map(|path| SkillRoot {
                source: module.name.clone(),
                path: path.clone(),
            })
        })
        .collect()
}

fn declared(tool: &BoundTool) -> DeclaredTool {
    let target = McpTarget {
        server: tool.server.clone(),
        tool: tool.tool.name.clone(),
    };
    let origin = ToolOrigin::Mcp { target };
    let name = origin.exposed();
    let description = match &tool.tool.description {
        Some(description) => {
            format!(
                "MCP tool '{}' from '{}': {description}",
                tool.tool.name, tool.server
            )
        }
        None => format!("MCP tool '{}' from '{}'", tool.tool.name, tool.server),
    };
    DeclaredTool {
        declaration: Tools::Function(FunctionTool {
            name,
            description: Some(description),
            parameters: Some(tool.tool.input_schema.clone()),
            strict: None,
        }),
        origin,
    }
}
