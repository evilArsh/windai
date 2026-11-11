//! mod 与内建 MCP 的资源适配

use crate::error::{CoreError, Result, panic_text};
use crate::llm::tools::mcp_tool_name;
use futures::FutureExt;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use wind_event::DispatchCx;
use wind_mcp::builtin::skills::{SkillRoot, SkillRoots};
use wind_mcp::builtin::{self, BuiltinResources, BuiltinScope};
use wind_mcp::{BoundTool, CallOutcome, RegistryHandle};
use wind_mod::installer::Installer;
use wind_mod::resources::{ModResources, Resources};
use wind_storage::Storage;
use wind_wire::tool::{FunctionTool, Tools};
use wind_wire::{FunctionCallOutput, McpDefinition, McpService, ModService, ToolCallParam};

/// 一次资源修改的结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    /// 持久化变更影响的话题
    pub affected_topics: Vec<i64>,
}

/// 资源发布、工具目录与调用的适配点
#[derive(Clone)]
pub struct ResourceService {
    installer: Installer,
    resources: Resources,
    storage: Storage,
    registry: RegistryHandle,
    /// 默认 topic 根
    topics_root: PathBuf,
    gate: Arc<AsyncMutex<()>>,
    topics: Arc<Mutex<HashMap<i64, TopicRefs>>>,
    closed: Arc<AtomicBool>,
}

impl ResourceService {
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
    pub async fn restore(&self) -> Result<()> {
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
        let released = self.lock_topics().remove(&topic_id);
        if let Some(topic) = released
            && !topic.main.is_empty()
        {
            self.registry.release(topic.main);
        }
        builtin::release(&self.registry, BuiltinScope::Topic(topic_id));
        Ok(())
    }

    /// 停止接纳新操作并关闭全部连接
    pub async fn shutdown(&self) -> Result<()> {
        self.mutate(move |this| async move {
            {
                let _gate = this.gate.lock().await;
                this.closed.store(true, Ordering::SeqCst);
            }
            this.registry.shutdown().await;
            Ok(())
        })
        .await
    }

    // 调用方取消只放弃等待，操作在独立任务里走完
    async fn mutate<T, F, Fut>(&self, op: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(ResourceService) -> Fut + Send + 'static,
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
        let roots = skill_roots(&mods, self.resources.public_skills());

        let (new_main, old_main, previous, resources) = {
            let mut topics = self.lock_topics();
            let refs = topics.entry(topic_id).or_default();
            refs.generation += 1;
            let new_main = TopicRefs::new_ref_name(topic_id, refs.generation);
            let old_main = std::mem::replace(&mut refs.main, new_main.clone());
            let previous = refs.resources.clone();
            let skill_roots = match &previous {
                Some(previous) => {
                    previous.skill_roots.replace(roots);
                    previous.skill_roots.clone()
                }
                None => SkillRoots::new(roots),
            };
            let resources = BuiltinResources {
                workspace,
                shared_roots: vec![self.resources.public_skills(), self.resources.mods_root()],
                skill_roots,
            };
            refs.resources = Some(resources.clone());
            (new_main, old_main, previous, resources)
        };

        for definition in definitions {
            self.registry.acquire(new_main.clone(), definition);
        }
        if !old_main.is_empty() {
            self.registry.release(old_main);
        }

        builtin::sync(
            &self.registry,
            BuiltinScope::Topic(topic_id),
            previous.as_ref(),
            &resources,
        )
        .await;
        Ok(())
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

    fn open(&self) -> std::result::Result<(), String> {
        self.ensure_open().map_err(|error| error.to_string())
    }

    async fn call_target(
        &self,
        deadline: Instant,
        token: CancellationToken,
        server: String,
        name: String,
        arguments: Value,
    ) -> std::result::Result<Value, String> {
        match self
            .registry
            .call(&server, &name, arguments, token, deadline)
            .await
        {
            CallOutcome::Ok(value) => Ok(value),
            CallOutcome::Failed(reason) => Err(reason),
        }
    }
}

#[async_trait::async_trait]
impl ModService for ResourceService {
    async fn prompt_fragments(&self, topic_id: i64) -> std::result::Result<Vec<String>, String> {
        let mut fragments = Vec::new();
        let modules = self
            .resources
            .topic(topic_id)
            .await
            .map_err(|error| error.to_string())?;
        for module in modules {
            fragments.extend(module.prompts);
        }
        Ok(fragments)
    }

    async fn version_of(&self, mod_name: &str) -> std::result::Result<i64, String> {
        let module = self
            .installer
            .get(mod_name)
            .await
            .map_err(|error| error.to_string())?;
        module
            .map(|module| i64::from(module.version))
            .ok_or_else(|| format!("mod {mod_name} is not installed"))
    }
}

#[async_trait::async_trait]
impl McpService for ResourceService {
    async fn list_tools(&self, topic_id: i64) -> std::result::Result<Vec<Tools>, String> {
        self.open()?;
        self.ensure_topic(topic_id)
            .await
            .map_err(|error| error.to_string())?;
        let mut refs = Vec::new();
        refs.extend(
            self.lock_topics()
                .get(&topic_id)
                .map(|topic| topic.main.clone()),
        );
        refs.extend(builtin::refs(BuiltinScope::Topic(topic_id)));
        let mut seen = HashSet::new();
        let mut tools = Vec::new();
        for ref_id in refs {
            for tool in self.registry.list_tools_by_ref(&ref_id).await.iter() {
                if !seen.insert((tool.server.clone(), tool.tool.name.clone())) {
                    continue;
                }
                tools.push(exported(tool));
            }
        }
        Ok(tools)
    }

    async fn tool_call(
        &self,
        deadline: Instant,
        token: CancellationToken,
        param: ToolCallParam,
    ) -> std::result::Result<Value, String> {
        self.open()?;
        self.call_target(deadline, token, param.server, param.name, param.arguments)
            .await
    }

    async fn tool_calls(
        &self,
        deadline: Instant,
        token: CancellationToken,
        params: Vec<ToolCallParam>,
    ) -> std::result::Result<Vec<FunctionCallOutput>, String> {
        self.open()?;
        let calls = params.into_iter().map(|param| {
            let token = token.clone();
            async move {
                let ToolCallParam {
                    call_id,
                    server,
                    name,
                    arguments,
                } = param;
                let content = match self
                    .call_target(deadline, token, server, name, arguments)
                    .await
                {
                    Ok(value) => value,
                    Err(reason) => serde_json::json!({ "error": reason }),
                };
                FunctionCallOutput {
                    id: call_id,
                    content,
                }
            }
        });
        Ok(futures::future::join_all(calls).await)
    }
}

/// 一个话题当前的引用名
#[derive(Default)]
struct TopicRefs {
    /// topic_{topic_id}@{generation}
    main: String,
    generation: u64,
    /// 上次同步出去的内建资源
    resources: Option<BuiltinResources>,
}

impl TopicRefs {
    fn new_ref_name(topic_id: i64, generation: u64) -> String {
        format!("topic_{topic_id}@{generation}")
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

fn skill_roots(mods: &[ModResources], public: PathBuf) -> Vec<SkillRoot> {
    let mut roots = vec![SkillRoot {
        source: "public".to_string(),
        path: public,
    }];
    roots.extend(mods.iter().filter_map(|module| {
        module.skills.as_ref().map(|path| SkillRoot {
            source: module.name.clone(),
            path: path.clone(),
        })
    }));
    roots
}

fn exported(tool: &BoundTool) -> Tools {
    let name = mcp_tool_name(&tool.server, &tool.tool.name);
    let description = match &tool.tool.description {
        Some(description) => {
            format!(
                "MCP tool '{}' from '{}': {description}",
                tool.tool.name, tool.server
            )
        }
        None => format!("MCP tool '{}' from '{}'", tool.tool.name, tool.server),
    };
    Tools::Function(FunctionTool {
        name,
        description: Some(description),
        parameters: Some(tool.tool.input_schema.clone()),
        strict: None,
    })
}
