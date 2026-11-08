//! mod 与独立 MCP 配置的资源适配

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, RwLock, oneshot};
use wind_event::{DispatchCx, Scope};
use wind_mcp::registry::{SOURCE_BUILTIN, SOURCE_CONFIGURED, SOURCE_MODS};
use wind_mcp::{
    BindingSpec, BindingStatus, BoundTool, BuiltinRoots, McpService as McpManager, Registry,
    ToolTarget,
};
use wind_mod::installer::{Installer, UnavailableMod};
use wind_mod::resources::Resources;
use wind_storage::McpServerRecord;
use wind_wire::McpDefinition;
use wind_wire::tool::{FunctionTool, Tools};

use crate::error::{CoreError, Result, panic_text};
use crate::llm::tools::{DeclaredTool, ToolOrigin};
use crate::llm::{McpService as LlmMcpService, ModService};

/// 一次同步失败的话题
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncFailure {
    /// 失败的话题
    pub topic_id: i64,
    /// 失败原因
    pub error: String,
}

/// 一次资源修改的结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    /// 持久化变更影响的话题
    pub affected_topics: Vec<i64>,
    /// 已提交但运行状态同步失败的话题
    pub sync_errors: Vec<SyncFailure>,
}

/// 资源发布、工具目录与调用的适配点
#[derive(Clone)]
pub struct ResourceServices {
    installer: Installer,
    resources: Resources,
    mcp: Arc<McpManager>,
    registry: Arc<Registry>,
    builtin: BuiltinRoots,
    config: Arc<RwLock<()>>,
    topics: Arc<Mutex<HashMap<i64, Arc<AsyncMutex<()>>>>>,
    closed: Arc<AtomicBool>,
}

impl ResourceServices {
    pub fn new(
        installer: Installer,
        resources: Resources,
        mcp: McpManager,
        registry: Arc<Registry>,
        builtin: BuiltinRoots,
    ) -> Self {
        Self {
            installer,
            resources,
            mcp: Arc::new(mcp),
            registry,
            builtin,
            config: Arc::new(RwLock::new(())),
            topics: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// 一个话题的运行绑定状态
    pub fn status(&self, topic_id: i64) -> Vec<BindingStatus> {
        self.registry.status(topic_id)
    }

    /// 从数据库恢复启用集合与订阅
    pub async fn restore(&self) -> Result<Vec<UnavailableMod>> {
        self.mutate(move |this| async move {
            let _write = this.config.write().await;
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
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.installer.install(&cx, &package).await?;
            }
            Ok(ApplyReport {
                affected_topics: Vec::new(),
                sync_errors: Vec::new(),
            })
        })
        .await
    }

    pub async fn update(&self, cx: &DispatchCx, package: &Path) -> Result<ApplyReport> {
        let cx = cx.clone();
        let package = package.to_path_buf();
        self.mutate(move |this| async move {
            let change = {
                let _write = this.config.write().await;
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
                let _write = this.config.write().await;
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
                let _write = this.config.write().await;
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
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.installer.disable(&cx, topic_id, &name).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn create_mcp(&self, definition: &McpDefinition) -> Result<ApplyReport> {
        let definition = definition.clone();
        self.mutate(move |this| async move {
            let change = {
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.mcp.create(&definition).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn update_mcp(
        &self,
        id: i64,
        expected_revision: u32,
        definition: &McpDefinition,
    ) -> Result<ApplyReport> {
        let definition = definition.clone();
        self.mutate(move |this| async move {
            let change = {
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.mcp.update(id, expected_revision, &definition).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn delete_mcp(&self, id: i64) -> Result<ApplyReport> {
        self.mutate(move |this| async move {
            let change = {
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.mcp.delete(id).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn enable_mcp(&self, topic_id: i64, id: i64) -> Result<ApplyReport> {
        self.mutate(move |this| async move {
            let change = {
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.mcp.enable(topic_id, id).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn disable_mcp(&self, topic_id: i64, id: i64) -> Result<ApplyReport> {
        self.mutate(move |this| async move {
            let change = {
                let _write = this.config.write().await;
                this.ensure_open()?;
                this.mcp.disable(topic_id, id).await?
            };
            this.apply(change.affected_topics).await
        })
        .await
    }

    pub async fn mcp_server(&self, id: i64) -> Result<Option<McpServerRecord>> {
        Ok(self.mcp.get(id).await?)
    }

    pub async fn mcp_servers(&self) -> Result<Vec<McpServerRecord>> {
        Ok(self.mcp.list().await?)
    }

    pub async fn enabled_mcp_servers(&self, topic_id: i64) -> Result<Vec<McpServerRecord>> {
        Ok(self.mcp.enabled(topic_id).await?)
    }

    /// 释放一个话题的运行绑定，保留持久启用关系
    pub async fn release_topic(&self, topic_id: i64) -> Result<()> {
        self.ensure_open()?;
        let gate = self.topic_gate(topic_id);
        let _gate = gate.lock_owned().await;
        self.registry.release_topic(topic_id).await?;
        Ok(())
    }

    /// 停止接纳新操作并关闭全部连接
    pub async fn shutdown(&self) -> Result<()> {
        self.mutate(move |this| async move {
            let _write = this.config.write().await;
            this.closed.store(true, Ordering::SeqCst);
            this.registry.shutdown().await?;
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

    fn topic_gate(&self, topic_id: i64) -> Arc<AsyncMutex<()>> {
        let mut topics = self
            .topics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            topics
                .entry(topic_id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    async fn apply(&self, topics: Vec<i64>) -> Result<ApplyReport> {
        let mut seen = HashSet::new();
        let affected_topics: Vec<i64> = topics
            .into_iter()
            .filter(|topic_id| seen.insert(*topic_id))
            .collect();
        let mut sync_errors = Vec::new();
        for &topic_id in &affected_topics {
            if let Err(error) = self.sync_topic(topic_id).await {
                sync_errors.push(SyncFailure {
                    topic_id,
                    error: error.to_string(),
                });
            }
        }
        Ok(ApplyReport {
            affected_topics,
            sync_errors,
        })
    }

    async fn sync_topic(&self, topic_id: i64) -> Result<()> {
        self.ensure_open()?;
        let gate = self.topic_gate(topic_id);
        let _gate = gate.lock_owned().await;
        self.ensure_open()?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let before = self.snapshot_gated(topic_id).await?;
            self.apply_snapshot(topic_id, &before).await?;
            let after = self.snapshot_gated(topic_id).await?;
            if after.identity() == before.identity() {
                return Ok(());
            }
            if attempt >= 2 {
                return Err(CoreError::ResourcesChanged);
            }
        }
    }

    async fn tools_locked(&self, topic_id: i64, cx: &DispatchCx) -> Result<Vec<BoundTool>> {
        self.ensure_open()?;
        let gate = self.topic_gate(topic_id);
        let _gate = gate.lock_owned().await;
        self.ensure_open()?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let before = self.snapshot_gated(topic_id).await?;
            self.apply_snapshot(topic_id, &before).await?;
            let tools = self.registry.tools(topic_id, cx.cancel().clone()).await?;
            self.ensure_open()?;
            let after = self.snapshot_gated(topic_id).await?;
            if after.identity() == before.identity() {
                return Ok(tools);
            }
            if attempt >= 2 {
                return Err(CoreError::ResourcesChanged);
            }
        }
    }

    async fn apply_snapshot(&self, topic_id: i64, snapshot: &Snapshot) -> Result<()> {
        self.registry
            .sync(topic_id, SOURCE_MODS, snapshot.mods.clone())
            .await?;
        self.registry
            .sync(topic_id, SOURCE_CONFIGURED, snapshot.configured.clone())
            .await?;
        self.registry
            .sync_builtin(topic_id, snapshot.builtin.clone())
            .await?;
        Ok(())
    }

    async fn snapshot_gated(&self, topic_id: i64) -> Result<Snapshot> {
        let _read = self.config.read().await;
        self.ensure_open()?;
        self.snapshot_read(topic_id).await
    }

    async fn snapshot_read(&self, topic_id: i64) -> Result<Snapshot> {
        let mut mods = Vec::new();
        for module in self.resources.topic(topic_id).await? {
            for (local, definition) in module.mcp {
                let key = serde_json::to_string(&(module.name.as_str(), local.as_str()))
                    .map_err(|error| CoreError::Internal(error.to_string()))?;
                mods.push(BindingSpec {
                    key,
                    revision: module.version,
                    definition,
                });
            }
        }
        mods.sort_by(|left, right| left.key.cmp(&right.key));
        let configured = self.mcp.enabled(topic_id).await?;
        let mut configured: Vec<BindingSpec> = configured
            .into_iter()
            .map(|record| BindingSpec {
                key: record.id.to_string(),
                revision: record.revision,
                definition: record.definition,
            })
            .collect();
        configured.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(Snapshot {
            mods,
            configured,
            builtin: self.builtin.clone(),
        })
    }

    async fn validate_target(&self, target: &ToolTarget) -> Result<()> {
        match target.source.as_str() {
            SOURCE_MODS => {
                let key: (String, String) = serde_json::from_str(&target.key)
                    .map_err(|_| CoreError::Denied("the tool target is invalid".to_string()))?;
                let enabled = self.resources.topic(target.topic_id).await?;
                let found = enabled.iter().any(|module| {
                    module.name == key.0
                        && module.version == target.revision
                        && module.mcp.contains_key(&key.1)
                });
                if found {
                    Ok(())
                } else {
                    Err(CoreError::Denied(
                        "the tool target is not enabled".to_string(),
                    ))
                }
            }
            SOURCE_CONFIGURED => {
                let id: i64 = target
                    .key
                    .parse()
                    .map_err(|_| CoreError::Denied("the tool target is invalid".to_string()))?;
                let enabled = self.mcp.enabled(target.topic_id).await?;
                let found = enabled
                    .iter()
                    .any(|record| record.id == id && record.revision == target.revision);
                if found {
                    Ok(())
                } else {
                    Err(CoreError::Denied(
                        "the tool target is not enabled".to_string(),
                    ))
                }
            }
            SOURCE_BUILTIN => Ok(()),
            _ => Err(CoreError::Denied("the tool source is unknown".to_string())),
        }
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
    async fn declarations(&self, cx: &DispatchCx, topic_id: i64) -> Result<Vec<DeclaredTool>> {
        let tools = self.tools_locked(topic_id, cx).await?;
        Ok(tools.into_iter().map(declared).collect())
    }

    async fn call(
        &self,
        cx: &DispatchCx,
        _call_id: &str,
        target: &ToolTarget,
        arguments: Value,
    ) -> Result<Value> {
        let admitted = {
            let _read = self.config.read().await;
            self.ensure_open()?;
            self.validate_target(target).await?;
            self.registry.admit(target)?
        };
        Ok(admitted
            .execute(arguments, cx.cancel().clone(), cx.deadline().into_std())
            .await?)
    }
}

fn declared(tool: BoundTool) -> DeclaredTool {
    let origin = ToolOrigin::Mcp {
        target: tool.target,
    };
    let ToolOrigin::Mcp { target } = &origin else {
        unreachable!("the origin is always an mcp tool");
    };
    let description = match &tool.description {
        Some(description) => {
            format!(
                "MCP tool '{}' from '{}': {description}",
                target.tool, target.source
            )
        }
        None => format!("MCP tool '{}' from '{}'", target.tool, target.source),
    };
    DeclaredTool {
        declaration: Tools::Function(FunctionTool {
            name: origin.exposed(),
            description: Some(description),
            parameters: Some(tool.input_schema),
            strict: None,
        }),
        origin,
    }
}

#[derive(Clone)]
struct Snapshot {
    mods: Vec<BindingSpec>,
    configured: Vec<BindingSpec>,
    builtin: BuiltinRoots,
}

impl Snapshot {
    fn identity(&self) -> Identity {
        Identity {
            mods: self
                .mods
                .iter()
                .map(|spec| (spec.key.clone(), spec.revision))
                .collect(),
            configured: self
                .configured
                .iter()
                .map(|spec| (spec.key.clone(), spec.revision))
                .collect(),
            builtin: self.builtin.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    mods: Vec<(String, u32)>,
    configured: Vec<(String, u32)>,
    builtin: BuiltinRoots,
}
