use super::directory::merged;
use super::error::RuntimeError;
use super::shutdown;
use super::tree::{Runtime, TreeHandle};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use wind_core::agent::domain::AgentSpec;
use wind_core::agent::events::{AgentCreate, AgentError, CORE_AGENT_ERROR};
use wind_core::agent::hub::SubmitOutcome;
use wind_core::agent::runtime::{AgentRunner, DEFAULT_MAX_ROUNDS};
use wind_core::agent::tree::CancelOrigin;
use wind_core::conversation::assembler::ProjectionInvalidations;
use wind_core::conversation::events::{CORE_INPUT_ACCEPTED, InputAccepted};
use wind_core::conversation::writer::BlockWriter;
use wind_core::db::Db;
use wind_core::error::CoreError;
use wind_core::feed::hub::FeedHub;
use wind_core::llm::ports::{McpTools, ModRequests, TopicMods};
use wind_core::models::{AgentMode, AgentPhase, PendingCreate, PendingKind};
use wind_core::storage::Storage;
use wind_core::usecase::UseCases;
use wind_event::context::DispatchCx;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{EventSeq, Origin, Scope, Source};
use wind_event::host::ModHost;
use wind_mcp::registry::Registry;
use wind_mcp::runtime::ModServers;
use wind_mcp::tools::McpToolSource;
use wind_mod::assembly::{NoHost, rebuild_subscriptions};
use wind_mod::catalogue::Catalogue;
use wind_mod::compiler::{Compiler, NoCompiler};
use wind_mod::dirs::AppDirs;
use wind_mod::enablement::Enablement;
use wind_mod::feed::ModFeed;
use wind_mod::installer::{Installer, TopicServers};
use wind_mod::manifest::DEFAULT_SUB_TIMEOUT_MS;
use wind_script::assembly::{DeferredHost, ScriptRuntime};
use wind_script::request::RequestCaller;
use wind_wire::message::Content;

pub struct Assembly {
    storage: Storage,
    bus: Arc<Dispatcher>,
    hub: Arc<FeedHub>,
    #[allow(dead_code)]
    feed: Arc<ModFeed>,
    installer: Installer,
    registry: Arc<Registry>,
    #[allow(dead_code)]
    requests: Option<Arc<RequestCaller>>,
    runtime: Runtime,
}

/// 一次提交操作的预算
const SUBMIT_BUDGET: Duration = Duration::from_millis(50);

struct NoScriptRuntime;

#[async_trait::async_trait]
impl ModRequests for NoScriptRuntime {
    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        _input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        Err(CoreError::Denied(
            "this deployment has no script runtime".to_string(),
        ))
    }
}

impl Assembly {
    pub async fn open(
        dirs: AppDirs,
        servers: Option<Arc<dyn TopicServers>>,
        host: Option<Arc<dyn ModHost>>,
        compiler: Option<Arc<dyn Compiler>>,
    ) -> Result<Self, RuntimeError> {
        let directory = merged();

        let db = Db::open(&dirs.db_file()).await?;
        db.init_schema().await?;
        let storage = Storage::new(db);

        let enablement = Arc::new(Enablement::load(storage.clone()).await?);

        // 内置观察者
        let hub = FeedHub::open(storage.clone(), None).await?;

        let deferred = Arc::new(DeferredHost::new());
        // 事件总线
        let bus = Arc::new(Dispatcher::new(
            directory,
            enablement.clone(),
            deferred.clone(),
            Limits::default(),
        ));

        // 附加内建观测者
        bus.attach_observer(hub.clone());

        let seed = storage.event_log_max_seq().await?;
        if seed > 0 {
            bus.seed_seq(EventSeq(seed));
            hub.seed_high_water(EventSeq(seed));
        }

        let feed = Arc::new(ModFeed::open(hub.clone(), bus.clone(), storage.clone()).await?);
        hub.set_baselines(feed.clone());

        let registry = Arc::new(Registry::new(bus.clone()));

        let caller: Option<Arc<RequestCaller>> = if compiler.is_some() {
            if host.is_some() {
                log::warn!(
                    "[runtime] both a compiler and a ModHost were passed; the script runtime's host wins and the passed host is dropped"
                );
            }
            // js 运行时
            let runtime =
                ScriptRuntime::open(storage.clone(), bus.clone(), dirs.clone(), feed.clone())
                    .await?;
            deferred.attach(runtime.host());
            Some(Arc::new(runtime.caller_for_agent()))
        } else {
            deferred.attach(host.unwrap_or_else(|| Arc::new(NoHost)));
            None
        };

        let servers = servers.unwrap_or_else(|| {
            Arc::new(ModServers::new(
                registry.clone(),
                storage.clone(),
                dirs.clone(),
                bus.clone(),
            ))
        });
        // mod 管理
        let installer = Installer::new(
            storage.clone(),
            bus.clone(),
            dirs,
            servers,
            enablement,
            feed.clone(),
            compiler.unwrap_or_else(|| Arc::new(NoCompiler)),
        );
        rebuild_subscriptions(&storage, &bus, DEFAULT_SUB_TIMEOUT_MS).await?;

        let mods: Arc<dyn TopicMods> =
            Arc::new(Catalogue::new(storage.clone(), bus.directory().clone()));
        let mcp: Arc<dyn McpTools> = Arc::new(McpToolSource::new(registry.clone()));
        let requests: Arc<dyn ModRequests> = match &caller {
            Some(caller) => caller.clone(),
            None => Arc::new(NoScriptRuntime),
        };
        let runner = Arc::new(AgentRunner::new(
            BlockWriter::new(
                hub.clone(),
                bus.clone(),
                storage.clone(),
                ProjectionInvalidations::default(),
            ),
            UseCases::new(storage.clone(), bus.clone()),
            bus.clone(),
            mods,
            Some(mcp),
            requests,
            storage.clone(),
            DEFAULT_MAX_ROUNDS,
        ));

        let runtime = Runtime::new(storage.clone(), bus.clone(), runner);
        Ok(Self {
            storage,
            bus,
            hub,
            feed,
            installer,
            registry,
            requests: caller,
            runtime,
        })
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn bus(&self) -> &Arc<Dispatcher> {
        &self.bus
    }

    pub fn hub(&self) -> &Arc<FeedHub> {
        &self.hub
    }

    pub fn installer(&self) -> &Installer {
        &self.installer
    }

    pub async fn tree_of(&self, topic_id: i64) -> Result<TreeHandle, RuntimeError> {
        self.runtime.tree_of(topic_id).await
    }

    pub async fn submit(
        &self,
        topic_id: i64,
        content: Vec<Content>,
    ) -> Result<SubmitOutcome, RuntimeError> {
        let tree = self.tree_of(topic_id).await?;
        let _submitting = tree.submit_lock.lock().await;
        let cx = runtime_cx(topic_id);

        if let Some(root) = root_of(&tree).await? {
            let pending_id = self
                .storage
                .pending_insert(PendingCreate {
                    instance_id: root,
                    kind: PendingKind::User,
                    source_id: None,
                    payload: content.to_vec(),
                })
                .await?;
            // 任务加入节点 pending 列表
            let outcome = tree.hub.submit(root, user_spec(root, pending_id)).await?;
            return self
                .settle_submission(&cx, topic_id, root, pending_id, outcome)
                .await;
        }

        let create = tree
            .hub
            .intercept_create(
                &cx,
                AgentCreate {
                    topic_id,
                    parent_id: None,
                    mode: AgentMode::Sync.to_string(),
                    task: None,
                },
            )
            .await;
        let mode: AgentMode = create.mode.parse().map_err(CoreError::from)?;

        let (root, pending_id) = self
            .storage
            .with_tx(async move |tx| {
                let root = tx
                    .agent_ensure_main(topic_id, mode, AgentPhase::Running)
                    .await?;
                let pending_id = tx
                    .pending_insert(PendingCreate {
                        instance_id: root,
                        kind: PendingKind::User,
                        source_id: None,
                        payload: content,
                    })
                    .await?;
                Ok((root, pending_id))
            })
            .await?;
        let spec = user_spec(root, pending_id);

        match tree
            .hub
            .spawn_at_with_spec(&cx, root, &create, spec.clone())
            .await
        {
            Ok(_) => Ok(SubmitOutcome::Launched),
            Err(err) => {
                self.bus
                    .notify(
                        &CORE_AGENT_ERROR,
                        &cx,
                        AgentError {
                            instance_id: root,
                            message_id: None,
                            error: err.to_string(),
                        },
                    )
                    .await;
                let outcome = tree.hub.submit(root, spec).await?;
                self.settle_submission(&cx, topic_id, root, pending_id, outcome)
                    .await
            }
        }
    }

    async fn settle_submission(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        root: i64,
        pending_id: i64,
        outcome: SubmitOutcome,
    ) -> Result<SubmitOutcome, RuntimeError> {
        match outcome {
            SubmitOutcome::Queued => {
                self.bus
                    .notify(
                        &CORE_INPUT_ACCEPTED,
                        cx,
                        InputAccepted {
                            topic_id,
                            instance_id: root,
                            pending_id,
                        },
                    )
                    .await;
            }
            SubmitOutcome::Dropped => {
                self.storage.pending_delete(&[pending_id]).await?;
            }
            SubmitOutcome::Launched => {}
        }
        Ok(outcome)
    }

    pub async fn cancel(&self, topic_id: i64, instance_id: i64) -> Result<(), RuntimeError> {
        let tree = self.tree_of(topic_id).await?;
        let _cancelling = tree.submit_lock.lock().await;
        let subtree = subtree_ids(&tree, instance_id).await?;
        tree.hub
            .cancel(&runtime_cx(topic_id), instance_id, CancelOrigin::External)
            .await?;
        self.storage.pending_delete_by_instances(&subtree).await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        let Some(trees) = self.runtime.begin_shutdown() else {
            return Ok(());
        };
        shutdown::stop(&self.bus, &self.registry, trees, &self.hub).await
    }
}

fn runtime_cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Topic { topic_id },
        Source::core("runtime"),
        SUBMIT_BUDGET,
    )
}

async fn has_node(tree: &TreeHandle, id: i64) -> Result<bool, RuntimeError> {
    Ok(tree.hub.snapshot().await?.iter().any(|node| node.id == id))
}

async fn root_of(tree: &TreeHandle) -> Result<Option<i64>, RuntimeError> {
    Ok(tree
        .hub
        .snapshot()
        .await?
        .iter()
        .find(|node| node.parent.is_none())
        .map(|node| node.id))
}

fn user_spec(root: i64, pending_id: i64) -> AgentSpec {
    AgentSpec {
        instance_id: root,
        message_id: None,
        pending_id: Some(pending_id),
        turn_id: None,
    }
}

async fn subtree_ids(tree: &TreeHandle, root: i64) -> Result<Vec<i64>, RuntimeError> {
    let nodes = tree.hub.snapshot().await?;
    let children: HashMap<i64, Vec<i64>> = nodes
        .iter()
        .map(|node| (node.id, node.children.clone()))
        .collect();
    let mut seen = HashSet::new();
    let mut order = Vec::new();
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        order.push(id);
        if let Some(kids) = children.get(&id) {
            stack.extend(kids.iter().copied());
        }
    }
    Ok(order)
}
