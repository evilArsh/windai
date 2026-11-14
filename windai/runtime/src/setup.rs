use super::topic::TopicAgents;
use super::{Inner, Runtime, RuntimeError, RuntimeOptions, directory};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::{Mutex, RwLock};
use wind_core::error::CoreError;
use wind_core::feed;
use wind_core::llm::LlmRunner;
use wind_core::resources::ResourceService;
use wind_core::script::ScriptRuntime;
use wind_event::{Dispatcher, Observer};
use wind_mcp::McpOptions;
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;

pub(super) async fn open(options: RuntimeOptions) -> Result<Runtime, RuntimeError> {
    let RuntimeOptions { dirs, feed, limits } = options;
    for dir in [
        dirs.root().to_path_buf(),
        dirs.mods_root(),
        dirs.topics_root(),
        dirs.public_skills(),
        dirs.bytecode_root(),
    ] {
        tokio::fs::create_dir_all(&dir).await.map_err(|error| {
            RuntimeError::Internal(format!("create {}: {error}", dir.display()))
        })?;
    }
    let storage = Storage::open(&dirs.db_file()).await?;
    let (feed, feed_runtime) = feed::open(storage.clone(), feed).await?;
    let caller = Arc::new(ScriptRuntime::quickjs(
        storage.clone(),
        dirs.clone(),
        Arc::new(feed.clone()),
        limits,
    )?);
    let roster = Arc::new(Roster::load(&storage).await.map_err(CoreError::from)?);
    let dispatcher = Arc::new(Dispatcher::new(
        directory::merged(),
        roster.clone(),
        caller,
        Arc::new(feed.clone()),
        limits,
    ));
    let registry = wind_mcp::init(McpOptions::new(dirs.mods_root()), dispatcher.clone());
    let installer = Installer::new(storage.clone(), dispatcher.clone(), roster, dirs.clone());
    let services = Arc::new(ResourceService::new(
        installer,
        Resources::new(storage.clone(), dirs.clone()),
        storage.clone(),
        registry,
        dirs.topics_root(),
    ));
    if let Err(error) = services.restore().await {
        let _ = services.shutdown().await;
        return Err(error.into());
    }
    let runner = Arc::new(LlmRunner::new(
        storage.clone(),
        dispatcher.clone(),
        services.clone(),
        services.clone(),
    ));
    let agents = TopicAgents::new(storage.clone(), dispatcher.clone(), runner);
    let observers: Vec<Arc<dyn Observer>> = vec![dispatcher.clone()];
    let weak = observers.iter().map(Arc::downgrade).collect();
    let feed_task = tokio::spawn(feed_runtime.run(weak));
    Ok(Runtime {
        inner: Arc::new(Inner {
            storage,
            dispatcher,
            services,
            agents,
            feed,
            _observers: observers,
            feed_task: Mutex::new(Some(feed_task)),
            stopped: AtomicBool::new(false),
            operations: RwLock::new(()),
            shutdown_task: Mutex::new(None),
        }),
    })
}
