use super::error::RuntimeError;
use super::tree::TreeHandle;
use std::sync::Arc;
use wind_core::feed::hub::FeedHub;
use wind_event::dispatch::Dispatcher;
use wind_mcp::registry::Registry;

pub(crate) async fn stop(
    bus: &Arc<Dispatcher>,
    registry: &Registry,
    trees: Vec<TreeHandle>,
    hub: &FeedHub,
) -> Result<(), RuntimeError> {
    bus.rebuild_index(Vec::new());

    registry.shutdown().await;

    for tree in trees {
        let _ = tree.hub.shutdown().await;
    }

    hub.flush().await?;
    Ok(())
}
