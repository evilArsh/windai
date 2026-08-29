use axum::serve;
use chrono::Utc;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;

#[cfg(not(unix))]
use std::future;
#[cfg(unix)]
use tokio::signal::unix;

use wind_core::WindCore;
use wind_http::app::app;
use wind_http::config::AppConfig;
use wind_http::state::AppState;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = AppConfig::from_env();
    let core = Arc::new(
        WindCore::init_local(config.db_path.as_deref())
            .await
            .expect("init core failed"),
    );
    let state = AppState::new(config.clone(), core, Utc::now().timestamp());

    let listener = TcpListener::bind((config.host.as_str(), config.port))
        .await
        .expect("bind failed");
    log::info!("wind-http listening on {}:{}", config.host, config.port);

    serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let ctrl_c = async { signal::ctrl_c().await.expect("ctrl-c handler") };
    #[cfg(unix)]
    let terminate = async {
        unix::signal(unix::SignalKind::terminate())
            .expect("signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
