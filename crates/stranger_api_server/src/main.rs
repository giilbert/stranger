use std::sync::Arc;

use axum::{Router, extract::State};
use stranger_jail::{JailConfig, StrangerConfig, StrangerRuntime};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self},
    prelude::*,
};

#[tracing::instrument(skip(runtime))]
async fn get_time_with_container(runtime: &StrangerRuntime) -> anyhow::Result<String> {
    let jail = runtime
        .create(JailConfig {
            image: "ubuntu:latest".to_string(),
            ..Default::default()
        })
        .await?;

    tracing::info!("created jail `{}`", jail.name());

    let mut exec = jail.sh("date").await?;

    let output = exec.output().all().await?;
    jail.destroy().await?;

    Ok(output)
}

pub struct AppState {
    runtime: StrangerRuntime,
}

async fn get_date(state: State<Arc<AppState>>) -> String {
    format!(
        "hewwo! the date is {}",
        get_time_with_container(&state.runtime)
            .await
            .unwrap_or_else(|e| format!("error: {:?}", e))
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const DEFAULT_LOG_SETTINGS: &str = "stranger_jail=info,stranger_api_server=info";
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::builder()
                .parse(std::env::var("RUST_LOG").unwrap_or(DEFAULT_LOG_SETTINGS.to_string()))?,
        )
        .init();

    tracing::info!("hello stranger!");

    let runtime = StrangerRuntime::new(StrangerConfig {
        blkio_device: "/dev/nvme0".to_string(),
    })?;

    let state = Arc::new(AppState { runtime });
    let app: Router<()> = Router::new()
        .route("/date", axum::routing::get(get_date))
        .with_state(state.clone());

    let address = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;

    tracing::info!("listening on {}", address.local_addr()?);

    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {}
        _ = axum::serve(address, app) => {}
    };

    state.runtime.cleanup().await?;
    tracing::info!("everything cleaned up! shutting down..");

    Ok(())
}
