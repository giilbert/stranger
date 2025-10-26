mod state;

use axum::{Router, extract::State, middleware::Next};
use stranger_jail::{JailConfig, StrangerRuntime};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self},
    prelude::*,
};

use crate::state::AppState;

#[tracing::instrument(skip(runtime))]
async fn get_time_with_container(runtime: &StrangerRuntime) -> anyhow::Result<String> {
    let jail = runtime
        .create(JailConfig {
            image: "ubuntu:latest".to_string(),
            cpu_set: Some("0".to_string()),
            memory_limit: Some(128 * 1024 * 1024), // 128 MB
            ..Default::default()
        })
        .await?;

    tracing::info!("created jail `{}`", jail.name());

    let mut exec = jail.sh("date").await?;

    let output = exec.output().all().await?;
    jail.destroy().await?;

    Ok(output)
}

async fn get_date(state: State<AppState>) -> String {
    format!(
        "hewwo! the date is {}",
        get_time_with_container(state.runtime())
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

    let state = AppState::init().await?;
    let app: Router<()> = Router::new()
        .route("/", axum::routing::get(|| async { "hewwo stranger!" }))
        .route("/date", axum::routing::get(get_date))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            |State(state): State<AppState>, req, next: Next| async move {
                state.runtime().wait_for_docker(None).await.ok();
                next.run(req).await
            },
        ))
        .with_state(state.clone());

    let address = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;

    tracing::info!("listening on {}", address.local_addr()?);

    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {}
        _ = axum::serve(address, app) => {}
    };

    state.runtime().cleanup().await?;
    tracing::info!("everything cleaned up! shutting down..");

    Ok(())
}
