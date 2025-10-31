mod api;
mod state;

use axum::{Router, extract::State, middleware::Next};
use stranger_jail::StrangerRuntime;
use tracing_subscriber::{
    EnvFilter,
    fmt::{self},
    prelude::*,
};

use crate::state::AppState;

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
        .merge(api::create_router())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            |State(state): State<AppState>, req, next: Next| async move {
                state.runtime().wait_for_docker(None).await.ok();
                if dotenvy::var("FLY_APP_NAME").is_ok() {
                    StrangerRuntime::weird_hack_for_fly_cgroups_fix().await;
                }
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
