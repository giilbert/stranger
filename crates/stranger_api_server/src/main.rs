use stranger_jail::{JailConfig, StrangerConfig, StrangerRuntime};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self},
    prelude::*,
};

#[tracing::instrument(skip(runtime))]
async fn test_jail(runtime: &StrangerRuntime) -> anyhow::Result<()> {
    let jail = runtime
        .create(JailConfig {
            image: "ubuntu:latest".to_string(),
            ..Default::default()
        })
        .await?;

    tracing::info!("created jail `{}`", jail.name());

    let mut exec = jail.sh("date").await?;

    let output = exec.output();
    tracing::info!(
        "output:\n  {}",
        output
            .all()
            .await?
            .split('\n')
            .map(|s| format!("  {s}"))
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
    );

    jail.destroy().await?;

    Ok(())
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

    let start = std::time::Instant::now();
    let runtime = StrangerRuntime::new(StrangerConfig {
        blkio_device: "/dev/nvme0".to_string(),
    })?;

    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {},
        res = test_jail(&runtime) => {
            if let Err(err) = res {
                tracing::error!("error during test_jail: {:?}", err);
            }
        }
    }

    runtime.cleanup().await?;
    tracing::info!("done in {:?}", start.elapsed());

    Ok(())
}
