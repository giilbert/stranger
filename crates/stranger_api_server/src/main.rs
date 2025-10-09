use stranger_jail::Jail;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

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

    tracing::info!("Starting a jail...");
    let start = std::time::Instant::now();

    let docker = bollard::Docker::connect_with_local_defaults()?;
    let jail = Jail::new(&docker).await?;

    tracing::info!("Jail started in {:?}", start.elapsed());

    tracing::info!("Running a command...");
    jail.sh("echo hello world").await?;

    jail.destroy().await?;
    tracing::info!("Done in {:?}", start.elapsed());

    Ok(())
}
