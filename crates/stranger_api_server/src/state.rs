use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use stranger_jail::{StrangerConfig, StrangerRuntime};

use crate::api::stateless::StatelessWorkers;

#[derive(Debug, Clone)]
pub struct AppState {
    pub(crate) inner: Arc<AppStateInner>,
}

#[derive(Debug)]
pub(crate) struct AppStateInner {
    environment: Environment,
    config_directory: PathBuf,
    config: StrangerApiServerConfig,
    runtime: StrangerRuntime,
    pub(super) stateless_workers: StatelessWorkers,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Environment {
    Development,
    Production,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrangerApiServerConfig {
    runtime: StrangerConfig,
}

impl Environment {
    pub fn is_production(&self) -> bool {
        matches!(self, Environment::Production)
    }

    pub fn is_development(&self) -> bool {
        matches!(self, Environment::Development)
    }
}

impl AppState {
    const CONFIG_FILE_NAME: &'static str = "config.toml";

    #[tracing::instrument]
    pub async fn init() -> anyhow::Result<Self> {
        if let Ok(path) = dotenvy::dotenv() {
            tracing::info!("loaded .env file from {}", path.display());
        }

        let environment = match dotenvy::var("STRANGER_ENV")
            .unwrap_or_else(|_| "development".to_string())
            .as_str()
        {
            "production" => Environment::Production,
            _ => Environment::Development,
        };
        tracing::info!("hello world! running in `{:?}` environment", environment);

        let config_directory = std::fs::canonicalize(match dotenvy::var("STRANGER_CONFIG_DIR") {
            Ok(dir) => PathBuf::from_str(&dir)
                .context(format!("failed to parse STRANGER_CONFIG_DIR `{}`", dir))?,
            Err(_) => match &environment {
                Environment::Development => PathBuf::from("./etc"),
                Environment::Production => PathBuf::from("/etc/stranger"),
            },
        })?;

        let config = Self::try_load_config(&config_directory)?;
        let runtime = StrangerRuntime::new(config.runtime.clone())?;

        Ok(Self {
            inner: Arc::new(AppStateInner {
                stateless_workers: StatelessWorkers::new(&runtime)
                    .context("failed to create stateless workers")?,
                runtime,
                config_directory,
                config,
                environment,
            }),
        })
    }

    /// Attempts to load the configuration file from the specified root directory.
    ///
    /// If the file is missing, a default configuration is created and returned.
    /// If the file exists but cannot be read or parsed, an error is returned.
    fn try_load_config(config_directory: &PathBuf) -> anyhow::Result<StrangerApiServerConfig> {
        let config_file_path = config_directory.join(Self::CONFIG_FILE_NAME);

        if std::fs::exists(&config_file_path)? {
            tracing::info!("configuration loaded from {}", config_file_path.display());
            let config_contents = std::fs::read_to_string(&config_file_path).context(format!(
                "failed to read config file `{}`",
                config_file_path.display()
            ))?;
            let config: StrangerApiServerConfig =
                toml::from_str(&config_contents).context(format!(
                    "failed to parse config file `{}`",
                    config_file_path.display()
                ))?;

            Ok(config)
        } else {
            tracing::info!(
                "config file `{}` not found, creating default",
                config_file_path.display()
            );

            let config = Self::default_config();

            std::fs::create_dir_all(&config_directory)?;
            std::fs::write(
                &config_file_path,
                toml::to_string_pretty(&config).context("failed to serialize default config")?,
            )?;

            Ok(config)
        }
    }

    /// Creates a default configuration. This is used if the configuration file is missing.
    fn default_config() -> StrangerApiServerConfig {
        StrangerApiServerConfig {
            runtime: StrangerConfig {
                blkio_device: "/dev/nvme0".to_string(),
            },
        }
    }

    pub fn runtime(&self) -> &StrangerRuntime {
        &self.inner.runtime
    }

    pub fn environment(&self) -> &Environment {
        &self.inner.environment
    }

    pub fn config_directory(&self) -> &PathBuf {
        &self.inner.config_directory
    }
}
