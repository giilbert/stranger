use std::{collections::HashMap, sync::Arc};

use bollard::Docker;
use tokio::sync::RwLock;

use crate::Jail;

/// A runtime for managing [`crate::Jail`] instances.
#[derive(Debug, Clone)]
pub struct StrangerRuntime {
    /// Cheap cloneable inner state.
    inner: Arc<RuntimeInner>,
}

/// Inner state of [`StrangerRuntime`]. This is wrapped in an [`Arc`] to allow for cheap cloning.
#[derive(Debug)]
pub(crate) struct RuntimeInner {
    config: StrangerConfig,
    docker: Docker,
    jails: RwLock<HashMap<String, crate::Jail>>,
}

#[derive(Debug, Clone)]
pub struct StrangerConfig {}

impl StrangerRuntime {
    /// Constructs a new [`StrangerRuntime`] with the given configuration, connecting to the local
    /// Docker daemon.
    pub fn new(config: StrangerConfig) -> anyhow::Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;

        Ok(Self {
            inner: Arc::new(RuntimeInner {
                docker,
                config,
                jails: RwLock::new(HashMap::new()),
            }),
        })
    }

    /// Get a reference to the Docker client.
    pub fn docker(&self) -> &Docker {
        &self.inner.docker
    }

    /// Get a reference to the configuration of the runtime.
    pub fn config(&self) -> &StrangerConfig {
        &self.inner.config
    }

    /// Create a new [`Jail`] managed by this runtime.
    pub async fn create(&self) -> anyhow::Result<Jail> {
        let jail = Jail::new(self).await?;
        self.inner
            .jails
            .write()
            .await
            .insert(jail.handle.name().to_string(), jail.clone());
        Ok(jail)
    }

    /// Remove a [`Jail`] from this runtime's management by name.
    pub async fn remove(&self, name: &str) -> Option<Jail> {
        let jail = self.inner.jails.write().await.remove(name);

        if let Some(jail) = jail.as_ref()
            && !jail.status().is_destroyed_or_destroying()
        {
            // jail.destroy() calls this method, so boxing the returned future is needed to avoid
            // infinitely-sized future types.
            let fut = Box::pin(jail.destroy());
            if let Err(e) = fut.await {
                tracing::error!("failed to destroy jail `{}` while removing: {:?}", name, e);
            }
        }

        jail
    }

    /// Clean up all jails managed by this runtime.
    ///
    /// If any jails are still running, they are "orphaned" and will be destroyed with a warning.
    #[tracing::instrument(skip(self))]
    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let jails = self.inner.jails.write().await.drain().collect::<Vec<_>>();
        for (_name, jail) in jails {
            if !jail.status().is_destroyed_or_destroying() {
                tracing::warn!(
                    "orphaned jail `{}` found in status `{:?}`, destroying...",
                    jail.name(),
                    jail.status()
                );
                jail.destroy().await?;
                tracing::warn!("--> jail `{}` destroyed successfully", jail.name());
            }
        }
        Ok(())
    }
}
