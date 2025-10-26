use std::{collections::HashMap, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use bollard::Docker;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Jail, jail::JailConfig};

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
    docker: ArcSwap<Option<Docker>>,
    jails: RwLock<HashMap<String, crate::Jail>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrangerConfig {
    /// The block device to limit disk I/O on (e.g., "/dev/nvme0"). This should normally be the
    /// same device that Docker is using for its storage.
    pub blkio_device: String,
}

impl StrangerRuntime {
    /// Constructs a new [`StrangerRuntime`] with the given configuration, connecting to the local
    /// Docker daemon.
    pub fn new(config: StrangerConfig) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                docker: ArcSwap::new(Arc::new(None)),
                config,
                jails: RwLock::new(HashMap::new()),
            }),
        })
    }

    /// Create a new [`StrangerRuntime`] and wait until the Docker daemon is reachable.
    pub async fn connect(
        config: StrangerConfig,
        timeout: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let runtime = Self::new(config)?;
        runtime.wait_for_docker(timeout).await?;
        Ok(runtime)
    }

    /// Get a reference to the Docker client.
    pub fn docker(&self) -> Docker {
        (&*self.inner.docker.load())
            .as_ref()
            .as_ref()
            .expect("docker client not initialized")
            .clone()
    }

    /// Get a reference to the configuration of the runtime.
    pub fn config(&self) -> &StrangerConfig {
        &self.inner.config
    }

    /// Wait until the Docker daemon is reachable with an optional timeout.
    ///
    /// This method will continuously attempt to ping the Docker daemon until it succeeds.
    #[tracing::instrument(skip(self))]
    pub async fn wait_for_docker(&self, timeout: Option<Duration>) -> anyhow::Result<()> {
        let start = tokio::time::Instant::now();

        loop {
            let is_connect_successful = if self.inner.docker.load().is_none() {
                match Docker::connect_with_local_defaults() {
                    Ok(docker) => {
                        self.inner.docker.store(Arc::new(Some(docker)));
                        Self::weird_hack_for_fly_cgroups_fix().await;
                        true
                    }
                    Err(e) => {
                        tracing::trace!("failed to create Docker client: {:?}, retrying...", e);
                        false
                    }
                }
            } else {
                true
            };

            if !is_connect_successful {
                tracing::trace!("waiting for Docker daemon to become reachable...",);

                if let Some(timeout) = timeout {
                    if start.elapsed() >= timeout {
                        return Err(anyhow::anyhow!(
                            "timed out waiting for Docker daemon to become reachable"
                        ));
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break Ok(());
            }
        }
    }

    /// FIXME: On Fly.io, the cpu.cpuset option with runsc does not work on first try.
    ///
    /// Running `echo 0 > /sys/fs/cgroup/cpu/docker/[insert]/cpuset.cpus` fails with permission
    /// denied: unknown. But after creating a container with
    /// `docker run --cpuset-cpus 0 --rm -it ubuntu /bin/bash`, it works?
    ///
    /// This does the same thing programmatically.
    async fn weird_hack_for_fly_cgroups_fix() {
        // create a temporary container to fix cgroup cpu.cpuset issue on Fly.io
        tracing::info!("creating temporary container to fix cpu.cpuset issue");
        let _ = tokio::process::Command::new("docker")
            .args(&[
                "run",
                "--rm",
                "--cpuset-cpus",
                "0",
                "--name",
                "stranger-jail-temp-cpuset-fix",
                "ubuntu",
                "sleep",
                "1",
            ])
            .status()
            .await;
    }

    /// Create a new [`Jail`] managed by this runtime.
    pub async fn create(&self, config: JailConfig) -> anyhow::Result<Jail> {
        let jail = Jail::new(self, config).await?;
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
