use std::{sync::Arc, time::Duration};

use anyhow::Context;
use bollard::{
    query_parameters::{
        CreateContainerOptionsBuilder, InspectContainerOptionsBuilder,
        RemoveContainerOptionsBuilder, StartContainerOptionsBuilder,
    },
    secret::{ContainerCreateBody, HostConfig},
};
use parking_lot::RwLock;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::runtime::StrangerRuntime;

/// Internal actor for managing the jail's state and operations.
///
/// [`JailActor::run`] should be spawned as a background task to manage the jail's lifecycle.
#[derive(Debug)]
pub(crate) struct JailActor {
    name: String,
    status: RwLock<JailStatus>,
    pub(crate) runtime: StrangerRuntime,

    /// Used to signal that the jail should stop responding to events and clean up resources.
    pub(crate) cancellation_token: CancellationToken,
    /// Used to signal that task behind the jail has fully stopped and cleaned up resources.
    pub(crate) destroyed_token: CancellationToken,
}

/// A sandboxed environment for executing untrusted code.
#[derive(Debug, Clone)]
pub struct Jail {
    pub(super) handle: Arc<JailActor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JailStatus {
    Running,
    Stopped,
    /// The container is in the process of being destroyed, but still hasn't been fully removed yet.
    Destroying,
    /// The container has been fully removed and resources cleaned up.
    Destroyed,
}

impl JailStatus {
    pub fn is_destroyed_or_destroying(&self) -> bool {
        matches!(self, JailStatus::Destroying | JailStatus::Destroyed)
    }
}

impl JailActor {
    async fn new(runtime: &StrangerRuntime) -> anyhow::Result<Self> {
        let cancellation_token = CancellationToken::new();
        let container_name = "test_container".to_string(); // TODO: change

        runtime
            .docker()
            .create_container(
                Some(
                    CreateContainerOptionsBuilder::new()
                        .name(&container_name)
                        .build(),
                ),
                ContainerCreateBody {
                    image: Some("ubuntu:latest".to_string()),
                    host_config: Some(HostConfig {
                        // `runsc` is the runtime for gVisor, which provides additional sandboxing
                        // capabilities needed for the jail.
                        runtime: Some("runsc".to_string()),
                        ..Default::default()
                    }),
                    cmd: Some(vec!["sleep".to_string(), "infinity".to_string()]),
                    ..Default::default()
                },
            )
            .await?;

        runtime
            .docker()
            .start_container(
                &container_name,
                Some(StartContainerOptionsBuilder::new().build()),
            )
            .await?;

        Ok(JailActor {
            name: container_name,
            status: RwLock::new(JailStatus::Running),
            runtime: runtime.clone(),
            cancellation_token,
            destroyed_token: CancellationToken::new(),
        })
    }

    /// Main loop to monitor and manage the jail.
    ///
    /// `tokio::select!` is used to concurrently handle
    /// many different tasks. `self.cancellation_token` is used to signal when the jail should
    /// stop responding to events and clean up resources.
    pub(crate) async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let mut check_disk_usage_interval = time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => break,
                _ = check_disk_usage_interval.tick() => self.task_check_disk_usage().await?,
            }
        }

        self.destroyed_token.cancel();

        Ok(())
    }

    async fn task_check_disk_usage(&self) -> anyhow::Result<()> {
        const MAX_DISK_USAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GiB
        let inspect = self
            .runtime
            .docker()
            .inspect_container(
                self.name(),
                Some(InspectContainerOptionsBuilder::new().size(true).build()),
            )
            .await
            .context(format!("failed to inspect container {}", self.name))?;

        let size = inspect
            .size_rw
            .ok_or_else(|| anyhow::anyhow!("failed to get container size"))?
            as u64;

        if size > MAX_DISK_USAGE_BYTES {
            tracing::warn!(
                "container {} exceeded max disk usage ({} bytes > {} bytes), destroying",
                self.name(),
                size,
                MAX_DISK_USAGE_BYTES
            );
            self.destroy().await?;
        }

        Ok(())
    }

    /// Implementation for [`Jail::destroy`].
    pub(crate) async fn destroy(&self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();

        // No-op if the jail is already being destroyed or has been destroyed.
        if self.status().is_destroyed_or_destroying() {
            return Ok(());
        }
        *self.status.write() = JailStatus::Destroying;

        self.runtime
            .docker()
            .remove_container(
                &self.name,
                Some(
                    RemoveContainerOptionsBuilder::new()
                        .v(true)
                        .force(true)
                        .build(),
                ),
            )
            .await
            .context(format!("failed to remove container {}", self.name))?;

        // Wait until the `run` task has fully stopped and cleaned up resources.
        self.destroyed_token.cancelled().await;
        self.runtime.remove(&self.name).await;

        *self.status.write() = JailStatus::Destroyed;

        Ok(())
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn status(&self) -> JailStatus {
        self.status.read().clone()
    }
}

impl Jail {
    pub(crate) async fn new(runtime: &StrangerRuntime) -> anyhow::Result<Self> {
        let actor = Arc::new(JailActor::new(runtime).await?);
        tokio::spawn(actor.clone().run());
        Ok(Self { handle: actor })
    }

    /// Get the name of the jail's underlying container.
    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Get the current status of the jail.
    pub fn status(&self) -> JailStatus {
        self.handle.status()
    }

    /// Destroys the jail, stopping and removing its underlying container.
    pub async fn destroy(&self) -> anyhow::Result<()> {
        self.handle.destroy().await
    }
}

impl Drop for JailActor {
    fn drop(&mut self) {
        if *self.status.read() != JailStatus::Destroyed {
            tracing::warn!(
                "Jail {} was not destroyed before being dropped. This may lead to resource leaks.",
                self.name
            );
        }
    }
}
