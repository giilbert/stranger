mod exec;

use bollard::{
    Docker,
    query_parameters::{
        CreateContainerOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptionsBuilder,
    },
    secret::{ContainerCreateBody, HostConfig},
};

pub struct Jail {
    status: JailStatus,
    name: String,
    docker: Docker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JailStatus {
    Running,
    Stopped,
    Destroyed,
}

impl Jail {
    pub async fn new(docker: &Docker) -> anyhow::Result<Self> {
        let container_name = "test_container".to_string(); // TODO: change

        docker
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

        docker
            .start_container(
                &container_name,
                Some(StartContainerOptionsBuilder::new().build()),
            )
            .await?;

        Ok(Jail {
            status: JailStatus::Running,
            name: container_name,
            docker: docker.clone(),
        })
    }

    pub async fn destroy(mut self) -> anyhow::Result<()> {
        self.status = JailStatus::Destroyed;

        self.docker
            .remove_container(
                &self.name,
                Some(
                    RemoveContainerOptionsBuilder::new()
                        .v(true)
                        .force(true)
                        .build(),
                ),
            )
            .await?;

        Ok(())
    }
}

impl Drop for Jail {
    fn drop(&mut self) {
        if self.status != JailStatus::Destroyed {
            tracing::warn!(
                "Jail {} was not destroyed before being dropped. This may lead to resource leaks.",
                self.name
            );
        }
    }
}
