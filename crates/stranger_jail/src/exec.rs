use bollard::{exec::StartExecResults, secret::ExecConfig};
use futures_util::StreamExt;

use crate::Jail;

pub struct JailExec {}

impl Jail {
    pub async fn sh(&self, command: impl Into<String>) -> anyhow::Result<JailExec> {
        let exec_results = self
            .docker
            .create_exec(
                &self.name,
                ExecConfig {
                    cmd: Some(vec!["sh".to_string(), "-c".to_string(), command.into()]),
                    attach_stdout: Some(true),
                    ..Default::default()
                },
            )
            .await?;

        let exec = self.docker.start_exec(&exec_results.id, None).await?;

        match exec {
            StartExecResults::Attached {
                mut output,
                input: _input,
            } => {
                while let Some(Ok(line)) = output.next().await {
                    let string = line.to_string();
                    tracing::info!("log output: {:?}", string);
                }
            }
            StartExecResults::Detached => {}
        }

        Ok(JailExec {})
    }
}
