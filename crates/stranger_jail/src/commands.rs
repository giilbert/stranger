use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::jail::JailActor;

/// A way of interacting with a [`crate::Jail`] that is serializable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JailCommand {
    pub id: u32,
    pub kind: JailCommandKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JailCommandKind {
    /// Calls [`crate::Jail::sh`] with the given command.
    ///
    /// Returns a [`JailCommandResponse::Sh`] to the given `return_to` channel.
    Sh { command: String },
}

/// Returned by a [`JailCommand::Sh`] command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JailCommandResponse {
    pub id: u32,
    pub response: Result<Value, String>,
}

/// The response from a `sh` command. See [`JailCommand::Sh`] and [`JailCommandResponse::Sh`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShCommandResponse {
    stdout: String,
    stderr: String,
    status: u8,
}

impl JailActor {
    pub(super) fn take_commands(&self) -> Option<mpsc::Receiver<JailCommand>> {
        self.commands_rx.lock().take()
    }

    /// Sets up handling for incoming commands.
    pub(super) async fn handle_command(
        self: Arc<Self>,
        commands_rx: &mut mpsc::Receiver<JailCommand>,
    ) -> anyhow::Result<()> {
        let req = commands_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("command channel closed unexpectedly"))?;

        tracing::debug!("received jail command: {:?}", req);

        let response = match req.kind {
            JailCommandKind::Sh { command } => {
                let actor = self.clone();
                let run = || async move {
                    let mut sh_result = actor.sh(command).await?;
                    let (stdout, stderr) = sh_result.output().all_split().await?;
                    let status = sh_result.exit_code().ok_or_else(|| {
                        anyhow::anyhow!("failed to get exit code from sh command")
                    })?;

                    Ok::<_, anyhow::Error>(ShCommandResponse {
                        status,
                        stdout,
                        stderr,
                    })
                };

                match run().await {
                    Ok(sh_response) => JailCommandResponse {
                        id: req.id,
                        response: Ok(serde_json::to_value(sh_response)?),
                    },
                    // TODO: better errors
                    Err(e) => JailCommandResponse {
                        id: req.id,
                        response: Err(format!("failed to run sh command: {}", e)),
                    },
                }
            }
        };

        tracing::debug!("sending jail command response: {:?}", response);
        self.response_tx.send(response).await?;

        Ok(())
    }
}
