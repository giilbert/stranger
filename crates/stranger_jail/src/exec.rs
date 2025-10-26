use std::{ops::ControlFlow, sync::Arc};

use anyhow::Context;
use bollard::{exec::StartExecResults, secret::ExecConfig};
use futures_util::StreamExt;
use tokio::{io::AsyncWriteExt, sync::mpsc};

use crate::{Jail, jail::JailActor};

/// Output from a command executed inside a [`Jail`].
pub enum JailExecOutput {
    Stdout(String),
    Stderr(String),
}

/// An execution instance inside a [`Jail`], representing a running command.
pub struct JailExec {
    input_tx: mpsc::Sender<Vec<u8>>,
    output: Option<JailExecOutputChannel>,
}

/// A channel for receiving output from a command executed inside a [`Jail`]. [`JailExec::output`]
/// should be used to take ownership of this channel.
pub struct JailExecOutputChannel {
    channel: mpsc::Receiver<Result<JailExecOutput, JailExecError>>,
    line_buffer: String,
}

#[derive(Debug, thiserror::Error)]
pub enum JailExecError {
    #[error("docker error: {0}")]
    Docker(bollard::errors::Error),
}

impl JailActor {
    /// Implementation for [`Jail::sh`].
    pub async fn sh(self: Arc<Self>, command: impl Into<String>) -> anyhow::Result<JailExec> {
        let exec_instance = self
            .runtime
            .docker()
            .create_exec(
                &self.name(),
                ExecConfig {
                    cmd: Some(vec!["sh".to_string(), "-c".to_string(), command.into()]),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    attach_stdin: Some(true),
                    ..Default::default()
                },
            )
            .await
            .context("failed to create exec instance")?;

        let start_exec = self
            .runtime
            .docker()
            .start_exec(&exec_instance.id, None)
            .await
            .context("failed to start exec instance")?;

        let (input_tx, mut input_rx) = mpsc::channel::<Vec<u8>>(64);
        let (output_tx, output_rx) = mpsc::channel(64);
        match start_exec {
            StartExecResults::Attached {
                mut output,
                mut input,
            } => {
                let actor = self.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            biased;
                            _ = actor.cancellation_token.cancelled() => break,
                            msg = output.next() => {
                                match actor.handle_exec_output_msg(msg, &output_tx).await {
                                    ControlFlow::Break(_) => break,
                                    ControlFlow::Continue(_) => { /* continue */ }
                                }
                            }
                            Some(input_data) = input_rx.recv() => {
                                input
                                    .write_all(&input_data)
                                    .await
                                    .unwrap_or_else(|e| {
                                        tracing::error!("failed to write to exec input: {}", e);
                                    });
                            }
                        }
                    }
                });
            }
            StartExecResults::Detached => anyhow::bail!("detached exec is not supported"),
        }

        Ok(JailExec {
            input_tx,
            output: Some(JailExecOutputChannel {
                channel: output_rx,
                line_buffer: String::new(),
            }),
        })
    }

    async fn handle_exec_output_msg(
        &self,
        msg: Option<Result<bollard::container::LogOutput, bollard::errors::Error>>,
        output_tx: &mpsc::Sender<Result<JailExecOutput, JailExecError>>,
    ) -> ControlFlow<(), ()> {
        match msg {
            Some(Ok(log_output)) => {
                let output = match log_output {
                    bollard::container::LogOutput::StdOut { message } => Ok(
                        JailExecOutput::Stdout(String::from_utf8_lossy(&message).to_string()),
                    ),
                    bollard::container::LogOutput::StdErr { message } => Ok(
                        JailExecOutput::Stderr(String::from_utf8_lossy(&message).to_string()),
                    ),
                    bollard::container::LogOutput::Console { message } => Ok(
                        JailExecOutput::Stdout(String::from_utf8_lossy(&message).to_string()),
                    ),
                    rest => {
                        tracing::warn!("unknown log output type, skipping: {:?}", rest);
                        return ControlFlow::Continue(());
                    }
                };

                if output_tx.send(output).await.is_err() {
                    // Receiver dropped, stop processing
                    return ControlFlow::Break(());
                }
            }
            Some(Err(e)) => {
                // An error occurred while reading the stream, send the error, and stop processing
                let _ = output_tx.send(Err(JailExecError::Docker(e))).await;
                return ControlFlow::Break(());
            }
            None => {
                // Stream ended
                return ControlFlow::Break(());
            }
        }

        ControlFlow::Continue(())
    }
}

impl Jail {
    /// A simple version of [`Jail::exec`] that executes a shell command using `sh -c <command>`.
    pub async fn sh(&self, command: impl Into<String>) -> anyhow::Result<JailExec> {
        self.handle.clone().sh(command).await
    }
}

impl JailExec {
    /// Takes ownership of the output channel for this [`JailExec`].
    pub fn output(&mut self) -> JailExecOutputChannel {
        self.output.take().expect("output channel already consumed")
    }

    /// Sends input to the executed command's standard input.
    pub fn input(&self, str: impl AsRef<str>) -> Result<(), mpsc::error::TrySendError<Vec<u8>>> {
        let str = str.as_ref();
        self.input_tx.try_send(str.as_bytes().to_vec())?;
        Ok(())
    }
}

impl JailExecOutputChannel {
    /// Receives the next output from the command execution *without buffering*.
    ///
    /// This method returns `Ok(None)` when the command has finished executing and there are no more
    /// outputs to receive.
    pub async fn recv(&mut self) -> Result<Option<JailExecOutput>, JailExecError> {
        match self.channel.recv().await {
            Some(Ok(output)) => Ok(Some(output)),
            Some(Err(e)) => Err(e),
            None => Ok(None), // Channel closed, no more outputs
        }
    }

    /// Receives the next line of output from the command execution, buffering as necessary.
    ///
    /// This method returns `Ok(None)` when the command has finished executing and there are no more
    /// lines to receive. The returned lines include the newline character at the end and the last
    /// line may not have a newline if the output did not end with one.
    pub async fn next_line(&mut self) -> Result<Option<String>, JailExecError> {
        loop {
            // First, check if self.line_buffer contains a newline
            if let Some(pos) = self.line_buffer.find('\n') {
                let line = self.line_buffer.drain(..=pos).collect::<String>();
                return Ok(Some(line));
            }

            // If not, receive the next output and append it to the buffer
            match self.recv().await? {
                Some(JailExecOutput::Stdout(s)) | Some(JailExecOutput::Stderr(s)) => {
                    self.line_buffer.push_str(&s);
                }
                None => {
                    // No more outputs, return any remaining buffer as the last line (if not empty)
                    if self.line_buffer.is_empty() {
                        return Ok(None);
                    } else {
                        let line = self.line_buffer.drain(..).collect::<String>();
                        return Ok(Some(line));
                    }
                }
            }
        }
    }

    /// Receives all remaining output from the command execution, buffering as necessary.
    pub async fn all(mut self) -> Result<String, JailExecError> {
        let mut all_output = String::new();
        while let Some(line) = self.next_line().await? {
            all_output.push_str(&line);
        }
        Ok(all_output)
    }
}
