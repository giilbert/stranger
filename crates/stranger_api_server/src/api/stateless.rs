use std::process::Stdio;

use anyhow::Context;
use axum::{Json, extract::State};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use stranger_jail::{Jail, JailConfig, StrangerRuntime};
use tokio::{
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec, LinesCodecError};

use crate::{
    api::worker::{StatelessHostCommand, StatelessWorkerCommand},
    state::AppState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum StatelessRunResponse {
    Success(StatelessRunSuccess),
    Error(StatelessRunError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatelessRunSuccess {
    pub output: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StatelessRunError {
    #[error("unable to create jail")]
    #[serde(rename = "JAIL_CREATION_ERROR")]
    JailCreationError,
    #[error("error parsing instructions")]
    #[serde(rename = "PARSE_ERROR")]
    ParsingError { message: String },
    #[error("error during execution")]
    #[serde(rename = "EXECUTION_ERROR")]
    ExecutionError { message: String },
    #[error("time limit exceeded")]
    #[serde(rename = "TIME_LIMIT_EXCEEDED")]
    TimeLimitExceeded,
    #[error("memory limit exceeded")]
    #[serde(rename = "MEMORY_LIMIT_EXCEEDED")]
    MemoryLimitExceeded,
    #[error("unknown error")]
    #[serde(rename = "UNKNOWN")]
    Unknown,
}

impl StatelessRunError {
    /// Create an [`Option::ok_or_else`] closure that logs the unknown error.
    pub fn ok_or_else(message: &'static str) -> impl FnOnce() -> Self {
        move || {
            tracing::error!("unknown error: `{message}`");
            StatelessRunError::Unknown
        }
    }

    /// Creates an [`Result::map_err`] closure that logs the unknown error.
    pub fn map_err<T: std::error::Error>(message: &'static str) -> impl FnOnce(T) -> Self {
        move |err: T| {
            tracing::error!("unknown error: `{message}`\n{err:?}");
            StatelessRunError::Unknown
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatelessRunRequest {
    /// Docker image to run the stateless code in.
    pub image: String,
    /// Lua code describing what steps to perform while executing.
    pub instructions: String,
}

/// POST /v1/stateless/run
#[axum::debug_handler]
pub async fn run_stateless(
    state: State<AppState>,
    Json(body): Json<StatelessRunRequest>,
) -> Json<StatelessRunResponse> {
    let response = state
        .stateless_workers()
        .enqueue(body)
        .await
        .expect("failed to enqueue stateless job");

    Json(response)
}

#[derive(Debug)]
pub struct StatelessRunCommand {
    pub req: Option<StatelessRunRequest>,
    chan: oneshot::Sender<StatelessRunResponse>,
}

impl StatelessRunCommand {
    /// Send a response back to the requester.
    pub fn reply(self, response: StatelessRunResponse) -> anyhow::Result<()> {
        self.chan.send(response).ok();
        Ok(())
    }
}

#[derive(Debug)]
pub struct StatelessWorkers {
    queue: mpsc::Sender<StatelessRunCommand>,
}

struct StatelessRunWorker {
    req: Option<StatelessRunRequest>,
    jail: Jail,
    child: Child,
    stdin: Mutex<FramedWrite<ChildStdin, LinesCodec>>,
    stdout: FramedRead<ChildStdout, LinesCodec>,
    stderr: FramedRead<ChildStderr, LinesCodec>,
}

impl StatelessRunWorker {
    pub async fn new(runtime: &StrangerRuntime, req: StatelessRunRequest) -> anyhow::Result<Self> {
        // Spawn a Jail for this stateless run worker
        let jail = runtime
            .create(req.image.clone(), JailConfig::default())
            .await
            .context("failed to create jail for stateless job")?;

        let mut child =
            Command::new(std::env::current_exe().expect("failed to get current executable"))
                .arg("worker")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .stdin(Stdio::piped())
                .env_clear()
                .env("RUST_LOG", "stranger_jail=debug,stranger_api_server=debug")
                .spawn()
                .context("failed to spawn stateless worker process")?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open stdin for stateless worker process"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open stdout for stateless worker process"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open stderr for stateless worker process"))?;

        Ok(Self {
            req: Some(req),
            jail,
            child,
            stdin: Mutex::new(FramedWrite::new(stdin, LinesCodec::new())),
            stdout: FramedRead::new(stdout, LinesCodec::new()),
            stderr: FramedRead::new(stderr, LinesCodec::new()),
        })
    }

    async fn send(&self, command: StatelessWorkerCommand) -> anyhow::Result<()> {
        self.stdin
            .lock()
            .await
            .send(serde_json::to_string(&command)?)
            .await
            .context("failed to send command to stateless worker")
    }

    async fn handle_command(
        &self,
        line: Result<String, LinesCodecError>,
    ) -> anyhow::Result<Option<StatelessRunResponse>> {
        match line {
            Err(e) => {
                tracing::error!("failed to read line from stateless worker stdout: {:?}", e);
            }
            Ok(line) => {
                let command: StatelessHostCommand = match serde_json::from_str(&line) {
                    Err(e) => {
                        tracing::error!(
                            "failed to parse stateless worker host command from line `{line}`: {:?}",
                            e
                        );
                        return Ok(None);
                    }
                    Ok(cmd) => cmd,
                };
                match command {
                    StatelessHostCommand::Debug(message) => {
                        tracing::debug!("worker: {}", message);
                    }
                    StatelessHostCommand::RunResponse(response) => {
                        return Ok(Some(response));
                    }
                    StatelessHostCommand::Jail(jail_command) => {
                        // tracing::debug!("forwarding jail command to jail: {:?}", jail_command);
                        let _ = self.jail.commands().send(jail_command).await;
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn run(&mut self) -> anyhow::Result<StatelessRunResponse> {
        tracing::debug!("starting stateless worker run loop");

        let mut responses = self.jail.responses().ok_or_else(|| {
            anyhow::anyhow!("failed to take jail responses receiver for stateless worker")
        })?;

        let req = self.req.take().expect("run cannot be called twice");
        self.send(StatelessWorkerCommand::Run(req))
            .await
            .context("failed to send run command to stateless worker")?;

        loop {
            tokio::select! {
                status = self.child.wait() => {
                    match status {
                        Err(e) => {
                            anyhow::bail!("failed to wait for stateless worker process: {:?}", e)
                        }
                        Ok(status) => {
                            tracing::info!("stateless worker process exited with status: {status}");
                            if !status.success() {
                                anyhow::bail!(
                                    "stateless worker process exited with status: {status}"
                                );
                            }
                        }
                    }
                }
                // Forward jail command responses to the worker
                Some(response) = responses.recv() => {
                    // tracing::debug!("forwarding jail command response to worker: {:?}", response);
                    let _ = self.send(StatelessWorkerCommand::JailResponse(response)).await;
                }
                // Handle commands from the worker's stdout
                Some(line) = self.stdout.next() => {
                    if let Some(response) = self.handle_command(line).await? {
                        return Ok(response);
                    }
                }
                Some(line) = self.stderr.next() => {
                    match line {
                        Err(e) => {
                            tracing::error!("failed to read line from stateless worker stderr: {:?}", e);
                        }
                        Ok(line) => {
                            tracing::error!("worker: {}", line);
                        }
                    }
                }
            };
        }
    }

    pub async fn cleanup(&self) {
        if let Err(e) = self.jail.destroy().await {
            tracing::error!("failed to destroy jail for stateless worker: {:?}", e);
        }
    }
}

impl StatelessWorkers {
    pub fn new(runtime: &StrangerRuntime) -> anyhow::Result<Self> {
        let (tx, mut rx) = mpsc::channel::<StatelessRunCommand>(64 * 1024);

        let runtime_handle = runtime.clone();
        tokio::spawn(async move {
            async fn run_job(
                runtime: &StrangerRuntime,
                req: StatelessRunRequest,
            ) -> Result<StatelessRunSuccess, StatelessRunError> {
                let mut worker = StatelessRunWorker::new(&runtime, req)
                    .await
                    .map_err(|_| StatelessRunError::JailCreationError)?;

                let res = worker.run().await;
                worker.cleanup().await;

                match res {
                    Ok(StatelessRunResponse::Success(success)) => Ok(success),
                    Ok(StatelessRunResponse::Error(error)) => Err(error),
                    Err(e) => Err(StatelessRunError::ExecutionError {
                        message: format!("{}", e),
                    }),
                }
            }

            // Process incoming stateless job commands and run them
            while let Some(mut command) = rx.recv().await {
                let result = run_job(
                    &runtime_handle,
                    command
                        .req
                        .take()
                        .expect("command.req has already been consumed"),
                )
                .await;
                if let Err(e) = command.reply(match result {
                    Ok(success) => StatelessRunResponse::Success(success),
                    Err(error) => StatelessRunResponse::Error(error),
                }) {
                    tracing::warn!("failed to send stateless job response: {:?}", e);
                }
            }
        });

        Ok(Self { queue: tx })
    }

    /// Enqueue a stateless job for processing and wait for the result.
    pub async fn enqueue(&self, job: StatelessRunRequest) -> anyhow::Result<StatelessRunResponse> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send(StatelessRunCommand {
                req: Some(job),
                chan: tx,
            })
            .await
            .context("failed to enqueue stateless job")?;

        Ok(rx
            .await
            .context("failed to receive stateless job response")?)
    }
}
impl AppState {
    pub fn stateless_workers(&self) -> &StatelessWorkers {
        &self.inner.stateless_workers
    }
}
