//! Code for running user-created JavaScript code as a child process.
//!
//! See [`crate::api::stateless`] for details as to how the JavaScript code is used.

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use anyhow::Context;
use deno_core::{
    JsRuntime, ModuleSpecifier, OpDecl, OpState, PollEventLoopOptions, RuntimeOptions,
    error::CoreErrorKind,
    v8::{CreateParams, OomDetails},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use stranger_jail::{JailCommandKind, JailCommandResponse, commands::JailCommand};
use tokio::{sync::oneshot, task::LocalSet};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::api::stateless::{
    StatelessRunError, StatelessRunRequest, StatelessRunResponse, StatelessRunSuccess,
};

// TODO: improve error messages
type JailResponseMap =
    Rc<RefCell<HashMap<u32, oneshot::Sender<Result<serde_json::Value, String>>>>>;

/// Information provided to Deno ops for stateless jobs.
#[derive(Debug)]
struct JsCtx {
    pub current_id: RefCell<u32>,
    pub jail_response: JailResponseMap,
    pub output: RefCell<Vec<serde_json::Value>>,
}

impl JsCtx {
    /// Send a [`JailCommand`] and wait for a response to be delivered.
    pub async fn command<T: DeserializeOwned>(
        &self,
        command: JailCommandKind,
    ) -> anyhow::Result<T> {
        let id = *self.current_id.borrow();
        *self.current_id.borrow_mut() += 1;

        StatelessHostCommand::Jail(JailCommand { id, kind: command }).send();

        let (tx, rx) = oneshot::channel();
        self.jail_response.borrow_mut().insert(id, tx);

        // tracing::debug!("waiting for jail command response with id {}", id);
        let value = rx
            .await
            .map_err(|e| anyhow::anyhow!("failed to receive jail command response: {}", e))?
            .map_err(|e| anyhow::anyhow!("jail command failed: {}", e))?;
        // tracing::debug!("received jail command response for id {}: {:?}", id, value);

        serde_json::from_value(value).context("failed to deserialize jail command response")
    }
}

/// A macro to simplify accessing the JsCtx from an OpState.
macro_rules! js_ctx {
    ($ctx:ident, &$state:ident) => {
        let _state = $state.borrow();
        let $ctx = _state.borrow::<JsCtx>();
    };
}

/// Sent from the host to stateless workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StatelessWorkerCommand {
    Run(StatelessRunRequest),
    JailResponse(JailCommandResponse),
}

/// Sent from stateless workers to the host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StatelessHostCommand {
    /// Send a debug message to the host.
    Debug(String),
    RunResponse(StatelessRunResponse),
    Jail(JailCommand),
}

impl StatelessHostCommand {
    pub fn send(&self) {
        let json = serde_json::to_string(self).expect("failed to serialize StatelessHostCommand");
        println!("{}", json);
    }
}

/// Runs a stateless job in a new Deno runtime.
async fn run_job(
    req: &StatelessRunRequest,
    jail_response: JailResponseMap,
) -> Result<StatelessRunSuccess, StatelessRunError> {
    std::thread_local! {
        pub static KILL_SIGNAL: CancellationToken = CancellationToken::new();
    }

    #[deno_core::op2(async)]
    async fn op_sleep(#[smi] millis: u32) {
        tokio::time::sleep(std::time::Duration::from_millis(millis as u64)).await;
    }

    #[derive(Debug, thiserror::Error, deno_error::JsError)]
    #[class(generic)]
    pub enum StatelessJsError {
        #[error("run failed: {message}")]
        RunFailed { message: String },
    }

    #[tracing::instrument(name = "stateless_run", skip(state))]
    #[deno_core::op2(async)]
    #[serde]
    async fn op_run(
        state: Rc<RefCell<OpState>>,
        #[string] command: String,
    ) -> Result<stranger_jail::commands::ShCommandResponse, StatelessJsError> {
        js_ctx!(ctx, &state);

        Ok(ctx
            .command(JailCommandKind::Sh { command })
            .await
            .map_err(|e| {
                tracing::error!("failed to run command in jail: {}", e);
                StatelessJsError::RunFailed {
                    message: format!("{}", e),
                }
            })?)
    }

    #[deno_core::op2]
    fn op_output(state: Rc<RefCell<OpState>>, #[serde] output: serde_json::Value) {
        js_ctx!(ctx, &state);
        ctx.output.borrow_mut().push(output);
    }

    #[deno_core::op2(fast)]
    fn op_print(#[string] msg: String) {
        tracing::info!("[stateless worker] {}", msg.trim());
    }

    fn middleware(op: OpDecl) -> OpDecl {
        if op.name == "op_print" {
            op.with_implementation_from(&op_print())
        } else {
            op
        }
    }

    deno_core::extension!(
        stranger_ops,
        ops = [op_sleep, op_run, op_output],
        js = ["00_helpers.js"],
        middleware = middleware,
    );

    const INITIAL_HEAP_SIZE: usize = 2 * 1024 * 1024; // 2 MiB
    const MAX_HEAP_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
    let mut runtime = JsRuntime::new(RuntimeOptions {
        create_params: Some(CreateParams::default().heap_limits(INITIAL_HEAP_SIZE, MAX_HEAP_SIZE)),
        extensions: vec![stranger_ops::init()],
        ..Default::default()
    });
    unsafe extern "C" fn oom_handler(_location: *const i8, _details: &OomDetails) {
        StatelessHostCommand::RunResponse(StatelessRunResponse::Error(
            StatelessRunError::MemoryLimitExceeded,
        ))
        .send();
        std::process::exit(1);
    }
    runtime.v8_isolate().set_oom_error_handler(oom_handler);

    let module = runtime
        .load_main_es_module_from_code(
            &ModuleSpecifier::parse("file://anonymous").expect("failed to parse url"),
            req.instructions.clone(),
        )
        .await
        .map_err(|e| match e.into_kind() {
            CoreErrorKind::Js(msg) => StatelessRunError::ParsingError {
                message: format!("{}", msg),
            },
            e => {
                tracing::error!("unknown error during module loading: {:?}", e);
                StatelessRunError::Unknown
            }
        })?;
    runtime.op_state().borrow_mut().put(JsCtx {
        current_id: RefCell::new(0),
        jail_response,
        output: RefCell::new(Vec::new()),
    });

    tracing::info!("starting module evaluation...");
    let kill_token = KILL_SIGNAL.with(|s| s.clone());
    let module_evaluate = runtime.mod_evaluate(module);
    tokio::select! {
        biased;
        _ = kill_token.cancelled() => {
            tracing::warn!("stateless job killed due to OOM");
            return Err(StatelessRunError::TimeLimitExceeded);
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            tracing::warn!("stateless job time limit exceeded");
            return Err(StatelessRunError::TimeLimitExceeded);
        }
        _ = runtime.run_event_loop(PollEventLoopOptions::default()) => {},
        res = module_evaluate => {
            res.map_err(|e| match e.into_kind() {
                CoreErrorKind::Js(msg) => StatelessRunError::ParsingError {
                    message: format!("{}", msg),
                },
                e => {
                    tracing::error!("unknown error during module evaluation: {:?}", e);
                    StatelessRunError::Unknown
                }
            })?;
        }
    }
    tracing::info!("module evaluation done!");

    let result = runtime
        .run_event_loop(PollEventLoopOptions::default())
        .await
        .map_err(|e| match e.into_kind() {
            CoreErrorKind::Js(msg) => StatelessRunError::ExecutionError {
                message: format!("{}", msg),
            },
            e => {
                tracing::error!("unknown error during event loop: {:?}", e);
                StatelessRunError::Unknown
            }
        })?;

    tracing::info!("job completed with result: {:?}", result);

    Ok(StatelessRunSuccess {
        output: runtime
            .op_state()
            .borrow_mut()
            .take::<JsCtx>()
            .output
            .into_inner(),
    })
}

fn make_custom_writer() -> impl std::io::Write + Send + 'static {
    // If a buffer exceeds this size, flush it.
    const BUFFER_MAX_SIZE: usize = 16 * 1024;

    struct CustomWriter {
        buffer: Vec<u8>,
    }

    impl std::io::Write for CustomWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer.extend_from_slice(buf);

            // Print complete lines from the buffer
            while let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
                let line = self.buffer.drain(..=pos).collect::<Vec<u8>>();
                if let Ok(line_str) = std::str::from_utf8(&line) {
                    StatelessHostCommand::Debug(line_str.trim_end().to_string()).send()
                }
            }

            // If the buffer is too large, ignore flushing complete lines and flush it entirely
            if self.buffer.len() >= BUFFER_MAX_SIZE {
                // Print the remaining buffer if any
                if !self.buffer.is_empty() {
                    let line = self.buffer.drain(..).collect::<Vec<u8>>();
                    if let Ok(line_str) = std::str::from_utf8(&line) {
                        StatelessHostCommand::Debug(line_str.trim_end().to_string()).send()
                    }
                }
            }

            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    CustomWriter {
        buffer: Vec::with_capacity(BUFFER_MAX_SIZE),
    }
}

async fn handle_command(
    jail_response: JailResponseMap,
    command: StatelessWorkerCommand,
) -> anyhow::Result<()> {
    match command {
        StatelessWorkerCommand::Run(req) => {
            StatelessHostCommand::RunResponse(match run_job(&req, jail_response).await {
                Ok(success) => StatelessRunResponse::Success(success),
                Err(err) => StatelessRunResponse::Error(err),
            })
            .send()
        }
        StatelessWorkerCommand::JailResponse(res) => {
            jail_response
                .borrow_mut()
                .remove(&res.id)
                .ok_or_else(|| {
                    anyhow::anyhow!("no pending jail command found for response id {}", res.id)
                })?
                .send(res.response)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "failed to send jail command response for id {}: {:?}",
                        res.id,
                        e
                    )
                })?;
        }
        #[allow(unused)]
        rest => {
            tracing::warn!("unhandled stateless worker command: {:?}", rest);
        }
    }

    Ok(())
}

pub async fn run_worker() -> anyhow::Result<()> {
    // Create a tracing subscriber that sends logs in the format of [`StatelessHostCommand::Debug`].
    const DEFAULT_LOG_SETTINGS: &str = "stranger_jail=info,stranger_api_server=info";
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(make_custom_writer)
                .with_ansi(false)
                .without_time(),
        )
        .with(
            EnvFilter::builder()
                .parse(std::env::var("RUST_LOG").unwrap_or(DEFAULT_LOG_SETTINGS.to_string()))?,
        )
        .init();

    tracing::info!("starting stateless worker...");

    let mut stdin = FramedRead::new(tokio::io::stdin(), LinesCodec::new());

    let jail_response = Rc::new(RefCell::new(HashMap::new()));
    let local_set = LocalSet::new();

    local_set
        .run_until(async move {
            loop {
                let jail_response = jail_response.clone();
                tokio::select! {
                    Some(line) = stdin.next() => {
                        let line = line.context("failed to read line from stdin")?;
                        let command: StatelessWorkerCommand = serde_json::from_str(&line)
                            .context("failed to parse command from stdin")?;

                        tracing::debug!("received command: {:?}", command);

                        tokio::task::spawn_local(async move {
                            if let Err(e) = handle_command(jail_response.clone(), command).await {
                                tracing::error!("failed to handle command: {}", e);
                            }
                        });

                        tracing::debug!("command handled successfully");
                    }
                }
            }
        })
        .await
}
