use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use axum::{Json, extract::State};
use deno_core::{
    JsRuntime, ModuleSpecifier, OpDecl, OpState, PollEventLoopOptions, RuntimeOptions,
    error::CoreErrorKind,
    v8::{CreateParams, OomDetails},
};
use serde::{Deserialize, Serialize};
use stranger_jail::{Jail, JailConfig, StrangerRuntime};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::state::AppState;

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum StatelessRunResponse {
    Success(StatelessRunSuccess),
    Error(StatelessRunError),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatelessRunSuccess {
    pub output: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, thiserror::Error)]
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

#[derive(Debug, Clone, Deserialize)]
pub struct StatelessRunRequest {
    /// Docker image to run the stateless code in.
    image: String,
    /// Lua code describing what steps to perform while executing.
    instructions: String,
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
    pub req: StatelessRunRequest,
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

/// Information provided to Deno ops for stateless jobs.
#[derive(Debug)]
struct JsCtx {
    pub jail: Jail,
    pub output: Vec<serde_json::Value>,
}

/// A macro to simplify accessing the JsCtx from an OpState.
macro_rules! js_ctx {
    ($ctx:ident, &$state:ident) => {
        let _state = $state.borrow();
        let $ctx = _state.borrow::<JsCtx>();
    };
    ($ctx:ident, &mut $state:ident) => {
        let mut _state = $state.borrow_mut();
        let $ctx = _state.borrow_mut::<JsCtx>();
    };
}

impl StatelessWorkers {
    pub fn new(runtime: &StrangerRuntime) -> Self {
        let (tx, rx) = mpsc::channel::<StatelessRunCommand>(64 * 1024);

        /// Runs a stateless job in a new Deno runtime.
        async fn run_job(
            runtime: &StrangerRuntime,
            req: &StatelessRunRequest,
        ) -> Result<StatelessRunSuccess, StatelessRunError> {
            std::thread_local! {
                pub static KILL_SIGNAL: CancellationToken = CancellationToken::new();
            }

            let container = runtime
                .create(&req.image, JailConfig::default())
                .await
                .map_err(|e| {
                    tracing::warn!("failed to create jail for stateless job: {e:?}");
                    StatelessRunError::JailCreationError
                })?;

            #[deno_core::op2(async)]
            async fn op_sleep(state: Rc<RefCell<OpState>>, #[smi] millis: u32) {
                state.borrow_mut().borrow_mut::<JsCtx>();
                tokio::time::sleep(std::time::Duration::from_millis(millis as u64)).await;
            }

            #[derive(Debug, thiserror::Error, deno_error::JsError)]
            #[class(generic)]
            pub enum StatelessJsError {
                #[error("run failed: {message}")]
                RunFailed { message: String },
            }

            #[derive(Debug, Serialize)]
            pub struct RunResults {
                pub exit_code: u8,
                pub stdout: String,
                pub stderr: String,
            }

            #[tracing::instrument(name = "stateless_run", skip(state))]
            #[deno_core::op2(async)]
            #[serde]
            async fn op_run(
                state: Rc<RefCell<OpState>>,
                #[string] code: String,
            ) -> Result<RunResults, StatelessJsError> {
                js_ctx!(ctx, &state);
                let mut result =
                    ctx.jail
                        .sh(&code)
                        .await
                        .map_err(|e| StatelessJsError::RunFailed {
                            message: format!("{}", e),
                        })?;

                let (stdout, stderr) =
                    result
                        .output()
                        .all_split()
                        .await
                        .map_err(|e| StatelessJsError::RunFailed {
                            message: format!("failed to get output: {}", e),
                        })?;
                let exit_code = result
                    .exit_code()
                    .ok_or_else(|| StatelessJsError::RunFailed {
                        message: "process did not exit normally".to_string(),
                    })?;

                Ok(RunResults {
                    exit_code,
                    stdout,
                    stderr,
                })
            }

            #[deno_core::op2]
            fn op_output(state: Rc<RefCell<OpState>>, #[serde] output: serde_json::Value) {
                js_ctx!(ctx, &mut state);
                ctx.output.push(output);
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
                create_params: Some(
                    CreateParams::default().heap_limits(INITIAL_HEAP_SIZE, MAX_HEAP_SIZE),
                ),
                extensions: vec![stranger_ops::init()],
                ..Default::default()
            });
            unsafe extern "C" fn oom_handler(location: *const i8, details: &OomDetails) {
                tracing::error!("JavaScript OOM. STOP RUNNING CODE!");
                KILL_SIGNAL.with(|token| {
                    token.cancel();
                });
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
                jail: container.clone(),
                output: Vec::new(),
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

            if let Err(e) = container.destroy().await {
                tracing::warn!("failed to destroy jail for stateless job: {e:?}");
            }

            Ok(StatelessRunSuccess {
                output: runtime.op_state().borrow_mut().take::<JsCtx>().output,
            })
        }

        #[tracing::instrument(name = "stateless_worker", skip(rx, runtime))]
        async fn run_stateless_worker(
            id: u8,
            runtime: StrangerRuntime,
            mut rx: mpsc::Receiver<StatelessRunCommand>,
        ) -> Result<(), anyhow::Error> {
            tracing::info!("stateless worker started. waiting for jobs...");

            while let Some(job) = rx.recv().await {
                tracing::info!("processing job {:?}", job.req);
                let result = run_job(&runtime, &job.req).await;
                if let Err(e) = job.reply(match result {
                    Ok(success) => StatelessRunResponse::Success(success),
                    Err(error) => StatelessRunResponse::Error(error),
                }) {
                    tracing::error!("failed to send job response: {:?}", e);
                }
            }

            Ok(())
        }

        // TODO: respawn workers on failure and run multiple workers
        let runtime = runtime.clone();
        std::thread::spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build stateless worker runtime")
                .block_on(run_stateless_worker(0, runtime, rx))
            {
                Ok(_) => (),
                Err(e) => tracing::error!("stateless worker exited with error: {:?}", e),
            }
        });

        Self { queue: tx }
    }

    /// Enqueue a stateless job for processing and wait for the result.
    pub async fn enqueue(&self, job: StatelessRunRequest) -> anyhow::Result<StatelessRunResponse> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send(StatelessRunCommand { req: job, chan: tx })
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
