use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize},
};

use axum::{Json, extract::State};
use gc_arena::Collect;
use piccolo::{
    BoxSequence, Callback, CallbackReturn, Closure, Context, Execution, Executor, FromMultiValue,
    Fuel, IntoMultiValue, IntoValue, Lua, PrototypeError, RuntimeError, Sequence, SequencePoll,
    Stack, StaticError, Value, Variadic,
};
use serde::{Deserialize, Serialize};
use stranger_jail::JailConfig;
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::LocalSet,
};

use crate::{api::lua, state::AppState};

#[derive(Debug, thiserror::Error)]
enum AsyncCallError {
    #[error("attempted to send a command to the actor, but the command channel was full")]
    ChannelFull,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum StatelessRunResponse {
    Success { output: serde_json::Value },
    Error(StatelessRunError),
}

#[derive(Debug, Serialize, thiserror::Error)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StatelessRunError {
    #[error("error parsing instructions")]
    #[serde(rename = "ParsingError")]
    ParsingError { message: String },
    #[error("error during execution")]
    #[serde(rename = "ExecutionError")]
    ExecutionError { message: String },
    #[error("time limit exceeded")]
    #[serde(rename = "TimeLimitExceeded")]
    TimeLimitExceeded,
}

/// Used to hook an async function into Lua via [`piccolo::Sequence`].
trait AsyncCall: Sized + 'static {
    type Params: Send + for<'gc> FromMultiValue<'gc>;
    type Ret: Send + for<'gc> IntoMultiValue<'gc>;

    /// Submits an async function call.
    ///
    /// The implementor should send the result back via the provided [`oneshot::Sender`].
    fn submit(ctx: StatelessCallCtx<Self>, params: Self::Params) -> Result<(), AsyncCallError>;
}

/// Context passed into [`AsyncCall::submit`].
///
/// The caller should interface with the container's actor using the provided MPSC channel.
#[derive(Debug)]
struct StatelessCallCtx<T: AsyncCall> {
    tx: mpsc::Sender<ActorCommand>,
    notify: Arc<Notify>,
    waiters: Arc<AtomicUsize>,
    ret: oneshot::Sender<T::Ret>,
}

impl<T: AsyncCall> StatelessCallCtx<T> {
    /// Returns the result of the async call. This notifies the driver of the Lua thread to wake the
    /// thread up and continue execution.
    fn finish(self, result: T::Ret) {
        let _ = self.ret.send(result);
        self.waiters
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.notify.notify_one();
    }

    /// Sends a command to the actor with the provided function to consume the context and create
    /// the command.
    fn send_with(self, f: impl FnOnce(Self) -> ActorCommand) -> Result<(), AsyncCallError> {
        let tx = self.tx.clone();
        let waiters = self.waiters.clone();
        tx.try_send(f(self))
            .map_err(|_| AsyncCallError::ChannelFull)?;
        waiters.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }
}

/// A Lua function that calls an async function in Rust.
struct Cmd<T: AsyncCall> {
    _phantom: std::marker::PhantomData<T>,
}

/// An instance of an async command call in Lua.
struct CmdCallInstance<T: AsyncCall> {
    ret: oneshot::Receiver<T::Ret>,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: AsyncCall> Collect for CmdCallInstance<T> {
    fn trace(&self, _cc: &gc_arena::Collection) {
        // No-op---this object does not contain any GC-managed references.
    }
}

impl<'gc, T: AsyncCall> Sequence<'gc> for CmdCallInstance<T> {
    fn poll(
        &mut self,
        ctx: Context<'gc>,
        _exec: Execution<'gc, '_>,
        mut stack: Stack<'gc, '_>,
    ) -> Result<SequencePoll<'gc>, piccolo::Error<'gc>> {
        match self.ret.try_recv() {
            Ok(ret) => {
                stack.replace(ctx, ret);
                Ok(SequencePoll::Return)
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(piccolo::SequencePoll::Pending),
            Err(oneshot::error::TryRecvError::Closed) => Err(piccolo::Error::Runtime(
                RuntimeError(anyhow::anyhow!("async call was cancelled").into()),
            )),
        }
    }
}

impl<T: AsyncCall> Cmd<T> {
    fn new() -> Self {
        Cmd {
            _phantom: std::marker::PhantomData,
        }
    }

    fn create_call_instance<'gc>(
        &self,
        tx: mpsc::Sender<ActorCommand>,
        notify: Arc<Notify>,
        ctx: Context<'gc>,
        stack: &mut Stack<'gc, '_>,
    ) -> Result<CmdCallInstance<T>, piccolo::Error<'gc>> {
        let (ret_tx, ret) = oneshot::channel();
        let params = stack.consume::<T::Params>(ctx)?;

        T::submit(
            StatelessCallCtx {
                tx,
                notify,
                ret: ret_tx,
                waiters: Arc::new(AtomicUsize::new(0)),
            },
            params,
        )
        .map_err(|e| format!("{e:?}").into_value(ctx))?;

        Ok(CmdCallInstance {
            ret,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Creates a Lua callback that invokes this command.
    fn into_callback<'gc>(self, thread: &LuaThreadCtx, ctx: &Context<'gc>) -> Callback<'gc> {
        let thread = thread.clone();

        Callback::from_fn(ctx, move |ctx, _, mut stack| {
            let call_instance = self.create_call_instance(
                thread.commands.clone(),
                thread.next.clone(),
                ctx,
                &mut stack,
            )?;

            Ok(CallbackReturn::Sequence(BoxSequence::new(
                &ctx,
                call_instance,
            )))
        })
    }
}

/// An asynchronous command sent to an actor driving a container. The actor will interact with the
/// Lua runtime via the provided context (see [`StatelessCallCtx`]).
#[derive(Debug)]
enum ActorCommand {
    Sh {
        command: String,
        ctx: StatelessCallCtx<ShCommand>,
    },
}

#[derive(Debug)]
struct ShCommand;

impl AsyncCall for ShCommand {
    type Params = (String,);
    type Ret = (u8, String, String); // (exit_code, stdout, stderr)

    fn submit(ctx: StatelessCallCtx<Self>, params: Self::Params) -> Result<(), AsyncCallError> {
        ctx.send_with(|ctx| ActorCommand::Sh {
            command: params.0,
            ctx,
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum LuaExecutionError {
    #[error("error parsing Lua code: {0}")]
    ParsingError(String),
    #[error("error compiling Lua code: {0}")]
    CompilerError(String),
    #[error("error serializing: {0}")]
    SerializationError(lua::serde::LuaSerdeError),
    #[error("runtime error: {0}")]
    PiccoloError(StaticError),
    #[error("fuel limit exceeded")]
    FuelLimitExceeded,
    #[error("out of memory")]
    OutOfMemory,
}

#[derive(Clone)]
struct LuaThreadCtx {
    /// Used to send commands to the container's actor.
    commands: mpsc::Sender<ActorCommand>,
    /// Notifies the driver of the thread that all execution has finished with a value.
    returned: Arc<(
        AtomicBool,
        mpsc::Sender<Result<serde_json::Value, LuaExecutionError>>,
    )>,
    /// Tracks the amount of async tasks that are waiting.
    waiters: Arc<AtomicUsize>,
    /// Notifies the Lua thread to wake up and continue execution.
    next: Arc<Notify>,
}

impl LuaThreadCtx {
    pub fn send_return(&self, value: Result<serde_json::Value, LuaExecutionError>) {
        if self
            .returned
            .0
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            tracing::warn!("Lua thread tried to return a value twice, ignoring second return");
        } else {
            self.returned
                .1
                .try_send(value)
                .expect("failed to send return value");
        }
    }
}

fn start_lua_thread(thread: LuaThreadCtx, instructions: String) {
    let mut lua = Lua::core();

    let ex = match lua.enter(|ctx| {
        let env = ctx.globals();
        env.set(
            ctx,
            "sh",
            Cmd::<ShCommand>::new().into_callback(&thread, &ctx),
        )
        .expect("failed to set callback");

        let closure = match Closure::load_with_env(ctx, None, instructions.as_bytes(), env) {
            Ok(closure) => Ok(closure),
            Err(e) => {
                thread.send_return(Err(match e {
                    PrototypeError::Parser(e) => LuaExecutionError::ParsingError(e.to_string()),
                    PrototypeError::Compiler(e) => LuaExecutionError::CompilerError(e.to_string()),
                }));

                return Err(());
            }
        }?;
        let ex = Executor::start(ctx, closure.into(), ());

        Ok(ctx.stash(ex))
    }) {
        Ok(ex) => ex,
        Err(_) => return,
    };

    // TODO: Limit the amount of fuel the Lua thread can consume.
    let local_set = LocalSet::new();
    local_set.spawn_local(async move {
        const FUEL_STEP: u64 = 4_096;
        const MAX_FUEL: u64 = FUEL_STEP * 4096;
        const MAX_ALLOCATIONS: usize = 1 * 1024 * 1024; // 1MB

        let mut fuel_used = 0;
        // Drive the Lua executor until completion, waking up on notifications.
        loop {
            let mut fuel = Fuel::with(FUEL_STEP as i32);

            let is_finished = lua.enter(|ctx| ctx.fetch(&ex).step(ctx, &mut fuel));
            if is_finished {
                break;
            } else {
                if thread.waiters.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                    thread.next.notified().await;
                }
            }

            fuel_used += FUEL_STEP;
            // tracing::info!("fuel used so far: {fuel_used}");
            if fuel_used >= MAX_FUEL {
                return thread.send_return(Err(LuaExecutionError::FuelLimitExceeded));
            }
            if lua.enter(|ctx| ctx.metrics().total_allocation() > MAX_ALLOCATIONS) {
                return thread.send_return(Err(LuaExecutionError::OutOfMemory));
            }
        }

        // Once finished, fetch the return value(s), serialize them to JSON, and send them back.
        let return_value = match lua.enter(|ctx| {
            match ctx
                .fetch(&ex)
                .take_result::<Variadic<Vec<Value>>>(ctx)
                .expect("executor should be in finished mode")
            {
                Ok(vals) => Ok(vals
                    .into_iter()
                    .map(|v| {
                        tracing::info!("serializing return value {v:?}");
                        lua::serde::to_json_value(v)
                    })
                    .collect::<Result<serde_json::Value, _>>()),
                Err(e) => Err(Err(LuaExecutionError::PiccoloError(e.into_static()))),
            }
        }) {
            Ok(Ok(json)) => Ok(json),
            Ok(Err(e)) => Err(LuaExecutionError::SerializationError(e)),
            Err(Err(e)) => Err(e),
            Err(Ok(err_val)) => Err(LuaExecutionError::PiccoloError(err_val)),
        };

        let (has_returned, channel) = &*thread.returned;
        if has_returned.swap(true, std::sync::atomic::Ordering::AcqRel) {
            tracing::warn!("Lua thread tried to return a value twice, ignoring second return");
        } else {
            channel
                .send(return_value)
                .await
                .expect("failed to send return value");
        }
    });

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build runtime")
        .block_on(local_set)
}

#[derive(Debug, Deserialize)]
pub struct RunStatelessBody {
    /// Docker image to run the stateless code in.
    image: String,
    /// Lua code describing what steps to perform while executing.
    instructions: String,
}

/// POST /v1/stateless/run
#[axum::debug_handler]
pub async fn run_stateless(
    state: State<AppState>,
    Json(body): Json<RunStatelessBody>,
) -> Json<StatelessRunResponse> {
    let (commands_tx, mut commands_rx) = mpsc::channel::<ActorCommand>(16);

    let container = state
        .runtime()
        .create(
            body.image,
            JailConfig {
                ..Default::default()
            },
        )
        .await
        .expect("failed to create jail for stateless execution");

    let (returned_tx, mut returned_rx) = mpsc::channel(1);
    let thread_ctx = LuaThreadCtx {
        commands: commands_tx.clone(),
        returned: Arc::new((AtomicBool::new(false), returned_tx)),
        next: Arc::new(Notify::new()),
        waiters: Arc::new(AtomicUsize::new(0)),
    };
    std::thread::spawn(move || start_lua_thread(thread_ctx, body.instructions));

    let mut response_value = None;
    // In the actor, wait for commands and execute them until the Lua execution is finished.
    loop {
        tokio::select! {
            command = commands_rx.recv() => {
                match command {
                    Some(ActorCommand::Sh { command, ctx }) => {
                        tracing::info!("received Sh command, executing...");
                        let mut exec = container
                            .sh(command)
                            .await
                            .expect("failed to create sh exec");
                        let (stdout, stderr) = exec
                            .output()
                            .all_split()
                            .await
                            .expect("failed to get output from sh exec");
                        let exit_code = exec.exit_code().expect("failed to get exit code");

                        tracing::info!("sh command completed with exit code {}", exit_code);

                        ctx.finish((exit_code, stdout, stderr));
                    }
                    None => {
                        tracing::info!("command channel closed, exiting actor loop");
                        break;
                    }
                }
            }
            Some(return_value) = returned_rx.recv() => {
                tracing::info!("Lua thread has finished execution with return value {return_value:?}");
                response_value = Some(match return_value {
                    Ok(json) => StatelessRunResponse::Success { output: json },
                    Err(LuaExecutionError::ParsingError(e)) => {
                        StatelessRunResponse::Error(StatelessRunError::ParsingError {
                            message: format!("{e}"),
                        })
                    },
                    Err(LuaExecutionError::CompilerError(e)) => {
                        StatelessRunResponse::Error(StatelessRunError::ExecutionError {
                            message: format!("{e}"),
                        })
                    },
                    Err(LuaExecutionError::FuelLimitExceeded) => {
                        StatelessRunResponse::Error(StatelessRunError::ExecutionError {
                            message: "fuel limit exceeded".to_string(),
                        })
                    },
                    Err(LuaExecutionError::OutOfMemory) => {
                        StatelessRunResponse::Error(StatelessRunError::ExecutionError {
                            message: "out of memory".to_string(),
                        })
                    },
                    Err(LuaExecutionError::SerializationError(e)) => {
                        StatelessRunResponse::Error(StatelessRunError::ExecutionError {
                            message: format!("serialization error: {e}"),
                        })
                    },
                    Err(LuaExecutionError::PiccoloError(e)) => {
                        StatelessRunResponse::Error(StatelessRunError::ExecutionError {
                            message: format!("runtime error: {e}"),
                        })
                    },
                });
                break
            }
        }
    }
    container.destroy().await.expect("failed to destroy jail");

    match response_value {
        Some(val) => Json(val),
        None => Json(StatelessRunResponse::Error(
            StatelessRunError::TimeLimitExceeded,
        )),
    }
}
