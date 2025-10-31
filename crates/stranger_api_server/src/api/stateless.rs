use std::sync::{Arc, atomic::AtomicBool};

use axum::{Json, extract::State};
use gc_arena::Collect;
use piccolo::{
    BoxSequence, Callback, CallbackReturn, Closure, Context, Execution, Executor, FromMultiValue,
    Fuel, IntoMultiValue, IntoValue, Lua, RuntimeError, Sequence, SequencePoll, Stack, Value,
};
use serde::Deserialize;
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
    ret: oneshot::Sender<T::Ret>,
}

impl<T: AsyncCall> StatelessCallCtx<T> {
    /// Returns the result of the async call. This notifies the driver of the Lua thread to wake the
    /// thread up and continue execution.
    fn finish(self, result: T::Ret) {
        let _ = self.ret.send(result);
        self.notify.notify_one();
    }

    /// Sends a command to the actor with the provided function to consume the context and create
    /// the command.
    fn send_with(self, f: impl FnOnce(Self) -> ActorCommand) -> Result<(), AsyncCallError> {
        let tx = self.tx.clone();
        tx.try_send(f(self))
            .map_err(|_| AsyncCallError::ChannelFull)
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
    #[error("error serializing: {0}")]
    SerializationError(lua::serde::LuaSerdeError),
    #[error("runtime error: {0}")]
    RuntimeError(RuntimeError),
    #[error("lua error: {0}")]
    LuaError(serde_json::Value),
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
    /// Notifies the Lua thread to wake up and continue execution.
    next: Arc<Notify>,
}

fn start_lua_thread(thread: LuaThreadCtx, instructions: String) {
    let mut lua = Lua::core();

    let ex = lua.enter(|ctx| {
        let env = ctx.globals();
        env.set(
            ctx,
            "sh",
            Cmd::<ShCommand>::new().into_callback(&thread, &ctx),
        )
        .expect("failed to set callback");

        let closure = Closure::load_with_env(ctx, None, instructions.as_bytes(), env)
            .expect("failed to load closure");
        let ex = Executor::start(ctx, closure.into(), ());

        ctx.stash(ex)
    });

    // TODO: Limit the amount of fuel the Lua thread can consume.
    let local_set = LocalSet::new();
    local_set.spawn_local(async move {
        // Drive the Lua executor until completion, waking up on notifications.
        loop {
            let mut fuel = Fuel::with(10_000);

            let is_finished = lua.enter(|ctx| ctx.fetch(&ex).step(ctx, &mut fuel));
            if is_finished {
                break;
            } else {
                tracing::info!("waiting for any async operation to complete...");
                thread.next.notified().await;
            }
        }

        // Once finished, fetch the return value(s), serialize them to JSON, and send them back.
        let return_value = match lua.enter(|ctx| {
            match ctx
                .fetch(&ex)
                .take_result::<Vec<Value>>(ctx)
                .expect("executor should be in finished mode")
            {
                Ok(vals) => Ok(vals
                    .into_iter()
                    .map(|v| lua::serde::to_json_value(v))
                    .collect::<Result<serde_json::Value, _>>()),
                Err(e) => Err(lua::serde::to_json_value(e.to_value(ctx))),
            }
        }) {
            Ok(Ok(json)) => Ok(json),
            Ok(Err(e)) | Err(Err(e)) => Err(LuaExecutionError::SerializationError(e)),
            Err(Ok(err_val)) => Err(LuaExecutionError::LuaError(err_val)),
        };

        let (has_returned, channel) = &*thread.returned;
        if has_returned.swap(true, std::sync::atomic::Ordering::SeqCst) {
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
pub async fn run_stateless(state: State<AppState>, Json(body): Json<RunStatelessBody>) -> String {
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
    };
    std::thread::spawn(move || start_lua_thread(thread_ctx, body.instructions));

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
            return_value = returned_rx.recv() => {
                tracing::info!("Lua thread has finished execution with return value {return_value:?}");
                break;
            }
        }
    }

    container.destroy().await.expect("failed to destroy jail");

    "stateless run completed".to_string()
}
