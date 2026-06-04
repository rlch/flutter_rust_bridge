//! An [`Executor`] that routes each call to a thread/worker lane chosen by
//! [`TaskInfo::thread`].
//!
//! This mirrors [`SimpleExecutor`](super::executor::SimpleExecutor) — the
//! port/sender/panic plumbing is identical — but instead of a single thread
//! pool it holds a [`ThreadRouter`] and asks it which pool a call goes
//! to, keyed by the typed [`Thread`]. `execute_sync` still runs inline on the
//! caller (the main thread), so a custom routing setup keeps Loro-free getters
//! on main and everything stateful on its lane.
//!
//! The default generated handler does NOT use this; it is opt-in by providing a
//! custom `FLUTTER_RUST_BRIDGE_HANDLER` built from `SimpleHandler<ThreadExecutor<...>, _>`.

use crate::codec::BaseCodec;
use crate::codec::Rust2DartMessageTrait;
use crate::generalized_isolate::Channel;
use crate::handler::error::Error;
use crate::handler::error_listener::ErrorListener;
use crate::handler::executor::Executor;
use crate::handler::handler::{TaskContext, TaskInfo, TaskRetFutTrait, Thread};
use crate::handler::implementation::error_listener::handle_non_sync_panic_error;
use crate::misc::panic_backtrace::{CatchUnwindWithBacktrace, PanicBacktrace};
use crate::platform_types::MessagePort;
use crate::rust2dart::sender::Rust2DartSender;
#[cfg(feature = "rust-async")]
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;

/// Supplies the per-[`Thread`] execution lanes that a [`ThreadExecutor`]
/// routes to. Implementors own the actual pools/workers (one per thread) and
/// know how to run a transferred closure on the lane for a given thread.
///
/// This is the seam the embedder (e.g. modality) implements to decide how many
/// workers each lane has and how they are spawned, while the executor here owns
/// the (identical-to-`SimpleExecutor`) task plumbing.
pub trait ThreadRouter {
    /// Run `job` on the worker lane for `thread` (off the main thread).
    ///
    /// On native, `job` is a `Send + 'static` closure. On web it is the same
    /// after the `transfer!` macro lowers it to a `TransferClosure`; the
    /// implementor forwards it to that lane's `BaseThreadPool::execute`.
    fn execute_on(&self, thread: Thread, job: ThreadJob);
}

// On web a transferred closure is a `TransferClosure<JsValue>`; on native it is
// a boxed `FnOnce`. The executor builds the right one via the `thread_job!` macro
// (which wraps `transfer!` and boxes on native) and hands it to
// `ThreadRouter::execute_on`, so the impl only deals with "run this
// lane's job".
#[cfg(not(target_family = "wasm"))]
pub type ThreadJob = Box<dyn FnOnce() + Send + 'static>;
#[cfg(target_family = "wasm")]
pub type ThreadJob =
    crate::web_transfer::transfer_closure::TransferClosure<wasm_bindgen::JsValue>;

/// Build a [`ThreadJob`] from a `transfer!`-style closure. On native it boxes
/// the closure into `Box<dyn FnOnce() + Send>`; on web `transfer!` already
/// yields the `TransferClosure`, so it passes through.
macro_rules! thread_job {
    (|$($param:ident: $ty:ty),* $(,)?| $block:block) => {{
        #[cfg(not(target_family = "wasm"))]
        {
            let job: ThreadJob = Box::new($crate::transfer!(|$($param: $ty),*| $block));
            job
        }
        #[cfg(target_family = "wasm")]
        {
            $crate::transfer!(|$($param: $ty),*| $block)
        }
    }};
}

/// An [`Executor`] that routes calls to per-thread lanes.
///
/// - `execute_normal` / `execute_async`: build the same closure
///   [`SimpleExecutor`](super::executor::SimpleExecutor) builds, then route it
///   to the `thread`'s lane via [`ThreadRouter::execute_on`].
/// - `execute_sync`: run inline on the caller (main thread), exactly like
///   `SimpleExecutor`. Callers must only expose Loro-free getters as `sync`.
pub struct ThreadExecutor<EL: ErrorListener, P: ThreadRouter> {
    error_listener: EL,
    pools: P,
}

impl<EL: ErrorListener, P: ThreadRouter> ThreadExecutor<EL, P> {
    pub fn new(error_listener: EL, pools: P) -> Self {
        Self {
            error_listener,
            pools,
        }
    }

    pub fn pools(&self) -> &P {
        &self.pools
    }
}

impl<EL: ErrorListener + Sync, P: ThreadRouter> Executor for ThreadExecutor<EL, P> {
    #[cfg(feature = "thread-pool")]
    fn execute_normal<Rust2DartCodec, TaskFn>(&self, task_info: TaskInfo, task: TaskFn)
    where
        TaskFn: FnOnce(TaskContext) -> Result<Rust2DartCodec::Message, Rust2DartCodec::Message>
            + Send
            + 'static,
        Rust2DartCodec: BaseCodec,
    {
        let el = self.error_listener;
        let el2 = self.error_listener;

        let thread = task_info.thread;
        let TaskInfo { port, .. } = task_info;
        let port: MessagePort = port.unwrap();

        // Identical body to SimpleExecutor::execute_normal (executor.rs), only
        // the destination differs: route to the thread lane instead of "any
        // idle worker".
        self.pools.execute_on(
            thread,
            thread_job!(|port: crate::platform_types::MessagePort| {
                #[allow(clippy::clone_on_copy)]
                let port2 = port.clone();
                let thread_result = PanicBacktrace::catch_unwind(AssertUnwindSafe(|| {
                    #[allow(clippy::clone_on_copy)]
                    let sender = Rust2DartSender::new(Channel::new(port2.clone()));
                    let task_context = TaskContext::new();

                    let ret = task(task_context);

                    ThreadExecuteUtils::handle_result::<Rust2DartCodec, _>(ret, sender, el2);
                }));

                if let Err(error) = thread_result {
                    handle_non_sync_panic_error::<Rust2DartCodec>(el, port, error);
                }
            }),
        );
    }

    fn execute_sync<Rust2DartCodec, SyncTaskFn>(
        &self,
        _task_info: TaskInfo,
        sync_task: SyncTaskFn,
    ) -> Rust2DartCodec::Message
    where
        SyncTaskFn: FnOnce() -> Result<Rust2DartCodec::Message, Rust2DartCodec::Message>,
        Rust2DartCodec: BaseCodec,
    {
        // sync == inline on the caller == main thread. Embedders must ensure no
        // Loro-touching function is exposed as `sync` (enforced at codegen via
        // the `#[frb(thread = ...)]` hybrid rule).
        match sync_task() {
            Ok(data) => data,
            Err(err) => {
                self.error_listener.on_error(Error::CustomError);
                err
            }
        }
    }

    #[cfg(feature = "rust-async")]
    fn execute_async<Rust2DartCodec, TaskFn, TaskRetFut>(&self, task_info: TaskInfo, task: TaskFn)
    where
        TaskFn: FnOnce(TaskContext) -> TaskRetFut + Send + 'static,
        TaskRetFut: Future<Output = Result<Rust2DartCodec::Message, Rust2DartCodec::Message>>
            + TaskRetFutTrait,
        Rust2DartCodec: BaseCodec,
    {
        let el = self.error_listener;
        let el2 = self.error_listener;

        let thread = task_info.thread;
        let TaskInfo { port, .. } = task_info;
        let port: MessagePort = port.unwrap();

        // Off-main async: transfer the `Send` closure (capturing the
        // transferable `MessagePort`, exactly like the normal branch) to the
        // thread lane; the lane worker drives the (possibly non-`Send`)
        // future on its own local executor via `spawn_local_compat`. Mirrors
        // `spawn_blocking_with` (rust_async/web.rs) + SimpleExecutor::execute_async.
        self.pools.execute_on(
            thread,
            thread_job!(|port: crate::platform_types::MessagePort| {
                spawn_local_compat(async move {
                    #[allow(clippy::clone_on_copy)]
                    let port2 = port.clone();

                    let async_result = AssertUnwindSafe(async {
                        #[allow(clippy::clone_on_copy)]
                        let sender = Rust2DartSender::new(Channel::new(port2.clone()));
                        let task_context = TaskContext::new();

                        let ret = task(task_context).await;

                        ThreadExecuteUtils::handle_result::<Rust2DartCodec, _>(ret, sender, el2);
                    })
                    .catch_unwind()
                    .await;

                    if let Err(err) = async_result {
                        let err = CatchUnwindWithBacktrace::new(err, PanicBacktrace::take_last());
                        handle_non_sync_panic_error::<Rust2DartCodec>(el, port, err);
                    }
                });
            }),
        );
    }
}

// Drive a future on the current worker thread's local executor. On web this is
// `wasm_bindgen_futures::spawn_local` (the closure already runs ON the worker,
// off the main thread, after `transfer!`). On native the closure runs on a pool
// thread (already off-main) so we block it on the future to completion.
#[cfg(target_family = "wasm")]
fn spawn_local_compat<F: Future<Output = ()> + 'static>(future: F) {
    wasm_bindgen_futures::spawn_local(future);
}
#[cfg(not(target_family = "wasm"))]
fn spawn_local_compat<F: Future<Output = ()>>(future: F) {
    futures::executor::block_on(future);
}

struct ThreadExecuteUtils;

impl ThreadExecuteUtils {
    fn handle_result<Rust2DartCodec, EL>(
        ret: Result<Rust2DartCodec::Message, Rust2DartCodec::Message>,
        sender: Rust2DartSender,
        el: EL,
    ) where
        EL: ErrorListener + Sync,
        Rust2DartCodec: BaseCodec,
    {
        match ret {
            Ok(result) => {
                sender.send_or_warn(result.into_dart_abi());
            }
            Err(error) => {
                el.on_error(Error::CustomError);
                sender.send_or_warn(error.into_dart_abi());
            }
        };
    }
}
