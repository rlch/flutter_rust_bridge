//! An [`Executor`] that routes each call to a thread/worker lane chosen by
//! [`TaskInfo::thread`].
//!
//! This mirrors [`SimpleExecutor`](super::executor::SimpleExecutor) — the
//! port/sender/panic plumbing is identical — but instead of a single thread
//! pool it holds a [`ThreadRouter`] and asks it which pool a call goes
//! to, keyed by the typed [`Thread`]. `execute_sync` still runs inline on the
//! caller (the main thread), so a custom routing setup keeps stateless getters
//! on main and everything else on its dedicated lane.
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
///   `SimpleExecutor`. Callers must only expose state-free getters as `sync`.
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
        // state-touching function is exposed as `sync` (which they can enforce
        // at codegen via the `lane_routing` `require_annotation_when` config).
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
// off the main thread, after `transfer!`). On native the lane worker runs inside
// a `LocalSet` (see `SingleThreadAsyncLane`), so we `spawn_local` too — the
// future yields the lane on `.await` instead of parking the single worker, which
// is what a `block_on` here would do. Routers whose lane is NOT a `LocalSet`
// (e.g. a bare single-worker `SimpleThreadPool`) must not be used for async
// tasks: `spawn_local` requires an entered `LocalSet`.
#[cfg(target_family = "wasm")]
fn spawn_local_compat<F: Future<Output = ()> + 'static>(future: F) {
    wasm_bindgen_futures::spawn_local(future);
}
#[cfg(not(target_family = "wasm"))]
fn spawn_local_compat<F: Future<Output = ()> + 'static>(future: F) {
    tokio::task::spawn_local(future);
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

// =============================================================================
// A single-worker lane must not pin on `.await`.
//
// `ThreadExecutor` routes each call to a worker lane. When a consumer makes a
// lane a single worker (the only way to get FIFO single-owner ordering), an
// async handler that `.await`s must YIELD that worker to other queued calls on
// the same lane — not park it. Native `execute_async` drives the future with
// `tokio::task::spawn_local` onto the lane's `LocalSet` (see `spawn_local_compat`
// above) precisely so it yields; a `block_on` there would park the single worker
// until the future resolved, starving every other call on the lane. The wasm
// path already yields via `spawn_local`.
//
// This routes two async tasks to one single-worker lane with a cross-task
// dependency: cooperative scheduling completes both; a parking driver would
// deadlock (A holds the worker awaiting B, which can never start).
#[cfg(all(test, feature = "rust-async", feature = "thread-pool", not(target_family = "wasm")))]
mod single_worker_lane_pinning_tests {
    use super::*;
    use crate::codec::dco::DcoCodec;
    use crate::codec::BaseCodec;
    use crate::handler::handler::{FfiCallMode, TaskInfo, Thread};
    use crate::handler::implementation::error_listener::NoOpErrorListener;
    use crate::thread_pool::{BaseThreadPool, SingleThreadAsyncLane};
    use std::sync::mpsc;
    use std::time::Duration;

    /// A router with one single-owner lane, backed by `SingleThreadAsyncLane` so
    /// async tasks are driven cooperatively (one thread, one `LocalSet`).
    struct SingleWorkerLane {
        worker: SingleThreadAsyncLane,
    }

    impl ThreadRouter for SingleWorkerLane {
        fn execute_on(&self, _thread: Thread, job: ThreadJob) {
            self.worker.execute(job);
        }
    }

    fn worker_task(port: i64) -> TaskInfo {
        TaskInfo {
            port: Some(port),
            debug_name: "repro",
            mode: FfiCallMode::Normal,
            thread: Thread::Worker("w"),
        }
    }

    // The result message is irrelevant (it posts to a dead port → just warns);
    // we observe scheduling through side channels captured by the task futures.
    fn ok_msg() -> Result<<DcoCodec as BaseCodec>::Message, <DcoCodec as BaseCodec>::Message> {
        Ok(<DcoCodec as BaseCodec>::Message::simplest())
    }

    #[test]
    fn awaiting_task_must_not_pin_single_worker_lane() {
        let exec = ThreadExecutor::new(
            NoOpErrorListener,
            SingleWorkerLane {
                worker: SingleThreadAsyncLane::new("repro-lane"),
            },
        );

        // gate: opened by B, awaited by A. `oneshot` needs only a waker (no
        // reactor), so awaiting it is valid on the reactor-less worker lane.
        let (gate_tx, gate_rx) = futures::channel::oneshot::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel::<&'static str>();

        // Task A — submitted first, so it occupies the single worker first, then
        // awaits B's gate. Cooperative scheduling yields the lane here; a parking
        // driver would hold the only worker.
        let done_a = done_tx.clone();
        exec.execute_async::<DcoCodec, _, _>(worker_task(1), move |_ctx| async move {
            let _ = gate_rx.await;
            let _ = done_a.send("A");
            ok_msg()
        });

        // Task B — queued behind A on the same lane. Opens the gate so A can
        // finish. It can only run if A's await freed the worker.
        let done_b = done_tx.clone();
        exec.execute_async::<DcoCodec, _, _>(worker_task(2), move |_ctx| async move {
            let _ = gate_tx.send(());
            let _ = done_b.send("B");
            ok_msg()
        });
        drop(done_tx);

        let mut done = Vec::new();
        while done.len() < 2 {
            match done_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(who) => done.push(who),
                Err(_) => panic!(
                    "single-worker lane PINNED: an awaiting task blocked a queued task on the \
                     same lane (only completed {done:?}). The lane's async driver parked its \
                     single worker on `.await` instead of yielding to the queued task. Fix: drive \
                     the lane with a cooperative single-threaded runtime (current_thread + \
                     LocalSet) so `.await` yields, matching the wasm `spawn_local` path."
                ),
            }
        }

        done.sort_unstable();
        assert_eq!(
            done,
            vec!["A", "B"],
            "both tasks on the single-worker lane must make progress"
        );
    }
}
