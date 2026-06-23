pub trait BaseThreadPool {
    fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

#[derive(Debug, Default)]
pub struct SimpleThreadPool(pub threadpool::ThreadPool);

impl BaseThreadPool for SimpleThreadPool {
    fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.execute(job)
    }
}

/// A single-owner worker lane that drives async jobs **cooperatively**.
///
/// One dedicated OS thread runs a `current_thread` runtime + a `LocalSet`,
/// pulling jobs off a channel and running each inside the LocalSet context. A
/// job that `spawn_local`s a future (what `ThreadExecutor::execute_async` does)
/// therefore yields the lane on `.await` to other queued jobs — unlike a
/// `block_on`-per-job thread pool, which parks the worker until the future
/// completes. This mirrors the wasm `spawn_local` lane on native, preserving
/// single-thread FIFO ownership (one thread, one `LocalSet`) while removing the
/// pinning hazard a single-worker `SimpleThreadPool` has under `block_on`.
#[cfg(feature = "rust-async")]
pub struct SingleThreadAsyncLane {
    tx: futures::channel::mpsc::UnboundedSender<Box<dyn FnOnce() + Send + 'static>>,
}

#[cfg(feature = "rust-async")]
impl SingleThreadAsyncLane {
    pub fn new(thread_name: impl Into<String>) -> Self {
        use futures::StreamExt;
        let (tx, mut rx) =
            futures::channel::mpsc::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        std::thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("SingleThreadAsyncLane: failed to build current_thread runtime");
                let local = tokio::task::LocalSet::new();
                // `run_until` drives the receive loop AND every `spawn_local`'d
                // task on this one thread, so an awaiting task yields back here.
                local.block_on(&runtime, async move {
                    while let Some(job) = rx.next().await {
                        job();
                    }
                });
            })
            .expect("SingleThreadAsyncLane: failed to spawn lane thread");
        Self { tx }
    }
}

#[cfg(feature = "rust-async")]
impl BaseThreadPool for SingleThreadAsyncLane {
    fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Process-global lanes outlive every caller; a send failure just means
        // the lane thread is gone, so dropping the job is the correct no-op.
        let _ = self.tx.unbounded_send(Box::new(job));
    }
}

// TODO migrate this documentation
// /// Spawn a task using the internal thread pool.
// /// Interprets the parameters as a list of captured transferables to
// /// send to this thread.
// ///
// /// Also see [`transfer`].
// ///
// /// Example:
// /// ```
// /// let (tx, rx) = std::sync::mpsc::channel();
// /// flutter_rust_bridge::spawn!(|| {
// ///     tx.send(true).unwrap();
// /// });
// /// assert_eq!(rx.recv(), Ok(true));
// /// ```
// ///
// /// Sending a JS transferable:
// ///
// /// ```
// /// # #[cfg(target_family = "wasm")] fn main() {
// /// use web_sys::{MessagePort, MessageEvent};
// /// use wasm_bindgen::prelude::*;
// ///
// /// let channel = web_sys::MessageChannel::new().unwrap();
// ///
// /// let onmessage = Closure::new(move |event: MessageEvent| {
// ///     assert!(event.data() == true);
// /// });
// /// channel.port1().set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
// /// onmessage.forget();
// /// let port2 = channel.port2();
// /// // Declare the transferable with the same name and type
// /// flutter_rust_bridge::spawn!(|port2: MessagePort| {
// ///     port2.post_message(&JsValue::from(true)).unwrap();
// /// });
// /// # } #[cfg(not(target_family = "wasm"))] fn main() {}
// /// ```
