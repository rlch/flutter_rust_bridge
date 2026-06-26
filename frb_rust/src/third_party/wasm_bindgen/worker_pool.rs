//! Copied and modified from the wasm_bindgen raytrace-parallel example
//!
//! File: https://github.com/rustwasm/wasm-bindgen/blob/main/examples/raytrace-parallel/src/pool.rs

use crate::misc::web_utils::script_path;
use crate::web_transfer::transfer_closure::TransferClosure;
use js_sys::{Array, Object, Reflect};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::BlobPropertyBag;
use web_sys::ErrorEvent;
use web_sys::MessageEvent;
use web_sys::{Blob, Url};
use web_sys::{Event, Worker};

#[wasm_bindgen]
pub struct WorkerPool {
    state: Rc<PoolState>,
    script_src: String,
    worker_js_preamble: String,
    wasm_bindgen_name: String,
}

struct PoolState {
    /// Idle workers ready to accept a new job.
    workers: RefCell<Vec<Worker>>,
    /// FIFO queue used once all workers up to `max_workers` are busy.
    pending: RefCell<VecDeque<TransferClosure<JsValue>>>,
    /// Number of workers spawned by this pool, idle + busy.
    live_workers: Cell<usize>,
    /// Hard upper bound for workers owned by this pool.
    max_workers: usize,
    callback: Closure<dyn FnMut(Event)>,
}

#[wasm_bindgen]
impl WorkerPool {
    pub fn new(
        initial: Option<usize>,
        script_src: Option<String>,
        worker_js_preamble: Option<String>,
        wasm_bindgen_name: Option<String>,
    ) -> Result<WorkerPool, JsValue> {
        let initial = initial.unwrap_or_else(get_wasm_hardware_concurrency);
        let max_workers = if initial == 0 {
            get_wasm_hardware_concurrency()
        } else {
            initial
        };
        Self::new_with_max(
            initial,
            max_workers,
            script_src,
            worker_js_preamble,
            wasm_bindgen_name,
        )
    }

    /// Creates a pool with separate prewarm and hard-cap counts.
    ///
    /// This is useful for embedders that want a hard one-worker lane but do not
    /// want to instantiate the large wasm module on page load. For example,
    /// `initial = 0, max_workers = 1` creates no worker until first use, then
    /// queues FIFO once that single worker is busy.
    pub fn new_with_max(
        initial: usize,
        max_workers: usize,
        script_src: Option<String>,
        worker_js_preamble: Option<String>,
        wasm_bindgen_name: Option<String>,
    ) -> Result<WorkerPool, JsValue> {
        Self::new_raw_with_max(
            initial,
            max_workers,
            script_src.unwrap_or_else(|| script_path().expect("fail to get script path")),
            worker_js_preamble.unwrap_or_default(),
            wasm_bindgen_name.unwrap_or_else(|| "wasm_bindgen".to_owned()),
        )
    }

    /// Creates a new `WorkerPool` which immediately creates `initial` workers.
    ///
    /// `initial` also acts as this pool's hard worker cap, except `initial == 0`
    /// means "spawn lazily, capped at `navigator.hardwareConcurrency`". When all
    /// workers are busy and the cap has been reached, additional jobs are queued
    /// FIFO instead of spawning more web workers. Use [`WorkerPool::new_raw_with_max`]
    /// when prewarm and hard-cap counts must differ.
    ///
    /// The pool created here can be used over a long period of time. Workers are
    /// retained for reuse until the whole pool is destroyed, but the pool will
    /// never retain more than the hard cap.
    ///
    /// # Errors
    ///
    /// Returns any error that may happen while a JS web worker is created and a
    /// message is sent to it.
    #[wasm_bindgen(constructor)]
    pub fn new_raw(
        initial: usize,
        script_src: String,
        worker_js_preamble: String,
        wasm_bindgen_name: String,
    ) -> Result<WorkerPool, JsValue> {
        let max_workers = if initial == 0 {
            get_wasm_hardware_concurrency()
        } else {
            initial
        };
        Self::new_raw_with_max(
            initial,
            max_workers,
            script_src,
            worker_js_preamble,
            wasm_bindgen_name,
        )
    }

    pub fn new_raw_with_max(
        initial: usize,
        max_workers: usize,
        script_src: String,
        worker_js_preamble: String,
        wasm_bindgen_name: String,
    ) -> Result<WorkerPool, JsValue> {
        let max_workers = max_workers.max(1);
        let initial = initial.min(max_workers);

        let pool = WorkerPool {
            script_src,
            state: Rc::new(PoolState {
                workers: RefCell::new(Vec::with_capacity(initial)),
                pending: RefCell::new(VecDeque::new()),
                live_workers: Cell::new(0),
                max_workers,
                callback: Closure::new(|event: Event| {
                    if let Some(event) = event.dyn_ref::<MessageEvent>() {
                        crate::console_error!("Dropped data:: {:?}", event.data());
                    } else if let Some(event) = event.dyn_ref::<ErrorEvent>() {
                        crate::console_error!("Failed to initialize: {}", event.message());
                    }
                }),
            }),
            worker_js_preamble,
            wasm_bindgen_name,
        };
        for _ in 0..initial {
            let worker = pool.spawn_for_pool()?;
            pool.state.push(worker);
        }

        Ok(pool)
    }

    /// Unconditionally spawns a new worker.
    ///
    /// The worker isn't registered with this `WorkerPool` but is capable of
    /// executing work for this wasm module.
    ///
    /// # Errors
    ///
    /// Returns any error that may happen while a JS web worker is created and a
    /// message is sent to it.
    fn spawn(&self) -> Result<Worker, JsValue> {
        let worker_js_preamble = &self.worker_js_preamble;
        let script_src = &self.script_src;
        let wasm_bindgen_name = &self.wasm_bindgen_name;
        let script = format!(
            "{worker_js_preamble}
            importScripts('{script_src}');
            const FRB_ACTION_PANIC = 3;
            onmessage = event => {{
                let init = {wasm_bindgen_name}(...event.data).catch(err => {{
                    setTimeout(() => {{ throw err }})
                    throw err
                }})
                onmessage = async event => {{
                    await init
                    const [payload, ...transfer] = event.data
                    try {{
                        {wasm_bindgen_name}.receive_transfer_closure(payload, transfer)
                    }} catch (err) {{
                        if (transfer[0] && typeof transfer[0].postMessage === 'function') {{
                            // panic
                            transfer[0].postMessage([FRB_ACTION_PANIC, err.toString()])
                        }}
                        setTimeout(() => {{ throw err }})
                        postMessage(null)
                        throw err
                    }}
                }}
            }}",
        );
        let blob = Blob::new_with_blob_sequence_and_options(
            &Array::from_iter([JsValue::from(script)]).into(),
            BlobPropertyBag::new().type_("text/javascript"),
        )?;
        let url = Url::create_object_url_with_blob(&blob)?;
        let worker = Worker::new(&url);
        let _ = Url::revoke_object_url(&url);
        let worker: Worker = worker?;

        // With a worker spun up send it the module/memory so it can start
        // instantiating the wasm module. Later it might receive further
        // messages about code to run on the wasm module.
        let module = wasm_bindgen::module();
        let memory = wasm_bindgen::memory();
        let wasm_init_object = Object::new();
        Reflect::set(
            &wasm_init_object,
            &JsValue::from_str("module_or_path"),
            &module,
        )?;
        Reflect::set(&wasm_init_object, &JsValue::from_str("memory"), &memory)?;
        let arr = Array::new();
        arr.push(&wasm_init_object);
        worker.post_message(&arr)?;

        Ok(worker)
    }

    fn spawn_for_pool(&self) -> Result<Worker, JsValue> {
        let worker = self.spawn()?;
        self.state
            .live_workers
            .set(self.state.live_workers.get() + 1);
        Ok(worker)
    }

    /// Dispatches work to an idle/new worker and arranges for it to be reclaimed
    /// on completion.
    fn dispatch_to_worker(
        &self,
        worker: Worker,
        closure: TransferClosure<JsValue>,
    ) -> Result<(), JsValue> {
        PoolState::dispatch(&self.state, worker.clone(), closure).map_err(|err| {
            // Preserve the worker if the post failed synchronously. This mirrors
            // native thread pools: the failed job is reported to the caller, but
            // the pool itself remains usable for later jobs.
            self.state.push(worker);
            err
        })
    }
}

impl WorkerPool {
    /// Executes `f` in a web worker.
    ///
    /// This pool manages a capped set of web workers. `f` will be spawned
    /// quickly into an idle worker if one is available. If no idle worker is
    /// available and the hard cap has not been reached, a new worker is spawned.
    /// If the cap has been reached, `f` is queued FIFO until a worker completes
    /// its current job.
    ///
    /// Once `f` returns the worker assigned to `f` is automatically reclaimed by
    /// this `WorkerPool`. This method provides no method of learning when `f`
    /// completes, and for that you'll need to use `run_notify`.
    ///
    /// ## Errors
    ///
    /// If an error happens while spawning a web worker or sending a message to
    /// a web worker immediately, that error is returned. Queued jobs return
    /// `Ok(())` once enqueued.
    ///
    /// ## Transferrables
    /// Items put inside `transfer` will have their ownership transferred from
    /// the invoking JS scope to the target, rendering the value unusable in the original
    /// scope. (This is similar to a `FnOnce` closure in Rust terms, but does not statically
    /// move items out of scope.)
    ///
    /// Certain types in [js_sys] and [web_sys] are transferrable, for which [Send]
    /// can be unsafely implemented **only if** they are passed to the transferrables of
    /// a `post_message`. Examples are `Buffer`s, `MessagePort`s, etc...
    // NOTE: It is originally named `run`, but rename to align with crate `threadpool`
    pub fn execute(&self, closure: TransferClosure<JsValue>) -> Result<(), JsValue> {
        let idle_worker = { self.state.workers.borrow_mut().pop() };
        if let Some(worker) = idle_worker {
            return self.dispatch_to_worker(worker, closure);
        }

        if self.state.live_workers.get() < self.state.max_workers {
            let worker = self.spawn_for_pool()?;
            return self.dispatch_to_worker(worker, closure);
        }

        self.state.pending.borrow_mut().push_back(closure);
        Ok(())
    }
}

impl PoolState {
    fn dispatch(
        state: &Rc<Self>,
        worker: Worker,
        closure: TransferClosure<JsValue>,
    ) -> Result<(), JsValue> {
        let weak_state = Rc::downgrade(state);
        let worker2 = worker.clone();
        let reclaim_slot = Rc::new(RefCell::new(None));
        let slot2 = reclaim_slot.clone();
        let reclaim = Closure::<dyn FnMut(_)>::new(move |_: MessageEvent| {
            if let Some(state) = weak_state.upgrade() {
                Self::complete(&state, worker2.clone());
            }
            *slot2.borrow_mut() = None;
        });
        worker.set_onmessage(Some(reclaim.as_ref().unchecked_ref()));
        *reclaim_slot.borrow_mut() = Some(reclaim);

        closure.apply(&worker).map_err(|err| {
            *reclaim_slot.borrow_mut() = None;
            err
        })
    }

    fn complete(state: &Rc<Self>, worker: Worker) {
        let next = { state.pending.borrow_mut().pop_front() };
        if let Some(closure) = next {
            if let Err(err) = Self::dispatch(state, worker.clone(), closure) {
                crate::console_error!("Failed to dispatch queued worker job: {:?}", err);
                state.push(worker);
            }
        } else {
            state.push(worker);
        }
    }

    fn push(&self, worker: Worker) {
        worker.set_onmessage(Some(self.callback.as_ref().unchecked_ref()));
        worker.set_onerror(Some(self.callback.as_ref().unchecked_ref()));
        let mut workers = self.workers.borrow_mut();
        for prev in workers.iter() {
            let prev: &JsValue = prev;
            let worker: &JsValue = &worker;
            assert!(prev != worker);
        }
        workers.push(worker);
    }
}

impl Default for WorkerPool {
    fn default() -> Self {
        // Use `Some(0)` instead of `None` so workers are spawned lazily on first
        // FFI dispatch rather than eagerly at module init. The hard cap is still
        // `navigator.hardwareConcurrency`, so a lazy default can grow under load
        // but cannot burst beyond the browser-reported CPU parallelism.
        Self::new(Some(0), None, None, None).expect("fail to create WorkerPool")
    }
}

fn get_wasm_hardware_concurrency() -> usize {
    let mut key;
    let global_object = js_sys::global();
    let global = global_object.as_ref();
    key = wasm_bindgen::JsValue::from_str("navigator");
    let navigator = js_sys::Reflect::get(global, &key).unwrap();
    key = wasm_bindgen::JsValue::from_str("hardwareConcurrency");
    let hardware_concurrency = js_sys::Reflect::get(&navigator, &key).unwrap();
    (hardware_concurrency.as_f64().unwrap() as usize).max(1)
}
