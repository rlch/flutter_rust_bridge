pub(crate) mod error;
pub(crate) mod error_listener;
pub(crate) mod executor;
#[allow(clippy::module_inception)]
pub(crate) mod handler;
pub(crate) mod implementation;

pub use error::Error as HandlerError;
pub use error_listener::ErrorListener;
pub use executor::Executor;
pub use handler::{FfiCallMode, TaskInfo, Thread};
pub use handler::{TaskContext, TaskRetFutTrait};
pub use implementation::thread_executor::{ThreadExecutor, ThreadJob, ThreadRouter};
pub use implementation::error_listener::NoOpErrorListener;
pub use implementation::executor::SimpleExecutor;
pub use implementation::handler::SimpleHandler;
