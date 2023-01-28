use pyo3::Python;
use std::future::Future;
use tokio::runtime::Runtime;

/// Utility to collect rust futures with GIL released
pub(crate) fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let rt = Runtime::new().unwrap();
    py.allow_threads(|| rt.block_on(f))
}
