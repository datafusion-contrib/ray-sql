extern crate core;

use pyo3::prelude::*;

mod proto;
pub use proto::generated::protobuf;

pub mod context;
pub mod planner;
pub mod shuffle;
pub mod utils;

/// A Python module implemented in Rust.
#[pymodule]
fn _raysql_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // register classes that can be created directly from Python code
    m.add_class::<context::PyContext>()?;
    Ok(())
}
