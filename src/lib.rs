extern crate core;

use pyo3::prelude::*;

mod proto;
use crate::context::{deserialize_execution_plan, serialize_execution_plan};
pub use proto::generated::protobuf;

pub mod context;
pub mod planner;
pub mod query_stage;
pub mod shuffle;
pub mod utils;

/// A Python module implemented in Rust.
#[pymodule]
fn _raysql_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // register classes that can be created directly from Python code
    m.add_class::<context::PyContext>()?;
    m.add_class::<query_stage::PyQueryStage>()?;
    m.add_function(wrap_pyfunction!(serialize_execution_plan, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_execution_plan, m)?)?;
    Ok(())
}
