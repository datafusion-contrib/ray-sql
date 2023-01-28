use datafusion_python::physical_plan::PyExecutionPlan;
use pyo3::prelude::*;

pub mod context;
pub mod planner;
pub mod shuffle;
pub mod utils;

/// A Python module implemented in Rust.
#[pymodule]
fn _raysql_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<context::PyContext>()?;
    m.add_class::<planner::PyQueryStage>()?;
    Ok(())
}
