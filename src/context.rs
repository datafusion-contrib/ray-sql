use crate::planner::{make_execution_graph, PyExecutionGraph};
use crate::shuffle::ShuffleCodec;
use crate::utils::wait_for_future;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_proto::bytes::{
    physical_plan_from_bytes, physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_python::physical_plan::PyExecutionPlan;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(name = "Context", module = "raysql", subclass)]
pub struct PyContext {
    pub(crate) ctx: SessionContext,
}

#[pymethods]
impl PyContext {
    #[new]
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::default(),
        }
    }

    pub fn register_csv(
        &self,
        name: &str,
        path: &str,
        has_header: bool,
        py: Python,
    ) -> PyResult<()> {
        let options = CsvReadOptions::default().has_header(has_header);
        wait_for_future(py, self.ctx.register_csv(name, path, options))?;
        Ok(())
    }

    pub fn register_parquet(&self, name: &str, path: &str, py: Python) -> PyResult<()> {
        let options = ParquetReadOptions::default();
        wait_for_future(py, self.ctx.register_parquet(name, path, options))?;
        Ok(())
    }

    pub fn plan(&self, sql: &str, py: Python) -> PyResult<PyExecutionGraph> {
        println!("Planning {}", sql);
        let df = wait_for_future(py, self.ctx.sql(sql))?;
        let plan = wait_for_future(py, df.create_physical_plan())?;

        let graph = make_execution_graph(plan.clone())?;

        // debug logging
        for stage in graph.query_stages.values() {
            println!(
                "Query stage #{}:\n{}",
                stage.id,
                displayable(stage.plan.as_ref()).indent()
            );
        }

        Ok(PyExecutionGraph::new(graph))
    }

    fn serialize_execution_plan(&self, plan: PyExecutionPlan) -> PyResult<Vec<u8>> {
        let codec = ShuffleCodec {};
        Ok(physical_plan_to_bytes_with_extension_codec(plan.plan, &codec)?.to_vec())
    }

    fn deserialize_execution_plan(&self, bytes: Vec<u8>) -> PyResult<PyExecutionPlan> {
        let codec = ShuffleCodec {};
        Ok(PyExecutionPlan::new(
            physical_plan_from_bytes_with_extension_codec(&bytes, &self.ctx, &codec)?,
        ))
    }

    /// Execute a partition of a query plan and write the results to disk
    pub fn execute_partition(&self, plan: PyExecutionPlan, part: usize) {
        println!("Executing: {}", plan.display_indent());
        // TODO wrap in shuffle writer
    }
}
