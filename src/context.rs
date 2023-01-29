use crate::planner::{make_execution_graph, PyExecutionGraph};
use crate::shuffle::ShuffleCodec;
use crate::utils::wait_for_future;
use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::prelude::*;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_python::physical_plan::PyExecutionPlan;
use futures::StreamExt;
use log::debug;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[pyclass(name = "Context", module = "raysql", subclass)]
pub struct PyContext {
    pub(crate) ctx: SessionContext,
}

#[pymethods]
impl PyContext {
    #[new]
    pub fn new(target_partitions: usize) -> Self {
        let config = SessionConfig::default().with_target_partitions(target_partitions);
        Self {
            ctx: SessionContext::with_config(config),
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
        debug!("Planning {}", sql);
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

    /// Execute a partition of a query plan. This will typically be executing a shuffle write and write the results to disk
    pub fn execute_partition(&self, plan: PyExecutionPlan, part: usize) -> PyResult<()> {
        let ctx = Arc::new(TaskContext::new(
            "task_id".to_string(),
            "session_id".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        ));

        // create a Tokio runtime to run the async code
        let rt = Runtime::new().unwrap();

        let fut = rt.spawn(async move {
            let mut stream = plan.plan.execute(part, ctx)?;
            let mut results = vec![];
            let mut row_count = 0_u32;
            while let Some(result) = stream.next().await {
                let input_batch = result?;
                row_count += 1;
                results.push(input_batch);
            }

            println!("Results:\n{}", pretty_format_batches(&results)?);

            // return a result set with metadata about this executed partition
            let schema = Arc::new(Schema::new(vec![
                Field::new("partition_index", DataType::UInt32, true),
                Field::new("partition_batches", DataType::UInt32, true),
                Field::new("partition_rows", DataType::UInt32, true),
            ]));
            let part_index = UInt32Array::from(vec![part as u32]);
            let part_batches = UInt32Array::from(vec![results.len() as u32]);
            let part_rows = UInt32Array::from(vec![row_count]);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(part_index),
                    Arc::new(part_batches),
                    Arc::new(part_rows),
                ],
            )?;
            MemoryStream::try_new(vec![batch], schema, None)
        });

        // block and wait on future
        let x = rt.block_on(fut).unwrap(); // TODO error handling
        let _stream = x?;

        Ok(())
    }
}
