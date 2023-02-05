use crate::planner::{make_execution_graph, PyExecutionGraph};
use crate::shuffle::ShuffleCodec;
use crate::utils::wait_for_future;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::displayable;
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
use tokio::task::JoinHandle;

#[pyclass(name = "Context", module = "raysql", subclass)]
pub struct PyContext {
    pub(crate) ctx: SessionContext,
}

#[pymethods]
impl PyContext {
    #[new]
    pub fn new(target_partitions: usize) -> Result<Self> {
        let config = SessionConfig::default()
            .with_target_partitions(target_partitions)
            .with_batch_size(16 * 1024)
            .with_repartition_aggregations(true)
            .with_repartition_windows(true)
            .with_repartition_joins(true)
            .with_parquet_pruning(true);

        let mem_pool_size = 1024 * 1024 * 1024;
        let runtime_config = datafusion::execution::runtime_env::RuntimeConfig::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(mem_pool_size)))
            .with_disk_manager(DiskManagerConfig::new_specified(vec!["/tmp".into()]));
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        let ctx = SessionContext::with_config_rt(config, runtime);
        Ok(Self { ctx })
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
            debug!(
                "Query stage #{}:\n{}",
                stage.id,
                displayable(stage.plan.as_ref()).indent()
            );
        }

        Ok(PyExecutionGraph::new(graph))
    }

    /// Execute a partition of a query plan. This will typically be executing a shuffle write and write the results to disk
    pub fn execute_partition(&self, plan: PyExecutionPlan, part: usize) -> PyResultSet {
        let batches = self
            ._execute_partition(plan, part)
            .unwrap()
            .iter()
            .map(|batch| PyRecordBatch::new(batch.clone()))
            .collect();
        PyResultSet::new(batches)
    }
}

#[pyfunction]
pub fn serialize_execution_plan(plan: PyExecutionPlan) -> PyResult<Vec<u8>> {
    let codec = ShuffleCodec {};
    Ok(physical_plan_to_bytes_with_extension_codec(plan.plan, &codec)?.to_vec())
}

#[pyfunction]
pub fn deserialize_execution_plan(bytes: Vec<u8>) -> PyResult<PyExecutionPlan> {
    let ctx = SessionContext::new();
    let codec = ShuffleCodec {};
    Ok(PyExecutionPlan::new(
        physical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?,
    ))
}

impl PyContext {
    /// Execute a partition of a query plan. This will typically be executing a shuffle write and
    /// write the results to disk, except for the final query stage, which will return the data
    fn _execute_partition(&self, plan: PyExecutionPlan, part: usize) -> Result<Vec<RecordBatch>> {
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

        let fut: JoinHandle<Result<Vec<RecordBatch>>> = rt.spawn(async move {
            let mut stream = plan.plan.execute(part, ctx)?;
            let mut results = vec![];
            while let Some(result) = stream.next().await {
                results.push(result?);
            }
            Ok(results)
        });

        // block and wait on future
        let results = rt.block_on(fut).unwrap()?;
        Ok(results)
    }
}

#[pyclass(name = "ResultSet", module = "raysql", subclass)]
pub struct PyResultSet {
    batches: Vec<PyRecordBatch>,
}

impl PyResultSet {
    fn new(batches: Vec<PyRecordBatch>) -> Self {
        Self { batches }
    }
}

#[pymethods]
impl PyResultSet {
    fn __repr__(&self) -> PyResult<String> {
        let batches: Vec<RecordBatch> = self.batches.iter().map(|b| b.batch.clone()).collect();
        Ok(format!("{}", pretty_format_batches(&batches).unwrap()))
    }
}

#[pyclass(name = "RecordBatch", module = "raysql", subclass)]
pub struct PyRecordBatch {
    pub(crate) batch: RecordBatch,
}

impl PyRecordBatch {
    fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "{}",
            pretty_format_batches(&[self.batch.clone()]).unwrap()
        ))
    }
}
