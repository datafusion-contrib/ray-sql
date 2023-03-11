use crate::planner::{make_execution_graph, PyExecutionGraph};
use crate::shuffle::{RayShuffleReaderExec, ShuffleCodec};
use crate::utils::wait_for_future;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_python::physical_plan::PyExecutionPlan;
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

#[pyclass(name = "Context", module = "raysql", subclass)]
pub struct PyContext {
    pub(crate) ctx: SessionContext,
    use_ray_shuffle: bool,
}

#[pymethods]
impl PyContext {
    #[new]
    pub fn new(target_partitions: usize, use_ray_shuffle: bool) -> Result<Self> {
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
        Ok(Self {
            ctx,
            use_ray_shuffle,
        })
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

        let graph = make_execution_graph(plan.clone(), self.use_ray_shuffle)?;

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

    /// Execute a partition of a query plan. This will typically be executing a shuffle write and write the results to disk
    pub fn execute_partition(
        &self,
        plan: PyExecutionPlan,
        part: usize,
        inputs: PyObject,
    ) -> PyResultSet {
        let batches = self
            ._execute_partition(plan, part, inputs)
            .unwrap()
            .iter()
            .map(|batch| PyRecordBatch::new(batch.clone())) // TODO(@lsf): avoid clone?
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

/// Iterate down an ExecutionPlan and set the input objects for RayShuffleReaderExec.
fn _set_inputs_for_ray_shuffle_reader(
    plan: Arc<dyn ExecutionPlan>,
    part: usize,
    inputs: &PyObject,
    py: Python,
) -> Result<()> {
    if let Some(reader_exec) = plan.as_any().downcast_ref::<RayShuffleReaderExec>() {
        // iterate over inputs, wrap in PyBytes and set as input objects
        let input_objects = inputs
            .as_ref(py)
            .iter()
            .expect("expected iterable")
            .map(|input| {
                input
                    .unwrap()
                    .downcast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec()
            })
            .collect();
        reader_exec.set_input_objects(part, input_objects)?;
    } else {
        for child in plan.children() {
            _set_inputs_for_ray_shuffle_reader(child, part, inputs, py)?;
        }
    }
    Ok(())
}

impl PyContext {
    /// Execute a partition of a query plan. This will typically be executing a shuffle write and
    /// write the results to disk, except for the final query stage, which will return the data
    fn _execute_partition(
        &self,
        plan: PyExecutionPlan,
        part: usize,
        inputs: PyObject,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = Arc::new(TaskContext::new(
            "task_id".to_string(),
            "session_id".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        ));
        Python::with_gil(|py| {
            _set_inputs_for_ray_shuffle_reader(plan.plan.clone(), part, &inputs, py)
        })?;

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
    #[new]
    fn py_new(py_obj: &PyBytes) -> PyResult<Self> {
        let reader = StreamReader::try_new(py_obj.as_bytes(), None).unwrap();
        let batches = reader
            .into_iter()
            .map(|r| PyRecordBatch::new(r.unwrap()))
            .collect::<Vec<_>>();
        Ok(Self { batches })
    }

    fn __repr__(&self) -> PyResult<String> {
        let batches: Vec<RecordBatch> = self.batches.iter().map(|b| b.batch.clone()).collect();
        Ok(format!("{}", pretty_format_batches(&batches).unwrap()))
    }

    fn tobyteslist(&self, py: Python) -> PyResult<PyObject> {
        let items: Vec<_> = self
            .batches
            .iter()
            .map(|b| b.tobytes(py).unwrap())
            .collect();
        Ok(PyList::new(py, &items).into())
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
}

#[pymethods]
impl PyRecordBatch {
    #[new]
    fn py_new(py_obj: &PyBytes) -> PyResult<Self> {
        let reader = StreamReader::try_new(py_obj.as_bytes(), None).unwrap();
        let mut batches: Vec<_> = reader.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>();
        Ok(Self {
            batch: batches.pop().unwrap(),
        })
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "{}",
            pretty_format_batches(&[self.batch.clone()]).unwrap()
        ))
    }

    fn tobytes(&self, py: Python) -> PyResult<PyObject> {
        // TODO(@lsf): wrap errors into PyErr
        let mut buf = Vec::<u8>::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, self.batch.schema().as_ref()).unwrap();
            writer.write(&self.batch).unwrap();
            writer.finish().unwrap();
        }
        Ok(PyBytes::new(py, &buf).into())
    }
}
