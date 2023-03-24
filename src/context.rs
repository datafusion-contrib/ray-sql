use crate::planner::{make_execution_graph, PyExecutionGraph};
use crate::shuffle::{RayShuffleReaderExec, ShuffleCodec};
use crate::utils::wait_for_future;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::config::Extensions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_python::errors::py_datafusion_err;
use datafusion_python::physical_plan::PyExecutionPlan;
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyLong, PyTuple};
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
            .with_disk_manager(DiskManagerConfig::new_specified(vec![
                "/mnt/data0/tmp".into()
            ]));
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
        execute_partition(plan, part, inputs)
    }
}

#[pyfunction]
pub fn execute_partition(plan: PyExecutionPlan, part: usize, inputs: PyObject) -> PyResultSet {
    let batches = _execute_partition(plan, part, inputs)
        .unwrap()
        .into_iter()
        .map(|batch| PyRecordBatch::new(batch))
        .collect();
    PyResultSet::new(batches)
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
    input_partitions: &PyList,
) -> Result<()> {
    if let Some(reader_exec) = plan.as_any().downcast_ref::<RayShuffleReaderExec>() {
        let exec_stage_id = reader_exec.stage_id;
        // iterate over inputs, wrap in PyBytes and set as input objects
        for item in input_partitions.iter() {
            let pytuple = item
                .downcast::<PyTuple>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            let stage_id = pytuple
                .get_item(0)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .downcast::<PyLong>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .extract::<usize>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            if stage_id != exec_stage_id {
                continue;
            }
            let part = pytuple
                .get_item(1)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .downcast::<PyLong>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .extract::<usize>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            let bytes = pytuple
                .get_item(2)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .downcast::<PyBytes>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .as_bytes()
                .to_vec(); // TODO(@lsf) avoid copy here
            reader_exec.add_input_partition(part, bytes)?;
        }
    } else {
        for child in plan.children() {
            _set_inputs_for_ray_shuffle_reader(child, part, input_partitions)?;
        }
    }
    Ok(())
}

/// Execute a partition of a query plan. This will typically be executing a shuffle write and
/// write the results to disk, except for the final query stage, which will return the data.
/// inputs is a list of tuples of (stage_id, partition_id, bytes) for each input partition.
fn _execute_partition(
    plan: PyExecutionPlan,
    part: usize,
    inputs: PyObject,
) -> Result<Vec<RecordBatch>> {
    let ctx = Arc::new(TaskContext::try_new(
        "task_id".to_string(),
        "session_id".to_string(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(RuntimeEnv::default()),
        Extensions::default(),
    )?);
    Python::with_gil(|py| {
        let input_partitions = inputs
            .as_ref(py)
            .downcast::<PyList>()
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
        _set_inputs_for_ray_shuffle_reader(plan.plan.clone(), part, &input_partitions)
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

#[pyclass(name = "ResultSet", module = "raysql", subclass)]
pub struct PyResultSet {
    batches: Vec<PyRecordBatch>,
}

impl PyResultSet {
    fn new(batches: Vec<PyRecordBatch>) -> Self {
        Self { batches }
    }
}

fn _read_pybytes(pyobj: &PyAny, batches: &mut Vec<PyRecordBatch>) -> PyResult<()> {
    let pybytes = pyobj
        .downcast::<PyBytes>()
        .map_err(|e| py_datafusion_err(e))?;
    let reader = StreamReader::try_new(pybytes.as_bytes(), None).map_err(py_datafusion_err)?;
    for batch in reader {
        let batch = batch.map_err(|e| py_datafusion_err(e))?;
        batches.push(PyRecordBatch::new(batch));
    }
    Ok(())
}

#[pymethods]
impl PyResultSet {
    /// This constructor takes either a list of bytes or a single bytes object.
    #[new]
    fn py_new(pyobj: &PyAny) -> PyResult<Self> {
        let mut batches = vec![];
        match pyobj.downcast::<PyList>() {
            Ok(pylist) => {
                for item in pylist.iter() {
                    _read_pybytes(item, &mut batches)?;
                }
                Ok(())
            }
            _ => _read_pybytes(&pyobj, &mut batches),
        }?;
        Ok(Self { batches })
    }

    fn __repr__(&self) -> PyResult<String> {
        let batches: Vec<RecordBatch> = self.batches.iter().map(|b| b.batch.clone()).collect();
        Ok(format!("{}", pretty_format_batches(&batches).unwrap()))
    }

    fn tobyteslist(&self, py: Python) -> PyResult<PyObject> {
        let mut items = vec![];
        for batch in &self.batches {
            items.push(batch.tobytes(py)?);
        }
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
        let reader =
            StreamReader::try_new(py_obj.as_bytes(), None).map_err(|e| py_datafusion_err(e))?;
        let mut batches = vec![];
        for r in reader {
            batches.push(r.map_err(|e| py_datafusion_err(e))?);
        }
        if let Some(batch) = batches.pop() {
            Ok(Self { batch })
        } else {
            Err(py_datafusion_err("no batches"))
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "{}",
            pretty_format_batches(&[self.batch.clone()]).unwrap()
        ))
    }

    fn tobytes(&self, py: Python) -> PyResult<PyObject> {
        let mut buf = Vec::<u8>::new();
        write_batch(&mut buf, &self.batch).map_err(|e| py_datafusion_err(e))?;
        Ok(PyBytes::new(py, &buf).into())
    }
}

fn write_batch(mut buf: &mut Vec<u8>, batch: &RecordBatch) -> Result<(), ArrowError> {
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()
}
