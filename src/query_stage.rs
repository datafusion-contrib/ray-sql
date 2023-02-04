use crate::shuffle::{ShuffleCodec, ShuffleReaderExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_python::physical_plan::PyExecutionPlan;
use pyo3::prelude::*;
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion::error::Result;
use datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec;

#[pyclass(name = "QueryStage", module = "raysql", subclass)]
pub struct PyQueryStage {
    stage: Arc<QueryStage>,
}

impl PyQueryStage {
    pub fn from_rust(stage: Arc<QueryStage>) -> Self {
        Self { stage }
    }
}

#[pymethods]
impl PyQueryStage {
    #[new]
    pub fn new(id: usize, bytes: Vec<u8>) -> Result<Self> {
        let ctx = SessionContext::new();
        let codec = ShuffleCodec {};
        let plan = physical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?;
        Ok(PyQueryStage {
            stage: Arc::new(QueryStage {
                id,
                plan
            })
        })
    }

    pub fn id(&self) -> usize {
        self.stage.id
    }

    pub fn get_execution_plan(&self) -> PyExecutionPlan {
        PyExecutionPlan::new(self.stage.plan.clone())
    }

    pub fn get_child_stage_ids(&self) -> Vec<usize> {
        self.stage.get_child_stage_ids()
    }

    pub fn get_input_partition_count(&self) -> usize {
        self.stage.get_input_partition_count()
    }

    pub fn get_output_partition_count(&self) -> usize {
        self.stage.plan.output_partitioning().partition_count()
    }
}

#[derive(Debug)]
pub struct QueryStage {
    pub id: usize,
    pub plan: Arc<dyn ExecutionPlan>,
}

impl QueryStage {
    pub fn new(id: usize, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { id, plan }
    }

    pub fn get_child_stage_ids(&self) -> Vec<usize> {
        let mut ids = vec![];
        collect_child_stage_ids(self.plan.as_ref(), &mut ids);
        ids
    }

    /// Get the input partition count. This is the same as the number of concurrent tasks
    /// when we schedule this query stage for execution
    pub fn get_input_partition_count(&self) -> usize {
        collect_input_partition_count(self.plan.as_ref())
    }
}

fn collect_child_stage_ids(plan: &dyn ExecutionPlan, ids: &mut Vec<usize>) {
    if let Some(shuffle_reader) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
        ids.push(shuffle_reader.stage_id);
    } else {
        for child_plan in plan.children() {
            collect_child_stage_ids(child_plan.as_ref(), ids);
        }
    }
}

fn collect_input_partition_count(plan: &dyn ExecutionPlan) -> usize {
    if plan.children().is_empty() {
        plan.output_partitioning().partition_count()
    } else {
        // invariants:
        // - all inputs must have the same partition count
        collect_input_partition_count(plan.children()[0].as_ref())
    }
}