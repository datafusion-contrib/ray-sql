use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::error::Result;
use datafusion::physical_plan::file_format::CsvExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{
    need_data_exchange, with_new_children_if_necessary, ExecutionPlan,
};
use datafusion_python::physical_plan::PyExecutionPlan;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[pyclass(name = "ExecutionGraph", module = "raysql", subclass)]
pub struct PyExecutionGraph {
    pub graph: ExecutionGraph,
}

impl PyExecutionGraph {
    pub fn new(graph: ExecutionGraph) -> Self {
        Self { graph }
    }
}

#[pymethods]
impl PyExecutionGraph {
    pub fn get_query_stages(&self) -> Vec<PyQueryStage> {
        let mut stages = vec![];
        for stage in self.graph.query_stages.values() {
            stages.push(PyQueryStage::new(stage.clone()));
        }
        stages
    }

    pub fn get_query_stage(&self, id: usize) -> PyResult<PyQueryStage> {
        if let Some(stage) = self.graph.query_stages.get(&id) {
            Ok(PyQueryStage::new(stage.clone()))
        } else {
            todo!()
        }
    }

    pub fn get_final_query_stage(&self) -> PyQueryStage {
        PyQueryStage::new(self.graph.get_final_query_stage())
    }
}

#[derive(Debug)]
pub struct ExecutionGraph {
    /// Query stages by id
    pub query_stages: HashMap<usize, Arc<QueryStage>>,
    id_generator: AtomicUsize,
}

impl ExecutionGraph {
    pub fn new() -> Self {
        Self {
            query_stages: HashMap::new(),
            id_generator: AtomicUsize::new(0),
        }
    }

    fn add_query_stage(&mut self, stage_id: usize, plan: Arc<dyn ExecutionPlan>) -> usize {
        let query_stage = QueryStage::new(stage_id, plan);
        self.query_stages.insert(stage_id, Arc::new(query_stage));
        stage_id
    }

    fn get_final_query_stage(&self) -> Arc<QueryStage> {
        // the final query stage is always the last to be created and
        // therefore has the highest id
        let mut max_id = 0;
        for (k, _) in &self.query_stages {
            if *k > max_id {
                max_id = *k;
            }
        }
        self.query_stages.get(&max_id).unwrap().clone()
    }

    fn next_id(&self) -> usize {
        self.id_generator.fetch_add(1, Ordering::Relaxed)
    }
}

#[pyclass(name = "QueryStage", module = "raysql", subclass)]
pub struct PyQueryStage {
    stage: Arc<QueryStage>,
}

impl PyQueryStage {
    pub fn new(stage: Arc<QueryStage>) -> Self {
        Self { stage }
    }
}

#[pymethods]
impl PyQueryStage {
    pub fn get_execution_plan(&self) -> PyExecutionPlan {
        PyExecutionPlan::new(self.stage.plan.clone())
    }

    pub fn get_child_stage_ids(&self) -> Vec<usize> {
        self.stage.get_child_stage_ids()
    }

    pub fn get_input_partition_count(&self) -> usize {
        self.stage.get_input_partition_count()
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

pub fn make_execution_graph(plan: Arc<dyn ExecutionPlan>) -> Result<ExecutionGraph> {
    let mut graph = ExecutionGraph::new();
    let root = generate_query_stages(plan, &mut graph)?;
    graph.add_query_stage(graph.next_id(), root);
    Ok(graph)
}

fn generate_query_stages(
    plan: Arc<dyn ExecutionPlan>,
    graph: &mut ExecutionGraph,
) -> Result<Arc<dyn ExecutionPlan>> {
    // recurse down first
    let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .iter()
        .map(|x| generate_query_stages(x.clone(), graph))
        .collect::<Result<Vec<_>>>()?;
    let plan = with_new_children_if_necessary(plan, new_children)?;

    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        match repart.partitioning() {
            &Partitioning::RoundRobinBatch(_) => {
                // DataFusion adds round-robin partitioning to increase parallelism
                // but that doesn't make so much sense for distributed because it
                // introduces unnecessary shuffle overhead
                Ok(plan.children()[0].clone())
            }
            &Partitioning::Hash(_, _) => {
                // create a shuffle query stage for this repartition
                let stage_id = graph.next_id();
                let shuffle_writer = ShuffleWriterExec::new(stage_id, plan.clone());
                let stage_id = graph.add_query_stage(stage_id, Arc::new(shuffle_writer));
                // replace the plan with a shuffle reader
                Ok(Arc::new(ShuffleReaderExec::new(stage_id, plan.schema())))
            }
            &Partitioning::UnknownPartitioning(_) => todo!(),
        }
    } else {
        Ok(plan)
    }
}
