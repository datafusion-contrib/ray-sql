use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::error::Result;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use datafusion_python::physical_plan::PyExecutionPlan;
use log::debug;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

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

impl Default for ExecutionGraph {
    fn default() -> Self {
        Self::new()
    }
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
        for k in self.query_stages.keys() {
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

pub fn make_execution_graph(plan: Arc<dyn ExecutionPlan>) -> Result<ExecutionGraph> {
    let mut graph = ExecutionGraph::new();
    let root = generate_query_stages(plan, &mut graph)?;
    graph.add_query_stage(graph.next_id(), root);
    Ok(graph)
}

/// Convert a physical query plan into a distributed physical query plan by breaking the query
/// into query stages based on changes in partitioning.
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
            &Partitioning::UnknownPartitioning(_) | &Partitioning::RoundRobinBatch(_) => {
                // just remove these
                Ok(repart.children()[0].clone())
            }
            partitioning_scheme => {
                create_shuffle_exchange(plan.as_ref(), graph, partitioning_scheme.clone())
            }
        }
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
    {
        create_shuffle_exchange(plan.as_ref(), graph, Partitioning::UnknownPartitioning(1))
    } else if plan
        .as_any()
        .downcast_ref::<SortPreservingMergeExec>()
        .is_some()
    {
        create_shuffle_exchange(plan.as_ref(), graph, Partitioning::UnknownPartitioning(1))
    } else {
        Ok(plan)
    }
}

/// Create a shuffle exchange.
///
/// The plan is wrapped in a ShuffleWriteExec and added as a new query plan in the execution graph
/// and a ShuffleReaderExec is returned to replace the plan.
fn create_shuffle_exchange(
    plan: &dyn ExecutionPlan,
    graph: &mut ExecutionGraph,
    partitioning_scheme: Partitioning,
) -> Result<Arc<dyn ExecutionPlan>> {
    // introduce shuffle to produce one output partition
    let stage_id = graph.next_id();

    // create temp dir for stage shuffle files
    let temp_dir = create_temp_dir(stage_id)?;

    let shuffle_writer_input = plan.children()[0].clone();
    let shuffle_writer = ShuffleWriterExec::new(
        stage_id,
        shuffle_writer_input,
        partitioning_scheme.clone(),
        &temp_dir,
    );
    let stage_id = graph.add_query_stage(stage_id, Arc::new(shuffle_writer));
    // replace the plan with a shuffle reader
    Ok(Arc::new(ShuffleReaderExec::new(
        stage_id,
        plan.schema(),
        partitioning_scheme,
        &temp_dir,
    )))
}

fn create_temp_dir(stage_id: usize) -> Result<String> {
    let uuid = Uuid::new_v4();
    let temp_dir = format!("/tmp/ray-sql-{uuid}-stage-{stage_id}");
    debug!("Creating temp shuffle dir: {temp_dir}");
    std::fs::create_dir(&temp_dir)?;
    Ok(temp_dir)
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use std::fs;
    use std::path::Path;

    #[tokio::test]
    async fn test_q1() -> Result<()> {
        do_test(1).await
    }

    #[tokio::test]
    async fn test_q2() -> Result<()> {
        do_test(2).await
    }

    #[tokio::test]
    async fn test_q3() -> Result<()> {
        do_test(3).await
    }

    #[tokio::test]
    async fn test_q4() -> Result<()> {
        do_test(4).await
    }

    #[tokio::test]
    async fn test_q5() -> Result<()> {
        do_test(5).await
    }

    #[tokio::test]
    async fn test_q6() -> Result<()> {
        do_test(6).await
    }

    #[tokio::test]
    async fn test_q7() -> Result<()> {
        do_test(7).await
    }

    #[tokio::test]
    async fn test_q8() -> Result<()> {
        do_test(8).await
    }

    #[tokio::test]
    async fn test_q9() -> Result<()> {
        do_test(9).await
    }

    #[tokio::test]
    async fn test_q10() -> Result<()> {
        do_test(10).await
    }

    #[tokio::test]
    async fn test_q11() -> Result<()> {
        do_test(11).await
    }

    #[tokio::test]
    async fn test_q12() -> Result<()> {
        do_test(12).await
    }

    #[tokio::test]
    async fn test_q13() -> Result<()> {
        do_test(13).await
    }

    #[tokio::test]
    async fn test_q14() -> Result<()> {
        do_test(14).await
    }

    #[ignore]
    #[tokio::test]
    async fn test_q15() -> Result<()> {
        do_test(15).await
    }

    #[tokio::test]
    async fn test_q16() -> Result<()> {
        do_test(16).await
    }

    #[tokio::test]
    async fn test_q17() -> Result<()> {
        do_test(17).await
    }

    #[tokio::test]
    async fn test_q18() -> Result<()> {
        do_test(18).await
    }

    #[tokio::test]
    async fn test_q19() -> Result<()> {
        do_test(19).await
    }

    #[tokio::test]
    async fn test_q20() -> Result<()> {
        do_test(20).await
    }

    #[tokio::test]
    async fn test_q21() -> Result<()> {
        do_test(21).await
    }

    #[tokio::test]
    async fn test_q22() -> Result<()> {
        do_test(22).await
    }

    async fn do_test(n: u8) -> Result<()> {
        let data_path = "/mnt/bigdata/tpch/sf10-parquet";
        if !Path::new(&data_path).exists() {
            return Ok(());
        }
        let file = format!("testdata/queries/q{n}.sql");
        let sql = fs::read_to_string(&file)?;
        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::with_config(config);
        let tables = &[
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];
        for table in tables {
            ctx.register_parquet(
                table,
                &format!("{data_path}/{table}.parquet"),
                ParquetReadOptions::default(),
            )
            .await?;
        }
        let mut output = String::new();

        let df = ctx.sql(&sql).await?;

        let plan = df.clone().into_optimized_plan()?;
        output.push_str(&format!(
            "DataFusion Logical Plan\n=======================\n\n{}\n\n",
            plan.display_indent()
        ));

        let plan = df.create_physical_plan().await?;
        output.push_str(&format!(
            "DataFusion Physical Plan\n========================\n\n{}\n",
            displayable(plan.as_ref()).indent()
        ));

        output.push_str("RaySQL Plan\n===========\n\n");
        let graph = make_execution_graph(plan)?;
        for id in 0..=graph.get_final_query_stage().id {
            let query_stage = graph.query_stages.get(&id).unwrap();
            output.push_str(&format!(
                "Query Stage #{id}:\n{}\n",
                displayable(query_stage.plan.as_ref()).indent()
            ));
        }
        let expected_file = format!("testdata/expected-plans/q{n}.txt");
        if !Path::new(&expected_file).exists() {
            fs::write(&expected_file, &output)?;
        }
        let expected_plan = fs::read_to_string(&expected_file)?;
        assert_eq!(expected_plan, output);
        Ok(())
    }
}
