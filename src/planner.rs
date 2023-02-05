use crate::query_stage::PyQueryStage;
use crate::query_stage::QueryStage;
use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::error::Result;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{displayable, Partitioning};
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
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
    /// Get a list of stages sorted by id
    pub fn get_query_stages(&self) -> Vec<PyQueryStage> {
        let mut stages = vec![];
        let max_id = self.graph.get_final_query_stage().id;
        for id in 0..=max_id {
            stages.push(PyQueryStage::from_rust(
                self.graph.query_stages.get(&id).unwrap().clone(),
            ));
        }
        stages
    }

    pub fn get_query_stage(&self, id: usize) -> PyResult<PyQueryStage> {
        if let Some(stage) = self.graph.query_stages.get(&id) {
            Ok(PyQueryStage::from_rust(stage.clone()))
        } else {
            todo!()
        }
    }

    pub fn get_final_query_stage(&self) -> PyQueryStage {
        PyQueryStage::from_rust(self.graph.get_final_query_stage())
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

    debug!("plan = {}", displayable(plan.as_ref()).one_line());
    debug!("output_part = {:?}", plan.output_partitioning());

    let new_plan = if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        match repart.partitioning() {
            &Partitioning::UnknownPartitioning(_) | &Partitioning::RoundRobinBatch(_) => {
                // just remove these
                Ok(repart.children()[0].clone())
            }
            partitioning_scheme => create_shuffle_exchange(
                plan.children()[0].clone(),
                graph,
                partitioning_scheme.clone(),
            ),
        }
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
    {
        let coalesce_input = plan.children()[0].clone();
        let partitioning_scheme = coalesce_input.output_partitioning();
        let new_input = create_shuffle_exchange(coalesce_input, graph, partitioning_scheme)?;
        with_new_children_if_necessary(plan, vec![new_input])
    } else if plan
        .as_any()
        .downcast_ref::<SortPreservingMergeExec>()
        .is_some()
    {
        let partitioned_sort_plan = plan.children()[0].clone();
        let partitioning_scheme = partitioned_sort_plan.output_partitioning();
        let new_input = create_shuffle_exchange(partitioned_sort_plan, graph, partitioning_scheme)?;
        with_new_children_if_necessary(plan, vec![new_input])
    } else {
        Ok(plan)
    }?;

    debug!("new_plan = {}", displayable(new_plan.as_ref()).one_line());
    debug!(
        "new_output_part = {:?}\n\n-------------------------\n\n",
        new_plan.output_partitioning()
    );

    Ok(new_plan)
}

/// Create a shuffle exchange.
///
/// The plan is wrapped in a ShuffleWriteExec and added as a new query plan in the execution graph
/// and a ShuffleReaderExec is returned to replace the plan.
fn create_shuffle_exchange(
    plan: Arc<dyn ExecutionPlan>,
    graph: &mut ExecutionGraph,
    partitioning_scheme: Partitioning,
) -> Result<Arc<dyn ExecutionPlan>> {
    // introduce shuffle to produce one output partition
    let stage_id = graph.next_id();

    // create temp dir for stage shuffle files
    let temp_dir = create_temp_dir(stage_id)?;

    let shuffle_writer_input = plan.clone();
    let shuffle_writer = ShuffleWriterExec::new(
        stage_id,
        shuffle_writer_input,
        partitioning_scheme.clone(),
        &temp_dir,
    );

    debug!(
        "Created shuffle writer with output partitioning {:?}",
        shuffle_writer.output_partitioning()
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
                "Query Stage #{id} ({} -> {}):\n{}\n",
                query_stage.get_input_partition_count(),
                query_stage.get_output_partition_count(),
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
