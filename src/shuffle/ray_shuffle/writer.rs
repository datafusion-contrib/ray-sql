use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures::StreamExt;
use futures::TryStreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct RayShuffleWriterExec {
    pub stage_id: usize,
    /// The child execution plan
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    partitioning: Partitioning,
    /// Metrics
    pub metrics: ExecutionPlanMetricsSet,
}

impl RayShuffleWriterExec {
    pub fn new(stage_id: usize, plan: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        let partitioning = match partitioning {
            Partitioning::Hash(expr, n) if expr.is_empty() => Partitioning::UnknownPartitioning(n),
            Partitioning::Hash(expr, n) => {
                // workaround for DataFusion bug https://github.com/apache/arrow-datafusion/issues/5184
                Partitioning::Hash(
                    expr.into_iter()
                        .filter(|e| e.as_any().downcast_ref::<UnKnownColumn>().is_none())
                        .collect(),
                    n,
                )
            }
            _ => partitioning,
        };

        Self {
            stage_id,
            plan,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for RayShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // TODO in the case of a single partition of a sorted plan this could be implemented
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayShuffleWriterExec(stage_id={}, output_partitioning={:?})",
            self.stage_id, self.partitioning
        )
    }
    fn execute(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        println!(
            "RayShuffleWriterExec[stage={}].execute(input_partition={input_partition})",
            self.stage_id
        );
        let mut stream = self.plan.execute(input_partition, context)?;

        let stage_id = self.stage_id;
        let partitioning = self.output_partitioning();
        let partition_count = partitioning.partition_count();
        let repart_time =
            MetricBuilder::new(&self.metrics).subset_time("repart_time", input_partition);
        let schema = Arc::new(self.schema().as_ref().clone());

        let results = async move {
            // TODO(@lsf): why can't I reference self in here?
            match &partitioning {
                Partitioning::UnknownPartitioning(_) => {
                    let mut batches: Vec<RecordBatch> = vec![];
                    while let Some(result) = stream.next().await {
                        batches.push(result?);
                    }
                    MemoryStream::try_new(
                        vec![concat_batches(&schema, &batches)?],
                        schema,
                        None,
                    )
                }
                Partitioning::Hash(_, _) => {
                    // TODO(@lsf) What happens if there are multiple RecordBatches
                    // assigned to the same writer?
                    let mut writers: Vec<Vec<RecordBatch>> = vec![];
                    for _ in 0..partition_count {
                        writers.push(vec![]);
                    }

                    let mut partitioner =
                        BatchPartitioner::try_new(partitioning, repart_time.clone())?;

                    let mut rows = 0;

                    while let Some(result) = stream.next().await {
                        let input_batch = result?;
                        rows += input_batch.num_rows();
                        partitioner.partition(input_batch, |output_partition, output_batch| {
                            println!(
                                "ShuffleWriterExec[stage={}] writing batch output (partition {})",
                                stage_id, output_partition
                            );
                            writers[output_partition].push(output_batch);
                            Ok(())
                        })?;
                    }
                    println!(
                        "RayShuffleWriterExec[stage={}] finished processing stream with {rows} rows",
                        stage_id
                    );
                    let mut result_batches = vec![];
                    for batches in &writers {
                        result_batches.push(concat_batches(&schema, batches)?);
                    }
                    MemoryStream::try_new(result_batches, schema, None)
                }
                _ => unimplemented!(),
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(results).try_flatten(),
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
