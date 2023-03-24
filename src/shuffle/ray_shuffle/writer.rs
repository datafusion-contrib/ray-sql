use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
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
                    let mut writer = InMemoryWriter::new(schema.clone());
                    while let Some(result) = stream.next().await {
                        writer.write(result?)?;
                    }
                    MemoryStream::try_new(vec![writer.finish()?], schema, None)
                }
                Partitioning::Hash(_, _) => {
                    // TODO(@lsf) What happens if there are multiple RecordBatches
                    // assigned to the same writer?
                    let mut writers: Vec<InMemoryWriter> = vec![];
                    for _ in 0..partition_count {
                        writers.push(InMemoryWriter::new(schema.clone()));
                    }

                    let mut partitioner =
                        BatchPartitioner::try_new(partitioning, repart_time.clone())?;

                    let mut rows = 0;

                    while let Some(result) = stream.next().await {
                        let input_batch = result?;
                        rows += input_batch.num_rows();
                        partitioner.partition(input_batch, |output_partition, output_batch| {
                            writers[output_partition].write(output_batch)
                        })?;
                    }
                    let mut result_batches = vec![];
                    for (i, w) in writers.iter_mut().enumerate() {
                        if w.num_batches > 0 {
                            println!(
                                "RayShuffleWriterExec[stage={}] Finished writing shuffle partition {}. Batches: {}. Rows: {}. Bytes: {}.",
                                stage_id,
                                i,
                                w.num_batches,
                                w.num_rows,
                                w.num_bytes
                            );
                        }
                        result_batches.push(w.finish()?);
                    }
                    debug!(
                        "RayShuffleWriterExec[stage={}] finished processing stream with {rows} rows",
                        stage_id
                    );
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

struct InMemoryWriter {
    /// batches buffer
    batches: Vec<RecordBatch>,
    /// schema
    schema: SchemaRef,
    /// batches written
    pub num_batches: u64,
    /// rows written
    pub num_rows: u64,
    /// bytes written
    pub num_bytes: u64,
}

impl InMemoryWriter {
    fn new(schema: SchemaRef) -> Self {
        Self {
            batches: vec![],
            schema: schema,
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
        }
    }

    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.num_batches += 1;
        self.num_rows += batch.num_rows() as u64;
        self.num_bytes += batch_byte_size(&batch) as u64;
        self.batches.push(batch);
        Ok(())
    }

    fn finish(&self) -> Result<RecordBatch> {
        concat_batches(&self.schema, &self.batches).map_err(DataFusionError::ArrowError)
    }
}
