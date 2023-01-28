use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    metrics, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PartitionStats;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShuffleWriterExec {
    pub stage_id: usize,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub metrics: ExecutionPlanMetricsSet,
}

impl ShuffleWriterExec {
    pub fn new(stage_id: usize, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            stage_id,
            plan,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleWriterExec(stage_id={})", self.stage_id)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let mut stream = self.plan.execute(partition, context)?;
        let write_time = MetricBuilder::new(&self.metrics).subset_time("write_time", partition);
        let results = async move {
            // stream the results from the query
            println!("Executing query");
            let stats = write_stream_to_disk(&mut stream, "/tmp/foo.arrow", &write_time).await?;
            println!(
                "Query completed. Shuffle write time: {}. Rows: {}.",
                write_time, stats.num_rows
            );

            // create a dummy batch to return - later this could be metadata about the
            // shuffle partitions that were written out
            let schema = Arc::new(Schema::new(vec![Field::new("foo", DataType::Int32, true)]));
            let array = Int32Array::from(vec![42]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;

            // return as a stream
            MemoryStream::try_new(vec![batch], schema, None)
        };
        let schema = self.schema().clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(results).try_flatten(),
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// Stream data to disk in Arrow IPC format
/// Copied from Ballista
pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
    let file = File::create(path).unwrap();

    /*.map_err(|e| {
        error!("Failed to create partition file at {}: {:?}", path, e);
        BallistaError::IoError(e)
    })?;*/

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;
    let mut writer = FileWriter::try_new(file, stream.schema().as_ref())?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch_byte_size(&batch);
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        let timer = disk_write_metric.timer();
        writer.write(&batch)?;
        timer.done();
    }
    let timer = disk_write_metric.timer();
    writer.finish()?;
    timer.done();
    Ok(PartitionStats {
        num_rows: num_rows as i64,
        num_batches: num_batches as i64,
        num_bytes: num_bytes as i64,
        column_stats: vec![],
    })
}
