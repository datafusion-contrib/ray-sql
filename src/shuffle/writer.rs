use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::common::{batch_byte_size, IPCWriter};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    metrics, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_proto::protobuf::PartitionStats;
use futures::StreamExt;
use futures::TryStreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::fs::File;
use std::path::Path;
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
        self.plan.output_partitioning()
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
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        println!("ShuffleWriteExec::execute(input_partition={input_partition})");

        let mut stream = self.plan.execute(input_partition, context)?;
        let write_time =
            MetricBuilder::new(&self.metrics).subset_time("write_time", input_partition);
        let repart_time =
            MetricBuilder::new(&self.metrics).subset_time("repart_time", input_partition);

        let stage_id = self.stage_id;
        let partitioning = self.output_partitioning();
        let partition_count = partitioning.partition_count();

        let results = async move {
            if partition_count == 1 {
                // stream the results from the query
                // TODO remove hard-coded path
                let file = format!("/tmp/raysql/stage_{}_part_0.arrow", stage_id);
                println!("Executing query and writing results to {file}");
                let stats = write_stream_to_disk(&mut stream, &file, &write_time).await?;
                println!(
                    "Query completed. Shuffle write time: {}. Rows: {}.",
                    write_time, stats.num_rows
                );
            } else {
                // we won't necessary produce output for every possible partition, so we
                // create writers on demand
                let mut writers: Vec<Option<IPCWriter>> = vec![];
                for _ in 0..partition_count {
                    writers.push(None);
                }

                let mut partitioner = BatchPartitioner::try_new(partitioning, repart_time.clone())?;

                let mut rows = 0;

                while let Some(result) = stream.next().await {
                    let input_batch = result?;
                    rows += input_batch.num_rows();

                    println!("batch: {:?}", input_batch);

                    //write_metrics.input_rows.add(input_batch.num_rows());

                    partitioner.partition(input_batch, |output_partition, output_batch| {
                        match &mut writers[output_partition] {
                            Some(w) => {
                                w.write(&output_batch)?;
                            }
                            None => {
                                // TODO remove hard-coded path
                                let path = format!(
                                    "/tmp/raysql/stage_{}_part_{}.arrow",
                                    stage_id, output_partition
                                );
                                let path = Path::new(&path);
                                println!("Writing results to {:?}", path);

                                let mut writer = IPCWriter::new(&path, stream.schema().as_ref())?;

                                writer.write(&output_batch)?;
                                writers[output_partition] = Some(writer);
                            }
                        }
                        Ok(())
                    })?;
                }

                println!("finished processing stream with {rows} rows");
            }

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
