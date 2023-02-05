use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
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
use log::debug;
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
    /// Output partitioning
    partitioning: Partitioning,
    /// Directory to write shuffle files from
    pub shuffle_dir: String,
    /// Metrics
    pub metrics: ExecutionPlanMetricsSet,
}

impl ShuffleWriterExec {
    pub fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        shuffle_dir: &str,
    ) -> Self {
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
            },
            _ => partitioning,
        };

        Self {
            stage_id,
            plan,
            partitioning,
            shuffle_dir: shuffle_dir.to_string(),
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
            "ShuffleWriterExec(stage_id={}, output_partitioning={:?})",
            self.stage_id, self.partitioning
        )
    }

    fn execute(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "ShuffleWriterExec[stage={}].execute(input_partition={input_partition})",
            self.stage_id
        );

        let mut stream = self.plan.execute(input_partition, context)?;
        let write_time =
            MetricBuilder::new(&self.metrics).subset_time("write_time", input_partition);
        let repart_time =
            MetricBuilder::new(&self.metrics).subset_time("repart_time", input_partition);

        let stage_id = self.stage_id;
        let partitioning = self.output_partitioning();
        let partition_count = partitioning.partition_count();
        let shuffle_dir = self.shuffle_dir.clone();

        let results = async move {
            match &partitioning {
                Partitioning::RoundRobinBatch(_) => {
                    unimplemented!()
                }
                Partitioning::UnknownPartitioning(_) => {
                    // stream the results from the query, preserving the input partitioning
                    let file =
                        format!("/{shuffle_dir}/shuffle_{stage_id}_{input_partition}_0.arrow");
                    debug!("Executing query and writing results to {file}");
                    let stats = write_stream_to_disk(&mut stream, &file, &write_time).await?;
                    debug!(
                        "Query completed. Shuffle write time: {}. Rows: {}.",
                        write_time, stats.num_rows
                    );
                }
                Partitioning::Hash(_, _) => {
                    // we won't necessary produce output for every possible partition, so we
                    // create writers on demand
                    let mut writers: Vec<Option<IPCWriter>> = vec![];
                    for _ in 0..partition_count {
                        writers.push(None);
                    }

                    let mut partitioner =
                        BatchPartitioner::try_new(partitioning, repart_time.clone())?;

                    let mut rows = 0;

                    while let Some(result) = stream.next().await {
                        let input_batch = result?;
                        rows += input_batch.num_rows();

                        debug!(
                            "ShuffleWriterExec[stage={}] writing batch:\n{}",
                            stage_id,
                            pretty_format_batches(&[input_batch.clone()])?
                        );

                        //write_metrics.input_rows.add(input_batch.num_rows());

                        partitioner.partition(input_batch, |output_partition, output_batch| {
                            match &mut writers[output_partition] {
                                Some(w) => {
                                    w.write(&output_batch)?;
                                }
                                None => {
                                    let path = format!(
                                        "/{shuffle_dir}/shuffle_{stage_id}_{input_partition}_{output_partition}.arrow",
                                    );
                                    let path = Path::new(&path);
                                    debug!("ShuffleWriterExec[stage={}] Writing results to {:?}", stage_id, path);

                                    let mut writer = IPCWriter::new(path, stream.schema().as_ref())?;

                                    writer.write(&output_batch)?;
                                    writers[output_partition] = Some(writer);
                                }
                            }
                            Ok(())
                        })?;
                    }

                    for (i, w) in writers.iter_mut().enumerate() {
                        match w {
                            Some(w) => {
                                w.finish()?;
                                debug!(
                                        "ShuffleWriterExec[stage={}] Finished writing shuffle partition {} at {:?}. Batches: {}. Rows: {}. Bytes: {}.",
                                        stage_id,
                                        i,
                                        w.path(),
                                        w.num_batches,
                                        w.num_rows,
                                        w.num_bytes
                                    );
                            }
                            None => {}
                        }
                    }
                    debug!(
                        "ShuffleWriterExec[stage={}] Finished processing stream with {rows} rows",
                        stage_id
                    );
                }
            }

            // create a dummy batch to return - later this could be metadata about the
            // shuffle partitions that were written out
            let schema = Arc::new(Schema::new(vec![
                Field::new("shuffle_repart_time", DataType::Int32, true),
                Field::new("shuffle_write_time", DataType::Int32, true),
            ]));
            let arr_repart_time = Int32Array::from(vec![repart_time.value() as i32]);
            let arr_write_time = Int32Array::from(vec![write_time.value() as i32]);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arr_repart_time), Arc::new(arr_write_time)],
            )?;

            // return as a stream
            MemoryStream::try_new(vec![batch], schema, None)
        };
        let schema = self.schema();
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
