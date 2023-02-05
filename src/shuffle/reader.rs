use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::union::CombinedRecordBatchStream;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use glob::glob;
use log::debug;
use std::any::Any;
use std::fmt::Formatter;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct ShuffleReaderExec {
    /// Query stage to read from
    pub stage_id: usize,
    /// The output schema of the query stage being read from
    schema: SchemaRef,
    /// Output partitioning
    partitioning: Partitioning,
    /// Directory to read shuffle files from
    pub shuffle_dir: String,
}

impl ShuffleReaderExec {
    pub fn new(
        stage_id: usize,
        schema: SchemaRef,
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
            schema,
            partitioning,
            shuffle_dir: shuffle_dir.to_string(),
        }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // TODO could be implemented in some cases
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let pattern = format!(
            "/{}/shuffle_{}_*_{partition}.arrow",
            self.shuffle_dir, self.stage_id
        );
        let mut streams: Vec<SendableRecordBatchStream> = vec![];
        for entry in glob(&pattern).expect("Failed to read glob pattern") {
            let file = entry.unwrap();
            debug!(
                "ShuffleReaderExec partition {} reading from stage {} file {}",
                partition,
                self.stage_id,
                file.display()
            );
            let reader = FileReader::try_new(File::open(&file)?, None)?;
            let stream = LocalShuffleStream::new(reader);
            if self.schema != stream.schema() {
                return Err(DataFusionError::Internal(
                    "Not all shuffle files have the same schema".to_string(),
                ));
            }
            streams.push(Box::pin(stream));
        }
        Ok(Box::pin(CombinedRecordBatchStream::new(
            self.schema.clone(),
            streams,
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReaderExec(stage_id={}, input_partitioning={:?})",
            self.stage_id, self.partitioning
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct LocalShuffleStream {
    reader: FileReader<File>,
}

impl LocalShuffleStream {
    pub fn new(reader: FileReader<File>) -> Self {
        LocalShuffleStream { reader }
    }
}

impl Stream for LocalShuffleStream {
    type Item = datafusion::arrow::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.reader.next() {
            return Poll::Ready(Some(batch));
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for LocalShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}
