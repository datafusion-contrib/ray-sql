use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::union::CombinedRecordBatchStream;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use glob::glob;
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
}

impl ShuffleReaderExec {
    pub fn new(stage_id: usize, schema: SchemaRef) -> Self {
        Self { stage_id, schema }
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
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        // TODO remove hard-coded path
        let pattern = format!("/tmp/raysql/shuffle_{}_*_{partition}.arrow", self.stage_id);
        let mut streams: Vec<SendableRecordBatchStream> = vec![];
        for entry in glob(&pattern).expect("Failed to read glob pattern") {
            let file = entry.unwrap();
            println!("Shuffle reader reading from {}", file.display());
            let reader = FileReader::try_new(File::open(&file)?, None)?;
            streams.push(Box::pin(LocalShuffleStream::new(reader)));
        }
        Ok(Box::pin(CombinedRecordBatchStream::new(
            self.schema.clone(),
            streams,
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleReaderExec(stage_id={})", self.stage_id)
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
