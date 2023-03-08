use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::union::CombinedRecordBatchStream;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct RayShuffleReaderExec {
    /// Query stage to read from
    pub stage_id: usize,
    /// The output schema of the query stage being read from
    schema: SchemaRef,
    /// Output partitioning
    partitioning: Partitioning,
    /// Input streams from Ray object store
    input_objects_map: RwLock<HashMap<usize, Vec<Vec<u8>>>>, // TODO(@lsf) can we not use Rwlock?
}

impl RayShuffleReaderExec {
    pub fn new(stage_id: usize, schema: SchemaRef, partitioning: Partitioning) -> Self {
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
            schema,
            partitioning,
            input_objects_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_input_objects(&self, partition: usize, input_objects: Vec<Vec<u8>>) {
        println!(
            "RayShuffleReaderExec[stage={}].execute(input_partition={partition}) is set with {} shuffle inputs",
            self.stage_id,
            input_objects.len(),
        );
        self.input_objects_map
            .write()
            .unwrap()
            .insert(partition, input_objects);
    }
}

impl ExecutionPlan for RayShuffleReaderExec {
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
        let map = self.input_objects_map.read().unwrap();
        let input_objects = map.get(&partition).expect(
            format!(
                "input objects for stage {} partition {}",
                self.stage_id, partition
            )
            .as_str(),
        );
        println!(
            "RayShuffleReaderExec[stage={}].execute(input_partition={partition}) with {} shuffle inputs",
            self.stage_id,
            input_objects.len(),
        );
        Ok(Box::pin(CombinedRecordBatchStream::new(
            self.schema.clone(),
            input_objects
                .iter()
                .map(|input| {
                    Box::pin(InMemoryShuffleStream::new(input)) as SendableRecordBatchStream
                })
                .collect(),
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayShuffleReaderExec(stage_id={}, input_partitioning={:?})",
            self.stage_id, self.partitioning
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct InMemoryShuffleStream {
    reader: StreamReader<Cursor<Vec<u8>>>,
}

impl InMemoryShuffleStream {
    fn new(bytes: &Vec<u8>) -> Self {
        let reader = StreamReader::try_new(Cursor::new(bytes.clone()), None).unwrap();
        Self { reader }
    }
}

impl Stream for InMemoryShuffleStream {
    type Item = datafusion::arrow::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.reader.next() {
            return Poll::Ready(Some(batch));
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for InMemoryShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}
