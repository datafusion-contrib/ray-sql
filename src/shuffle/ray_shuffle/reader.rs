use crate::shuffle::ray_shuffle::CombinedRecordBatchStream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream
};
use futures::Stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

type PartitionId = usize;
type StageId = usize;

#[derive(Debug)]
pub struct RayShuffleReaderExec {
    /// Query stage to read from
    pub stage_id: StageId,
    /// The output schema of the query stage being read from
    schema: SchemaRef,
    /// Input streams from Ray object store
    input_partitions_map: RwLock<HashMap<PartitionId, Vec<RecordBatch>>>, // TODO(@lsf) can we not use Rwlock?

    properties: PlanProperties,
}

impl RayShuffleReaderExec {
    pub fn new(stage_id: StageId, schema: SchemaRef, partitioning: Partitioning) -> Self {
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

        let properties = PlanProperties::new(EquivalenceProperties::new(schema.clone()), partitioning, datafusion::physical_plan::ExecutionMode::Unbounded);

        Self {
            stage_id,
            schema,
            properties,
            input_partitions_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_input_partition(
        &self,
        partition: PartitionId,
        input_batch: RecordBatch,
    ) -> Result<(), DataFusionError> {
        let mut map = self.input_partitions_map.write().unwrap();
        let input_partitions = map.entry(partition).or_insert(vec![]);
        input_partitions.push(input_batch);
        Ok(())
    }
}

impl ExecutionPlan for RayShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        let mut map = self.input_partitions_map.write().expect("got lock");
        let input_objects = map.remove(&partition).unwrap_or(vec![]);
        println!(
            "RayShuffleReaderExec[stage={}].execute(input_partition={partition}) with {} shuffle inputs",
            self.stage_id,
            input_objects.len(),
        );
        let mut streams = vec![];
        for input in input_objects {
            streams.push(
                Box::pin(InMemoryShuffleStream::try_new(input)?) as SendableRecordBatchStream
            );
        }
        Ok(Box::pin(CombinedRecordBatchStream::new(
            self.schema.clone(),
            streams,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
    
    fn name(&self) -> &str {
        "ray suffle reader"
    }
    
    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }
}

impl DisplayAs for RayShuffleReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayShuffleReaderExec(stage_id={}, input_partitioning={:?})",
            self.stage_id, self.properties().partitioning
        )
    }
}

struct InMemoryShuffleStream {
    batch: Arc<RecordBatch>,
    read: bool,
}

impl InMemoryShuffleStream {
    fn try_new(batch: RecordBatch) -> Result<Self, DataFusionError> {
        Ok(Self {
            batch: Arc::new(batch),
            read: false,
        })
    }
}

impl Stream for InMemoryShuffleStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.read {
            None
        } else {
            self.read = true;
            Some(Ok(self.batch.as_ref().clone()))
        })
    }
}

impl RecordBatchStream for InMemoryShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}
