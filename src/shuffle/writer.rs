use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShuffleWriterExec {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
}

impl ShuffleWriterExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let mut stream = self.plan.execute(partition, context)?;
        let results = async move {
            // stream the results from the query
            while let Some(result) = stream.next().await {
                let input_batch = result?;
                println!("received batch with {} rows", input_batch.num_rows());

                // TODO write to disk (copy code from Ballista)
            }

            // create a dummy batch to return - later this could be metadata about the
            // shuffle partitions that were written out
            let schema = Arc::new(Schema::new(vec![Field::new("foo", DataType::Int32, true)]));
            let array = Int32Array::from(vec![42]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;

            // return as a stream
            MemoryStream::try_new(vec![batch], schema, None)
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
