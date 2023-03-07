use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
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
        self.plan.execute(input_partition, context)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
