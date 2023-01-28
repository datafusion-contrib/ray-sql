// use datafusion_proto::physical_plan::from_proto::*;
// use datafusion_proto::physical_plan::to_proto::*;
use crate::protobuf::ray_sql_exec_node::PlanType;
use crate::protobuf::{RaySqlExecNode, ShuffleReaderExecNode, ShuffleWriterExecNode};
use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShuffleCodec {}

impl PhysicalExtensionCodec for ShuffleCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // decode bytes to protobuf struct
        let node = RaySqlExecNode::decode(buf)
            .map_err(|e| DataFusionError::Internal(format!("failed to decode plan: {e:?}")))?;
        match node.plan_type {
            Some(PlanType::ShuffleReader(reader)) => {
                let schema = reader.schema.as_ref().unwrap();
                Ok(Arc::new(ShuffleReaderExec::new(
                    0, //TODO
                    Arc::new(schema.try_into().unwrap()),
                )))
            }
            Some(PlanType::ShuffleWriter(writer)) => {
                let function_registry = RaySqlFunctionRegistry {};
                let plan = writer.plan.unwrap().try_into_physical_plan(
                    &function_registry,
                    &RuntimeEnv::default(),
                    self,
                )?;
                Ok(Arc::new(ShuffleWriterExec::new(
                    writer.stage_id as usize,
                    plan,
                )))
            }
            _ => unreachable!(),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let plan = if let Some(reader) = node.as_any().downcast_ref::<ShuffleReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into().unwrap();
            let reader = ShuffleReaderExecNode {
                stage_id: reader.stage_id as u32,
                partition: 0,
                schema: Some(schema),
                num_output_partitions: 1,
                shuffle_dir: "/tmp/raysql-shuffle".to_string(),
            };
            PlanType::ShuffleReader(reader)
        } else if let Some(writer) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            let plan = PhysicalPlanNode::try_from_physical_plan(writer.plan.clone(), self)?;
            let writer = ShuffleWriterExecNode {
                stage_id: 0,
                plan: Some(plan),
                partition_expr: vec![],
                num_output_partitions: 1,
                shuffle_dir: "/tmp/raysql-shuffle".to_string(),
            };
            PlanType::ShuffleWriter(writer)
        } else {
            unreachable!()
        };
        plan.encode(buf);
        Ok(())
    }
}

struct RaySqlFunctionRegistry {}

impl FunctionRegistry for RaySqlFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> datafusion::common::Result<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(format!("Invalid UDF: {}", name)))
    }

    fn udaf(&self, name: &str) -> datafusion::common::Result<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(format!("Invalid UDAF: {}", name)))
    }
}
