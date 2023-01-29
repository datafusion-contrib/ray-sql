use crate::protobuf::ray_sql_exec_node::PlanType;
use crate::protobuf::{RaySqlExecNode, ShuffleReaderExecNode, ShuffleWriterExecNode};
use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
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
        registry: &dyn FunctionRegistry,
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
                let plan = writer.plan.unwrap().try_into_physical_plan(
                    registry,
                    &RuntimeEnv::default(),
                    self,
                )?;
                let hash_part = parse_protobuf_hash_partitioning(
                    writer.partitioning.as_ref(),
                    registry,
                    plan.schema().as_ref(),
                )?;
                match hash_part {
                    Some(Partitioning::Hash(expr, count)) => Ok(Arc::new(ShuffleWriterExec::new(
                        writer.stage_id as usize,
                        plan,
                        expr,
                        count as usize,
                    ))),
                    _ => todo!(),
                }
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
                schema: Some(schema),
                num_output_partitions: reader.output_partitioning().partition_count() as u32,
                shuffle_dir: "/tmp/raysql".to_string(), // TODO remove hard-coded path
            };
            PlanType::ShuffleReader(reader)
        } else if let Some(writer) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            let plan = PhysicalPlanNode::try_from_physical_plan(writer.plan.clone(), self)?;
            match writer.output_partitioning() {
                Partitioning::Hash(expr, partition_count) => {
                    let partitioning = protobuf::PhysicalHashRepartition {
                        hash_expr: expr
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, DataFusionError>>()?,
                        partition_count: partition_count as u64,
                    };
                    let writer = ShuffleWriterExecNode {
                        stage_id: writer.stage_id as u32,
                        plan: Some(plan),
                        partitioning: Some(partitioning),
                        shuffle_dir: "/tmp/raysql".to_string(), // TODO remove hard-coded path
                    };
                    PlanType::ShuffleWriter(writer)
                }
                _ => todo!(),
            }
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
