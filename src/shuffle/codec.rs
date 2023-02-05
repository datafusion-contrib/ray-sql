use crate::protobuf::ray_sql_exec_node::PlanType;
use crate::protobuf::{RaySqlExecNode, ShuffleReaderExecNode, ShuffleWriterExecNode};
use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::{PhysicalHashRepartition, PhysicalPlanNode};
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
                let schema: SchemaRef = Arc::new(schema.try_into().unwrap());
                let hash_part = parse_protobuf_hash_partitioning(
                    reader.partitioning.as_ref(),
                    registry,
                    &schema,
                )?;
                Ok(Arc::new(ShuffleReaderExec::new(
                    reader.stage_id as usize,
                    schema,
                    hash_part.unwrap(),
                    &reader.shuffle_dir,
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
                Ok(Arc::new(ShuffleWriterExec::new(
                    writer.stage_id as usize,
                    plan,
                    hash_part.unwrap(),
                    &writer.shuffle_dir,
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
            let partitioning = encode_partitioning_scheme(&reader.output_partitioning())?;
            let reader = ShuffleReaderExecNode {
                stage_id: reader.stage_id as u32,
                schema: Some(schema),
                partitioning: Some(partitioning),
                shuffle_dir: reader.shuffle_dir.clone(),
            };
            PlanType::ShuffleReader(reader)
        } else if let Some(writer) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            let plan = PhysicalPlanNode::try_from_physical_plan(writer.plan.clone(), self)?;
            let partitioning = encode_partitioning_scheme(&writer.output_partitioning())?;
            let writer = ShuffleWriterExecNode {
                stage_id: writer.stage_id as u32,
                plan: Some(plan),
                partitioning: Some(partitioning),
                shuffle_dir: writer.shuffle_dir.clone(),
            };
            PlanType::ShuffleWriter(writer)
        } else {
            unreachable!()
        };
        plan.encode(buf);
        Ok(())
    }
}

fn encode_partitioning_scheme(partitioning: &Partitioning) -> Result<PhysicalHashRepartition> {
    match partitioning {
        Partitioning::Hash(expr, partition_count) => Ok(protobuf::PhysicalHashRepartition {
            hash_expr: expr
                .iter()
                .map(|expr| expr.clone().try_into())
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            partition_count: *partition_count as u64,
        }),
        Partitioning::UnknownPartitioning(n) => Ok(protobuf::PhysicalHashRepartition {
            hash_expr: vec![],
            partition_count: *n as u64,
        }),
        other => Err(DataFusionError::Plan(format!(
            "Unsupported shuffle partitioning scheme: {other:?}"
        ))),
    }
}

struct RaySqlFunctionRegistry {}

impl FunctionRegistry for RaySqlFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> datafusion::common::Result<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(format!("Invalid UDF: {name}")))
    }

    fn udaf(&self, name: &str) -> datafusion::common::Result<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(format!("Invalid UDAF: {name}")))
    }
}
