#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaySqlExecNode {
    #[prost(oneof = "ray_sql_exec_node::PlanType", tags = "1, 2")]
    pub plan_type: ::core::option::Option<ray_sql_exec_node::PlanType>,
}
/// Nested message and enum types in `RaySqlExecNode`.
pub mod ray_sql_exec_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PlanType {
        #[prost(message, tag = "1")]
        ShuffleReader(super::ShuffleReaderExecNode),
        #[prost(message, tag = "2")]
        ShuffleWriter(super::ShuffleWriterExecNode),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleReaderExecNode {
    /// stage to read from
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    /// partition to read - this is the *output* partition of the shuffle stage
    #[prost(uint32, tag = "2")]
    pub partition: u32,
    /// schema of the shuffle stage
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    /// this must match the output partitions of the writer we are reading from
    #[prost(uint32, tag = "4")]
    pub num_output_partitions: u32,
    /// directory for shuffle files
    #[prost(string, tag = "5")]
    pub shuffle_dir: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleWriterExecNode {
    /// stage that is writing the shuffle files
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    /// plan to execute
    #[prost(message, optional, tag = "2")]
    pub plan: ::core::option::Option<::datafusion_proto::protobuf::PhysicalPlanNode>,
    /// how to partition - can be empty
    #[prost(message, repeated, tag = "3")]
    pub partition_expr: ::prost::alloc::vec::Vec<::datafusion_proto::protobuf::PhysicalExprNode>,
    /// number of output partitions
    #[prost(uint32, tag = "4")]
    pub num_output_partitions: u32,
    /// directory for shuffle files
    #[prost(string, tag = "5")]
    pub shuffle_dir: ::prost::alloc::string::String,
}
