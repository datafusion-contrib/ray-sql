#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleReaderExecNode {
    ///   repeated PartitionId partition = 1;
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
}
///
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleWriterExecNode {
    ///   Partitioning output_partitioning = 2;
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
}
