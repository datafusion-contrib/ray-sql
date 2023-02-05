# RaySQL Design Documentation

RaySQL is a distributed SQL query engine that is powered by DataFusion.

DataFusion provides a high-performance query engine that is already partition-aware, with partitions being executed
in parallel in separate threads. RaySQL provides a distributed query planner that translates a DataFusion physical
plan into a distributed plan.

Let's walk through an example to see how that works. We'll use [SQLBench-H](https://github.com/sql-benchmarks/sqlbench-h)
query 3 for the example. This is an aggregate query with a three-way join.

_SQLBench-H Query 3_

```sql
-- SQLBench-H query 3 derived from TPC-H query 3 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'HOUSEHOLD'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-21'
	and l_shipdate > date '1995-03-21'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate limit 10;
```

## DataFusion's Logical Plan

DataFusion produces the following optimized _logical_ query plan. Note that this plan does not have the
concept of partitions yet.

```text
Limit: skip=0, fetch=10
  Sort: revenue DESC NULLS FIRST, orders.o_orderdate ASC NULLS LAST, fetch=10
    Projection: lineitem.l_orderkey, SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS revenue, orders.o_orderdate, orders.o_shippriority
      Aggregate: groupBy=[[lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority]], aggr=[[SUM(CAST(lineitem.l_extendedprice AS Decimal128(35, 4)) * CAST(Decimal128(Some(100),23,2) - CAST(lineitem.l_discount AS Decimal128(23, 2)) AS Decimal128(35, 4))) AS SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]]
        Inner Join: orders.o_orderkey = lineitem.l_orderkey
          Inner Join: customer.c_custkey = orders.o_custkey
            Filter: customer.c_mktsegment = Utf8("BUILDING")
              TableScan: customer projection=[c_custkey, c_mktsegment], partial_filters=[customer.c_mktsegment = Utf8("BUILDING")]
            Filter: orders.o_orderdate < Date32("9204")
              TableScan: orders projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], partial_filters=[orders.o_orderdate < Date32("9204")]
          Filter: lineitem.l_shipdate > Date32("9204")
            TableScan: lineitem projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], partial_filters=[lineitem.l_shipdate > Date32("9204")]
```

## DataFusion's Physical Plan

DataFusion's physical plan lists all the files to be queried, and they are organized into partitions to allow for
parallel execution within a single process. In this example, the level of concurrency was configured to be four, so
we see `partitions={4 groups: [[ ... ]]` in the leaf `ParquetExec` nodes, with the filenames listed in four groups.

_DataFusion will soon support parallel execution for single Parquet files but for now the parallelism is based on
splitting the available files into separate groups, so RaySQL will not yet scale well for single-file inputs._

Here is the full physical plan for query 3.

```text
GlobalLimitExec: skip=0, fetch=10
  SortPreservingMergeExec: [revenue@1 DESC,o_orderdate@2 ASC NULLS LAST]
    SortExec: [revenue@1 DESC,o_orderdate@2 ASC NULLS LAST]
      ProjectionExec: expr=[l_orderkey@0 as l_orderkey, SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@3 as revenue, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority]
        AggregateExec: mode=FinalPartitioned, gby=[l_orderkey@0 as l_orderkey, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority], aggr=[SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([Column { name: "l_orderkey", index: 0 }, Column { name: "o_orderdate", index: 1 }, Column { name: "o_shippriority", index: 2 }], 4), input_partitions=4
              AggregateExec: mode=Partial, gby=[l_orderkey@6 as l_orderkey, o_orderdate@4 as o_orderdate, o_shippriority@5 as o_shippriority], aggr=[SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                CoalesceBatchesExec: target_batch_size=8192
                  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "o_orderkey", index: 2 }, Column { name: "l_orderkey", index: 0 })]
                    CoalesceBatchesExec: target_batch_size=8192
                      RepartitionExec: partitioning=Hash([Column { name: "o_orderkey", index: 2 }], 4), input_partitions=4
                        CoalesceBatchesExec: target_batch_size=8192
                          HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "c_custkey", index: 0 }, Column { name: "o_custkey", index: 1 })]
                            CoalesceBatchesExec: target_batch_size=8192
                              RepartitionExec: partitioning=Hash([Column { name: "c_custkey", index: 0 }], 4), input_partitions=4
                                CoalesceBatchesExec: target_batch_size=8192
                                  FilterExec: c_mktsegment@1 = BUILDING
                                    ParquetExec: limit=None, partitions={4 groups: [[mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-5.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-10.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-11.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-13.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-6.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-14.parquet], [mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-20.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-2.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-22.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-19.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-0.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-16.parquet], [mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-21.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-23.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-4.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-17.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-9.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-1.parquet], [mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-18.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-8.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-12.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-15.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-7.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-3.parquet]]}, predicate=c_mktsegment = Utf8("BUILDING"), pruning_predicate=c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, projection=[c_custkey, c_mktsegment]
                            CoalesceBatchesExec: target_batch_size=8192
                              RepartitionExec: partitioning=Hash([Column { name: "o_custkey", index: 1 }], 4), input_partitions=4
                                CoalesceBatchesExec: target_batch_size=8192
                                  FilterExec: o_orderdate@2 < 9204
                                    ParquetExec: limit=None, partitions={4 groups: [[mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-5.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-10.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-11.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-13.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-6.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-14.parquet], [mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-20.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-2.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-22.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-19.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-0.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-16.parquet], [mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-21.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-23.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-4.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-17.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-9.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-1.parquet], [mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-18.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-8.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-12.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-15.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-7.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-3.parquet]]}, predicate=o_orderdate < Date32("9204"), pruning_predicate=o_orderdate_min@0 < 9204, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority]
                    CoalesceBatchesExec: target_batch_size=8192
                      RepartitionExec: partitioning=Hash([Column { name: "l_orderkey", index: 0 }], 4), input_partitions=4
                        CoalesceBatchesExec: target_batch_size=8192
                          FilterExec: l_shipdate@3 > 9204
                            ParquetExec: limit=None, partitions={4 groups: [[mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-5.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-10.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-11.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-13.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-6.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-14.parquet], [mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-20.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-2.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-22.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-19.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-0.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-16.parquet], [mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-21.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-23.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-4.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-17.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-9.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-1.parquet], [mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-18.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-8.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-12.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-15.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-7.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-3.parquet]]}, predicate=l_shipdate > Date32("9204"), pruning_predicate=l_shipdate_max@0 > 9204, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate]
```

## Partitioning & Distribution

The partitioning scheme changes throughout the plan and this is the most important concept to
understand in order to understand RaySQL's design. Changes in partitioning are implemented by the `RepartitionExec`
operator in DataFusion and are happen in the following scenarios.

### Joins

The first join to happen is between `customer` and `orders`. The join condition is
`customers.c_custkey = orders.o_custkey`. To perform this join in parallel we first
need to repartition the data on the join keys so that the data for customers with `c_custkey = 1`
and the data for orders with `o_custkey = 1` can be sent to the same thread or node for
processing. This allows the join to happen in parallel. This is known as a hash-partitioned
join and is implemented by the `HashJoinExec` operator in DataFusion.

We can see that `DataFusion` has inserted `RepartitionExec` operators around both inputs to the join.

### Aggregates

There are multiple approaches to distributed aggregate queries. Here are two popular approaches:

- Perform aggregates in parallel on each partition, where the resulting data for each partition could contain
  duplicate grouping keys and then perform a final aggregate on the intermediate aggregates to remove the duplicates.
- Partition the input data by the grouping keys so that the aggregates from each partition can simply be merged to
  produce the final result.

For this example query, the data is already partitioned on the aggregate's grouping key so the latter approach is used.

### Sort

Sort also has multiple approaches.

- The input partitions can be collapsed down to a single partition and then sorted
- Partitions can be sorted in parallel and then merged using a sort-preserving merge

DataFusion and RaySQL currently the first approach, but there is a DataFusion PR open for implementing the second.

### Limit

- The input partitions can be collapsed down to a single partition and then have the limit applied
- The limit can be pushed down to each partition as well

## Query Stages

The first two query stages to be executed will read the `customer` and `order` parquet files and reparition them by the join keys `c_custkey` and `o_custkey`.

```text
Query Stage #0:
ShuffleWriterExec(stage_id=0, output_partitioning=Hash([Column { name: "c_custkey", index: 0 }], 4))
  CoalesceBatchesExec: target_batch_size=8192
    FilterExec: c_mktsegment@1 = BUILDING
      ParquetExec: limit=None, partitions={4 groups: [[mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-5.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-10.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-11.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-13.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-6.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-14.parquet], [mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-20.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-2.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-22.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-19.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-0.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-16.parquet], [mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-21.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-23.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-4.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-17.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-9.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-1.parquet], [mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-18.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-8.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-12.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-15.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-7.parquet, mnt/bigdata/tpch/sf10-parquet/customer.parquet/part-3.parquet]]}, predicate=c_mktsegment = Utf8("BUILDING"), pruning_predicate=c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, projection=[c_custkey, c_mktsegment]

Query Stage #1:
ShuffleWriterExec(stage_id=1, output_partitioning=Hash([Column { name: "o_custkey", index: 1 }], 4))
  CoalesceBatchesExec: target_batch_size=8192
    FilterExec: o_orderdate@2 < 9204
      ParquetExec: limit=None, partitions={4 groups: [[mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-5.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-10.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-11.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-13.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-6.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-14.parquet], [mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-20.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-2.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-22.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-19.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-0.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-16.parquet], [mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-21.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-23.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-4.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-17.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-9.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-1.parquet], [mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-18.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-8.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-12.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-15.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-7.parquet, mnt/bigdata/tpch/sf10-parquet/orders.parquet/part-3.parquet]]}, predicate=o_orderdate < Date32("9204"), pruning_predicate=o_orderdate_min@0 < 9204, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority]
```

Now that these inputs are partitioned by join key, we can execute the join itself. The two inputs to the join are instances of `ShuffleReaderExec` that
read the shuffle files produced by the previous stages. The result of the join is repartitioned by `o_order_key` in preparation for the next join.

```text
Query Stage #2:
ShuffleWriterExec(stage_id=2, output_partitioning=Hash([Column { name: "o_orderkey", index: 2 }], 4))
  CoalesceBatchesExec: target_batch_size=8192
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "c_custkey", index: 0 }, Column { name: "o_custkey", index: 1 })]
      CoalesceBatchesExec: target_batch_size=8192
        ShuffleReaderExec(stage_id=0, input_partitions=4)
      CoalesceBatchesExec: target_batch_size=8192
        ShuffleReaderExec(stage_id=1, input_partitions=4)
```

We continue to prepare for the next join with `lineitem` by repartitioning the parquet files by the join key.

```text
Query Stage #3:
ShuffleWriterExec(stage_id=3, output_partitioning=Hash([Column { name: "l_orderkey", index: 0 }], 4))
  CoalesceBatchesExec: target_batch_size=8192
    FilterExec: l_shipdate@3 > 9204
      ParquetExec: limit=None, partitions={4 groups: [[mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-5.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-10.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-11.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-13.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-6.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-14.parquet], [mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-20.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-2.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-22.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-19.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-0.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-16.parquet], [mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-21.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-23.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-4.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-17.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-9.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-1.parquet], [mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-18.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-8.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-12.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-15.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-7.parquet, mnt/bigdata/tpch/sf10-parquet/lineitem.parquet/part-3.parquet]]}, predicate=l_shipdate > Date32("9204"), pruning_predicate=l_shipdate_max@0 > 9204, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate]
```

With the inputs repartitioned by the join keys, we can execute a query stage to perform the join. However, in this case, we also perform an aggregate in the
same stage without any additional repartitioning. This is possible because the aggregate grouping includes `l_orderkey` which we already partitioned on. This
means that we can perform this aggregate in parallel and guarantee that there will be no duplicates in the output of each aggregate. Pretty neat!

The output of this shuffle is partitioned by `l_orderkey`, `o_orderdate`, and `o_shippriority` in preparation for the `ORDER BY` part of the query.

```text
Query Stage #4:
ShuffleWriterExec(stage_id=4, output_partitioning=Hash([Column { name: "l_orderkey", index: 0 }, Column { name: "o_orderdate", index: 1 }, Column { name: "o_shippriority", index: 2 }], 4))
  AggregateExec: mode=Partial, gby=[l_orderkey@6 as l_orderkey, o_orderdate@4 as o_orderdate, o_shippriority@5 as o_shippriority], aggr=[SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
    CoalesceBatchesExec: target_batch_size=8192
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "o_orderkey", index: 2 }, Column { name: "l_orderkey", index: 0 })]
        CoalesceBatchesExec: target_batch_size=8192
          ShuffleReaderExec(stage_id=2, input_partitions=4)
        CoalesceBatchesExec: target_batch_size=8192
          ShuffleReaderExec(stage_id=3, input_partitions=4)
```

Now we perform a final aggregate (which is maybe redundant?) and then sort the results in parallel across the
partitions.

```text
Query Stage #5:
ShuffleWriterExec(stage_id=5, output_partitioning=Hash([Column { name: "l_orderkey", index: 0 }, Column { name: "o_orderdate", index: 2 }, Column { name: "o_shippriority", index: 3 }], 4))
  SortExec: [revenue@1 DESC,o_orderdate@2 ASC NULLS LAST]
    ProjectionExec: expr=[l_orderkey@0 as l_orderkey, SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@3 as revenue, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority]
      AggregateExec: mode=FinalPartitioned, gby=[l_orderkey@0 as l_orderkey, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority], aggr=[SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
        CoalesceBatchesExec: target_batch_size=8192
          ShuffleReaderExec(stage_id=4, input_partitions=4)
```

Finally, we have a query stage that reads the sorted results and merges them into a single partition (preserving the
sort order) and applies the `LIMIT` clause.

```text
Query Stage #6:
GlobalLimitExec: skip=0, fetch=10
  SortPreservingMergeExec: [revenue@1 DESC,o_orderdate@2 ASC NULLS LAST]
    ShuffleReaderExec(stage_id=5, input_partitions=4)
```

## Distributed Scheduling

In the previous section we walked through the plan as if we were executing one query stage at a time. However, some
of these query stages could execute in parallel (assuming enough resource is available).

For example, we could execute query stage 3 to repartition the `lineitem` input without waiting for query stages 0, 1,
and 2 to finish.

More generally, we can typically execute all remaining leaf nodes of the plan concurrently.

The `execute_query_stage` method in `context.py` is a remote method that recursively walks the query plan and executes
child plans, building up a DAG of futures.

## Distributed Shuffle

The output of each query stage needs to be persisted somewhere so that the next query stage can read it. Currently,
RaySQL is just writing the output to disk in Arrow IPC format, and this means that RaySQL is not truly distributed
yet because it requires a shared file system. It would be better to use the Ray object store instead, as
proposed [here](https://github.com/andygrove/ray-sql/issues/22).

DataFusion's `RepartitionExec` uses threads and channels within a single process and is not suitable for a
distributed query engine, so RaySQL rewrites the physical plan and replaces the `RepartionExec` with a pair of
operators to perform a "shuffle". These are the `ShuffleWriterExec` and `ShuffleReaderExec`.

### Shuffle Writes

`ShuffleWriterExec` reads input partitions and repartitions them, using the same `BatchPartitioner` that DataFusion
uses, then writes the output to disk in Arrow IPC format.

### Shuffle Reads

`ShuffleReaderExec` reads the shuffle files written by the `ShuffleWriterExec`.
