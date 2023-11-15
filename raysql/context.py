import json
import os
import time
from typing import Iterable

import pyarrow as pa
import ray

import raysql
from raysql import Context, ExecutionGraph, QueryStage


def schedule_execution(
    graph: ExecutionGraph,
    stage_id: int,
    is_final_stage: bool,
) -> list[ray.ObjectRef]:
    stage = graph.get_query_stage(stage_id)
    # execute child stages first
    # A list of (stage ID, list of futures) for each child stage
    # Each list is a 2-D array of (input partitions, output partitions).
    child_outputs = []
    for child_id in stage.get_child_stage_ids():
        child_outputs.append((child_id, schedule_execution(graph, child_id, False)))
        # child_outputs.append((child_id, schedule_execution(graph, child_id)))

    concurrency = stage.get_input_partition_count()
    output_partitions_count = stage.get_output_partition_count()
    if is_final_stage:
        print("Forcing reduce stage concurrency from {} to 1".format(concurrency))
        concurrency = 1

    print(
        "Scheduling query stage #{} with {} input partitions and {} output partitions".format(
            stage.id(), concurrency, output_partitions_count
        )
    )

    def _get_worker_inputs(
        part: int,
    ) -> tuple[list[tuple[int, int, int]], list[ray.ObjectRef]]:
        ids = []
        futures = []
        for child_stage_id, child_futures in child_outputs:
            for i, lst in enumerate(child_futures):
                if isinstance(lst, list):
                    for j, f in enumerate(lst):
                        if concurrency == 1 or j == part:
                            # If concurrency is 1, pass in all shuffle partitions. Otherwise,
                            # only pass in the partitions that match the current worker partition.
                            ids.append((child_stage_id, i, j))
                            futures.append(f)
                elif concurrency == 1 or part == 0:
                    ids.append((child_stage_id, i, 0))
                    futures.append(lst)
        return ids, futures

    # schedule the actual execution workers
    plan_bytes = raysql.serialize_execution_plan(stage.get_execution_plan())
    futures = []
    opt = {}
    opt["resources"] = {"worker": 1e-3}
    opt["num_returns"] = output_partitions_count
    for part in range(concurrency):
        ids, inputs = _get_worker_inputs(part)
        futures.append(
            execute_query_partition.options(**opt).remote(
                stage_id, plan_bytes, part, ids, *inputs
            )
        )
    return futures


@ray.remote(num_cpus=0)
def execute_query_stage(
    query_stages: list[QueryStage],
    stage_id: int,
    use_ray_shuffle: bool,
) -> tuple[int, list[ray.ObjectRef]]:
    """
    Execute a query stage on the workers.

    Returns the stage ID, and a list of futures for the output partitions of the query stage.
    """
    stage = QueryStage(stage_id, query_stages[stage_id])

    # execute child stages first
    child_futures = []
    for child_id in stage.get_child_stage_ids():
        child_futures.append(
            execute_query_stage.remote(query_stages, child_id, use_ray_shuffle)
        )

    # if the query stage has a single output partition then we need to execute for the output
    # partition, otherwise we need to execute in parallel for each input partition
    concurrency = stage.get_input_partition_count()
    output_partitions_count = stage.get_output_partition_count()
    if output_partitions_count == 1:
        # reduce stage
        print("Forcing reduce stage concurrency from {} to 1".format(concurrency))
        concurrency = 1

    print(
        "Scheduling query stage #{} with {} input partitions and {} output partitions".format(
            stage.id(), concurrency, output_partitions_count
        )
    )

    # A list of (stage ID, list of futures) for each child stage
    # Each list is a 2-D array of (input partitions, output partitions).
    child_outputs = ray.get(child_futures)

    def _get_worker_inputs(
        part: int,
    ) -> tuple[list[tuple[int, int, int]], list[ray.ObjectRef]]:
        ids = []
        futures = []
        if use_ray_shuffle:
            for child_stage_id, child_futures in child_outputs:
                for i, lst in enumerate(child_futures):
                    if isinstance(lst, list):
                        for j, f in enumerate(lst):
                            if concurrency == 1 or j == part:
                                # If concurrency is 1, pass in all shuffle partitions. Otherwise,
                                # only pass in the partitions that match the current worker partition.
                                ids.append((child_stage_id, i, j))
                                futures.append(f)
                    elif concurrency == 1 or part == 0:
                        ids.append((child_stage_id, i, 0))
                        futures.append(lst)
        return ids, futures

    # if we are using disk-based shuffle, wait until the child stages to finish
    # writing the shuffle files to disk first.
    if not use_ray_shuffle:
        ray.get([f for _, lst in child_outputs for f in lst])

    # schedule the actual execution workers
    plan_bytes = raysql.serialize_execution_plan(stage.get_execution_plan())
    futures = []
    opt = {}
    opt["resources"] = {"worker": 1e-3}
    if use_ray_shuffle:
        opt["num_returns"] = output_partitions_count
    for part in range(concurrency):
        ids, inputs = _get_worker_inputs(part)
        futures.append(
            execute_query_partition.options(**opt).remote(
                stage_id, plan_bytes, part, ids, *inputs
            )
        )

    return stage_id, futures


@ray.remote
def execute_query_partition(
    stage_id: int,
    plan_bytes: bytes,
    part: int,
    input_partition_ids: list[tuple[int, int, int]],
    *input_partitions: list[pa.RecordBatch],
) -> Iterable[pa.RecordBatch]:
    start_time = time.time()
    plan = raysql.deserialize_execution_plan(plan_bytes)
    # print(
    #     "Worker executing plan {} partition #{} with shuffle inputs {}".format(
    #         plan.display(),
    #         part,
    #         input_partition_ids,
    #     )
    # )
    partitions = [
        (s, j, p) for (s, _, j), p in zip(input_partition_ids, input_partitions)
    ]
    # This is delegating to DataFusion for execution, but this would be a good place
    # to plug in other execution engines by translating the plan into another engine's plan
    # (perhaps via Substrait, once DataFusion supports converting a physical plan to Substrait)
    ret = raysql.execute_partition(plan, part, partitions)
    duration = time.time() - start_time
    event = {
        "cat": f"{stage_id}-{part}",
        "name": f"{stage_id}-{part}",
        "pid": ray.util.get_node_ip_address(),
        "tid": os.getpid(),
        "ts": int(start_time * 1_000_000),
        "dur": int(duration * 1_000_000),
        "ph": "X",
    }
    print(json.dumps(event), end=",")
    return ret[0] if len(ret) == 1 else ret


class RaySqlContext:
    def __init__(self, num_workers: int = 1, use_ray_shuffle: bool = False):
        self.ctx = Context(num_workers, use_ray_shuffle)
        self.num_workers = num_workers
        self.use_ray_shuffle = use_ray_shuffle

    def register_csv(self, table_name: str, path: str, has_header: bool):
        self.ctx.register_csv(table_name, path, has_header)

    def register_parquet(self, table_name: str, path: str):
        self.ctx.register_parquet(table_name, path)

    def register_data_lake(self, table_name: str, paths: List[str]):
        self.ctx.register_datalake_table(table_name, paths)

    def sql(self, sql: str) -> pa.RecordBatch:
        # TODO we should parse sql and inspect the plan rather than
        # perform a string comparison here
        sql_str = sql.lower()
        if "create view" in sql_str or "drop view" in sql_str:
            self.ctx.sql(sql)
            return []

        graph = self.ctx.plan(sql)
        final_stage_id = graph.get_final_query_stage().id()
        if self.use_ray_shuffle:
            partitions = schedule_execution(graph, final_stage_id, True)
        else:
            # serialize the query stages and store in Ray object store
            query_stages = [
                raysql.serialize_execution_plan(
                    graph.get_query_stage(i).get_execution_plan()
                )
                for i in range(final_stage_id + 1)
            ]
            # schedule execution
            future = execute_query_stage.remote(
                query_stages,
                final_stage_id,
                self.use_ray_shuffle,
            )
            _, partitions = ray.get(future)
        # assert len(partitions) == 1, len(partitions)
        result_set = ray.get(partitions[0])
        return result_set
