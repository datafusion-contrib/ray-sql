import ray

import raysql
from raysql import Context, QueryStage, ResultSet, ray_utils


@ray.remote
def execute_query_stage(
    query_stages: list[QueryStage],
    stage_id: int,
    num_workers: int,
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
            execute_query_stage.options(**ray_utils.current_node_aff()).remote(
                query_stages, child_id, num_workers, use_ray_shuffle
            )
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

    def _get_worker_inputs(part: int) -> list[tuple[int, int, int, ray.ObjectRef]]:
        ret = []
        if not use_ray_shuffle:
            return []
        for child_stage_id, child_futures in child_outputs:
            for i, lst in enumerate(child_futures):
                if isinstance(lst, list):
                    for j, f in enumerate(lst):
                        if concurrency == 1 or j == part:
                            # If concurrency is 1, pass in all shuffle partitions. Otherwise,
                            # only pass in the partitions that match the current worker partition.
                            ret.append((child_stage_id, i, j, f))
                elif concurrency == 1 or part == 0:
                    ret.append((child_stage_id, i, 0, lst))
        return ret

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
        futures.append(
            execute_query_partition.options(**opt).remote(
                plan_bytes, part, _get_worker_inputs(part)
            )
        )

    return stage_id, futures


@ray.remote
def execute_query_partition(
    plan_bytes: bytes,
    part: int,
    input_partition_refs: list[tuple[int, int, int, ray.ObjectRef]],
) -> list[bytes]:
    plan = raysql.deserialize_execution_plan(plan_bytes)
    print(
        "Worker executing plan {} partition #{} with shuffle inputs {}".format(
            plan.display(),
            part,
            [(s, i, j) for s, i, j, _ in input_partition_refs],
        )
    )

    input_data = ray.get([f for _, _, _, f in input_partition_refs])
    input_partitions = [
        (s, j, d) for (s, _, j, _), d in zip(input_partition_refs, input_data)
    ]
    # This is delegating to DataFusion for execution, but this would be a good place
    # to plug in other execution engines by translating the plan into another engine's plan
    # (perhaps via Substrait, once DataFusion supports converting a physical plan to Substrait)
    result_set = raysql.execute_partition(plan, part, input_partitions)

    ret = result_set.tobyteslist()
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

    def sql(self, sql: str) -> ResultSet:
        # TODO we should parse sql and inspect the plan rather than
        # perform a string comparison here
        if 'create view' in sql or 'drop view' in sql:
            self.ctx.sql(sql)
            return raysql.empty_result_set()

        graph = self.ctx.plan(sql)
        final_stage_id = graph.get_final_query_stage().id()

        # serialize the query stages and store in Ray object store
        query_stages = [
            raysql.serialize_execution_plan(
                graph.get_query_stage(i).get_execution_plan()
            )
            for i in range(final_stage_id + 1)
        ]

        # schedule execution
        future = execute_query_stage.options(**ray_utils.current_node_aff()).remote(
            query_stages, final_stage_id, self.num_workers, self.use_ray_shuffle
        )
        _, partitions = ray.get(future)
        # final stage should have a concurrency of 1
        assert len(partitions) == 1, partitions
        result_set = ray.get(partitions[0])
        return result_set
