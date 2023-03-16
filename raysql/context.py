import ray

from raysql import Context, QueryStage, ResultSet, serialize_execution_plan
from raysql.worker import Worker


@ray.remote
def execute_query_stage(
    query_stages: list[QueryStage],
    stage_id: int,
    workers: list[Worker],
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
            execute_query_stage.remote(query_stages, child_id, workers, use_ray_shuffle)
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

    # round-robin allocation across workers
    plan_bytes = serialize_execution_plan(stage.get_execution_plan())
    futures = []
    for part in range(concurrency):
        worker_index = part % len(workers)
        opt = {}
        if use_ray_shuffle:
            opt["num_returns"] = output_partitions_count
        futures.append(
            workers[worker_index]
            .execute_query_partition.options(**opt)
            .remote(plan_bytes, part, _get_worker_inputs(part))
        )

    return stage_id, futures


@ray.remote
class RaySqlContext:
    def __init__(self, workers: list[Worker], use_ray_shuffle: bool):
        self.ctx = Context(len(workers), use_ray_shuffle)
        self.workers = workers
        self.use_ray_shuffle = use_ray_shuffle

    def register_csv(self, table_name: str, path: str, has_header: bool):
        self.ctx.register_csv(table_name, path, has_header)

    def register_parquet(self, table_name: str, path: str):
        self.ctx.register_parquet(table_name, path)

    def sql(self, sql: str) -> ResultSet:
        graph = self.ctx.plan(sql)
        final_stage_id = graph.get_final_query_stage().id()

        # serialize the query stages and store in Ray object store
        query_stages = [
            serialize_execution_plan(graph.get_query_stage(i).get_execution_plan())
            for i in range(final_stage_id + 1)
        ]

        # schedule execution
        future = execute_query_stage.remote(
            query_stages, final_stage_id, self.workers, self.use_ray_shuffle
        )
        _, partitions = ray.get(future)
        # TODO(@lsf): we only support a single output partition for now?
        result = ray.get(partitions[0])
        return result
