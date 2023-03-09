import ray
from raysql import Context, QueryStage, serialize_execution_plan


@ray.remote
def execute_query_stage(query_stages, stage_id, workers, use_ray_shuffle):
    plan_bytes = query_stages[stage_id]
    stage = QueryStage(stage_id, plan_bytes)

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
        concurrency = 1

    print(
        "Scheduling query stage #{} with {} input partitions and {} output partitions".format(
            stage.id(), concurrency, output_partitions_count
        )
    )

    plan_bytes = serialize_execution_plan(stage.get_execution_plan())

    # TODO(@lsf): deal with more than 1 child futures
    inputs = child_futures[0] if len(child_futures) > 0 else []
    inputs = ray.get(inputs)  # 2-D array of input_partitions * output_partitions
    print("Stage #{}'s child inputs: {}".format(stage.id(), inputs))

    def _get_worker_inputs(part, concurrency):
        ret = []
        for lst in inputs:
            num_parts = len(lst)
            parts_per_worker = num_parts // concurrency
            ret.extend(lst[part * parts_per_worker : (part + 1) * parts_per_worker])
        return ret

    # round-robin allocation across workers
    futures = []
    for part in range(concurrency):
        worker_index = part % len(workers)
        opt = {}
        if use_ray_shuffle:
            opt["num_returns"] = output_partitions_count
        futures.append(
            workers[worker_index]
            .execute_query_partition.options(**opt)
            .remote(plan_bytes, part, *_get_worker_inputs(part, concurrency))
        )

    return futures


@ray.remote
class RaySqlContext:
    def __init__(self, workers, use_ray_shuffle):
        self.ctx = Context(len(workers), use_ray_shuffle)
        self.workers = workers
        self.use_ray_shuffle = use_ray_shuffle

    def register_csv(self, table_name, path, has_header):
        self.ctx.register_csv(table_name, path, has_header)

    def register_parquet(self, table_name, path):
        self.ctx.register_parquet(table_name, path)

    def sql(self, sql):
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
        stage_futures = ray.get(future)
        # TODO(@lsf): we only support a single output partition for now
        return ray.get(stage_futures[0])
