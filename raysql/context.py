import ray
from raysql import Context, QueryStage, serialize_execution_plan
import time

@ray.remote
def execute_query_stage(query_stages, stage_id, workers):
    plan_bytes = ray.get(query_stages[stage_id]).plan_bytes
    stage = QueryStage(stage_id, plan_bytes)

    # execute child stages first
    child_futures = []
    for child_id in stage.get_child_stage_ids():
        child_futures.append(execute_query_stage.remote(query_stages, child_id, workers))
    ray.get(child_futures)

    # if the query stage has a single output partition then we need to execute for the output
    # partition, otherwise we need to execute in parallel for each input partition
    concurrency = stage.get_input_partition_count()
    if stage.get_output_partition_count() == 1:
        # reduce stage
        concurrency = 1

    print("Scheduling query stage #{} with {} input partitions and {} output partitions".format(stage.id(), stage.get_input_partition_count(), stage.get_output_partition_count()))

    plan_bytes = ray.put(serialize_execution_plan(stage.get_execution_plan()))

    # round-robin allocation across workers
    futures = []
    for part in range(concurrency):
        worker_index = part % len(workers)
        futures.append(workers[worker_index].execute_query_partition.remote(plan_bytes, part))

    print("Waiting for query stage #{} to complete".format(stage.id()))
    start = time.time()
    result_set = ray.get(futures)
    end = time.time()
    print("Query stage #{} completed in {} seconds".format(stage.id(), end-start))

    return result_set

@ray.remote
class RaySqlContext:

    def __init__(self, workers):
        self.ctx = Context(len(workers))
        self.workers = workers
        self.debug = False

    def register_csv(self, table_name, path, has_header):
        self.ctx.register_csv(table_name, path, has_header)

    def register_parquet(self, table_name, path):
        self.ctx.register_parquet(table_name, path)

    def sql(self, sql):
        if self.debug:
            print(sql)

        graph = self.ctx.plan(sql)
        final_stage_id = graph.get_final_query_stage().id()

        # serialize the query stages and store in Ray object store
        query_stages = []
        for i in range(0, final_stage_id+1):
            stage = graph.get_query_stage(i)
            plan_bytes = serialize_execution_plan(stage.get_execution_plan())
            query_stage_serde = QueryStageSerde(i, plan_bytes)
            query_stages.append(ray.put(query_stage_serde))

        # schedule execution
        future = execute_query_stage.remote(query_stages, final_stage_id, self.workers)
        return ray.get(future)

class QueryStageSerde:
    def __init__(self, id, plan_bytes):
        self.id = id
        self.plan_bytes = plan_bytes

    def __reduce__(self):
        return (self.__class__, (self.id, self.plan_bytes))