import ray
from raysql import Context

class RaySqlContext:

    def __init__(self, workers):
        self.ctx = Context()
        self.workers = workers

    def register_csv(self, table_name, path, has_header):
        self.ctx.register_csv(table_name, path, has_header)

    def register_parquet(self, table_name, path):
        self.ctx.register_parquet(table_name, path)

    def sql(self, sql):
        graph = self.ctx.plan(sql)
        # recurse down the tree and build a DAG of futures
        final_stage = graph.get_final_query_stage()
        # schedule execution
        self.execute_query_stage(graph, final_stage)

    def execute_query_stage(self, graph, stage):

        # TODO make better use of futures here so that more runs in parallel

        # execute child stages first
        for child_id in stage.get_child_stage_ids():
            child_stage = graph.get_query_stage(child_id)
            self.execute_query_stage(graph, child_stage)

        partition_count = stage.get_input_partition_count()
        print("Scheduling query stage #{} with {} input partitions and {} output partitions".format(stage.id(), partition_count, stage.get_output_partition_count()))

        # serialize the plan
        plan_bytes = self.ctx.serialize_execution_plan(stage.get_execution_plan())

        # round-robin allocation across workers
        futures = []
        for part in range(partition_count):
            worker_index = part % len(self.workers)
            print("Asking worker {} to execute partition {}".format(worker_index, part))
            futures.append(self.workers[worker_index].execute_query_partition.remote(plan_bytes, part))

        print("Waiting for query stage #{} to complete".format(stage.id()))
        print(ray.get(futures))
        print("Query stage #{} has completed".format(stage.id()))