import ray
from raysql import Context


# TODO move worker into library
@ray.remote
class Worker:
    def __init__(self):
        self.ctx = Context()

    def execute_query_partition(self, plan_bytes, part):
        plan = self.ctx.deserialize_execution_plan(plan_bytes)
        print("Executing partition #{}:\n{}".format(part, plan.display_indent()))

        # This is delegating to DataFusion for execution, but this would be a good place
        # to plug in other execution engines by translating the plan into another engine's plan
        # (perhaps via Substrait, once DataFusion supports converting a physical plan to Substrait)
        self.ctx.execute_partition(plan, part)

        return True


def execute_query_stage(ctx, graph, stage, workers):

    # TODO make better use of futures here so that more runs in parallel

    # execute child stages first
    for child_id in stage.get_child_stage_ids():
        child_stage = graph.get_query_stage(child_id)
        execute_query_stage(ctx, graph, child_stage, workers)

    print("Scheduling query stage #{}".format(stage.id()))

    # serialize the plan
    plan_bytes = ctx.serialize_execution_plan(stage.get_execution_plan())

    # round-robin allocation across workers
    futures = []
    for part in range(stage.get_input_partition_count()):
        worker_index = part % len(workers)
        futures.append(workers[worker_index].execute_query_partition.remote(plan_bytes, part))

    print("Waiting for query stage #{} to complete".format(stage.id()))
    print(ray.get(futures))
    print("Query stage #{} has completed".format(stage.id()))


if __name__ == "__main__":
    # Start our cluster
    ray.init()

    # create some remote Workers
    workers = [Worker.remote() for i in range(2)]

    # create context and plan a query
    ctx = Context()
    ctx.register_csv('tips', 'tips.csv', True)
    graph = ctx.plan('select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker')

    # recurse down the tree and build a DAG of futures
    final_stage = graph.get_final_query_stage()

    # schedule execution
    execute_query_stage(ctx, graph, final_stage, workers)