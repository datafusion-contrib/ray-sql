import ray
from raysql import Context, deserialize_execution_plan


@ray.remote
class Worker:
    def __init__(self):
        self.ctx = Context(1, False)
        self.debug = True

    def execute_query_partition(self, plan_bytes, part, inputs):
        plan = deserialize_execution_plan(plan_bytes)
        inputs = inputs[part : part + 1]

        if self.debug:
            print("Executing partition #{}:\n{}".format(part, plan.display_indent()))

        # This is delegating to DataFusion for execution, but this would be a good place
        # to plug in other execution engines by translating the plan into another engine's plan
        # (perhaps via Substrait, once DataFusion supports converting a physical plan to Substrait)
        results = self.ctx.execute_partition(plan, part, inputs)

        return results.tobytes()
