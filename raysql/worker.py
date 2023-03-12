import ray

from raysql import Context, deserialize_execution_plan


@ray.remote
class Worker:
    def __init__(self):
        self.ctx = Context(1, False)
        self.debug = True

    def execute_query_partition(
        self,
        plan_bytes: bytes,
        part: int,
        input_partitions_map: dict[int, list[ray.ObjectRef]],
    ) -> list[bytes]:
        plan = deserialize_execution_plan(plan_bytes)

        if self.debug:
            print(
                "Worker executing plan {} partition #{} with {} shuffle inputs:\n{}".format(
                    plan.display(),
                    part,
                    {i: len(parts) for i, parts in input_partitions_map.items()},
                    plan.display_indent(),
                )
            )

        input_partitions_map = {
            i: ray.get(parts) for i, parts in input_partitions_map.items()
        }
        # This is delegating to DataFusion for execution, but this would be a good place
        # to plug in other execution engines by translating the plan into another engine's plan
        # (perhaps via Substrait, once DataFusion supports converting a physical plan to Substrait)
        result_set = self.ctx.execute_partition(plan, part, input_partitions_map)

        ret = result_set.tobyteslist()
        return ret[0] if len(ret) == 1 else ret
