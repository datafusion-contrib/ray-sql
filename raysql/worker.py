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
        input_partition_refs: list[tuple[int, int, int, ray.ObjectRef]],
    ) -> list[bytes]:
        plan = deserialize_execution_plan(plan_bytes)

        if self.debug:
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
        result_set = self.ctx.execute_partition(plan, part, input_partitions)

        ret = result_set.tobyteslist()
        return ret[0] if len(ret) == 1 else ret
