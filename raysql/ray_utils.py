import ray


def node_aff(node_id: ray.NodeID, *, soft: bool = False) -> dict:
    return {
        "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=node_id,
            soft=soft,
        )
    }


def current_node_aff() -> dict:
    return node_aff(ray.get_runtime_context().get_node_id())
