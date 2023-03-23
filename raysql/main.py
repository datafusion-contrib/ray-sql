import time
import os

import ray
from raysql import RaySqlContext, ResultSet

NUM_CPUS_PER_WORKER = 8

SF = 10
DATA_DIR = f"/mnt/data0/tpch/sf{SF}-parquet"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
QUERIES_DIR = os.path.join(SCRIPT_DIR, f"../sqlbench-h/queries/sf={SF}")


def setup_context(use_ray_shuffle: bool, num_workers: int = 2) -> RaySqlContext:
    print(f"Using {num_workers} workers")
    ctx = RaySqlContext(num_workers, use_ray_shuffle)
    for table in [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]:
        ctx.register_parquet(table, f"{DATA_DIR}/{table}.parquet")
    return ctx


def load_query(n: int) -> str:
    with open(f"{QUERIES_DIR}/q{n}.sql") as fin:
        return fin.read()


def tpch_query(ctx: RaySqlContext, q: int = 1):
    sql = load_query(q)
    result_set = ctx.sql(sql)
    return result_set


def tpch_timing(ctx: RaySqlContext, q: int = 1):
    sql = load_query(q)
    start = time.perf_counter()
    ctx.sql(sql)
    end = time.perf_counter()
    return end - start


def compare(q: int):
    ctx = setup_context(False)
    result_set_truth = tpch_query(ctx, q)

    ctx = setup_context(True)
    result_set_ray = tpch_query(ctx, q)

    assert result_set_truth == result_set_ray, (
        q,
        ResultSet(result_set_truth),
        ResultSet(result_set_ray),
    )


def tpch_bench():
    ray.init("auto")
    num_workers = int(ray.cluster_resources().get("worker", 1)) * NUM_CPUS_PER_WORKER
    ctx = setup_context(True, num_workers)
    run_id = time.strftime("%Y-%m-%d-%H-%M-%S")
    with open(f"results-sf{SF}-{run_id}.csv", "w") as fout:
        for i in range(1, 22 + 1):
            if i == 15:
                continue
            result = tpch_timing(ctx, i)
            print(f"query,{i},{result}")
            print(f"query,{i},{result}", file=fout, flush=True)


tpch_bench()
