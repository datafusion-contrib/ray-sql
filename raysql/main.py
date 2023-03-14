import os

import ray
from raysql import ResultSet
from raysql.context import RaySqlContext
from raysql.worker import Worker

DATA_DIR = "/home/ubuntu/tpch/sf1-parquet"
# DATA_DIR = "/home/ubuntu/sf10-parquet"

ray.init()
# ray.init(local_mode=True)


def setup_context(use_ray_shuffle: bool) -> RaySqlContext:
    num_workers = 2
    # num_workers = os.cpu_count()
    workers = [Worker.remote() for _ in range(num_workers)]
    ctx = RaySqlContext.remote(workers, use_ray_shuffle)
    register_tasks = []
    register_tasks.append(ctx.register_csv.remote("tips", "examples/tips.csv", True))
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
        register_tasks.append(
            ctx.register_parquet.remote(table, f"{DATA_DIR}/{table}.parquet")
        )
    return ctx


def load_query(n: int) -> str:
    with open(f"testdata/queries/q{n}.sql") as fin:
        return fin.read()


def tpchq(ctx: RaySqlContext, q: int = 14):
    sql = load_query(q)
    result_set = ray.get(ctx.sql.remote(sql))
    return result_set


def compare(q: int):
    ctx = setup_context(False)
    result_set_truth = tpchq(ctx, q)

    ctx = setup_context(True)
    result_set_ray = tpchq(ctx, q)

    assert result_set_truth == result_set_ray, (
        q,
        ResultSet(result_set_truth),
        ResultSet(result_set_ray),
    )


# use_ray_shuffle = True
# ctx = setup_context(use_ray_shuffle)
# result_set = tpchq(ctx, 1)
# print("Result:")
# print(ResultSet(result_set))

for i in range(1, 22 + 1):
    if i == 15:
        continue
    compare(i)
