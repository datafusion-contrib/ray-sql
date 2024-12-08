# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import time
import os

from pyarrow import csv as pacsv
import ray
from raysql import RaySqlContext

NUM_CPUS_PER_WORKER = 8

SF = 10
DATA_DIR = f"/mnt/data0/tpch/sf{SF}-parquet"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
QUERIES_DIR = os.path.join(SCRIPT_DIR, f"../sqlbench-h/queries/sf={SF}")
RESULTS_DIR = f"results-sf{SF}"
TRUTH_DIR = (
    "/home/ubuntu/raysort/ray-sql/sqlbench-runners/spark/{RESULTS_DIR}/{RESULTS_DIR}"
)


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


def tpch_timing(
    ctx: RaySqlContext,
    q: int = 1,
    print_result: bool = False,
    write_result: bool = False,
):
    sql = load_query(q)
    start = time.perf_counter()
    result = ctx.sql(sql)
    end = time.perf_counter()
    if print_result:
        print("Result:", result)
        if isinstance(result, list):
            for r in result:
                print(r.to_pandas())
        else:
            print(result.to_pandas())
    if write_result:
        opt = pacsv.WriteOptions(quoting_style="none")
        if isinstance(result, list):
            for r in result:
                pacsv.write_csv(r, f"{RESULTS_DIR}/q{q}.csv", write_options=opt)
        else:
            pacsv.write_csv(result, f"{RESULTS_DIR}/q{q}.csv", write_options=opt)
    return end - start


def compare(q: int):
    ctx = setup_context(False)
    result_set_truth = tpch_query(ctx, q)

    ctx = setup_context(True)
    result_set_ray = tpch_query(ctx, q)

    assert result_set_truth == result_set_ray, (
        q,
        result_set_truth,
        result_set_ray,
    )


def tpch_bench():
    ray.init("auto")
    num_workers = int(ray.cluster_resources().get("worker", 1)) * NUM_CPUS_PER_WORKER
    use_ray_shuffle = False
    ctx = setup_context(use_ray_shuffle, num_workers)
    # t = tpch_timing(ctx, 11, print_result=True)
    # print(f"query,{t},{use_ray_shuffle},{num_workers}")
    # return
    run_id = time.strftime("%Y-%m-%d-%H-%M-%S")
    with open(f"results-sf{SF}-{run_id}.csv", "w") as fout:
        for i in range(1, 22 + 1):
            if i == 15:
                continue
            result = tpch_timing(ctx, i, write_result=True)
            print(f"query,{i},{result}")
            print(f"query,{i},{result}", file=fout, flush=True)


tpch_bench()
