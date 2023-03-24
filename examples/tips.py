import os

import ray

from raysql import RaySqlContext, ResultSet

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Start a local cluster
ray.init(resources={"worker": 1})

# Create a context and register a table
ctx = RaySqlContext(2, use_ray_shuffle=True)
# Register either a CSV or Parquet file
# ctx.register_csv("tips", f"{SCRIPT_DIR}/tips.csv", True)
ctx.register_parquet("tips", f"{SCRIPT_DIR}/tips.parquet")

result_set = ctx.sql(
    "select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker"
)
print("Result:")
print(ResultSet(result_set))
