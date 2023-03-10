import ray
from raysql.context import RaySqlContext
from raysql.worker import Worker

# Start our cluster
ray.init()

# create some remote Workers
workers = [Worker.remote() for i in range(2)]

# create a remote context and register a table
ctx = RaySqlContext.remote(workers)
ray.get(ctx.register_csv.remote('tips', 'tips.csv', True))

# Parquet is also supported
# ctx.register_parquet('tips', 'tips.parquet')

result_set = ray.get(ctx.sql.remote('select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker'))
print(result_set)
