import ray
from raysql.context import RaySqlContext
from raysql.worker import Worker

# Start our cluster
ray.init()

# create some remote Workers
workers = [Worker.remote() for i in range(2)]

# create context and plan a query
ctx = RaySqlContext(workers)
ctx.register_csv('tips', 'tips.csv', True)

ctx.sql('select day, sum(total_bill) from tips group by day')
# ctx.sql('select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker')
