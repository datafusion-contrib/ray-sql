import pytest
from raysql import Context

def test():
    ctx = Context()
    ctx.register_csv('tips', 'examples/tips.csv', True)
    ctx.plan("SELECT * FROM tips")