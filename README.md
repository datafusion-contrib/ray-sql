# RaySQL: DataFusion on Ray

This is an experimental research project to evaluate the concept of performing distributed SQL queries from Python, using 
[Ray](https://www.ray.io/) and [DataFusion](https://github.com/apache/arrow-datafusion).

## Motivation

This would make a good conference talk to demonstrate the power of building new query engines on top of the DataFusion
framework. This project currently contains fewer than 500 lines of code.

## Features

- Mature SQL support (CTEs, joins, subqueries, etc)
- Support for CSV and Parquet files

## Limitations

- Simplistic shuffle mechanism that produces lots of files
- Requires a shared file system currently

## Building

```bash
# prepare development environment (used to build wheel / install in development)
python3 -m venv venv
# activate the venv
source venv/bin/activate
# update pip itself if necessary
python -m pip install -U pip
# install dependencies (for Python 3.8+)
python -m pip install -r requirements-in.txt
```

Whenever rust code changes (your changes or via `git pull`):

```bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop
python -m pytest
```

## How to update dependencies

To change test dependencies, change the `requirements.in` and run

```bash
# install pip-tools (this can be done only once), also consider running in venv
python -m pip install pip-tools
python -m piptools compile --generate-hashes -o requirements-310.txt
```

To update dependencies, run with `-U`

```bash
python -m piptools compile -U --generate-hashes -o requirements-310.txt
```

More details [here](https://github.com/jazzband/pip-tools)
