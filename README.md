# Synaptiq Data Pipeline Exercise

This is Ryan Hammang's submission for the Synaptiq take-home exercise: a daily retail sales pipeline built with local PySpark and Delta Lake.

## What the exercise asks

A retail order system drops one CSV of orders per day, and a separate file describes each product. The task is to build a pipeline that runs every day as a new file lands and produces one table showing, per day, per product category, and per region: net revenue, order count, units sold, and average order value. The original brief is in [`candidate_exercise.md`](candidate_exercise.md), and the column definitions are in [`data_dictionary.md`](data_dictionary.md).

## Where to start

- [`NOTES.md`](NOTES.md) is the main write-up: assumptions, the data problems found in the files, design decisions, the final output table, and how this would change on Databricks.
- [`output/daily_sales.csv`](output/daily_sales.csv) is the final output table.
- [`output/trap_forensics.csv`](output/trap_forensics.csv) lists each data problem found and how the pipeline handles it.
- [`presentation/`](presentation/) has the slide deck as PowerPoint, PDF, and Markdown, plus the architecture diagrams.

## How it's laid out

| Path | What it is |
| --- | --- |
| `orders_2024-01-01.csv`, `orders_2024-01-02.csv`, `products.csv` | The input files provided with the exercise |
| `src/synaptiq_pipeline/bronze.py` | Step 1: loads the raw CSVs into Bronze Delta tables |
| `src/synaptiq_pipeline/silver.py` | Step 2: cleans the data, removes duplicates, and applies late corrections into Silver Delta tables |
| `src/synaptiq_pipeline/gold.py` | Step 3: joins orders to products and builds the daily sales table in Gold |
| `src/synaptiq_pipeline/pipeline.py`, `cli.py`, `spark.py`, `paths.py` | Runs the steps in order, the command-line entry point, Spark setup, and file locations |
| `tests/test_pipeline.py` | Automated tests |
| `warehouse/` | The Bronze, Silver, Gold, and intermediate tables from the last run, kept so you can inspect them without running anything |
| `output/` | CSV exports of the final tables |
| `pyproject.toml`, `uv.lock`, `.python-version` | Python project and pinned dependencies |

## How to run it

You need Python 3.12, [uv](https://docs.astral.sh/uv/), and Java 17 or 21.

```bash
uv sync

# Day 1, starting from an empty warehouse
uv run synaptiq-pipeline --reset --orders orders_2024-01-01.csv --products products.csv

# Day 2, run as the next daily load
uv run synaptiq-pipeline --orders orders_2024-01-02.csv --products products.csv

# Tests
uv run pytest -v
```

Running the pipeline rewrites `warehouse/` and `output/`.

## What's not in this repo

The local Python virtual environment (`.venv`, about 549 MB), tool caches (`.ruff_cache`, `.pytest_cache`, `__pycache__`), and macOS metadata files were left out. `uv sync` recreates the environment. Everything else from the original submission is here.
