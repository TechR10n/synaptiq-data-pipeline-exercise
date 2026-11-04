"""End-to-end functional orchestrator composing Move 1 (Bronze), Move 2 (Silver Delta), and Move 3 (Gold Delta)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from synaptiq_pipeline.bronze import (
    ingest_orders_to_bronze,
    ingest_products_to_bronze,
    read_raw_orders_csv,
)
from synaptiq_pipeline.gold import build_gold_outputs
from synaptiq_pipeline.paths import LakehousePaths
from synaptiq_pipeline.silver import build_silver_orders, build_silver_products


@dataclass(frozen=True, slots=True)
class PipelineOutputs:
    """Lazy DataFrame handles to every Medallion table produced by a pipeline run."""

    bronze_orders: DataFrame
    bronze_products: DataFrame
    silver_orders: DataFrame
    silver_products: DataFrame
    gold_daily_sales: DataFrame
    gold_trap_forensics: DataFrame


def execute_pipeline(
    spark: SparkSession,
    paths: LakehousePaths,
    *,
    order_csv_paths: Sequence[Path | str],
    products_csv_path: Path | str,
) -> PipelineOutputs:
    """Execute Move 1 (CSV -> Bronze), Move 2 (Bronze -> Silver Delta), and Move 3 (Silver -> Gold Delta)."""
    # Move 1: CSV -> DataFrame -> Bronze Delta
    incoming_drop_dates = read_raw_orders_csv(spark, order_csv_paths).select("drop_date").distinct()
    bronze_products = ingest_products_to_bronze(spark, paths, products_csv_path)
    bronze_orders = ingest_orders_to_bronze(spark, paths, order_csv_paths)

    # Move 2: Bronze -> Clean & Standardize (saving intermediate Parquet steps) -> Silver Delta
    silver_products = build_silver_products(spark, paths)
    silver_orders = build_silver_orders(spark, paths)

    # Identify all order_dates touched by the landed drop_date(s) (captures late restatements like O1003)
    impacted_order_dates = (
        silver_orders.join(incoming_drop_dates, on="drop_date", how="left_semi")
        .select("order_date")
        .distinct()
    )

    # Move 3: Read Silver Delta -> Joins & Aggregations -> Gold Delta + CSV exports
    gold_daily_sales, gold_trap_forensics = build_gold_outputs(
        spark,
        paths,
        impacted_order_dates_df=impacted_order_dates,
    )

    return PipelineOutputs(
        bronze_orders=bronze_orders,
        bronze_products=bronze_products,
        silver_orders=silver_orders,
        silver_products=silver_products,
        gold_daily_sales=gold_daily_sales,
        gold_trap_forensics=gold_trap_forensics,
    )
