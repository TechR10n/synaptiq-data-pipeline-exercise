"""Command-line entry point for running daily or multi-day drops through the Medallion pipeline."""

from __future__ import annotations

import argparse
import shutil
from collections.abc import Sequence

from synaptiq_pipeline.paths import LakehousePaths
from synaptiq_pipeline.pipeline import execute_pipeline
from synaptiq_pipeline.spark import build_spark_session


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for daily or multi-file pipeline execution."""
    parser = argparse.ArgumentParser(
        prog="synaptiq-pipeline",
        description="Run the functional PySpark + Delta Lake daily retail sales pipeline.",
    )
    parser.add_argument(
        "--orders",
        nargs="+",
        required=True,
        help="One or more daily order CSV drop files (e.g. orders_2024-01-01.csv).",
    )
    parser.add_argument(
        "--products",
        default="products.csv",
        help="Path to products reference CSV (default: products.csv).",
    )
    parser.add_argument(
        "--warehouse-dir",
        default="warehouse",
        help="Root directory for Bronze, Intermediate Parquet, Silver Delta, and Gold Delta.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for exported analyst CSV reports.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clean warehouse and output directories before running.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the end-to-end Medallion pipeline from CLI options and display summary tables."""
    args = parse_args(argv)
    paths = LakehousePaths.from_roots(args.warehouse_dir, args.output_dir)

    if args.reset:
        shutil.rmtree(paths.warehouse_root, ignore_errors=True)
        shutil.rmtree(paths.output_root, ignore_errors=True)

    spark = build_spark_session()
    try:
        outputs = execute_pipeline(
            spark,
            paths,
            order_csv_paths=args.orders,
            products_csv_path=args.products,
        )

        print("\n=== SILVER ORDERS (CLEANED & RECONCILED DELTA) ===")
        outputs.silver_orders.orderBy("order_id").select(
            "order_id",
            "order_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "region",
            "order_status",
            "rescued_extra",
            "drop_date",
            "trap_flags",
        ).show(50, truncate=False)

        print("=== GOLD DAILY SALES (PER DAY, PER CATEGORY, PER REGION) ===")
        outputs.gold_daily_sales.show(50, truncate=False)

        print("=== FORENSIC TRAP AUDIT SUMMARY ===")
        outputs.gold_trap_forensics.select(
            "trap_code",
            "pipeline_stage",
            "affected_records",
            "affected_ids",
        ).show(50, truncate=False)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
