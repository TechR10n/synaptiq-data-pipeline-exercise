"""End-to-end verification of AST functional purity, Medallion Delta/Parquet stages, CDC idempotency, CLI, and trap fixes."""

from __future__ import annotations

import ast
import datetime
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from synaptiq_pipeline import LakehousePaths, build_spark_session, execute_pipeline
from synaptiq_pipeline import cli as cli_module
from synaptiq_pipeline.gold import build_gold_outputs

REPO_ROOT = Path(__file__).resolve().parents[1]
DAY_1_CSV = REPO_ROOT / "orders_2024-01-01.csv"
DAY_2_CSV = REPO_ROOT / "orders_2024-01-02.csv"
PRODUCTS_CSV = REPO_ROOT / "products.csv"

FORBIDDEN_LOOP_NODES = (
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Provide a shared UTC-pinned Spark + Delta session across the test suite."""
    session = build_spark_session("synaptiq-pipeline-tests")
    yield session
    session.stop()


def test_zero_loops_and_zero_udfs_in_pipeline_code() -> None:
    """Enforce functional PySpark discipline: zero Python loops/comprehensions and zero Spark UDFs in src/."""
    src_files = sorted((REPO_ROOT / "src" / "synaptiq_pipeline").glob("*.py"))
    assert len(src_files) >= 6

    for py_file in src_files:
        source_text = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source_text, filename=str(py_file))
        for node in ast.walk(tree):
            assert not isinstance(node, FORBIDDEN_LOOP_NODES), (
                f"Forbidden loop/comprehension {type(node).__name__} in {py_file.name}:{getattr(node, 'lineno', '?')}"
            )
            if isinstance(node, ast.Attribute):
                assert node.attr not in {"udf", "pandas_udf"}, (
                    f"Forbidden Spark UDF usage '{node.attr}' in {py_file.name}:{node.lineno}"
                )
            if isinstance(node, ast.Name):
                assert node.id not in {"udf", "pandas_udf"}, (
                    f"Forbidden Spark UDF symbol '{node.id}' in {py_file.name}:{node.lineno}"
                )


def test_sequential_daily_runs_and_out_of_order_idempotency(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Verify Move 1 -> Move 2 -> Move 3 across Day 1, Day 2 restatement, and out-of-order Day 1 replay."""
    paths = LakehousePaths.from_roots(tmp_path / "warehouse", tmp_path / "output")

    # Day 1 arrival
    day1_out = execute_pipeline(
        spark,
        paths,
        order_csv_paths=[DAY_1_CSV],
        products_csv_path=PRODUCTS_CSV,
    )
    assert day1_out.bronze_orders.count() == 12
    assert day1_out.silver_orders.count() == 12

    day1_stationery = (
        day1_out.gold_daily_sales.filter(
            (F.col("order_date") == datetime.date(2024, 1, 1))
            & (F.col("category") == "Stationery")
            & (F.col("region") == "North")
        )
        .collect()[0]
    )
    assert day1_stationery["net_revenue"] == Decimal("17.50")
    assert day1_stationery["order_count"] == 1
    assert day1_stationery["units_sold"] == 5
    assert day1_stationery["average_order_value"] == Decimal("17.50")

    # Day 2 arrival: O1002 exact duplicate + O1003 late restatement (qty 5 -> 6) + 10 Day-2 orders
    day2_out = execute_pipeline(
        spark,
        paths,
        order_csv_paths=[DAY_2_CSV],
        products_csv_path=PRODUCTS_CSV,
    )

    # Row conservation: 24 raw Bronze rows = 22 unique Silver orders + 2 superseded CDC rows
    assert day2_out.bronze_orders.count() == 24
    assert day2_out.silver_orders.count() == 22
    superseded_count = spark.read.format("delta").load(str(paths.silver_orders_superseded)).count()
    assert superseded_count == 2

    # Intermediate Parquet enrichment steps must all be materialized on disk
    assert spark.read.parquet(str(paths.step_01_products_cleaned_parquet)).count() == 10
    assert spark.read.parquet(str(paths.step_01_orders_parsed_parquet)).count() == 24
    assert spark.read.parquet(str(paths.step_02_orders_traps_parquet)).count() == 24
    assert spark.read.parquet(str(paths.step_03_orders_reconciled_parquet)).count() == 22
    assert spark.read.parquet(str(paths.step_03_orders_superseded_parquet)).count() == 2
    assert spark.read.parquet(str(paths.step_04_joined_order_facts_parquet)).count() == 22

    # Verify Day 1 Stationery/North updated from 17.50 -> 21.00 while Electronics/West was NOT double-counted
    gold_snapshot_after_day2 = day2_out.gold_daily_sales.collect()
    assert len(gold_snapshot_after_day2) == 17

    jan1_stationery = next(
        r
        for r in gold_snapshot_after_day2
        if r["order_date"] == datetime.date(2024, 1, 1)
        and r["category"] == "Stationery"
        and r["region"] == "North"
    )
    assert jan1_stationery["net_revenue"] == Decimal("21.00")
    assert jan1_stationery["units_sold"] == 6
    assert jan1_stationery["order_count"] == 1
    assert jan1_stationery["average_order_value"] == Decimal("21.00")

    jan1_elec_west = next(
        r
        for r in gold_snapshot_after_day2
        if r["order_date"] == datetime.date(2024, 1, 1)
        and r["category"] == "Electronics"
        and r["region"] == "West"
    )
    assert jan1_elec_west["net_revenue"] == Decimal("2287.90")
    assert jan1_elec_west["order_count"] == 3
    assert jan1_elec_west["units_sold"] == 12

    # Re-run Day 1 out of order: must NOT regress O1003 back to qty=5 or alter Gold
    replay_day1_out = execute_pipeline(
        spark,
        paths,
        order_csv_paths=[DAY_1_CSV],
        products_csv_path=PRODUCTS_CSV,
    )
    assert replay_day1_out.gold_daily_sales.collect() == gold_snapshot_after_day2

    # Unscoped full Gold refresh must also produce identical results
    full_sales, _ = build_gold_outputs(spark, paths, impacted_order_dates_df=None)
    assert full_sales.collect() == gold_snapshot_after_day2


def test_forensic_trap_repairs_and_solo_return_aov_guard(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Verify exact resolution of CSV framing, date/UTC, price imputation, unmapped SKUs, and solo-return AOV."""
    paths = LakehousePaths.from_roots(tmp_path / "warehouse", tmp_path / "output")
    outputs = execute_pipeline(
        spark,
        paths,
        order_csv_paths=[DAY_1_CSV, DAY_2_CSV],
        products_csv_path=PRODUCTS_CSV,
    )

    silver_by_id = {row["order_id"]: row for row in outputs.silver_orders.collect()}

    # Trap 1: O1011 trailing EXTRA_FLAG rescued without losing $24.99 order
    assert silver_by_id["O1011"]["rescued_extra"] == "EXTRA_FLAG"
    assert silver_by_id["O1011"]["unit_price"] == Decimal("24.99")

    # Trap 4 & Trap 5: US MM/dd/yyyy ('01/02/2024') and UTC epoch ('1704153600') both resolve to 2024-01-02
    assert silver_by_id["O2001"]["order_date"] == datetime.date(2024, 1, 2)
    assert silver_by_id["O2003"]["order_date"] == datetime.date(2024, 1, 2)

    # Trap 2 & 2B: Truncated O2009 gets region='Unknown' in Silver and catalog list_price=29.99 in Gold join
    assert silver_by_id["O2009"]["unit_price"] is None
    assert silver_by_id["O2009"]["region"] == "Unknown"
    joined_o2009 = (
        spark.read.parquet(str(paths.step_04_joined_order_facts_parquet))
        .filter(F.col("order_id") == "O2009")
        .collect()[0]
    )
    assert joined_o2009["effective_unit_price"] == Decimal("29.99")
    assert joined_o2009["line_net_revenue"] == Decimal("59.98")

    # Trap 13: Solo return bucket (2024-01-02, Electronics, West) -> net_revenue=-1099.00, order_count=0, AOV=None
    solo_return = (
        outputs.gold_daily_sales.filter(
            (F.col("order_date") == datetime.date(2024, 1, 2))
            & (F.col("category") == "Electronics")
            & (F.col("region") == "West")
        )
        .collect()[0]
    )
    assert solo_return["net_revenue"] == Decimal("-1099.00")
    assert solo_return["order_count"] == 0
    assert solo_return["units_sold"] == -1
    assert solo_return["average_order_value"] is None

    # All 14 forensic traps must register >= 1 affected record across the two drops
    trap_counts = {
        row["trap_code"]: row["affected_records"]
        for row in outputs.gold_trap_forensics.collect()
    }
    assert len(trap_counts) == 14
    assert all(count >= 1 for count in trap_counts.values())
    assert paths.daily_sales_csv.exists()
    assert paths.trap_forensics_csv.exists()


def test_cli_entry_point_end_to_end(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify CLI argument parsing, reset handling, and console summary output."""
    monkeypatch.setattr(cli_module, "build_spark_session", lambda: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    warehouse_dir = tmp_path / "cli_warehouse"
    output_dir = tmp_path / "cli_output"
    exit_code = cli_module.main(
        [
            "--reset",
            "--orders",
            str(DAY_1_CSV),
            str(DAY_2_CSV),
            "--products",
            str(PRODUCTS_CSV),
            "--warehouse-dir",
            str(warehouse_dir),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr().out
    assert "=== GOLD DAILY SALES (PER DAY, PER CATEGORY, PER REGION) ===" in captured
    assert "TRAP_11_LATE_ARRIVING_RESTATEMENT" in captured
    assert (output_dir / "daily_sales.csv").exists()
