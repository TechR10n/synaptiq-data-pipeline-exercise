"""SparkSession factory and pure functional Parquet / Delta / CSV sink combinators."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession

Transform = Callable[[DataFrame], DataFrame]


def build_spark_session(app_name: str = "synaptiq-daily-sales-pipeline") -> SparkSession:
    """Create a local Spark 4 + Delta Lake session pinned to UTC and ANSI SQL semantics."""
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def checkpoint_parquet(
    spark: SparkSession,
    target_path: Path,
    *,
    partition_by: tuple[str, ...] = (),
) -> Transform:
    """Materialize an intermediate enrichment step as Parquet and return a lazy Parquet reader."""

    def _materialize(df: DataFrame) -> DataFrame:
        writer = df.write.mode("overwrite")
        partitioned_writer = writer.partitionBy(*partition_by) if partition_by else writer
        partitioned_writer.parquet(str(target_path))
        return spark.read.parquet(str(target_path))

    return _materialize


def write_delta_dynamic(
    spark: SparkSession,
    target_path: Path,
    *,
    partition_by: tuple[str, ...] = (),
) -> Transform:
    """Write a DataFrame to Delta Lake (dynamic partition overwrite when partitioned) and return lazy Delta reader."""

    def _sink(df: DataFrame) -> DataFrame:
        base_writer = df.write.format("delta").mode("overwrite")
        configured_writer = (
            base_writer.option("partitionOverwriteMode", "dynamic").partitionBy(*partition_by)
            if partition_by
            else base_writer.option("overwriteSchema", "true")
        )
        configured_writer.save(str(target_path))
        return spark.read.format("delta").load(str(target_path))

    return _sink


def export_single_csv(target_csv_path: Path) -> Transform:
    """Write a sorted DataFrame as a single RFC-4180 CSV file via Spark's native CSV writer."""

    def _export(df: DataFrame) -> DataFrame:
        target_csv_path.parent.mkdir(parents=True, exist_ok=True)
        staging_dir = target_csv_path.parent / f".staging_{target_csv_path.stem}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(staging_dir))
        part_file = next(staging_dir.glob("part-*.csv"))
        shutil.copyfile(part_file, target_csv_path)
        shutil.rmtree(staging_dir)
        return df

    return _export
