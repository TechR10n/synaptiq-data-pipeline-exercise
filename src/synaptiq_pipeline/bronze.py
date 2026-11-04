"""Move 1: Raw CSV ingestion to Bronze Delta tables with permissive corrupt-record capture."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from synaptiq_pipeline.paths import LakehousePaths
from synaptiq_pipeline.spark import write_delta_dynamic

RAW_ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), True),
        T.StructField("order_date", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("quantity", T.StringType(), True),
        T.StructField("unit_price", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)

RAW_PRODUCTS_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("list_price", T.StringType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)


def attach_order_file_metadata(df: DataFrame) -> DataFrame:
    """Attach source filename, extracted drop_date, ingestion timestamp, and materialized corrupt_record."""
    extracted_drop_date = F.try_to_date(
        F.regexp_extract(F.col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1),
        "yyyy-MM-dd",
    )
    return df.select(
        F.col("order_id"),
        F.col("order_date"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("region"),
        F.col("_corrupt_record").alias("corrupt_record"),
        F.col("source_file"),
        F.coalesce(extracted_drop_date, F.current_date()).alias("drop_date"),
        F.current_timestamp().alias("ingested_at"),
    )


def attach_product_file_metadata(df: DataFrame) -> DataFrame:
    """Attach source filename, ingestion timestamp, and materialized corrupt_record to raw products."""
    return df.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("list_price"),
        F.col("_corrupt_record").alias("corrupt_record"),
        F.col("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def read_raw_orders_csv(spark: SparkSession, order_csv_paths: Sequence[Path | str]) -> DataFrame:
    """Lazily read one or more daily order CSV drops in PERMISSIVE mode without dropping malformed rows."""
    resolved_paths = list(map(lambda p: str(Path(p).resolve()), order_csv_paths))
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_ORDERS_SCHEMA)
        .csv(resolved_paths)
        .select("*", F.col("_metadata.file_name").alias("source_file"))
        .transform(attach_order_file_metadata)
    )


def read_raw_products_csv(spark: SparkSession, products_csv_path: Path | str) -> DataFrame:
    """Lazily read the product reference CSV in PERMISSIVE mode."""
    resolved_path = str(Path(products_csv_path).resolve())
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_PRODUCTS_SCHEMA)
        .csv(resolved_path)
        .select("*", F.col("_metadata.file_name").alias("source_file"))
        .transform(attach_product_file_metadata)
    )


def ingest_orders_to_bronze(
    spark: SparkSession,
    paths: LakehousePaths,
    order_csv_paths: Sequence[Path | str],
) -> DataFrame:
    """Move 1a: Read raw daily order CSV drop(s) and save idempotently to Bronze Delta partitioned by drop_date."""
    return read_raw_orders_csv(spark, order_csv_paths).transform(
        write_delta_dynamic(spark, paths.bronze_orders, partition_by=("drop_date",))
    )


def ingest_products_to_bronze(
    spark: SparkSession,
    paths: LakehousePaths,
    products_csv_path: Path | str,
) -> DataFrame:
    """Move 1b: Read raw products CSV and save idempotently to Bronze Delta."""
    return read_raw_products_csv(spark, products_csv_path).transform(
        write_delta_dynamic(spark, paths.bronze_products)
    )
