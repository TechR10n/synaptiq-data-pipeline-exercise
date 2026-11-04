"""Move 3: Read Silver Delta tables, perform catalog joins & business/forensic aggregations, save Gold Delta + CSV."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from synaptiq_pipeline.paths import LakehousePaths
from synaptiq_pipeline.spark import checkpoint_parquet, export_single_csv, write_delta_dynamic

TRAP_CATALOG_ROWS: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "TRAP_01_EXTRA_CSV_COLUMN",
        "Bronze -> Silver",
        "CSV Framing",
        "FAILFAST crashes job; DROPMALFORMED drops valid $24.99 order O1011.",
        "Read CSV in PERMISSIVE mode with _corrupt_record; extract trailing token into rescued_extra and retain valid 7 columns.",
    ),
    (
        "TRAP_02_TRUNCATED_CSV_ROW",
        "Bronze -> Silver",
        "CSV Framing",
        "Ragged 6-field line (O2009) misses unit_price and region, crashing strict parsers or leaking NULL grouping keys.",
        "Capture raw line via _corrupt_record, default missing region to 'Unknown', and backfill unit_price from catalog list_price in Gold join.",
    ),
    (
        "TRAP_02B_UNIT_PRICE_IMPUTED_FROM_LIST_PRICE",
        "Silver -> Gold Join",
        "Catalog Price Backfill",
        "NULL unit_price on truncated order O2009 yields NULL line revenue and silently drops $59.98 of Home & Office sales.",
        "Apply COALESCE(orders.unit_price, products.list_price) during Silver-to-Gold product join while flagging imputed rows.",
    ),
    (
        "TRAP_03_CURRENCY_SYMBOL_AND_COMMA",
        "Silver Parsing",
        "Numeric Formatting",
        "Direct CAST('$1,099.00' AS DECIMAL) throws ANSI CAST_INVALID_INPUT or yields NULL; naive comma split shifts columns.",
        "Parse RFC-4180 quotes in CSV reader and strip [$,] via regexp_replace before try_cast('decimal(12,2)').",
    ),
    (
        "TRAP_04_US_SLASH_DATE_FORMAT",
        "Silver Parsing",
        "Date Formatting",
        "Default ISO to_date fails on '01/01/2024' & '01/02/2024'; dd/MM/yyyy misclassifies Jan 2 order O2001 into February.",
        "Parse via deterministic coalesce(try_to_date(yyyy-MM-dd), try_to_date(MM/dd/yyyy), epoch_utc).",
    ),
    (
        "TRAP_05_UNIX_EPOCH_DATE_UTC",
        "Silver Parsing",
        "Date & Timezone",
        "Epoch string '1704153600' fails date cast, or shifts from 2024-01-02 UTC back to 2024-01-01 in US local timezones.",
        "Pin spark.sql.session.timeZone=UTC and convert 10-digit epochs via to_date(timestamp_seconds(try_cast(bigint))).",
    ),
    (
        "TRAP_06_REGION_CASE_DRIFT",
        "Silver Standardization",
        "Dimension Key Casing",
        "'west' and 'east' split Gold aggregations into duplicate lowercase/uppercase region rows.",
        "Normalize via initcap(lower(trim(region))) in Silver orders.",
    ),
    (
        "TRAP_07_CATEGORY_CASE_DRIFT",
        "Silver Standardization",
        "Dimension Key Casing",
        "'electronics' (P002) and 'kitchen' (P006) split Gold category aggregations while 'Home & Office' contains '&'.",
        "Normalize via initcap(lower(trim(category))) in Silver products (preserving 'Home & Office').",
    ),
    (
        "TRAP_08_GUEST_NULL_CUSTOMER",
        "Silver Validation",
        "Nullable Attribute",
        "Over-eager NOT NULL filtering on customer_id discards $80.95 of valid guest-checkout sales (O1004, O2005).",
        "Normalize blank strings to NULL and retain guest orders in Silver and Gold.",
    ),
    (
        "TRAP_09_ZERO_QUANTITY_ORDER",
        "Silver -> Gold Filter",
        "Metric Integrity",
        "Zero-unit order O1008 adds $0 revenue but inflates Stationery/North order_count (1 -> 2), cutting AOV in half.",
        "Classify qty=0 as ZERO_QUANTITY in Silver and exclude qty=0 lines from Gold daily_sales aggregation.",
    ),
    (
        "TRAP_10_EXACT_CROSS_FILE_DUPLICATE",
        "Silver Reconciliation",
        "Idempotency / Replay",
        "Order O1002 ($1,099.00) appears identically in both 01-01 and 01-02 drops; append-only pipelines double-count revenue.",
        "Deduplicate via Window(partitionBy order_id orderBy drop_date DESC) + idempotent Delta MERGE on order_id.",
    ),
    (
        "TRAP_11_LATE_ARRIVING_RESTATEMENT",
        "Silver MERGE & Gold Scope",
        "CDC / Late Update",
        "Order O1003 (order_date 2024-01-01) updates qty 5 -> 6 in 01-02 drop; overwriting only drop_date=01-02 leaves 01-01 stale.",
        "MERGE Silver where src.drop_date >= tgt.drop_date and dynamically recompute Gold for all impacted order_dates.",
    ),
    (
        "TRAP_12_UNMAPPED_PRODUCT_ID",
        "Silver -> Gold Join",
        "Referential Integrity",
        "Orders O1007 (P099) and O2007 (P011) reference SKUs absent from products.csv; INNER JOIN silently loses $49.99.",
        "LEFT JOIN silver_products and COALESCE(category, 'Unknown') so total daily revenue reconciles 100%.",
    ),
    (
        "TRAP_13_NEGATIVE_QUANTITY_RETURN",
        "Gold Aggregation",
        "Net Revenue & AOV Guard",
        "Return O2004 (qty=-1, -$1,099) is the sole row in 2024-01-02/Electronics/West -> divide-by-zero or negative AOV.",
        "Include qty<0 in net_revenue and units_sold; count qty>0 orders in order_count; guard AOV when order_count == 0 (NULL).",
    ),
)

TRAP_CATALOG_SCHEMA = T.StructType(
    [
        T.StructField("trap_code", T.StringType(), False),
        T.StructField("pipeline_stage", T.StringType(), False),
        T.StructField("trap_category", T.StringType(), False),
        T.StructField("naive_failure_mode", T.StringType(), False),
        T.StructField("functional_fix_applied", T.StringType(), False),
    ]
)


def join_orders_with_products(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """LEFT JOIN Silver orders with Silver products, imputing catalog list_price only when unit_price is missing."""
    orders = orders_df.alias("o")
    products = products_df.alias("p")

    is_unmapped_product = F.col("p.product_id").isNull()
    is_price_imputed = F.col("o.unit_price").isNull() & F.col("p.list_price").isNotNull()
    effective_unit_price = F.coalesce(F.col("o.unit_price"), F.col("p.list_price"))

    join_trap_flags = F.filter(
        F.array(
            F.when(is_unmapped_product, F.lit("TRAP_12_UNMAPPED_PRODUCT_ID")),
            F.when(is_price_imputed, F.lit("TRAP_02B_UNIT_PRICE_IMPUTED_FROM_LIST_PRICE")),
        ),
        lambda item: item.isNotNull(),
    )

    return orders.join(products, on="product_id", how="left").select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.order_date").alias("order_date"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("product_id"),
        F.coalesce(F.col("p.product_name"), F.lit("Unmapped Product")).alias("product_name"),
        F.coalesce(F.col("p.category"), F.lit("Unknown")).alias("category"),
        F.col("o.region").alias("region"),
        F.col("o.quantity").alias("quantity"),
        F.col("o.unit_price").alias("order_unit_price"),
        F.col("p.list_price").alias("catalog_list_price"),
        effective_unit_price.alias("effective_unit_price"),
        (F.col("o.quantity") * effective_unit_price).cast("decimal(14,2)").alias("line_net_revenue"),
        F.col("o.order_status").alias("order_status"),
        is_unmapped_product.alias("is_unmapped_product"),
        is_price_imputed.alias("is_price_imputed"),
        F.array_distinct(F.concat(F.col("o.trap_flags"), join_trap_flags)).alias("all_trap_flags"),
        F.col("o.drop_date").alias("drop_date"),
        F.col("o.source_file").alias("source_file"),
    )


def aggregate_daily_sales(joined_facts_df: DataFrame) -> DataFrame:
    """Compute daily sales metrics per (order_date, category, region) with return-safe AOV division guard."""
    net_revenue_expr = F.sum("line_net_revenue").cast("decimal(14,2)")
    order_count_expr = F.count_distinct(
        F.when(F.col("quantity") > 0, F.col("order_id"))
    )
    units_sold_expr = F.sum("quantity").cast("bigint")
    aov_expr = F.round(
        net_revenue_expr / F.when(order_count_expr > 0, order_count_expr),
        2,
    ).cast("decimal(14,2)")

    return (
        joined_facts_df.filter(F.col("quantity") != 0)
        .groupBy("order_date", "category", "region")
        .agg(
            net_revenue_expr.alias("net_revenue"),
            order_count_expr.alias("order_count"),
            units_sold_expr.alias("units_sold"),
            aov_expr.alias("average_order_value"),
        )
    )


def build_trap_forensics_summary(
    spark: SparkSession,
    joined_facts_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Aggregate every detected forensic trap across Silver products and enriched order facts purely in Spark."""
    order_trap_occurrences = joined_facts_df.select(
        F.explode("all_trap_flags").alias("trap_code"),
        F.col("order_id").alias("record_id"),
    )
    product_trap_occurrences = products_df.select(
        F.explode("trap_flags").alias("trap_code"),
        F.col("product_id").alias("record_id"),
    )
    observed_counts = (
        order_trap_occurrences.unionByName(product_trap_occurrences)
        .groupBy("trap_code")
        .agg(
            F.count("*").alias("affected_records"),
            F.concat_ws(", ", F.array_sort(F.collect_set("record_id"))).alias("affected_ids"),
        )
    )

    catalog_df = spark.createDataFrame(TRAP_CATALOG_ROWS, schema=TRAP_CATALOG_SCHEMA)
    return (
        catalog_df.join(observed_counts, on="trap_code", how="left")
        .select(
            F.col("trap_code"),
            F.col("pipeline_stage"),
            F.col("trap_category"),
            F.coalesce(F.col("affected_records"), F.lit(0)).alias("affected_records"),
            F.coalesce(F.col("affected_ids"), F.lit("")).alias("affected_ids"),
            F.col("naive_failure_mode"),
            F.col("functional_fix_applied"),
        )
        .orderBy("trap_code")
    )


def build_gold_outputs(
    spark: SparkSession,
    paths: LakehousePaths,
    *,
    impacted_order_dates_df: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Move 3: Read Silver Delta tables, execute joins & aggregations, and persist Gold Delta + CSV outputs."""
    silver_orders = spark.read.format("delta").load(str(paths.silver_orders))
    silver_products = spark.read.format("delta").load(str(paths.silver_products))

    scoped_orders = (
        silver_orders.join(
            impacted_order_dates_df.select("order_date").distinct(),
            on="order_date",
            how="left_semi",
        )
        if impacted_order_dates_df is not None
        else silver_orders
    )

    # Materialize complete enriched order facts Parquet across Silver for auditability & downstream queries
    full_joined_facts = join_orders_with_products(silver_orders, silver_products).transform(
        checkpoint_parquet(spark, paths.step_04_joined_order_facts_parquet)
    )

    scoped_joined_facts = (
        join_orders_with_products(scoped_orders, silver_products)
        if impacted_order_dates_df is not None
        else full_joined_facts
    )

    # Dynamically overwrite only impacted order_date partitions in Gold Delta, then read full Gold table
    scoped_joined_facts.transform(aggregate_daily_sales).transform(
        write_delta_dynamic(spark, paths.gold_daily_sales, partition_by=("order_date",))
    )

    gold_daily_sales = (
        spark.read.format("delta")
        .load(str(paths.gold_daily_sales))
        .orderBy("order_date", "category", "region")
        .transform(export_single_csv(paths.daily_sales_csv))
    )

    gold_trap_forensics = (
        build_trap_forensics_summary(spark, full_joined_facts, silver_products)
        .transform(write_delta_dynamic(spark, paths.gold_trap_forensics))
        .orderBy("trap_code")
        .transform(export_single_csv(paths.trap_forensics_csv))
    )

    return gold_daily_sales, gold_trap_forensics
