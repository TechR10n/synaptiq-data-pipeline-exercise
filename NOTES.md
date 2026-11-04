# NOTES.md — Daily Retail Sales Pipeline (Functional PySpark + Delta Lake)

## 1. Quick Start & Reproduction

Requires Python `3.12` (pinned in `.python-version`), `uv`, and a Java 17/21 runtime.

```bash
# Install dependencies (pyspark 4.2 + delta-spark 4.4 + pytest)
uv sync

# Simulate sequential daily schedule: Day 1 drop, then Day 2 drop
uv run synaptiq-pipeline --reset --orders orders_2024-01-01.csv --products products.csv
uv run synaptiq-pipeline --orders orders_2024-01-02.csv --products products.csv

# Run automated verification suite (AST zero-loop/zero-UDF guard, CDC restatement, trap fixes)
uv run pytest -v
```

Exported deliverables:
- **Gold Output Table:** [`output/daily_sales.csv`](output/daily_sales.csv) & `warehouse/gold/daily_sales/` (Delta)
- **Forensic Trap Audit:** [`output/trap_forensics.csv`](output/trap_forensics.csv) & `warehouse/gold/trap_forensics/` (Delta)
- **Executive & Forensic Slide Deck:** [`presentation/SYNAPTIQ_PIPELINE_PRESENTATION.pptx`](presentation/SYNAPTIQ_PIPELINE_PRESENTATION.pptx), [`presentation/SYNAPTIQ_PIPELINE_PRESENTATION.pdf`](presentation/SYNAPTIQ_PIPELINE_PRESENTATION.pdf), and [`presentation/SYNAPTIQ_PIPELINE_PRESENTATION.md`](presentation/SYNAPTIQ_PIPELINE_PRESENTATION.md)

---

## 2. Architecture: Three-Move Functional Medallion Pipeline

The pipeline is structured strictly as **pure `DataFrame -> DataFrame` transformations** composed via `DataFrame.transform(...)`, Spark higher-order array expressions (`F.filter`), and lazy `Window` specifications—with **zero Python loops/comprehensions and zero Spark UDFs** (enforced by an AST static test in `tests/test_pipeline.py`) so all execution compiles to native Catalyst JVM plans:

1. **Move 1 — Raw CSV to Bronze Delta (`src/synaptiq_pipeline/bronze.py`):**
   - Reads daily `orders_*.csv` drops and `products.csv` in `PERMISSIVE` mode with `columnNameOfCorruptRecord="_corrupt_record"`, preserving raw strings alongside `source_file` (`_metadata.file_name`) and `drop_date` extracted from the filename.
   - Writes `warehouse/bronze/orders` partitioned by `drop_date` with `partitionOverwriteMode="dynamic"` so re-running any daily file atomically replaces only that drop's raw partition without duplicating rows.
2. **Move 2 — Bronze to Cleaned & Reconciled Silver Delta + Parquet Checkpoints (`src/synaptiq_pipeline/silver.py`):**
   - Cleans `products` and `orders` independently (no cross-entity joins in Silver), materializing intermediate enrichment steps as inspectable Parquet datasets (`warehouse/intermediate/01_products_cleaned.parquet`, `01_orders_parsed_fields.parquet`, `02_orders_row_traps.parquet`, `03_orders_reconciled.parquet`, `03_orders_cdc_superseded.parquet`).
   - Reconciles cross-file duplicates and restatements via `Window.partitionBy("order_id").orderBy(F.col("drop_date").desc())` and upserts into `warehouse/silver/orders` and `warehouse/silver/products` via `DeltaTable.merge()` gated by `source.drop_date >= target.drop_date` (safe even under out-of-order historical replays).
3. **Move 3 — Silver Joins & Aggregations to Gold Delta (`src/synaptiq_pipeline/gold.py`):**
   - Reads `silver/orders` and `silver/products` from Delta, performs a `LEFT JOIN` on `product_id`, materializes `warehouse/intermediate/04_joined_order_facts.parquet`, dynamically overwrites only **impacted `order_date` partitions** in `warehouse/gold/daily_sales`, and exports `output/daily_sales.csv` and `output/trap_forensics.csv`.

---

## 3. Data Anomalies Discovered & How They Shaped the Design

Auditing all 24 raw order lines and 10 product lines uncovered **13 distinct data traps** (tracked as 14 pipeline audit codes in `output/trap_forensics.csv`):

| Trap Code | Raw Evidence | Impact on Naive Pipeline | Resolution & Engineering Rationale |
| :--- | :--- | :--- | :--- |
| `TRAP_01_EXTRA_CSV_COLUMN` | `O1011` (`01-01` L12) has 8 fields (`...,East,EXTRA_FLAG`) | `FAILFAST` aborts job; `DROPMALFORMED` drops valid `$24.99` order | Read `PERMISSIVE` + `_corrupt_record`; extract trailing field into `rescued_extra` via quote-aware regex and retain the 7 valid columns. |
| `TRAP_02_TRUNCATED_CSV_ROW` & `TRAP_02B_UNIT_PRICE_IMPUTED` | `O2009` (`01-02` L12): `O2009,2024-01-02,C019,P004,2,` (missing `unit_price` & `region`) | Strict parsers crash; `NULL` price drops `$59.98` of sales; `NULL` region breaks grouping | Default missing `region` to `'Unknown'` in Silver; backfill missing `unit_price` via `COALESCE(orders.unit_price, products.list_price)` (`$29.99` for `P004`, matching 100% of other rows where `unit_price == list_price`). |
| `TRAP_03_CURRENCY_SYMBOL_AND_COMMA` | `O1001` (`$24.99`), `O1002` & `O2004` (`"$1,099.00"`) | Spark 4 ANSI `CAST` throws `CAST_INVALID_INPUT`; naive comma split shifts columns | Quote-aware CSV parsing + `regexp_replace(trim(unit_price), '[$,]', '')` before `try_cast('decimal(12,2)')` (exact base-10 arithmetic). |
| `TRAP_04_US_SLASH_DATE_FORMAT` | `O1012` (`01/01/2024`), `O2001` (`01/02/2024`) | ISO-only `to_date` yields `NULL`; `dd/MM/yyyy` misplaces `O2001` into Feb 1 | `O2001` (`O2xxx` ID in Jan 2 drop) proves US `MM/dd/yyyy`. Parsed via `coalesce(try_to_date(iso), try_to_date(MM/dd/yyyy), epoch_utc)`. |
| `TRAP_05_UNIX_EPOCH_DATE_UTC` | `O2003` (`1704153600` = `2024-01-02T00:00:00Z`) | Fails date cast, or shifts back to `2024-01-01` in US local timezones | Pin `spark.sql.session.timeZone = UTC` and convert 10-digit epochs via `to_date(timestamp_seconds(try_cast(bigint)))`. |
| `TRAP_06_REGION_CASE_DRIFT` & `TRAP_07_CATEGORY_CASE_DRIFT` | `west` (`O1005`), `east` (`O2006`); `electronics` (`P002`), `kitchen` (`P006`) | Splits Gold aggregates across duplicate uppercase/lowercase groups | Normalize with `initcap(lower(trim(...)))` in Silver (which also preserves `'Home & Office'` cleanly because `&` is space-delimited). |
| `TRAP_08_GUEST_NULL_CUSTOMER` | `O1004` & `O2005` have blank `customer_id` | Blanket `dropna()` / `NOT NULL` checks discard `$80.95` of valid guest sales | Normalize `""` to `NULL` and retain in Silver and Gold (`customer_id` is not a Gold grouping key). |
| `TRAP_09_ZERO_QUANTITY_ORDER` | `O1008` (`qty = 0`, `$9.99`, `North`) | Adds `$0` revenue while inflating `2024-01-01 / Stationery / North` `order_count` (`1 → 2`), halving AOV | Tag as `ZERO_QUANTITY` in Silver for auditability and exclude `quantity == 0` lines from Gold `daily_sales`. |
| `TRAP_10_EXACT_CROSS_FILE_DUPLICATE` | `O1002` (`$1,099.00`) appears identically in `01-01` and `01-02` drops | Append-only pipelines double-count `$1,099.00` on `2024-01-01` | Window deduplication + idempotent `DeltaTable.merge()` on `order_id`; superseded copy archived in `silver/orders_cdc_superseded`. |
| `TRAP_11_LATE_ARRIVING_RESTATEMENT` | `O1003` (`order_date = 2024-01-01`) updates `quantity` from `5` to `6` inside `orders_2024-01-02.csv` | Overwriting Gold only for `drop_date = 2024-01-02` leaves `2024-01-01` stale at `$17.50` instead of `$21.00` | `MERGE` Silver where `source.drop_date >= target.drop_date`, then identify all `order_date`s touched by the landed drop (`left_semi` join) and dynamically overwrite both `2024-01-01` and `2024-01-02` in Gold. |
| `TRAP_12_UNMAPPED_PRODUCT_ID` | `O1007` (`P099`) & `O2007` (`P011`, sequential new SKU lagging `products.csv`) | `INNER JOIN` silently drops `$49.99` of real sales | `LEFT JOIN` to `silver/products` with `COALESCE(category, 'Unknown')` so finance totals reconcile 100% while surfacing catalog lag. |
| `TRAP_13_NEGATIVE_QUANTITY_RETURN` | `O2004` (`qty = -1`, `-$1,099.00`) is the **sole** row in `(2024-01-02, Electronics, West)` | Explains why spec asks for **net revenue**; naive `net_revenue / order_count` divides by zero or reports negative AOV on 1 "order" | Include `qty < 0` in `net_revenue` (`-1099.00`) and net `units_sold` (`-1`); count only positive orders (`qty > 0`) in `order_count` (`0`); guard `average_order_value` to `NULL` when `order_count == 0`. |

*(Note: `data_dictionary.md` line 34 references `candidate_brief.md`, which corresponds to `candidate_exercise.md`.)*

---

## 4. Key Assumptions & Judgement Trade-offs

1. **Order Grain (`order_id` as Primary Key):** Every `order_id` in both drops represents a single-SKU order (`O1003` keeps `P003` when its quantity updates `5 → 6`). If upstream evolved to multi-line orders sharing one `order_id`, Silver's merge key would widen to `(order_id, line_id)` (or `(order_id, product_id)`), while Gold's `order_count` (`COUNT(DISTINCT CASE WHEN quantity > 0 THEN order_id END)`) already handles multi-line orders without double-counting an order within a category/region bucket. Note that if a multi-line order spanned *multiple* categories, category-level AOV (`category_net_revenue / distinct_orders_in_category`) measures average category spend per order rather than whole-basket AOV—worth confirming with analysts.
2. **Truncated Row `O2009` (Impute + `'Unknown'` vs. Hard Quarantine):** Because every matched row in the dataset sells at exact `products.list_price` (`0.00` variance) and `products.csv` explicitly supplies `list_price`, backfilling `O2009`'s missing `unit_price` from `P004` (`$29.99`) and assigning `region = 'Unknown'` preserves `$59.98` of real revenue visibly rather than hiding it. If promotional discounting were common in production, `O2009` should instead be routed to a hard quarantine table pending upstream re-drop.
3. **Returns (`quantity < 0`) vs. Cancellations (`quantity = 0`):** The business prompt specifically requests **net revenue** despite having no discount or tax columns—pointing directly to `O2004` (`quantity = -1`) as a return/refund booked on `2024-01-02`. Counting only positive purchase orders (`quantity > 0`) in `order_count` and returning `NULL` AOV when `order_count == 0` avoids division-by-zero and avoids distorting AOV with negative denominators.

---

## 5. Final Output Table (`output/daily_sales.csv`)

| `order_date` | `category` | `region` | `net_revenue` | `order_count` | `units_sold` | `average_order_value` |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| `2024-01-01` | Accessories | South | `49.95` | `1` | `1` | `49.95` |
| `2024-01-01` | Electronics | East | `74.97` | `2` | `3` | `37.49` |
| `2024-01-01` | Electronics | West | `2287.90` | `3` | `12` | `762.63` |
| `2024-01-01` | Home & Office | West | `29.99` | `1` | `1` | `29.99` |
| `2024-01-01` | Kitchen | East | `36.00` | `1` | `3` | `36.00` |
| `2024-01-01` | Kitchen | South | `30.00` | `1` | `4` | `30.00` |
| `2024-01-01` | Stationery | North | `21.00` | `1` | `6` | `21.00` |
| `2024-01-01` | Unknown | East | `30.00` | `1` | `2` | `30.00` |
| `2024-01-02` | Accessories | West | `49.95` | `1` | `1` | `49.95` |
| `2024-01-02` | Electronics | East | `74.97` | `1` | `3` | `74.97` |
| `2024-01-02` | Electronics | North | `44.95` | `1` | `5` | `44.95` |
| `2024-01-02` | Electronics | West | `-1099.00` | `0` | `-1` | *(NULL)* |
| `2024-01-02` | Home & Office | Unknown | `59.98` | `1` | `2` | `59.98` |
| `2024-01-02` | Kitchen | South | `24.00` | `1` | `2` | `24.00` |
| `2024-01-02` | Kitchen | West | `22.50` | `1` | `3` | `22.50` |
| `2024-01-02` | Stationery | East | `46.96` | `2` | `6` | `23.48` |
| `2024-01-02` | Unknown | South | `19.99` | `1` | `1` | `19.99` |

---

## 6. Production Scaling & How This Maps to Databricks / Delta Lake

While this local `uv` project already uses real **PySpark 4.2 + Delta Lake 4.4** (`MERGE INTO`, dynamic partition overwrite, ANSI mode, UTC session pinning), taking this to production on **Databricks Unity Catalog** at 100M+ rows/day would swap local filesystem mechanics for managed Lakehouse primitives:

1. **Ingestion (Auto Loader / Lakeflow Connect vs. Local CSV Glob):**
   - Replace explicit file-list CLI arguments with **Databricks Auto Loader (`cloudFiles`)** landing from S3/ADLS/GCS Unity Catalog External Locations or Volumes, using `cloudFiles.schemaEvolutionMode = "rescue"` so extra trailing columns like `O1011`'s `EXTRA_FLAG` land natively in `_rescued_data` JSON with RocksDB checkpointed exactly-once file state.
2. **Silver Incremental Windowing & SCD Type 2 Dimensions:**
   - Instead of scanning all historical Bronze partitions during Window deduplication, stream incremental Bronze batches via Structured Streaming / **Lakeflow Spark Declarative Pipelines (`APPLY CHANGES INTO` / `AUTO CDC`)** sequenced by `(drop_date, ingested_at)` directly into `catalog.silver.orders`.
   - Convert `products` into an **SCD Type 2** dimension (`effective_from`, `effective_to`, `is_current`) so historical orders join to the product category and list price active on `order_date` rather than retroactively shifting historical revenue when a product changes category or price.
   - Trigger an automated backfill/re-enrichment of `'Unknown'` category orders (`P099`, `P011`) when late product dimension drops arrive.
3. **Storage Layout, Constraints & Governance:**
   - Govern tables under **Unity Catalog** (`retail_prod.{bronze,silver,gold}`), enable **Liquid Clustering** (`CLUSTER BY (order_date, category, region)` on Gold; `CLUSTER BY (order_id, order_date)` on Silver) instead of static Hive directory partitioning to avoid small-file amplification, and attach Delta `CHECK` constraints (`order_id IS NOT NULL`, `order_date IS NOT NULL`) plus Lakeflow Expectations (`@dlt.expect_or_drop` / quarantine routing) with alerting when quarantine or `'Unknown'` SKU rates breach SLA thresholds.
4. **Orchestration:**
   - Package as a **Databricks Asset Bundle (`databricks.yml`)** triggered by a **Workflows File Arrival Trigger** on the landing Volume/bucket rather than a fixed cron clock, eliminating race conditions when daily upstream drops arrive late.
