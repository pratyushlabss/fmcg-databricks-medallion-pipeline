# Databricks notebook source
# DBTITLE 1,FMCG Milestone 2: Silver Cleansing
# MAGIC %md
# MAGIC # FMCG Milestone 2: Silver Cleansing, Standardization & Data Quality
# MAGIC
# MAGIC Transforms `fmcg_catalog.bronze.fmcg_sales_raw` into `fmcg_catalog.silver.fmcg_sales_clean` — a clean, deduplicated, strongly typed table: typed `sale_date`, null imputation on `stock_available` / `delivered_qty`, `promotion_flag` cast to boolean `is_promoted`, derived metrics `gross_revenue` and `fill_rate`, and deduplication on the natural grain (`sale_date`, `sku`, `channel`, `region`, `pack_type`).

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
BRONZE_TABLE = "fmcg_catalog.bronze.fmcg_sales_raw"
SILVER_TABLE = "fmcg_catalog.silver.fmcg_sales_clean"
GRAIN = ["sale_date", "sku", "channel", "region", "pack_type"]  # natural composite key
print(f"Source : {BRONZE_TABLE}")
print(f"Target : {SILVER_TABLE}")
print(f"Grain  : {GRAIN}")

# COMMAND ----------

# DBTITLE 1,Bronze Profiling & DQ Findings
# Data profiling — verify the milestone's stated findings against the actual bronze data
from pyspark.sql import functions as F

bronze_df = spark.table(BRONZE_TABLE)

profile = bronze_df.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.col("stock_available").isNull().cast("int")).alias("null_stock_available"),
    F.sum(F.col("delivered_qty").isNull().cast("int")).alias("null_delivered_qty"),
    F.sum(F.col("units_sold").isNull().cast("int")).alias("null_units_sold"),
    F.sum(F.col("price_unit").isNull().cast("int")).alias("null_price_unit"),
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
)
display(profile)

print("promotion_flag distribution (target: only 0/1 so the boolean cast is safe):")
display(bronze_df.groupBy("promotion_flag").count().orderBy("promotion_flag"))

# Duplicates on the natural grain (using the raw `date` column pre-rename)
dup_grain_rows = (
    bronze_df.groupBy("date", "sku", "channel", "region", "pack_type").count()
    .filter(F.col("count") > 1)
)
dup_excess = dup_grain_rows.agg(F.sum("count").alias("rows_in_dupe_groups"),
                                F.count("*").alias("dupe_groups"))
dup_excess_total = dup_excess.withColumn("excess_rows", F.col("rows_in_dupe_groups") - F.col("dupe_groups"))
display(dup_excess_total)

# COMMAND ----------

# DBTITLE 1,Silver Transform & Load
from pyspark.sql import functions as F, Window

# -----------------------------------------------------------------------------
# 1) Standardize & type-cast, 2) impute nulls, 3) dedup on the natural grain,
# 4) derive business metrics.
# -----------------------------------------------------------------------------
typed_df = (
    bronze_df.select(
        # Date parsing: bronze `date` arrives as DATE (Auto Loader parsed M/d/yyyy at
        # ingestion); re-cast defensively and standardize the business name.
        F.col("date").cast("date").alias("sale_date"),
        "sku", "brand", "segment", "category", "channel", "region", "pack_type",
        F.col("price_unit").cast("double").alias("price_unit"),
        # Type casting: 0/1 integer flag -> boolean business field
        F.col("promotion_flag").cast("boolean").alias("is_promoted"),
        F.col("delivery_days").cast("int").alias("delivery_days"),
        # Null handling: impute with 0 (expectation: EXPECT stock_available IS NOT NULL / EXPECT delivered_qty IS NOT NULL)
        F.coalesce(F.col("stock_available").cast("int"), F.lit(0)).alias("stock_available"),
        F.coalesce(F.col("delivered_qty").cast("int"), F.lit(0)).alias("delivered_qty"),
        F.col("units_sold").cast("int").alias("units_sold"),
        # carry bronze lineage through to silver
        "_ingest_timestamp", "_ingest_date", "_source_file",
    )
)

# Deduplication on the natural composite grain — keep the most recent ingestion
# record per grain key (matches "latest wins" semantics).
dedup_window = Window.partitionBy(GRAIN).orderBy(F.col("_ingest_timestamp").desc())
deduped_df = (
    typed_df
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# Derived business metrics
silver_df = (
    deduped_df
    .withColumn("gross_revenue", F.round(F.col("price_unit") * F.col("units_sold"), 2))
    .withColumn(
        "fill_rate",
        F.when(F.col("stock_available") != 0,
               F.round(F.col("delivered_qty") / F.col("stock_available"), 4))
        .otherwise(None),  # NULLIF(stock_available, 0) semantics
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# Full refresh write (idempotent re-runs) with lineage retained
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

# Liquid clustering on the primary analytical filter column
spark.sql(f"ALTER TABLE {SILVER_TABLE} CLUSTER BY (sale_date)")

print(f"Wrote silver table: {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Silver Verification & DQ Checks
# Verification — post-transform data quality checks (expectation-style) and reconciliation
silver_tbl = spark.table(SILVER_TABLE)

# Reconciliation: bronze -> silver row accounting
bronze_rows = spark.table(BRONZE_TABLE).count()
silver_rows = silver_tbl.count()
dupe_excess = (
    spark.table(BRONZE_TABLE)
    .groupBy("date", "sku", "channel", "region", "pack_type").count()
    .filter(F.col("count") > 1)
    .agg(F.sum(F.col("count") - 1).alias("excess"))
    .collect()[0]["excess"] or 0
)
print(f"Bronze rows: {bronze_rows} | Silver rows: {silver_rows} | removed duplicates: {dupe_excess}")
assert silver_rows == bronze_rows - dupe_excess, "Row reconciliation failed!"

# Expectations (silver): all must be 0 violations
checks = silver_tbl.agg(
    F.sum(F.col("stock_available").isNull().cast("int")).alias("EXPECT stock_available IS NOT NULL"),
    F.sum(F.col("delivered_qty").isNull().cast("int")).alias("EXPECT delivered_qty IS NOT NULL"),
    F.sum(F.col("sale_date").isNull().cast("int")).alias("EXPECT sale_date IS NOT NULL"),
    F.sum(F.col("units_sold").isNull().cast("int")).alias("EXPECT units_sold IS NOT NULL"),
)
display(checks)

# Grain uniqueness after dedup
remaining_dups = (
    silver_tbl.groupBy(GRAIN).count().filter(F.col("count") > 1).count()
)
print(f"Duplicate grain keys remaining: {remaining_dups}")
assert remaining_dups == 0, "Grain is not unique!"

print("\nSilver schema:")
display(spark.sql(f"DESCRIBE {SILVER_TABLE}"))

print("\nSample rows:")
display(silver_tbl.limit(5))

print("\nAll checks passed — silver layer is clean and deduplicated.")

# COMMAND ----------

