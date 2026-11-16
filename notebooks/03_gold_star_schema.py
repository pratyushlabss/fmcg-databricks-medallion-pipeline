# Databricks notebook source
# DBTITLE 1,FMCG Milestone 3: Gold Star Schema
# MAGIC %md
# MAGIC # FMCG Milestone 3: Gold Star Schema Modeling
# MAGIC
# MAGIC De-normalizes `fmcg_catalog.silver.fmcg_sales_clean` into a Kimball star schema in `fmcg_catalog.gold`: `dim_date`, `dim_product`, `dim_channel`, `dim_region`, and `fact_fmcg_sales`.
# MAGIC
# MAGIC **Surrogate keys** use `xxhash64()` (deterministic — stable across full refreshes).
# MAGIC
# MAGIC **Grain note (from profiling):** each of the 30 SKUs is sold in all 3 pack types (90 sku × pack_type combinations), while brand/segment/category are fixed per SKU. `dim_product` therefore has grain `(sku, pack_type)` with `sku` as the natural key and `pack_type` as a product attribute, and `fact_fmcg_sales` joins on both. The fact grain `(sale_date, sku, channel, region, pack_type)` matches the silver natural grain.
# MAGIC
# MAGIC Applies `CLUSTER BY (date_sk, product_sk)` on the fact and declares `PRIMARY KEY` / `FOREIGN KEY ... RELY` constraints so the Databricks optimizer and Genie understand join paths automatically.

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
SILVER_TABLE = "fmcg_catalog.silver.fmcg_sales_clean"
GOLD = "fmcg_catalog.gold"

DIM_DATE = f"{GOLD}.dim_date"
DIM_PRODUCT = f"{GOLD}.dim_product"
DIM_CHANNEL = f"{GOLD}.dim_channel"
DIM_REGION = f"{GOLD}.dim_region"
FACT_SALES = f"{GOLD}.fact_fmcg_sales"

print(f"Source : {SILVER_TABLE}")
print(f"Dims   : {DIM_DATE}, {DIM_PRODUCT}, {DIM_CHANNEL}, {DIM_REGION}")
print(f"Fact   : {FACT_SALES}")

# COMMAND ----------

# DBTITLE 1,Build Dimension Tables
from pyspark.sql import functions as F

silver = spark.table(SILVER_TABLE)

# ---------------------------------------------------------------------------
# dim_date — one row per sale_date (grain: day)
# ---------------------------------------------------------------------------
dim_date_df = (
    silver.select("sale_date").distinct()
    .withColumn("date_sk", F.xxhash64("sale_date"))
    .withColumn("year", F.year("sale_date"))
    .withColumn("quarter", F.quarter("sale_date"))
    .withColumn("month", F.month("sale_date"))
    .withColumn("month_name", F.date_format("sale_date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("sale_date"))
    .withColumn("day_of_week", F.date_format("sale_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("sale_date").isin(1, 7))  # Sat=7, Sun=1
    .select("date_sk", "sale_date", "year", "quarter", "month", "month_name",
            "week_of_year", "day_of_week", "is_weekend")
)

# ---------------------------------------------------------------------------
# dim_product — grain (sku, pack_type): each SKU sells in all 3 pack types,
# while brand/segment/category are fixed per SKU.
# ---------------------------------------------------------------------------
dim_product_df = (
    silver.select("sku", "brand", "segment", "category", "pack_type").distinct()
    .withColumn("product_sk", F.xxhash64("sku", "pack_type"))
    .select("product_sk", "sku", "brand", "segment", "category", "pack_type")
)

# ---------------------------------------------------------------------------
# dim_channel — grain (channel); channel_tier is a derived grouping
# ---------------------------------------------------------------------------
dim_channel_df = (
    silver.select(F.col("channel").alias("channel_name")).distinct()
    .withColumn(
        "channel_tier",
        F.when(F.col("channel_name").isin("Retail", "Discount"), "Offline")
         .otherwise("Online"),
    )
    .withColumn("channel_sk", F.xxhash64("channel_name"))
    .select("channel_sk", "channel_name", "channel_tier")
)

# ---------------------------------------------------------------------------
# dim_region — grain (region); country is constant (Poland)
# ---------------------------------------------------------------------------
dim_region_df = (
    silver.select(F.col("region").alias("region_code")).distinct()
    .withColumn("country", F.lit("Poland"))
    .withColumn("region_sk", F.xxhash64("region_code"))
    .select("region_sk", "region_code", "country")
)

for table_name, df in [
    (DIM_DATE, dim_date_df),
    (DIM_PRODUCT, dim_product_df),
    (DIM_CHANNEL, dim_channel_df),
    (DIM_REGION, dim_region_df),
]:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Wrote {table_name}: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Fact Table
# ---------------------------------------------------------------------------
# fact_fmcg_sales — grain (sale_date, sku, channel, region, pack_type), one row
# per silver row, with surrogate keys resolved via natural-key joins to the dims.
# ---------------------------------------------------------------------------
fact_df = (
    silver
    .join(dim_date_df.select("date_sk", "sale_date"), "sale_date")
    .join(dim_product_df.select("product_sk", "sku", "pack_type"), ["sku", "pack_type"])
    .join(dim_channel_df.select("channel_sk", "channel_name"),
          silver["channel"] == dim_channel_df["channel_name"])
    .join(dim_region_df.select("region_sk", "region_code"),
          silver["region"] == dim_region_df["region_code"])
    .select(
        F.xxhash64(
            F.concat_ws("|",
                        F.col("sale_date").cast("string"),
                        F.col("sku"), F.col("channel"), F.col("region"), F.col("pack_type"))
        ).alias("sales_sk"),
        "date_sk", "product_sk", "channel_sk", "region_sk",
        "price_unit",
        F.col("is_promoted").alias("promotion_flag"),
        "delivery_days", "stock_available", "delivered_qty", "units_sold",
        "gross_revenue",
    )
)

(
    fact_df.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(FACT_SALES)
)

# Optimization: Liquid Clustering on the fact's dominant co-filtering columns
spark.sql(f"ALTER TABLE {FACT_SALES} CLUSTER BY (date_sk, product_sk)")

print(f"Wrote {FACT_SALES}: {fact_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Declare PK/FK Constraints (RELY)
# Declare PRIMARY KEY / FOREIGN KEY constraints (RELY) so the Databricks query
# optimizer and Genie understand join paths. Idempotent: skips existing constraints.
existing_constraints = {
    r["constraint_name"]
    for r in spark.sql(
        "SELECT constraint_name FROM fmcg_catalog.information_schema.table_constraints "
        "WHERE table_schema = 'gold'"
    ).collect()
}

def add_constraint(name: str, ddl: str):
    if name in existing_constraints:
        print(f"  {name}: already exists — skipped")
    else:
        spark.sql(ddl)
        print(f"  {name}: added")

# Enforce NOT NULL on dimension surrogate keys first — UC rejects PRIMARY KEY
# constraints on nullable columns.
for dim, sk in [
    (DIM_DATE, "date_sk"), (DIM_PRODUCT, "product_sk"),
    (DIM_CHANNEL, "channel_sk"), (DIM_REGION, "region_sk"),
    (FACT_SALES, "sales_sk"),
]:
    spark.sql(f"ALTER TABLE {dim} ALTER COLUMN {sk} SET NOT NULL")
    print(f"  {dim}.{sk}: SET NOT NULL")

add_constraint(
    "pk_dim_date",
    f"ALTER TABLE {DIM_DATE} ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_sk) RELY",
)
add_constraint(
    "pk_dim_product",
    f"ALTER TABLE {DIM_PRODUCT} ADD CONSTRAINT pk_dim_product PRIMARY KEY (product_sk) RELY",
)
add_constraint(
    "pk_dim_channel",
    f"ALTER TABLE {DIM_CHANNEL} ADD CONSTRAINT pk_dim_channel PRIMARY KEY (channel_sk) RELY",
)
add_constraint(
    "pk_dim_region",
    f"ALTER TABLE {DIM_REGION} ADD CONSTRAINT pk_dim_region PRIMARY KEY (region_sk) RELY",
)
add_constraint(
    "pk_fact_fmcg_sales",
    f"ALTER TABLE {FACT_SALES} ADD CONSTRAINT pk_fact_fmcg_sales PRIMARY KEY (sales_sk) RELY",
)
add_constraint(
    "fk_fact_date",
    f"ALTER TABLE {FACT_SALES} ADD CONSTRAINT fk_fact_date FOREIGN KEY (date_sk) "
    f"REFERENCES {DIM_DATE} (date_sk) RELY",
)
add_constraint(
    "fk_fact_product",
    f"ALTER TABLE {FACT_SALES} ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) "
    f"REFERENCES {DIM_PRODUCT} (product_sk) RELY",
)
add_constraint(
    "fk_fact_channel",
    f"ALTER TABLE {FACT_SALES} ADD CONSTRAINT fk_fact_channel FOREIGN KEY (channel_sk) "
    f"REFERENCES {DIM_CHANNEL} (channel_sk) RELY",
)
add_constraint(
    "fk_fact_region",
    f"ALTER TABLE {FACT_SALES} ADD CONSTRAINT fk_fact_region FOREIGN KEY (region_sk) "
    f"REFERENCES {DIM_REGION} (region_sk) RELY",
)

# COMMAND ----------

# DBTITLE 1,Star Schema Verification
# Verification — star schema integrity checks
silver_rows = spark.table(SILVER_TABLE).count()

print("Row counts & surrogate key uniqueness (hash collisions would break uniqueness):")
for table_name in [DIM_DATE, DIM_PRODUCT, DIM_CHANNEL, DIM_REGION, FACT_SALES]:
    tbl = spark.table(table_name)
    n, n_distinct_sk = tbl.count(), tbl.select(
        [c for c in tbl.columns if c.endswith("_sk")][0]
    ).distinct().count()
    status = "OK" if n == n_distinct_sk else "COLLISION!"
    print(f"  {table_name}: rows={n}, distinct_sk={n_distinct_sk} [{status}]")

assert spark.table(FACT_SALES).count() == silver_rows, "Fact rows must match silver rows"

print("\nReferential integrity (orphan foreign keys — all must be 0):")
fact = spark.table(FACT_SALES)
for fk_col, dim in [
    ("date_sk", DIM_DATE), ("product_sk", DIM_PRODUCT),
    ("channel_sk", DIM_CHANNEL), ("region_sk", DIM_REGION),
]:
    sk_col = [c for c in spark.table(dim).columns if c.endswith("_sk")][0]
    # Rename both sides to a common key to avoid cross-DataFrame column references
    dim_sks = spark.table(dim).select(F.col(sk_col).alias("sk"))
    orphans = (
        fact.select(F.col(fk_col).alias("sk"))
        .join(dim_sks, "sk", "left_anti")
        .count()
    )
    assert orphans == 0, f"Orphan {fk_col}: {orphans}"
    print(f"  {fk_col} -> {dim}: 0 orphans")

print("\nDimension attribute values:")
display(spark.table(DIM_CHANNEL).orderBy("channel_sk"))
display(spark.table(DIM_REGION).orderBy("region_sk"))
print(f"Categories in dim_product: "
      f"{sorted([r[0] for r in spark.table(DIM_PRODUCT).select('category').distinct().collect()])}")
print(f"Pack types in dim_product: "
      f"{sorted([r[0] for r in spark.table(DIM_PRODUCT).select('pack_type').distinct().collect()])}")

print("\nFact sample (with surrogate keys):")
display(fact.limit(5))

print("\nSample star join (fact + dimensions — validates join paths end-to-end):")
display(
    fact.join(spark.table(DIM_DATE), "date_sk")
        .join(spark.table(DIM_PRODUCT), "product_sk")
        .join(spark.table(DIM_CHANNEL), "channel_sk")
        .join(spark.table(DIM_REGION), "region_sk")
        .groupBy("channel_name", "region_code")
        .agg(F.round(F.sum("gross_revenue"), 2).alias("total_gross_revenue"),
             F.sum("units_sold").alias("total_units_sold"))
        .orderBy("channel_name", "region_code")
)

print("\nAll star schema checks passed.")

# COMMAND ----------

