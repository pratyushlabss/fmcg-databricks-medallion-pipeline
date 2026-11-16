# Databricks notebook source
# DBTITLE 1,FMCG Milestone 1: UC Setup & Bronze Ingestion
# MAGIC %md
# MAGIC # FMCG Milestone 1: Unity Catalog Setup & Bronze Ingestion
# MAGIC
# MAGIC Establishes the `fmcg_catalog` namespace (`bronze` / `silver` / `gold` schemas, managed volume `bronze.raw_files`) and incrementally ingests `FMCG_2022_2024.csv` from the volume into `fmcg_catalog.bronze.fmcg_sales_raw` using Auto Loader with schema evolution, a rescued-data column, lineage/audit metadata, and Delta Liquid Clustering on the ingestion date.

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration — paths and object names for Milestone 1
CATALOG = "fmcg_catalog"
BRONZE_SCHEMA = "bronze"

# Landing zone for CSV drops (upload FMCG_2022_2024.csv here)
SOURCE_VOLUME = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_files"

# Pipeline state lives under an underscore-prefixed directory, which Auto Loader
# ignores, so checkpoints/schema never mix with raw drops.
STATE_DIR = f"{SOURCE_VOLUME}/_meta/fmcg_sales_raw"
CHECKPOINT_PATH = f"{STATE_DIR}/checkpoints"
SCHEMA_LOCATION = f"{STATE_DIR}/schema"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.fmcg_sales_raw"

print(f"Landing zone  : {SOURCE_VOLUME}")
print(f"Checkpoint    : {CHECKPOINT_PATH}")
print(f"Schema loc    : {SCHEMA_LOCATION}")
print(f"Target table  : {BRONZE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Namespace Setup (idempotent)
# One-time namespace setup: catalog + medallion schemas + landing volume (safe to re-run)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'FMCG sales analytics workspace'")

for schema, comment in [
    (BRONZE_SCHEMA, "Raw, as-ingested FMCG data with lineage columns"),
    ("silver", "Cleaned and validated FMCG data"),
    ("gold", "Business-ready aggregates and dimensional models"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_files "
    "COMMENT 'Landing zone for raw FMCG CSV drops'"
)

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# DBTITLE 1,Landing Zone Check
# Inspect the landing volume before ingesting
file_entries = dbutils.fs.ls(SOURCE_VOLUME)
drop_files = [f.name for f in file_entries if not f.name.startswith(("_", "."))]

if not drop_files:
    print(f"No CSV files in {SOURCE_VOLUME} yet.")
    print("Upload FMCG_2022_2024.csv via Catalog Explorer:")
    print("  Catalog Explorer > fmcg_catalog > bronze > raw_files > 'Upload to this volume'")
else:
    print(f"{len(drop_files)} drop file(s) ready for ingestion:")
    display(file_entries)

# COMMAND ----------

# DBTITLE 1,Auto Loader Bronze Ingestion
from pyspark.sql import functions as F

# Skip the run when the landing zone has no drops yet (Auto Loader cannot infer
# a schema from an empty directory — CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE).
drop_files = [
    f.name for f in dbutils.fs.ls(SOURCE_VOLUME)
    if not f.name.startswith(("_", "."))
]

if not drop_files:
    print(f"No source files in {SOURCE_VOLUME} yet — skipping ingestion run.")
    print("Upload FMCG_2022_2024.csv to the volume, then re-run this cell.")
else:
    # Auto Loader: incremental, schema-evolving CSV ingestion from the managed volume
    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")            # typed columns instead of all-strings
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # evolve schema as new columns appear
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")   # park malformed/unmapped data for audit
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)      # persisted inferred schema
        .load(SOURCE_VOLUME)
    )

    # Lineage + auditability columns. NOTE: _metadata.file_path is the Spark Connect-safe
    # way to capture the source file (input_file_name() is blocked on serverless compute).
    bronze_df = (
        raw_df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.col("_ingest_timestamp")))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    def write_to_bronze(batch_df, batch_id):
        """Append each micro-batch to the bronze Delta table, syncing any evolved schema."""
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(BRONZE_TABLE)
        )

    query = (
        bronze_df.writeStream
        .foreachBatch(write_to_bronze)
        .option("checkpointLocation", CHECKPOINT_PATH)  # exactly-once processing across runs
        .trigger(availableNow=True)                       # process available files, then stop
        .start()
    )
    query.awaitTermination()
    print("Ingestion run completed (availableNow trigger drained all new files).")

# COMMAND ----------

# DBTITLE 1,Liquid Clustering & Verification
# Enable Delta Liquid Clustering on the ingestion date — better than partitioning for
# incremental multi-file drops (clustering is applied incrementally on write/optimization).
if spark.catalog.tableExists(BRONZE_TABLE):
    spark.sql(f"ALTER TABLE {BRONZE_TABLE} CLUSTER BY (_ingest_date)")

    row_count = spark.table(BRONZE_TABLE).count()
    print(f"Target table    : {BRONZE_TABLE}")
    print(f"Rows ingested   : {row_count}")
    print("\nClustering keys:")
    display(spark.sql(f"DESCRIBE {BRONZE_TABLE}"))
    print("\nSample rows:")
    display(spark.table(BRONZE_TABLE).limit(5))
else:
    print(f"{BRONZE_TABLE} not created yet — no source files were present in the volume.")
    print("Upload FMCG_2022_2024.csv to the landing zone, then re-run the ingestion cell.")