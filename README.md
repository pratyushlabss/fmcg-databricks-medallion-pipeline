# FMCG Medallion Pipeline & AI/BI Analytics on Databricks

An end-to-end, production-style FMCG (fast-moving consumer goods) sales & supply-chain analytics platform built entirely on Databricks: a **Lakeflow Job** orchestrates a **medallion architecture** (Bronze → Silver → Gold) into a **Kimball star schema** in Unity Catalog, validated by automated SQL integrity gates, exposed through an **AI/BI Genie space** and an executive **AI/BI dashboard**, with all source code version-controlled in this repository and executed **directly from Git**.

## Architecture

```
                       ┌─────────────────────────────────────────────────────────────────┐
                       │              Lakeflow Job: fmcg_medallion_pipeline             │
                       │        (Git-sourced, serverless compute, run-as owner)          │
│ CSV drop ──────────▶ │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────┴──────┐
  /Volumes/fmcg_      │  │ ingest_bronze│─▶│transform_    │─▶│build_gold_   │─▶│validate_   │
  catalog/bronze/     │  │ (Auto Loader)│  │silver        │  │star_schema   │  │gold_       │
  raw_files/  ──────── ▶  └──────────────┘  └──────────────┘  └──────────────┘  │integrity    │
  (File Arrival        └──────────────────────────────────────────────────────┴──────────────┘
   Trigger)                                                    │
                                                              ▼
   fmcg_catalog.gold:  fact_fmcg_sales | dim_date · dim_product · dim_channel · dim_region
                                                              │
                          ┌───────────────────────────────────┼───────────────────────────┐
                          ▼                                   ▼                            
        AI/BI Genie Space                      AI/BI Executive Dashboard
  "FMCG Sales & Supply Chain Genie"        "FMCG Sales & Supply Chain Executive
  (NL Q&A over the 5 gold tables)           Dashboard" (2 pages, 4 global filters)
```

- **Ingestion trigger:** a File Arrival Trigger watches the managed Unity Catalog volume `/Volumes/fmcg_catalog/bronze/raw_files/` — dropping a new CSV there starts the pipeline automatically.
- **Compute:** all notebook tasks run on Databricks serverless compute; the SQL validation task runs on a serverless SQL warehouse.

## Medallion Layers

| Layer | Table | Rows | Purpose |
| --- | --- | --- | --- |
| Bronze | `fmcg_catalog.bronze.fmcg_sales_raw` | 190,757 | Raw, as-ingested CSV via **Auto Loader** (`cloudFiles`, `inferColumnTypes`, `schemaEvolutionMode=addNewColumns`, `_rescued_data`), append-only with exactly-once checkpoints; lineage columns `_ingest_timestamp/_ingest_date/_source_file`. |
| Silver | `fmcg_catalog.silver.fmcg_sales_clean` | 190,750 | Typed & deduplicated (latest-wins on 7 duplicate grain rows), null imputation (`stock_available`, `delivered_qty` → 0), `promotion_flag` 0/1 → boolean `is_promoted`, derived `gross_revenue` and `fill_rate`; unique grain `(sale_date, sku, channel, region, pack_type)`. |
| Gold | `fmcg_catalog.gold.*` (5 tables) | 190,750 | Kimball star schema — see below. |

## Kimball Star Schema (Gold)

**1 Fact + 4 Dimensions**, all surrogate keys generated with deterministic `xxhash64()`:

| Table | Grain | Rows | Notes |
| --- | --- | --- | --- |
| `fact_fmcg_sales` | sale_date × SKU × channel × region × pack_type | 190,750 | 4 FKs + measures: price_unit, promotion_flag, delivery_days, stock_available, delivered_qty, units_sold, gross_revenue |
| `dim_date` | day | 1,076 | 2022-01-21 → 2024-12-31; year/quarter/month/week/day-of-week/is_weekend |
| `dim_product` | SKU × pack_type | 90 | 30 SKUs × 3 pack types; natural key `sku`, attribute `pack_type` |
| `dim_channel` | channel | 3 | Retail, Discount, E-commerce + derived Offline/Online tier |
| `dim_region` | region | 3 | PL-Central, PL-North, PL-South (Poland) |

### RELY PK/FK Constraints (9)

5 PRIMARY KEYs (one per table) + 4 FOREIGN KEYs on the fact, all declared `RELY` so the Databricks query optimizer and Genie understand join paths automatically:

- `pk_fact_fmcg_sales (sales_sk)` and `pk_dim_* ` on every dimension surrogate key
- `fk_fact_date → dim_date(date_sk)`, `fk_fact_product → dim_product(product_sk)`, `fk_fact_channel → dim_channel(channel_sk)`, `fk_fact_region → dim_region(region_sk)`

Verified: **0 orphan foreign keys** on every dimension.

### Liquid Clustering

| Table | Cluster keys |
| --- | --- |
| `bronze.fmcg_sales_raw` | `_ingest_date` |
| `silver.fmcg_sales_clean` | `sale_date` |
| `gold.fact_fmcg_sales` | `date_sk`, `product_sk` |

## Orchestration — Lakeflow Job (Git-sourced)

The job definition lives in [`resources/fmcg_medallion_pipeline.job.json`](resources/fmcg_medallion_pipeline.job.json) and runs **directly from this repository's `main` branch**:

1. `ingest_bronze` → [`notebooks/01_bronze_ingestion.py`](notebooks/01_bronze_ingestion.py) (Auto Loader, idempotent)
2. `transform_silver` → [`notebooks/02_silver_cleansing.py`](notebooks/02_silver_cleansing.py) (cleanse + dedupe)
3. `build_gold_star_schema` → [`notebooks/03_gold_star_schema.py`](notebooks/03_gold_star_schema.py) (dims + fact, PK/FK constraints)
4. `validate_gold_integrity` → [`sql/04_validate_gold_integrity.sql`](sql/04_validate_gold_integrity.sql) (row-count & orphan-FK gates that **fail the run** on any violation)

**Trigger:** File Arrival on `/Volumes/fmcg_catalog/bronze/raw_files/` — the pipeline runs automatically whenever a new source file lands.

## AI/BI Genie Space & UC Metadata

The gold tables carry full Unity Catalog table/column comments (see [`sql/05_genie_uc_metadata_and_benchmarks.sql`](sql/05_genie_uc_metadata_and_benchmarks.sql)), which power the **“FMCG Sales & Supply Chain Genie”** space for natural-language Q&A over the star schema. The same file contains **6 benchmark queries** (revenue by year, top-10 SKUs, promotional lift by brand, fill rate by channel, revenue by region × category, stockouts by month) used to validate Genie's SQL generation and join paths — all benchmarks pass.

## Verified Results

- End-to-end run from Git: **SUCCESS** (all 4 tasks) in ~2 minutes
- Row counts: fact 190,750 · dim_date 1,076 · dim_product 90 · dim_channel 3 · dim_region 3
- 0 orphans on all 4 FKs · 0 null keys · ingestion idempotent (re-runs append 0 duplicate rows)

## Repository Layout

```text
fmcg-databricks-medallion-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py      # Auto Loader bronze ingestion (Milestone 1)
│   ├── 02_silver_cleansing.py      # Cleanse + dedupe (Milestone 2)
│   └── 03_gold_star_schema.py      # Star schema build + constraints (Milestone 3)
├── sql/
│   ├── 04_validate_gold_integrity.sql     # Job task 4: row-count & orphan-FK gates
│   └── 05_genie_uc_metadata_and_benchmarks.sql  # UC comments + Genie benchmarks
├── resources/
│   └── fmcg_medallion_pipeline.job.json  # Exported Git-sourced Lakeflow Job
├── README.md
└── .gitignore
```

## How to Run

1. Drop an FMCG sales CSV into `/Volumes/fmcg_catalog/bronze/raw_files/` — the **File Arrival Trigger** starts the job automatically; or
2. Run the `fmcg_medallion_pipeline` Lakeflow Job manually from **Jobs & Pipelines** (it clones this repo's `main` branch and executes the four tasks in order).

> The validation task intentionally asserts the current dataset's expected counts (190,750 fact rows). When ingesting *additional* data, adjust the expected row counts in `sql/04_validate_gold_integrity.sql` accordingly.