-- =====================================================================
-- 04_validate_gold_integrity.sql
-- Final task of the fmcg_medallion_pipeline Lakeflow Job.
-- Verifies the gold star schema: 1 fact (190,750 rows) + 4 dimensions
-- (dim_date 1,076 / dim_product 90 / dim_channel 3 / dim_region 3)
-- and 0 orphan foreign keys across all 4 dimensions.
--
-- NOTE: the SQL `ASSERT` statement is not available on this workspace's
-- compute, so each gate uses the equivalent lazy-evaluated pattern
--   SELECT raise_error('<message>') WHERE <violation>;
-- which only raises when the violating condition is true, failing the task.
-- =====================================================================

-- 1) Expected row counts: 1 fact + 4 dimensions
SELECT raise_error('fact_fmcg_sales must contain exactly 190750 rows') WHERE (SELECT COUNT(*) FROM fmcg_catalog.gold.fact_fmcg_sales) <> 190750;
SELECT raise_error('dim_date must contain exactly 1076 rows') WHERE (SELECT COUNT(*) FROM fmcg_catalog.gold.dim_date) <> 1076;
SELECT raise_error('dim_product must contain exactly 90 rows') WHERE (SELECT COUNT(*) FROM fmcg_catalog.gold.dim_product) <> 90;
SELECT raise_error('dim_channel must contain exactly 3 rows') WHERE (SELECT COUNT(*) FROM fmcg_catalog.gold.dim_channel) <> 3;
SELECT raise_error('dim_region must contain exactly 3 rows') WHERE (SELECT COUNT(*) FROM fmcg_catalog.gold.dim_region) <> 3;

-- 2) Referential integrity: 0 orphan foreign keys from fact to each dimension
SELECT raise_error('orphan date_sk rows found in fact_fmcg_sales') WHERE EXISTS (SELECT 1 FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_date d ON f.date_sk = d.date_sk WHERE d.date_sk IS NULL);
SELECT raise_error('orphan product_sk rows found in fact_fmcg_sales') WHERE EXISTS (SELECT 1 FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_product p ON f.product_sk = p.product_sk WHERE p.product_sk IS NULL);
SELECT raise_error('orphan channel_sk rows found in fact_fmcg_sales') WHERE EXISTS (SELECT 1 FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_channel c ON f.channel_sk = c.channel_sk WHERE c.channel_sk IS NULL);
SELECT raise_error('orphan region_sk rows found in fact_fmcg_sales') WHERE EXISTS (SELECT 1 FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_region r ON f.region_sk = r.region_sk WHERE r.region_sk IS NULL);

-- 3) Summary result (returned as the task output for auditing)
SELECT 'fact_fmcg_sales row_count' AS check_name, COUNT(*) AS result FROM fmcg_catalog.gold.fact_fmcg_sales
UNION ALL SELECT 'dim_date row_count', COUNT(*) FROM fmcg_catalog.gold.dim_date
UNION ALL SELECT 'dim_product row_count', COUNT(*) FROM fmcg_catalog.gold.dim_product
UNION ALL SELECT 'dim_channel row_count', COUNT(*) FROM fmcg_catalog.gold.dim_channel
UNION ALL SELECT 'dim_region row_count', COUNT(*) FROM fmcg_catalog.gold.dim_region
UNION ALL SELECT 'orphan date_sk', COUNT(*) FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_date d ON f.date_sk = d.date_sk WHERE d.date_sk IS NULL
UNION ALL SELECT 'orphan product_sk', COUNT(*) FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_product p ON f.product_sk = p.product_sk WHERE p.product_sk IS NULL
UNION ALL SELECT 'orphan channel_sk', COUNT(*) FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_channel c ON f.channel_sk = c.channel_sk WHERE c.channel_sk IS NULL
UNION ALL SELECT 'orphan region_sk', COUNT(*) FROM fmcg_catalog.gold.fact_fmcg_sales f LEFT JOIN fmcg_catalog.gold.dim_region r ON f.region_sk = r.region_sk WHERE r.region_sk IS NULL;