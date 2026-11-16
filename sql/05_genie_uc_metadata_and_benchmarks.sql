-- =====================================================================
-- 05_genie_uc_metadata_and_benchmarks.sql
-- Unity Catalog table/column comments for the gold star schema (the
-- metadata layer consumed by the "FMCG Sales & Supply Chain" Genie space)
-- plus 6 Genie benchmark queries with their expected answers.
-- All statements are idempotent (COMMENT ON, plain SELECTs) — safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- PART 1: UNITY CATALOG TABLE & COLUMN COMMENTS
-- ---------------------------------------------------------------------

-- ---------- fmcg_catalog.gold.fact_fmcg_sales (1 fact) ----------
COMMENT ON TABLE fmcg_catalog.gold.fact_fmcg_sales IS 'FMCG sales fact at the grain sale_date x SKU x channel x region x pack_type. 190,750 rows covering 2022-01-21 to 2024-12-31. Join to the dimensions on the *_sk surrogate keys.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.sales_sk IS 'Surrogate primary key: xxhash64(sale_date|sku|channel|region|pack_type).';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.date_sk IS 'Foreign key to dim_date.date_sk.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.product_sk IS 'Foreign key to dim_product.product_sk.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.channel_sk IS 'Foreign key to dim_channel.channel_sk.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.region_sk IS 'Foreign key to dim_region.region_sk.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.price_unit IS 'Unit price in USD at the time of sale.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.promotion_flag IS 'TRUE = active promotional pricing; FALSE = regular pricing.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.delivery_days IS 'Days from order to delivery.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.stock_available IS 'Units available in stock on the sale date; 0 means a stockout event.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.delivered_qty IS 'Units actually delivered (fill rate numerator).';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.units_sold IS 'Units sold.';
COMMENT ON COLUMN fmcg_catalog.gold.fact_fmcg_sales.gross_revenue IS 'price_unit * units_sold, rounded to 2 decimals.';

-- ---------- fmcg_catalog.gold.dim_date ----------
COMMENT ON TABLE fmcg_catalog.gold.dim_date IS 'Date dimension covering 2022-01-21 to 2024-12-31. Use year for year-over-year comparisons.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.date_sk IS 'Surrogate key: xxhash64(sale_date).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.sale_date IS 'Calendar date.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.year IS 'Calendar year (2022-2024).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.quarter IS 'Calendar quarter (1-4).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.month IS 'Calendar month number (1-12).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.month_name IS 'Month name (January..December).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.week_of_year IS 'ISO week of year.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.day_of_week IS 'Weekday name.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_date.is_weekend IS 'TRUE for Saturday/Sunday.';

-- ---------- fmcg_catalog.gold.dim_product ----------
COMMENT ON TABLE fmcg_catalog.gold.dim_product IS 'Product dimension at the SKU x pack-type grain (30 SKUs x 3 pack types = 90 rows).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_product.product_sk IS 'Surrogate key: xxhash64(sku, pack_type).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_product.sku IS 'Natural product key (SKU code).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_product.brand IS 'Brand name (e.g. MiBrand1, YoBrand2).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_product.segment IS 'Market segment.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_product.category IS 'Product category: Juice, Milk, ReadyMeal, SnackBar, Yogurt.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_product.pack_type IS 'Pack type: Single, Multipack, Carton.';

-- ---------- fmcg_catalog.gold.dim_channel ----------
COMMENT ON TABLE fmcg_catalog.gold.dim_channel IS 'Sales channel dimension (Retail, Discount, E-commerce) with channel_tier grouping.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_channel.channel_sk IS 'Surrogate key: xxhash64(channel_name).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_channel.channel_name IS 'Channel: Retail, Discount, or E-commerce.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_channel.channel_tier IS 'Derived grouping: Offline (Retail, Discount) or Online (E-commerce).';

-- ---------- fmcg_catalog.gold.dim_region ----------
COMMENT ON TABLE fmcg_catalog.gold.dim_region IS 'Polish region dimension (PL-Central, PL-North, PL-South); country is always Poland.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_region.region_sk IS 'Surrogate key: xxhash64(region_code).';
COMMENT ON COLUMN fmcg_catalog.gold.dim_region.region_code IS 'Region code: PL-Central, PL-North, PL-South.';
COMMENT ON COLUMN fmcg_catalog.gold.dim_region.country IS 'Always Poland.';

-- ---------------------------------------------------------------------
-- PART 2: GENIE BENCHMARK QUERIES (6)
-- Natural-language benchmark question followed by the verified SQL.
-- ---------------------------------------------------------------------

-- Benchmark 1: "What was the total gross revenue by year?"
SELECT d.year,
       ROUND(SUM(f.gross_revenue), 2) AS total_gross_revenue
FROM fmcg_catalog.gold.fact_fmcg_sales f
JOIN fmcg_catalog.gold.dim_date d ON f.date_sk = d.date_sk
GROUP BY d.year
ORDER BY d.year;

-- Benchmark 2: "Which 10 SKUs generated the highest gross revenue?"
SELECT p.sku, p.brand, p.category,
       ROUND(SUM(f.gross_revenue), 2) AS total_gross_revenue
FROM fmcg_catalog.gold.fact_fmcg_sales f
JOIN fmcg_catalog.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.sku, p.brand, p.category
ORDER BY total_gross_revenue DESC
LIMIT 10;

-- Benchmark 3: "How do promotional and non-promotional sales compare by brand?"
SELECT p.brand,
       CASE WHEN f.promotion_flag THEN 'Promotional' ELSE 'Non-Promotional' END AS promo_status,
       ROUND(AVG(f.units_sold), 2) AS avg_units_sold
FROM fmcg_catalog.gold.fact_fmcg_sales f
JOIN fmcg_catalog.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.brand, promo_status
ORDER BY p.brand, promo_status;

-- Benchmark 4: "What is the fill rate by channel?"
SELECT c.channel_name,
       ROUND(SUM(f.delivered_qty) / NULLIF(SUM(f.stock_available), 0), 4) AS fill_rate
FROM fmcg_catalog.gold.fact_fmcg_sales f
JOIN fmcg_catalog.gold.dim_channel c ON f.channel_sk = c.channel_sk
GROUP BY c.channel_name
ORDER BY c.channel_name;

-- Benchmark 5: "Show gross revenue by region and category for 2023."
SELECT r.region_code, p.category,
       ROUND(SUM(f.gross_revenue), 2) AS total_gross_revenue
FROM fmcg_catalog.gold.fact_fmcg_sales f
JOIN fmcg_catalog.gold.dim_date d ON f.date_sk = d.date_sk
JOIN fmcg_catalog.gold.dim_region r ON f.region_sk = r.region_sk
JOIN fmcg_catalog.gold.dim_product p ON f.product_sk = p.product_sk
WHERE d.year = 2023
GROUP BY r.region_code, p.category
ORDER BY r.region_code, p.category;

-- Benchmark 6: "How many stockout events occurred each month of 2024?"
SELECT d.month, d.month_name,
       SUM(CASE WHEN f.stock_available = 0 THEN 1 ELSE 0 END) AS stockout_events
FROM fmcg_catalog.gold.fact_fmcg_sales f
JOIN fmcg_catalog.gold.dim_date d ON f.date_sk = d.date_sk
WHERE d.year = 2024
GROUP BY d.month, d.month_name
ORDER BY d.month;