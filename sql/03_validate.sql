-- 1. Row counts
SELECT 'raw.transactions'            AS tbl, COUNT(*) FROM raw.transactions
UNION ALL
SELECT 'dim_customer',                         COUNT(*) FROM analytics.dim_customer
UNION ALL
SELECT 'dim_product',                          COUNT(*) FROM analytics.dim_product
UNION ALL
SELECT 'dim_location',                         COUNT(*) FROM analytics.dim_location
UNION ALL
SELECT 'dim_season',                           COUNT(*) FROM analytics.dim_season
UNION ALL
SELECT 'fact_transactions',                    COUNT(*) FROM analytics.fact_transactions
UNION ALL
SELECT 'customer_features',                    COUNT(*) FROM analytics.customer_features;

-- Expected:
-- raw.transactions   → 3900
-- dim_customer       → number of unique customer_ids
-- dim_product        → 25 (unique items)
-- dim_location       → up to 50 (unique states)
-- dim_season         → 4
-- fact_transactions  → 3900 (one row per source row)
-- customer_features  → matches dim_customer count

-- 2. Null check on fact measures
SELECT
    COUNT(*)                                    AS total_rows,
    SUM(CASE WHEN purchase_amount_usd IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN review_rating IS NULL THEN 1 ELSE 0 END)       AS null_rating,
    SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END)         AS unmatched_customers,
    SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END)          AS unmatched_products,
    SUM(CASE WHEN location_sk IS NULL THEN 1 ELSE 0 END)         AS unmatched_locations,
    SUM(CASE WHEN season_sk IS NULL THEN 1 ELSE 0 END)           AS unmatched_seasons
FROM analytics.fact_transactions;
-- All null_* columns should be 0

-- 3. Revenue sanity check
SELECT
    SUM(purchase_amount_usd) AS total_revenue,
    AVG(purchase_amount_usd::NUMERIC)::NUMERIC(8,2) AS avg_order_value,
    MIN(purchase_amount_usd) AS min_order,
    MAX(purchase_amount_usd) AS max_order
FROM analytics.fact_transactions;

-- 4. Dimension cardinality
SELECT category, COUNT(*) FROM analytics.dim_product GROUP BY category ORDER BY 1;
-- Should show: Accessories, Clothing, Footwear, Outerwear

SELECT region, COUNT(*) FROM analytics.dim_location GROUP BY region ORDER BY 1;

-- 5. Feature distribution sanity
SELECT
    engagement_tier,
    churn_risk_tier,
    rfm_segment,
    COUNT(*) AS n
FROM analytics.customer_features
GROUP BY engagement_tier, churn_risk_tier, rfm_segment
ORDER BY n DESC;