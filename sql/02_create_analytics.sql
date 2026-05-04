CREATE SCHEMA IF NOT EXISTS analytics;

-- Dimensions 

DROP TABLE IF EXISTS analytics.dim_customer CASCADE;
CREATE TABLE analytics.dim_customer (
    customer_sk            SERIAL PRIMARY KEY,
    customer_id            INTEGER UNIQUE NOT NULL,
    age                    INTEGER,
    age_group              TEXT,
    gender                 TEXT,
    subscription_status    BOOLEAN,
    previous_purchases     INTEGER,
    frequency_of_purchases TEXT,
    freq_ordinal           INTEGER
);
CREATE INDEX idx_dim_cust_id   ON analytics.dim_customer(customer_id);
CREATE INDEX idx_dim_cust_sub  ON analytics.dim_customer(subscription_status);
CREATE INDEX idx_dim_cust_age  ON analytics.dim_customer(age_group);

DROP TABLE IF EXISTS analytics.dim_product CASCADE;
CREATE TABLE analytics.dim_product (
    product_sk     SERIAL PRIMARY KEY,
    item_purchased TEXT NOT NULL,
    category       TEXT NOT NULL,
    UNIQUE(item_purchased, category)
);
CREATE INDEX idx_dim_prod_cat ON analytics.dim_product(category);

DROP TABLE IF EXISTS analytics.dim_location CASCADE;
CREATE TABLE analytics.dim_location (
    location_sk SERIAL PRIMARY KEY,
    state       TEXT UNIQUE NOT NULL,
    region      TEXT
);
CREATE INDEX idx_dim_loc_region ON analytics.dim_location(region);

DROP TABLE IF EXISTS analytics.dim_season CASCADE;
CREATE TABLE analytics.dim_season (
    season_sk   SERIAL PRIMARY KEY,
    season_name TEXT UNIQUE NOT NULL,
    sort_order  INTEGER
);
INSERT INTO analytics.dim_season(season_name, sort_order) VALUES
    ('Winter', 1), ('Spring', 2), ('Summer', 3), ('Fall', 4);

-- Fact 

DROP TABLE IF EXISTS analytics.fact_transactions CASCADE;
CREATE TABLE analytics.fact_transactions (
    transaction_sk      BIGSERIAL PRIMARY KEY,
    customer_sk         INTEGER REFERENCES analytics.dim_customer(customer_sk),
    product_sk          INTEGER REFERENCES analytics.dim_product(product_sk),
    location_sk         INTEGER REFERENCES analytics.dim_location(location_sk),
    season_sk           INTEGER REFERENCES analytics.dim_season(season_sk),
    purchase_amount_usd INTEGER NOT NULL,
    review_rating       NUMERIC(3,1),
    size                TEXT,
    color               TEXT,
    discount_applied    BOOLEAN,
    promo_code_used     BOOLEAN,
    shipping_type       TEXT,
    payment_method      TEXT
);
CREATE INDEX idx_fact_cust     ON analytics.fact_transactions(customer_sk);
CREATE INDEX idx_fact_prod     ON analytics.fact_transactions(product_sk);
CREATE INDEX idx_fact_loc      ON analytics.fact_transactions(location_sk);
CREATE INDEX idx_fact_season   ON analytics.fact_transactions(season_sk);
CREATE INDEX idx_fact_discount ON analytics.fact_transactions(discount_applied);
CREATE INDEX idx_fact_shipping ON analytics.fact_transactions(shipping_type);

-- Feature Table 

DROP TABLE IF EXISTS analytics.customer_features CASCADE;
CREATE TABLE analytics.customer_features (
    customer_id             INTEGER PRIMARY KEY,
    clv_proxy               INTEGER,
    engagement_score        NUMERIC(5,2),
    loyalty_score           NUMERIC(5,2),
    promo_sensitivity_score NUMERIC(5,2),
    engagement_tier         TEXT,
    churn_risk_tier         TEXT,
    frequency_bucket        TEXT,
    rfm_segment             TEXT
);