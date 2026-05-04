CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.transactions;

CREATE TABLE raw.transactions (
    customer_id            INTEGER,
    age                    INTEGER,
    gender                 TEXT,
    item_purchased         TEXT,
    category               TEXT,
    purchase_amount_usd    INTEGER,
    location               TEXT,
    size                   TEXT,
    color                  TEXT,
    season                 TEXT,
    review_rating          NUMERIC(3,1),   -- nullable: 37 missing values
    subscription_status    TEXT,           -- raw: 'Yes' / 'No'
    shipping_type          TEXT,
    discount_applied       TEXT,           -- raw: 'Yes' / 'No'
    promo_code_used        TEXT,           -- raw: 'Yes' / 'No'
    previous_purchases     INTEGER,
    payment_method         TEXT,
    frequency_of_purchases TEXT
);