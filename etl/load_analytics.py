import pandas as pd
from sqlalchemy import text
from etl.config import get_engine
from etl.transform import FREQ_MAP, REGION_MAP, _age_group


# Entry point
def load_analytics(df: pd.DataFrame, features: pd.DataFrame):
    """
    Loads all dimension tables, fact_transactions, and customer_features.
    Always runs as full refresh (TRUNCATE + INSERT).
    Dimensions are loaded first to satisfy FK constraints in fact.
    """
    engine = get_engine()

    print("Loading dim_customer...")
    _load_dim_customer(df, engine)

    print("Loading dim_product...")
    _load_dim_product(df, engine)

    print("Loading dim_location...")
    _load_dim_location(df, engine)


    print("  Loading fact_transactions...")
    _load_fact(df, engine)
 
    print("  Loading customer_features...")
    _load_customer_features(features, engine)
 
    _verify(engine)
    print("Analytics schema load complete.")



# Dimension loaders
def _load_dim_customer(df: pd.DataFrame, engine):
    dim = (
        df.drop_duplicates('customer_id')
        [[
            'customer_id', 'age', 'gender', 'subscription_status',
            'previous_purchases', 'frequency_of_purchases', 'freq_ordinal'
        ]]
        .copy()
    )
    dim['age_group'] = dim['age'].apply(_age_group)
 
    dim.to_sql(
        'dim_customer', engine, schema='analytics',
        if_exists='append',     # table already exists — only insert rows
        index=False,
        method='multi',
        chunksize=500
    )
 
 
def _load_dim_product(df: pd.DataFrame, engine):
    dim = (
        df[['item_purchased', 'category']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.to_sql(
        'dim_product', engine, schema='analytics',
        if_exists='append', index=False, method='multi'
    )
 
 
def _load_dim_location(df: pd.DataFrame, engine):
    dim = (
        df[['location', 'region']]
        .drop_duplicates()
        .rename(columns={'location': 'state'})
        .reset_index(drop=True)
    )
    dim.to_sql(
        'dim_location', engine, schema='analytics',
        if_exists='append', index=False, method='multi'
    )


# Fact loader
def _load_fact(df: pd.DataFrame, engine):
    """
    Two-phase load to avoid resolving surrogate keys in Python:
 
    Phase 1 — Push staging table with business keys only (no SKs).
              Uses if_exists='replace' because staging is ephemeral and
              has no FK constraints pointing at it.
 
    Phase 2 — SQL JOIN resolves SKs from dimension tables and inserts
              into fact_transactions. staging table is dropped after.
 
    Why not merge in pandas?
      SERIAL sequences reset on every TRUNCATE RESTART IDENTITY.
      Resolving in SQL always reads the live sequence values — no drift.
    """
    # Phase 1: staging
    staging = df[[
        'customer_id', 'item_purchased', 'category', 'location',
        'season', 'purchase_amount_usd', 'review_rating',
        'size', 'color', 'discount_applied', 'promo_code_used',
        'shipping_type', 'payment_method'
    ]].copy()
 
    staging.to_sql(
        'fact_staging', engine, schema='analytics',
        if_exists='replace', index=False, method='multi', chunksize=500
    )
 
    # Phase 2: SK resolution + final insert
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO analytics.fact_transactions (
                customer_sk,
                product_sk,
                location_sk,
                season_sk,
                purchase_amount_usd,
                review_rating,
                size,
                color,
                discount_applied,
                promo_code_used,
                shipping_type,
                payment_method
            )
            SELECT
                c.customer_sk,
                p.product_sk,
                l.location_sk,
                s.season_sk,
                t.purchase_amount_usd,
                t.review_rating,
                t.size,
                t.color,
                t.discount_applied,
                t.promo_code_used,
                t.shipping_type,
                t.payment_method
            FROM  analytics.fact_staging t
            JOIN  analytics.dim_customer c ON t.customer_id    = c.customer_id
            JOIN  analytics.dim_product  p ON t.item_purchased = p.item_purchased
                                          AND t.category       = p.category
            JOIN  analytics.dim_location l ON t.location       = l.state
            JOIN  analytics.dim_season   s ON INITCAP(t.season) = s.season_name;
        """))
        conn.execute(text("DROP TABLE IF EXISTS analytics.fact_staging;"))
        conn.commit()


# Customer features loader 
 
def _load_customer_features(features: pd.DataFrame, engine):
    features.to_sql(
        'customer_features', engine, schema='analytics',
        if_exists='append', index=False, method='multi', chunksize=500
    )
 
 
# Post-load verification 
 
def _verify(engine):
    """
    Quick row count sanity check after load.
    Prints a warning if any FK-resolvable rows were lost (unmatched joins).
    """
    queries = {
        'dim_customer':      "SELECT COUNT(*) FROM analytics.dim_customer",
        'dim_product':       "SELECT COUNT(*) FROM analytics.dim_product",
        'dim_location':      "SELECT COUNT(*) FROM analytics.dim_location",
        'fact_transactions': "SELECT COUNT(*) FROM analytics.fact_transactions",
        'customer_features': "SELECT COUNT(*) FROM analytics.customer_features",
    }
 
    print("\n  Post-load row counts:")
    with engine.connect() as conn:
        for label, q in queries.items():
            n = conn.execute(text(q)).scalar()
            print(f"    {label:<25} {n:>6} rows")
 
        # FK integrity: any NULL SKs in fact = unmatched staging rows
        nulls = conn.execute(text("""
            SELECT
                SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer,
                SUM(CASE WHEN product_sk  IS NULL THEN 1 ELSE 0 END) AS null_product,
                SUM(CASE WHEN location_sk IS NULL THEN 1 ELSE 0 END) AS null_location,
                SUM(CASE WHEN season_sk   IS NULL THEN 1 ELSE 0 END) AS null_season
            FROM analytics.fact_transactions
        """)).fetchone()
 
        issues = {k: v for k, v in zip(
            ['null_customer_sk', 'null_product_sk', 'null_location_sk', 'null_season_sk'],
            nulls
        ) if v and v > 0}
 
        if issues:
            print(f"\n  WARNING: Unresolved FK references in fact_transactions: {issues}")
        else:
            print("\n  ✓ No FK resolution issues.")
 
