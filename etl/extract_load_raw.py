import pandas as pd
import numpy as np
from sqlalchemy import text
from etl.config import get_engine

# Column rename: source CSV headers → database snake_case names
COL_RENAME = {
    'Customer ID':            'customer_id',
    'Age':                    'age',
    'Gender':                 'gender',
    'Item Purchased':         'item_purchased',
    'Category':               'category',
    'Purchase Amount (USD)':  'purchase_amount_usd',
    'Location':               'location',
    'Size':                   'size',
    'Color':                  'color',
    'Season':                 'season',
    'Review Rating':          'review_rating',
    'Subscription Status':    'subscription_status',
    'Shipping Type':          'shipping_type',
    'Discount Applied':       'discount_applied',
    'Promo Code Used':        'promo_code_used',
    'Previous Purchases':     'previous_purchases',
    'Payment Method':         'payment_method',
    'Frequency of Purchases': 'frequency_of_purchases',
}

def load_raw(filepath: str = 'data/shopping_trends.csv') -> pd.DataFrame:
    """
    Read CSV, rename columns to snake_case, push to raw.transactions.
    Returns the renamed DataFrame for use in the next ETL stage.
    """
    engine = get_engine()

    # 1. Read CSV
    df = pd.read_csv(filepath)
    print(f"CSV loaded: {len(df)} rows, {df.shape[1]} columns")

    # 2. Validate expected columns exist
    missing = set(COL_RENAME.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    # 3. Rename
    df.rename(columns=COL_RENAME, inplace=True)

    # 4. Validate critical dtypes match expectations
    assert df['customer_id'].dtype == np.int64,         "customer_id must be int64"
    assert df['purchase_amount_usd'].dtype == np.int64, "purchase_amount_usd must be int64"
    assert df['previous_purchases'].dtype == np.int64,  "previous_purchases must be int64"

    # 5. Push to raw schema — raw layer stores data as-is (Yes/No strings preserved)
    # if_exists='replace' truncates and reloads; use 'append' for incremental loads
    df.to_sql(
        name='transactions',
        con=engine,
        schema='raw',
        if_exists='replace',   # change to 'append' for incremental
        index=False,
        method='multi',        # batch insert, faster than row-by-row
        chunksize=500
    )
    print(f"raw.transactions loaded: {len(df)} rows")

    return df   # pass downstream to ETL without re-reading from DB