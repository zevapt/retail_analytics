# etl/transform.py

import pandas as pd
import numpy as np


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input:  raw DataFrame with snake_case column names (output of load_raw)
    Output: clean, typed DataFrame ready for feature engineering and loading
    """
    df = df.copy()

    # 1. Boolean coercion: Yes/No → True/False 
    bool_cols = ['discount_applied', 'promo_code_used', 'subscription_status']
    for col in bool_cols:
        df[col] = df[col].str.strip().str.lower().map({'yes': True, 'no': False})
        nulls = df[col].isna().sum()
        if nulls > 0:
            raise ValueError(f"Unexpected nulls in {col} after boolean coercion: {nulls}")

    # 2. Normalize text: strip whitespace + title case 
    text_cols = [
        'gender', 'category', 'item_purchased', 'season',
        'shipping_type', 'payment_method', 'size', 'color',
        'frequency_of_purchases', 'location'
    ]
    for col in text_cols:
        df[col] = df[col].str.strip().str.title()

    # 3. Review rating: impute 37 nulls with category-level median 
    # Verify expected null count; alert if data has changed upstream
    null_count = df['review_rating'].isna().sum()
    print(f"  review_rating nulls before imputation: {null_count}")  # expect 37

    df['review_rating'] = df.groupby('category')['review_rating'].transform(
        lambda x: x.fillna(x.median())
    )
    assert df['review_rating'].isna().sum() == 0, "review_rating still has nulls"

    # 4. Engineered columns used by dimensions 
    df['age_group'] = df['age'].apply(_age_group)
    df['region']    = df['location'].map(REGION_MAP).fillna('Other')
    df['freq_ordinal'] = df['frequency_of_purchases'].map(FREQ_MAP).fillna(0).astype(int)

    print(f"  Transform complete. Shape: {df.shape}")
    return df


# Lookup tables 

FREQ_MAP = {
    'Weekly': 4, 'Bi-Weekly': 3, 'Fortnightly': 3,
    'Monthly': 2, 'Every 3 Months': 1, 'Quarterly': 1, 'Annually': 0
}

FREQ_BUCKET = {
    'Weekly': 'Weekly',
    'Bi-Weekly': 'Frequent', 'Fortnightly': 'Frequent',
    'Monthly': 'Occasional',
    'Every 3 Months': 'Infrequent', 'Quarterly': 'Infrequent', 'Annually': 'Infrequent'
}

REGION_MAP = {
    # Northeast
    'Connecticut': 'Northeast', 'Maine': 'Northeast', 'Massachusetts': 'Northeast',
    'New Hampshire': 'Northeast', 'Rhode Island': 'Northeast', 'Vermont': 'Northeast',
    'New Jersey': 'Northeast', 'New York': 'Northeast', 'Pennsylvania': 'Northeast',
    # Midwest
    'Illinois': 'Midwest', 'Indiana': 'Midwest', 'Michigan': 'Midwest',
    'Ohio': 'Midwest', 'Wisconsin': 'Midwest', 'Iowa': 'Midwest',
    'Kansas': 'Midwest', 'Minnesota': 'Midwest', 'Missouri': 'Midwest',
    'Nebraska': 'Midwest', 'North Dakota': 'Midwest', 'South Dakota': 'Midwest',
    # South
    'Delaware': 'South', 'Florida': 'South', 'Georgia': 'South',
    'Maryland': 'South', 'North Carolina': 'South', 'South Carolina': 'South',
    'Virginia': 'South', 'West Virginia': 'South', 'Alabama': 'South',
    'Kentucky': 'South', 'Mississippi': 'South', 'Tennessee': 'South',
    'Arkansas': 'South', 'Louisiana': 'South', 'Oklahoma': 'South', 'Texas': 'South',
    # West
    'Arizona': 'West', 'Colorado': 'West', 'Idaho': 'West', 'Montana': 'West',
    'Nevada': 'West', 'New Mexico': 'West', 'Utah': 'West', 'Wyoming': 'West',
    'Alaska': 'West', 'California': 'West', 'Hawaii': 'West',
    'Oregon': 'West', 'Washington': 'West',
}

def _age_group(age: int) -> str:
    if age <= 25:   return '18-25'
    elif age <= 35: return '26-35'
    elif age <= 50: return '36-50'
    else:           return '51+'