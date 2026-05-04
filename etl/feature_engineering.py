import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from etl.transform import FREQ_MAP, FREQ_BUCKET


def build_customer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input:  transformed transaction-level DataFrame
    Output: one row per customer_id with all engineered features
    """
    scaler = MinMaxScaler(feature_range=(0, 100))

    # Aggregate to customer level
    cust = df.groupby('customer_id').agg(
        total_spend          = ('purchase_amount_usd', 'sum'),
        avg_spend            = ('purchase_amount_usd', 'mean'),
        transaction_count    = ('purchase_amount_usd', 'count'),
        avg_rating           = ('review_rating', 'mean'),
        promo_used_count     = ('promo_code_used', 'sum'),
        discount_used_count  = ('discount_applied', 'sum'),
        previous_purchases   = ('previous_purchases', 'first'),
        frequency_raw        = ('frequency_of_purchases', 'first'),
        subscription         = ('subscription_status', 'first'),
    ).reset_index()

    # CLV proxy
    # No timestamp available → total spend is the most defensible CLV proxy.
    # Integer because source purchase_amount_usd is integer.
    cust['clv_proxy'] = cust['total_spend'].astype(int)

    # Frequency ordinal
    cust['freq_ordinal'] = cust['frequency_raw'].map(FREQ_MAP).fillna(0).astype(int)

    # Normalize components (independently, to 0-100 range)
    # Using fit_transform on each column separately to avoid cross-feature leakage
    cust['freq_norm'] = scaler.fit_transform(cust[['freq_ordinal']]).flatten()
    cust['prev_norm'] = scaler.fit_transform(cust[['previous_purchases']]).flatten()
    cust['rate_norm'] = scaler.fit_transform(cust[['avg_rating']]).flatten()
    cust['sub_val']   = cust['subscription'].astype(int) * 100  # 0 or 100

    # Engagement score (0-100)
    # Weights: frequency pattern 40% | purchase history depth 40% | subscription 20%
    cust['engagement_score'] = (
        cust['freq_norm'] * 0.40 +
        cust['prev_norm'] * 0.40 +
        cust['sub_val']   * 0.20
    ).round(2)

    # Loyalty score (0-100)
    # Weights: purchase history 60% | subscription 20% | rating 20%
    cust['loyalty_score'] = (
        cust['prev_norm'] * 0.60 +
        cust['sub_val']   * 0.20 +
        cust['rate_norm'] * 0.20
    ).round(2)

    # Promo sensitivity score (0-100)
    # % of transactions where any discount/promo was used
    cust['promo_sensitivity_score'] = (
        (cust['promo_used_count'] + cust['discount_used_count']) /
        (2 * cust['transaction_count'].clip(lower=1)) * 100
    ).round(2)

    # Engagement tier
    cust['engagement_tier'] = pd.cut(
        cust['engagement_score'],
        bins=[-0.01, 33, 66, 100],
        labels=['Low', 'Medium', 'High']
    ).astype(str)

    # Churn risk (PROXY — no timestamps, purely behavioral)
    # Inverse of engagement; amplify risk for non-subscribers with low frequency
    cust['churn_risk_score'] = (100 - cust['engagement_score']).round(2)
    mask_high_risk = (~cust['subscription']) & (cust['freq_ordinal'] <= 1)
    cust.loc[mask_high_risk, 'churn_risk_score'] = (
        cust.loc[mask_high_risk, 'churn_risk_score'] * 1.25
    ).clip(upper=100).round(2)

    cust['churn_risk_tier'] = pd.cut(
        cust['churn_risk_score'],
        bins=[-0.01, 33, 66, 125],
        labels=['Low', 'Medium', 'High']
    ).astype(str)

    # Frequency bucket
    cust['frequency_bucket'] = cust['frequency_raw'].map(FREQ_BUCKET).fillna('Infrequent')

    # RFM segment (proxy R = previous_purchases)
    # True Recency requires timestamps; previous_purchases is the closest proxy
    cust['r_score'] = pd.qcut(
        cust['previous_purchases'], q=4, labels=[1,2,3,4], duplicates='drop'
    ).astype(int)
    cust['f_score'] = pd.qcut(
        cust['freq_ordinal'].rank(method='first'), q=4, labels=[1,2,3,4]
    ).astype(int)
    cust['m_score'] = pd.qcut(
        cust['total_spend'], q=4, labels=[1,2,3,4], duplicates='drop'
    ).astype(int)
    cust['rfm_sum'] = cust['r_score'] + cust['f_score'] + cust['m_score']

    cust['rfm_segment'] = cust.apply(_rfm_label, axis=1)

    # Final output: only the columns that go into analytics.customer_features
    return cust[[
        'customer_id', 'clv_proxy', 'engagement_score', 'loyalty_score',
        'promo_sensitivity_score', 'engagement_tier', 'churn_risk_tier',
        'frequency_bucket', 'rfm_segment'
    ]]


def _rfm_label(row) -> str:
    s   = row['rfm_sum']
    sub = row['subscription']
    if s >= 10:             return 'Champions'
    elif s >= 8 and sub:    return 'Loyal Subscribers'
    elif s >= 8:            return 'Loyal'
    elif s >= 6:            return 'Potential Loyalists'
    elif s >= 4 and not sub: return 'At Risk'
    else:                   return 'Hibernating'