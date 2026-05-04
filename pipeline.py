# pipeline.py

from etl.extract_load_raw import load_raw
from etl.transform import transform
from etl.feature_engineering import build_customer_features
from etl.load_analytics import load_analytics

def run(filepath: str = 'data/shopping_trends.csv'):
    print("=" * 55)
    print("RETAIL ANALYTICS PIPELINE")
    print("=" * 55)

    print("\n[1/4] Loading CSV → raw.transactions")
    raw_df = load_raw(filepath)

    print("\n[2/4] Transforming data")
    clean_df = transform(raw_df)

    print("\n[3/4] Building customer features")
    features = build_customer_features(clean_df)

    print("\n[4/4] Loading analytics schema")
    load_analytics(clean_df, features)

    print("\nPipeline complete. All tables ready for Power BI.")

if __name__ == '__main__':
    run()