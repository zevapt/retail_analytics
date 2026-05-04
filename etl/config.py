import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)

def execute_sql(sql: str, engine=None):
    """Utility: run raw SQL with autocommit."""
    eng = engine or get_engine()
    with eng.connect() as conn:
        conn.execute(text(sql))
        conn.commit()