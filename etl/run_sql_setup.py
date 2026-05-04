# You can run them from Python during setup:
from config import get_engine
from sqlalchemy import text

engine = get_engine()

for sql_file in ['sql/01_create_raw.sql', 'sql/02_create_analytics.sql']:
    with open(sql_file) as f:
        sql = f.read()
    with engine.connect() as conn:
        # Execute multi-statement SQL block
        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
    print(f"Executed: {sql_file}")