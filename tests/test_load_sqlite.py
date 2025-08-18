
from sqlalchemy import text
from my_project.core.load import make_engine, to_sql

def test_to_sql_writes_rows(tmp_db_url, sample_df):
    engine = make_engine(tmp_db_url)
    to_sql(sample_df, engine, table="sales_unit_test", if_exists="replace")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM sales_unit_test")).scalar()
        assert count == len(sample_df)
