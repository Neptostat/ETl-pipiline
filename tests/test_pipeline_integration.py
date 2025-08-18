
import os
from sqlalchemy import create_engine, text
from my_project.services.pipeline import ETLPipeline

def test_pipeline_end_to_end(tmp_db_url):
    sample = os.path.join(os.path.dirname(__file__), "data", "input_sample.csv")
    table = "sales_e2e"
    ETLPipeline(source_path=sample, db_url=tmp_db_url, table_name=table, chunk_size=2).run()
    engine = create_engine(tmp_db_url, future=True)
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        assert count == 2  # one row dropped due to null customer
