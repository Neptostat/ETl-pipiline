
import pandas as pd
import pytest

@pytest.fixture
def sample_df():
    return pd.DataFrame([
        {"order_id": 1, "customer": "A", "quantity": 2, "unit_price": 5.0},
        {"order_id": 2, "customer": "B", "quantity": 1, "unit_price": 10.0},
    ])

@pytest.fixture
def tmp_db_url(tmp_path):
    db_path = tmp_path / "warehouse.db"
    return f"sqlite:///{db_path}"
