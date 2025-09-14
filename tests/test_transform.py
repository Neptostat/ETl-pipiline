import os
import pandas as pd
import pytest
from src.transform_module import transform

@pytest.mark.parametrize(
    "filename, expected_rows, check_total",
    [
        ("input_sample.csv", 2, True),
        ("input_with_all_customers.csv", 3, True),
        ("input_with_nulls.csv", 1, True),
    ]
)
def test_transform_computes_total_and_drops_null_customer(filename, expected_rows, check_total):
    data_path = os.path.join(os.path.dirname(__file__), "data", filename)
    df = pd.read_csv(data_path)
    out = transform(df)

    assert "total" in out.columns
    # One row has empty customer -> dropped
    assert len(out) == 2

    if check_total:
        assert "total" in out.columns
        row = out.iloc[0]
        assert abs(float(row["total"]) - (row["quantity"] * row["unit_price"])) < 1e-6

    assert len(out) == expected_rows


