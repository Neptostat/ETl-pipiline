
import os
import pandas as pd
from my_project.core.transform import transform

def test_transform_computes_total_and_drops_null_customer():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "data", "input_sample.csv"))
    out = transform(df)
    assert "total" in out.columns
    # One row has empty customer -> dropped
    assert len(out) == 2
    # Check derived total on first valid row
    row = out.iloc[0]
    assert abs(float(row["total"]) - (row["quantity"] * row["unit_price"])) < 1e-6
