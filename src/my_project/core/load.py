# src/my_project/core/load.py
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def make_engine(db_url: str) -> Engine:
    return create_engine(db_url, future=True)

def to_sql(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    if_exists: str = "append",
    chunksize: Optional[int] = None,
) -> None:
    df.to_sql(
        table,
        con=engine,
        if_exists=if_exists,   # "append" | "replace" | "fail"
        index=False,
        chunksize=chunksize,
        method=None,
    )
