# src/my_project/core/load.py
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def make_engine(db_name: str) -> Engine:


    connection_url = (
        f"mssql+pyodbc://@localhost/test"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )
    return create_engine(connection_url, future=True)

def to_sql(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    if_exists: str = "append",  # "append" | "replace" | "fail"
    chunksize: Optional[int] = None,
) -> None:

    df.to_sql(
        table,
        con=engine,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method=None,
    )


if __name__ == "__main__":

    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})


    engine = make_engine("Test")

    # Load DataFrame into table "my_table"
    to_sql(df, engine, "planets_pscomppars")
