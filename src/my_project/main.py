# src/my_project/core/main.py
from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from my_project.core import extract, transform, load





def run(url: Optional[str] = None, out: Optional[str] = None, table: str = "exoplanets") -> pd.DataFrame:
    """
    Execute the pipeline:
      1) extract: exoplanets from NASA
      2) transform: optional transforms if present
      3) load: save to CSV/Parquet or SQLite

    Returns the final DataFrame.
    """
    # ---- Extract -----------------------------------------------------------
    # Support either signature: exoplanets_pscomppars() or exoplanets_pscomppars(url)
    try:
        df = extract.exoplanets_pscomppars(url) if url else extract.exoplanets_pscomppars()
    except TypeError:
        # If the function only accepts the other signature, try the alternative
        df = extract.exoplanets_pscomppars() if url else extract.exoplanets_pscomppars(url)  # type: ignore

    # ---- Transform (optional) ----------------------------------------------
    if hasattr(transform, "transform"):
        df = transform.transform(df)  # type: ignore[attr-defined]
    elif hasattr(transform, "apply"):
        df = transform.apply(df)      # type: ignore[attr-defined]

    # ---- Load / Save -------------------------------------------------------
    if out:
        path = Path(out)
        path.parent.mkdir(parents=True, exist_ok=True)

        suffix = path.suffix.lower()
        if suffix in ("", ".csv"):
            df.to_csv(path, index=False)
            logging.info("Saved %d rows to %s", len(df), path)
        elif suffix == ".parquet":
            df.to_parquet(path, index=False)
            logging.info("Saved %d rows to %s", len(df), path)
        elif suffix in (".db", ".sqlite", ".sqlite3"):
            if hasattr(load, "to_sqlite"):
                load.to_sqlite(df, database=str(path), table=table, if_exists="replace")  # type: ignore[attr-defined]
            else:
                conn = sqlite3.connect(str(path))
                try:
                    df.to_sql(table, conn, if_exists="replace", index=False)
                finally:
                    conn.close()
            logging.info("Loaded %d rows into %s (table=%s)", len(df), path, table)
        elif hasattr(load, "save"):
            load.save(df, str(path))  # custom sink if you have one
        else:
            raise ValueError(f"Unsupported output path/format: {out}")

    return df


def cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the exoplanets ETL pipeline.")
    parser.add_argument("--url", help="Optional: override source URL for extraction.")
    parser.add_argument("--out", help="Where to write results: .csv, .parquet, or .db/.sqlite")
    parser.add_argument("--table", default="exoplanets", help="Destination table name for SQLite.")
    parser.add_argument("--head", type=int, default=0, help="Print first N rows after run.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    df = run(url=args.url, out=args.out, table=args.table)
    if args.head:
        print(df.head(args.head).to_string(index=False))
    return 0


# Alias so tests can import `main` if they expect it.
main = cli

if __name__ == "__main__":
    raise SystemExit(cli())
