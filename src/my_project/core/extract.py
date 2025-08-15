import pandas as pd

def exoplanets_pscomppars(url: str) -> pd.DataFrame:
    return pd.read_csv(url)