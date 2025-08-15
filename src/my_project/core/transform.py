import pandas as pd

def tidy_exoplanets(df: pd.DataFrame) -> pd.DataFrame:
    # choose a compact set of useful columns; rename to snake_case
    colmap = {
        "pl_name": "planet_name",
        "hostname": "host",
        "disc_year": "discovery_year",
        "pl_orbper": "orbital_period_days",
        "pl_rade": "radius_rearth",
        "pl_bmasse": "mass_mearth",
    }
    keep = [c for c in colmap if c in df.columns]
    out = df[keep].rename(columns=colmap)
    if {"radius_rearth", "mass_mearth"}.issubset(out.columns):
        out["density_proxy"] = out["mass_mearth"] / (out["radius_rearth"] ** 3)
    return out

# make this the default transform for now
def transform(df: pd.DataFrame) -> pd.DataFrame:
    return tidy_exoplanets(df)
