import pandas as pd
from urllib.parse import urlencode, quote_plus

BASE = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"

def exoplanets_pscomppars() -> pd.DataFrame:
    query = (
        "SELECT TOP 100 "
        "pl_name, hostname, disc_year, disc_method, st_teff, st_mass, st_rad, "
        "pl_orbper, pl_rade, pl_bmasse, pl_orbsmax, pl_eqt, sy_dist, rastr, decstr "
        "FROM pscomppars"
    )
    url = f"{BASE}?{urlencode({'query': ' '.join(query.split()), 'format': 'csv'}, quote_via=quote_plus)}"
    return pd.read_csv(url)

if __name__=='__main__':

    print(exoplanets_pscomppars())