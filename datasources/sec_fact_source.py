from typing import Generator, List

import pandas as pd
import requests

from findrum.interfaces import DataSource

class SecFactSource(DataSource):
    
    def __init__(self,  **params):
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": f"FinDrum Platform ({params["email"]})"})
        self.ciks = params["ciks"]
        
    def fetch(self):
        dfs = list(self._fetch_entity(self.ciks))
        return pd.concat(dfs, ignore_index=True)
    
    def _fetch_entity(self, ciks: List[str]) -> Generator[pd.DataFrame, None, None]:
        for cik in ciks:
            cik = cik.zfill(10)
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            response = self._session.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
            yield pd.DataFrame([data])
