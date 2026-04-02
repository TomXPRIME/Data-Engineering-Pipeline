"""DuckDB connection wrapper for Gold layer queries."""
import duckdb
import pandas as pd
from pathlib import Path


class GoldDataProvider:
    def __init__(self, db_path: str = "duckdb/spx_analytics.duckdb"):
        self._db_path = Path(db_path)
        self._conn = None

    def __enter__(self):
        self._conn = duckdb.connect(str(self._db_path))
        return self

    def __exit__(self, *args):
        if self._conn:
            self._conn.close()

    def execute(self, query: str) -> pd.DataFrame:
        return self._conn.execute(query).fetchdf()