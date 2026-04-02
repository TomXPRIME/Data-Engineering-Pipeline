"""DuckDB connection wrapper for Gold layer queries."""
import logging
import duckdb
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class GoldDataProvider:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path(__file__).parent.parent.parent / "duckdb" / "spx_analytics.duckdb"
        self._db_path = Path(db_path)
        self._conn = None

    def __enter__(self):
        self._conn = duckdb.connect(str(self._db_path))
        return self

    def __exit__(self, *args):
        if self._conn:
            self._conn.close()

    def execute(self, query: str, params: tuple = None) -> pd.DataFrame:
        try:
            if params:
                return self._conn.execute(query, params).fetchdf()
            return self._conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"Query failed: {query[:200]}... params={params}")
            raise