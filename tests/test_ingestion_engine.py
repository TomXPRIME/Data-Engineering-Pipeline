"""
Unit tests for pipeline.ingestion_engine batch insert and helper functions.

Run: python -m pytest tests/test_ingestion_engine.py -v
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.ingestion_engine import _safe_float, _safe_int


class TestSafeHelpers:
    """Tests for _safe_float and _safe_int helper functions."""

    def test_safe_float_正常值(self):
        assert _safe_float(100.5) == 100.5

    def test_safe_float_None(self):
        assert _safe_float(None) is None

    def test_safe_float_nan(self):
        import math
        assert _safe_float(float('nan')) is None

    def test_safe_int_正常值(self):
        assert _safe_int(100) == 100

    def test_safe_int_None(self):
        assert _safe_int(None) is None

    def test_safe_int_nan(self):
        import math
        assert _safe_int(float('nan')) is None


class TestBatchInsert:
    """Tests for batch insert logic (requires DuckDB)."""

    def test_ingest_price_file_事务成功(self, tmp_path):
        """Test successful batch insert commits transaction."""
        import duckdb
        from pipeline.ingestion_engine import IngestionEngine

        # Create test DB
        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("""
            CREATE SEQUENCE IF NOT EXISTS raw_price_stream_seq;
            CREATE TABLE IF NOT EXISTS raw_price_stream (
                id BIGINT DEFAULT NEXTVAL('raw_price_stream_seq') PRIMARY KEY,
                ticker VARCHAR(20), date DATE, open DECIMAL(18,6), high DECIMAL(18,6),
                low DECIMAL(18,6), close DECIMAL(18,6), adj_close DECIMAL(18,6), volume BIGINT
            );
            CREATE TABLE IF NOT EXISTS ingestion_audit (
                id INTEGER PRIMARY KEY DEFAULT NEXTVAL('raw_price_stream_seq'),
                source VARCHAR(50), ticker VARCHAR(20), market_date DATE,
                file_hash VARCHAR(64), status VARCHAR(20), error_message TEXT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        con.close()

        # Create test CSV
        csv_path = tmp_path / "price_2024-01-02.csv"
        csv_path.write_text(
            "Date,Ticker,Open,High,Low,Close,Adj Close,Volume\n"
            "2024-01-02,AAPL,185.0,186.0,184.5,185.5,185.5,1000000\n"
            "2024-01-02,MSFT,370.0,371.0,369.5,370.5,370.5,500000\n"
        )

        engine = IngestionEngine(str(db_path))
        count = engine.ingest_price_file(csv_path)
        engine.close()

        con = duckdb.connect(str(db_path), read_only=True)
        row_count = con.execute("SELECT COUNT(*) FROM raw_price_stream").fetchone()[0]
        con.close()

        assert row_count == 2, f"Expected 2 rows, got {row_count}"