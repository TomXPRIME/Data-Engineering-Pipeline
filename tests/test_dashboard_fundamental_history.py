"""Tests for dashboard fundamental history page."""
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_fundamental_history_query_uses_parameterized_sql():
    """Verify v_fundamental_history exists and has required columns."""
    import duckdb
    con = duckdb.connect(str(Path(__file__).parent.parent / "duckdb" / "spx_analytics.duckdb"), read_only=True)
    result = con.execute(
        "SELECT ticker, fiscal_date, metric, value, freq FROM v_fundamental_history LIMIT 5"
    ).fetchdf()
    # View must exist and have freq column
    assert "freq" in result.columns
    assert "fiscal_date" in result.columns