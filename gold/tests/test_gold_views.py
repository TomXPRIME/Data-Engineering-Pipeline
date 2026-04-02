"""
Phase 5 Gold Layer — Validation Tests

Run: python gold/tests/test_gold_views.py
Requires: DuckDB with Silver data loaded and Gold layer built.
"""

import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "duckdb" / "spx_analytics.duckdb"

def test_view_exists(con, view_name: str) -> bool:
    """Check if a view exists in the database."""
    result = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{view_name}' AND table_type = 'VIEW'"
    ).fetchone()[0]
    return result > 0

def test_table_exists(con, table_name: str) -> bool:
    """Check if a table exists in the database."""
    result = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}' AND table_type = 'BASE TABLE'"
    ).fetchone()[0]
    return result > 0

def test_row_count(con, name: str, is_view: bool) -> int:
    """Check row count for a view or table. Returns -1 if not found."""
    if is_view:
        if not test_view_exists(con, name):
            return -1
    else:
        if not test_table_exists(con, name):
            return -1
    count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    return count

def test_columns(con, name: str, expected_cols: list) -> list:
    """Check view/table has expected columns. Returns missing columns."""
    actual = [row[0] for row in con.execute(f"DESCRIBE {name}").fetchall()]
    missing = [c for c in expected_cols if c not in actual]
    return missing

def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # --- Star Schema Tables ---
    star_tables = {
        "dim_ticker": ["ticker", "company_name", "sector", "is_current"],
        "dim_date": ["date", "year", "month", "quarter"],
        "fact_daily_price": ["ticker", "date", "close", "volume", "daily_return", "next_1d_return", "next_5d_return"],
        "fact_quarterly_fundamentals": ["ticker", "fiscal_date", "report_type", "freq", "revenue", "net_income"],
        "fact_earnings_transcript": ["ticker", "event_date", "sentiment_polarity", "close_on_event", "next_1d_return", "next_5d_return"],
    }

    # --- Materialized Fact Tables ---
    materialized_tables = {
        "fact_rolling_volatility": ["ticker", "date", "close", "annualized_vol_20d", "annualized_vol_60d", "annualized_return_20d"],
        "fact_momentum_signals": ["ticker", "date", "close", "momentum_5d", "momentum_20d", "momentum_60d", "dist_pct_from_ma20", "dist_pct_from_ma60", "trend_signal"],
        "fact_ar1_results": ["ticker", "date", "close", "daily_return", "alpha_ar1", "beta_ar1", "r_squared_ar1", "n_obs"],
    }

    # --- OLAP Views ---
    olap_views = {
        "v_market_daily_summary": ["trade_date", "number_of_tickers", "avg_close", "avg_return", "total_volume"],
        "v_ticker_profile": ["ticker", "company_name", "sector", "latest_close", "latest_volume", "latest_trade_date"],
        "v_fundamental_snapshot": ["ticker", "fiscal_date", "latest_report_date", "revenue", "net_income"],
        "v_fundamental_history": ["ticker", "price_date", "fiscal_date", "report_type", "freq", "revenue", "net_income"],
        "v_sentiment_price_view": ["ticker", "transcript_date", "sentiment_score", "close_on_event", "next_1d_return", "next_5d_return"],
        "v_sentiment_binned_returns": ["sentiment_bucket", "transcript_count", "avg_1d_return", "avg_5d_return", "std_1d_return", "avg_subjectivity"],
        "v_sector_rotation": ["sector", "year", "quarter", "avg_close", "total_volume", "avg_volatility", "avg_ticker_count", "qoq_return", "momentum_rank"],
    }

    passed = failed = 0

    # Test star schema tables
    for table_name, expected_cols in star_tables.items():
        print(f"\n--- {table_name} (star table) ---")
        if test_table_exists(con, table_name):
            print(f"  [PASS] Table exists")
            passed += 1
        else:
            print(f"  [FAIL] Table does NOT exist")
            failed += 1
            continue

        count = test_row_count(con, table_name, is_view=False)
        if count >= 0:
            print(f"  [PASS] Has {count:,} rows")
            passed += 1
        else:
            print(f"  [FAIL] Could not read table")
            failed += 1
            continue

        missing = test_columns(con, table_name, expected_cols)
        if not missing:
            print(f"  [PASS] All expected columns present")
            passed += 1
        else:
            print(f"  [FAIL] Missing columns: {missing}")
            failed += 1

    # Test materialized tables
    for table_name, expected_cols in materialized_tables.items():
        print(f"\n--- {table_name} (materialized) ---")
        if test_table_exists(con, table_name):
            print(f"  [PASS] Table exists")
            passed += 1
        else:
            print(f"  [FAIL] Table does NOT exist")
            failed += 1
            continue

        count = test_row_count(con, table_name, is_view=False)
        if count >= 0:
            print(f"  [PASS] Has {count:,} rows")
            passed += 1
        else:
            print(f"  [FAIL] Could not read table")
            failed += 1
            continue

        missing = test_columns(con, table_name, expected_cols)
        if not missing:
            print(f"  [PASS] All expected columns present")
            passed += 1
        else:
            print(f"  [FAIL] Missing columns: {missing}")
            failed += 1

    # Test OLAP views
    for view_name, expected_cols in olap_views.items():
        print(f"\n--- {view_name} (view) ---")
        if test_view_exists(con, view_name):
            print(f"  [PASS] View exists")
            passed += 1
        else:
            print(f"  [FAIL] View does NOT exist")
            failed += 1
            continue

        count = test_row_count(con, view_name, is_view=True)
        if count > 0:
            print(f"  [PASS] Has {count:,} rows")
            passed += 1
        else:
            print(f"  [WARN] View is empty (0 rows)")
            failed += 1
            continue

        missing = test_columns(con, view_name, expected_cols)
        if not missing:
            print(f"  [PASS] All expected columns present")
            passed += 1
        else:
            print(f"  [FAIL] Missing columns: {missing}")
            failed += 1

    con.close()

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
