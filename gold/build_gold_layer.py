"""
Phase 5 — Standalone Gold Layer Builder
Loads Silver Parquet → DuckDB tables → Creates Gold star schema.

This script ONLY handles the Gold phase. For the full pipeline
(simulator → ingestion → ELT → gold), use run_pipeline.py instead.

Usage:
  python build_gold_layer.py
  python build_gold_layer.py --db-path /custom/path/to/spx_analytics.duckdb
  python build_gold_layer.py --verify-only  (just check if tables exist and have data)
  python build_gold_layer.py --sql-file /custom/path/to/create_star_schema.sql
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Encoding-safe Unicode helpers (Windows GBK console compatibility)
# ---------------------------------------------------------------------------
try:
    # Test if the console can encode a common Unicode character
    "\u2713".encode(sys.stdout.encoding or "utf-8")
    _USE_UNICODE = True
except (UnicodeEncodeError, LookupError):
    _USE_UNICODE = False

# Box-drawing characters (ASCII fallback uses +, -, |)
if _USE_UNICODE:
    _BOX_TL = "\u2554"    # ╔
    _BOX_TM = "\u2566"    # ╦
    _BOX_TR = "\u2557"    # ╗
    _BOX_H  = "\u2550"    # ═
    _BOX_V  = "\u2551"    # ║
    _BOX_ML = "\u2560"    # ╠
    _BOX_MM = "\u256c"    # ╬
    _BOX_MR = "\u2563"    # ╣
    _BOX_BL = "\u255a"    # ╚
    _BOX_BM = "\u2569"    # ╩
    _BOX_BR = "\u255d"    # ╝
    _CHECK  = "\u2713"   # ✓
    _WARN   = "\u26a0"    # ⚠
    _CROSS  = "\u2717"    # ✗
else:
    _BOX_TL = "+"
    _BOX_TM = "+"
    _BOX_TR = "+"
    _BOX_H  = "-"
    _BOX_V  = "|"
    _BOX_ML = "+"
    _BOX_MM = "+"
    _BOX_MR = "+"
    _BOX_BL = "+"
    _BOX_BM = "+"
    _BOX_BR = "+"
    _CHECK  = "[OK]"
    _WARN   = "[EMPTY]"
    _CROSS  = "[FAIL]"

# ---------------------------------------------------------------------------
# Path auto-detection
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent          # gold/
PROJECT_ROOT = SCRIPT_DIR.parent                      # repo root
DEFAULT_DB_PATH = PROJECT_ROOT / "duckdb" / "spx_analytics.duckdb"
DEFAULT_STAR_SQL = SCRIPT_DIR / "sql" / "create_star_schema.sql"
DEFAULT_MATERIALIZED_SQL = SCRIPT_DIR / "sql" / "create_materialized.sql"
DEFAULT_OLAP_SQL = SCRIPT_DIR / "sql" / "create_olap_views.sql"

# Legacy gold views (kept for backward compat with --sql-file pointing to old SQL)
LEGACY_GOLD_VIEWS = (
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_sentiment_price_view",
    "v_rolling_volatility",
    "v_momentum_signals",
    "v_sector_rotation",
    "v_sentiment_binned_returns",
    "v_fundamental_history",
)

# Star schema tables produced by create_star_schema.sql
STAR_TABLES = (
    "dim_ticker",
    "dim_date",
    "fact_daily_price",
    "fact_quarterly_fundamentals",
    "fact_earnings_transcript",
)

# Materialized fact tables produced by create_materialized.sql
MATERIALIZED_TABLES = (
    "fact_rolling_volatility",
    "fact_momentum_signals",
    "fact_ar1_results",
)

# OLAP views produced by create_olap_views.sql
OLAP_VIEWS = (
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_fundamental_snapshot",
    "v_fundamental_history",
    "v_sentiment_price_view",
    "v_sentiment_binned_returns",
    "v_sector_rotation",
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("build_gold_layer")


# ===========================================================================
# Pretty-print helpers
# ===========================================================================

def _print_summary_table(rows: list[tuple[str, str, str]]):
    """Print a Unicode box-drawing summary table.

    Each row is (name, row_count_str, status_str).
    """
    # Column widths (minimum)
    col_w = [30, 12, 21]
    # Adjust to actual content
    for name, cnt, status in rows:
        col_w[0] = max(col_w[0], len(name) + 2)
        col_w[1] = max(col_w[1], len(cnt) + 2)
        col_w[2] = max(col_w[2], len(status) + 2)

    def _row_line(left, mid, right, fill=None):
        fill = _BOX_H if fill is None else fill
        parts = [fill * w for w in col_w]
        return left + mid.join(parts) + right

    header_names = ("Table / View", "Rows", "Status")
    # Top border
    print(_row_line(_BOX_TL, _BOX_TM, _BOX_TR))
    # Header
    print(
        _BOX_V
        + _BOX_V.join(
            f" {h:<{col_w[i] - 1}}" for i, h in enumerate(header_names)
        )
        + _BOX_V
    )
    # Header-body separator
    print(_row_line(_BOX_ML, _BOX_MM, _BOX_MR))
    # Data rows
    for name, cnt, status in rows:
        print(
            _BOX_V
            + f" {name:<{col_w[0] - 1}}"
            + _BOX_V
            + f" {cnt:>{col_w[1] - 1}}"
            + _BOX_V
            + f" {status:<{col_w[2] - 1}}"
            + _BOX_V
        )
    # Bottom border
    print(_row_line(_BOX_BL, _BOX_BM, _BOX_BR))


# ===========================================================================
# Core logic
# ===========================================================================

def _check_silver_parquet_exists() -> dict[str, int]:
    """Return a dict of {label: file_count} for each Silver dataset."""
    silver_dir = PROJECT_ROOT / "output" / "silver"
    result = {}
    for label, subdir in [
        ("price", "price"),
        ("fundamentals", "fundamentals"),
        ("sentiment", "transcript_sentiment"),
    ]:
        d = silver_dir / subdir
        if d.exists():
            count = len(list(d.rglob("*.parquet")))
            result[label] = count
        else:
            result[label] = 0
    return result


def _connect_duckdb(db_path: Path):
    """Import duckdb and return a connection. Exits on failure."""
    try:
        import duckdb
    except ImportError:
        logger.error(
            "duckdb package not installed. Run:  pip install duckdb"
        )
        sys.exit(1)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Connecting to DuckDB: {db_path}")
    return duckdb.connect(str(db_path))


def _execute_gold_sql(con, sql_path: Path) -> dict[str, str]:
    """Execute the Gold SQL file statement-by-statement.

    Returns a dict of {statement_summary: 'OK' | error_message}.
    """
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    results: dict[str, str] = {}

    for i, stmt in enumerate(statements):
        # Skip blocks that are purely comments
        meaningful_lines = [
            ln for ln in stmt.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        if not meaningful_lines:
            continue

        # Extract a short description from the first meaningful SQL line
        desc = meaningful_lines[0].strip()[:80]

        try:
            con.execute(stmt)
            results[desc] = "OK"
            logger.info(f"  [{i + 1}] OK  — {desc}")
        except Exception as exc:
            err_msg = str(exc).splitlines()[0][:120]
            results[desc] = err_msg
            logger.error(f"  [{i + 1}] FAIL — {desc}")
            logger.error(f"         {err_msg}")
            raise SystemExit(1)

    return results


def _verify_tables(con, table_names: tuple[str, ...]) -> list[tuple[str, int | None, str]]:
    """Query each table for row count.

    Returns list of (table_name, row_count_or_None, status_string).
    """
    results = []
    for tname in table_names:
        try:
            row = con.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()
            count = row[0] if row else 0
            status = f"{_CHECK} OK" if count > 0 else f"{_WARN} EMPTY"
            results.append((tname, count, status))
            logger.info(f"  {tname}: {count:,} rows")
        except Exception as exc:
            err_short = str(exc).splitlines()[0][:60]
            results.append((tname, None, f"{_CROSS} {err_short}"))
            logger.error(f"  {tname}: FAILED — {err_short}")
    return results


def _print_sample_data(con, table_results: list[tuple[str, int | None, str]]):
    """Print a few sample rows from each successfully queried table."""
    for tname, count, status in table_results:
        if count is None or count == 0:
            continue
        try:
            sample = con.execute(f"SELECT * FROM {tname} LIMIT 3").fetchdf()
            logger.info(f"\n  Sample from {tname}:")
            for line in sample.to_string(index=False).splitlines():
                logger.info(f"    {line}")
        except Exception:
            pass  # non-critical


# ===========================================================================
# Entry points
# ===========================================================================

def build_gold(db_path: Path, sql_path: Path, materialized_sql_path: Path, olap_sql_path: Path) -> bool:
    """Full Gold build: execute star schema SQL → execute materialized SQL → execute OLAP views SQL → verify tables."""
    t0 = time.perf_counter()

    # 1. Check Silver Parquet availability
    logger.info("Checking Silver Parquet files...")
    parquet_counts = _check_silver_parquet_exists()
    for label, count in parquet_counts.items():
        if count == 0:
            logger.warning(f"  Silver/{label}: NO Parquet files found")
        else:
            logger.info(f"  Silver/{label}: {count} Parquet file(s)")

    if all(c == 0 for c in parquet_counts.values()):
        logger.error(
            "No Silver Parquet data found at all. "
            "Run the ELT phase first (python run_pipeline.py --phase elt)."
        )
        return False

    # 2. Validate SQL files
    if not sql_path.exists():
        logger.error(f"Gold SQL file not found: {sql_path}")
        return False
    logger.info(f"Gold SQL file: {sql_path}")

    if not materialized_sql_path.exists():
        logger.error(f"Materialized SQL file not found: {materialized_sql_path}")
        return False
    logger.info(f"Materialized SQL file: {materialized_sql_path}")

    if not olap_sql_path.exists():
        logger.error(f"OLAP views SQL file not found: {olap_sql_path}")
        return False
    logger.info(f"OLAP views SQL file: {olap_sql_path}")

    # 3. Change CWD so relative Parquet paths in SQL resolve correctly
    original_cwd = os.getcwd()
    os.chdir(str(PROJECT_ROOT))
    logger.info(f"Working directory set to: {PROJECT_ROOT}")

    try:
        # 4. Connect to DuckDB
        con = _connect_duckdb(db_path)

        # 5. Execute Gold SQL (creates staging tables + star tables + drops staging)
        logger.info(f"Executing Gold SQL ({sql_path.name})...")
        exec_results = _execute_gold_sql(con, sql_path)

        failures = [k for k, v in exec_results.items() if v != "OK"]
        if failures:
            logger.error(
                f"{len(failures)} SQL statement(s) failed. "
                "Exiting with error."
            )
            con.close()
            sys.exit(1)

        # 6. Execute materialized SQL (creates fact_rolling_volatility, fact_momentum_signals, fact_ar1_results)
        logger.info(f"Executing materialized SQL ({materialized_sql_path.name})...")
        mat_results = _execute_gold_sql(con, materialized_sql_path)

        mat_failures = [k for k, v in mat_results.items() if v != "OK"]
        if mat_failures:
            logger.error(
                f"{len(mat_failures)} materialized SQL statement(s) failed. "
                "Exiting with error."
            )
            con.close()
            sys.exit(1)

        # 7. Execute OLAP views SQL (creates 7 lightweight OLAP views)
        logger.info(f"Executing OLAP views SQL ({olap_sql_path.name})...")
        olap_results = _execute_gold_sql(con, olap_sql_path)

        olap_failures = [k for k, v in olap_results.items() if v != "OK"]
        if olap_failures:
            logger.error(
                f"{len(olap_failures)} OLAP views SQL statement(s) failed. "
                "Exiting with error."
            )
            con.close()
            sys.exit(1)

        # 8. Verify star schema tables
        logger.info("Verifying star schema tables...")
        star_table_results = _verify_tables(con, STAR_TABLES)

        # 9. Verify materialized tables
        logger.info("Verifying materialized tables...")
        mat_table_results = _verify_tables(con, MATERIALIZED_TABLES)

        # 10. Verify OLAP views
        logger.info("Verifying OLAP views...")
        olap_view_results = _verify_tables(con, OLAP_VIEWS)

        # 11. Sample data
        _print_sample_data(con, star_table_results)
        _print_sample_data(con, mat_table_results)
        _print_sample_data(con, olap_view_results)

        con.close()

    finally:
        os.chdir(original_cwd)

    elapsed = time.perf_counter() - t0

    # 12. Summary table (star schema + materialized + OLAP views)
    print()
    logger.info(f"Gold layer build completed in {elapsed:.1f}s")
    print()
    summary_rows = []
    for tname, count, status in star_table_results:
        count_str = f"{count:,}" if count is not None else "—"
        summary_rows.append((tname, count_str, status))
    for tname, count, status in mat_table_results:
        count_str = f"{count:,}" if count is not None else "—"
        summary_rows.append((tname, count_str, status))
    for tname, count, status in olap_view_results:
        count_str = f"{count:,}" if count is not None else "—"
        summary_rows.append((tname, count_str, status))
    _print_summary_table(summary_rows)
    print()

    nonexistent = [tname for tname, count, _ in star_table_results if count is None]
    nonexistent.extend(tname for tname, count, _ in mat_table_results if count is None)
    nonexistent.extend(tname for tname, count, _ in olap_view_results if count is None)
    return len(nonexistent) == 0


def verify_only(db_path: Path) -> bool:
    """Just check if Gold star tables exist and have data — no rebuild."""
    logger.info("Verify-only mode: checking existing Gold star schema tables...")

    if not db_path.exists():
        logger.error(f"DuckDB file not found: {db_path}")
        return False

    con = _connect_duckdb(db_path)
    table_results = _verify_tables(con, STAR_TABLES)
    _print_sample_data(con, table_results)
    con.close()

    print()
    summary_rows = []
    for tname, count, status in table_results:
        count_str = f"{count:,}" if count is not None else "—"
        summary_rows.append((tname, count_str, status))
    _print_summary_table(summary_rows)
    print()

    nonexistent = [tname for tname, count, _ in table_results if count is None]
    return len(nonexistent) == 0


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Phase 5 — Standalone Gold Layer Builder (Star Schema)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python build_gold_layer.py                      # build star schema with defaults
  python build_gold_layer.py --verify-only        # just check existing tables
  python build_gold_layer.py --db-path D:/my.duckdb
  python build_gold_layer.py --sql-file custom_star_schema.sql
        """,
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help=f"Path to DuckDB file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--sql-file",
        type=str,
        default=None,
        help=f"Path to Gold SQL file (default: {DEFAULT_STAR_SQL})",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing Gold tables — do not rebuild",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH
    sql_path = Path(args.sql_file) if args.sql_file else DEFAULT_STAR_SQL
    materialized_sql_path = DEFAULT_MATERIALIZED_SQL
    olap_sql_path = DEFAULT_OLAP_SQL

    # Banner
    sep = "=" * 60
    logger.info(sep)
    logger.info("  Phase 5 — Gold Layer Builder (Star Schema + Materialized Tables + OLAP Views)")
    logger.info(sep)
    logger.info(f"  DuckDB:            {db_path}")
    logger.info(f"  Gold SQL:         {sql_path}")
    logger.info(f"  Materialized SQL: {materialized_sql_path}")
    logger.info(f"  OLAP views SQL:   {olap_sql_path}")
    logger.info(f"  Project root:     {PROJECT_ROOT}")
    logger.info(f"  Python:           {sys.executable}")
    logger.info(f"  Mode:             {'verify-only' if args.verify_only else 'full build'}")
    logger.info(sep)
    print()

    if args.verify_only:
        ok = verify_only(db_path)
    else:
        ok = build_gold(db_path, sql_path, materialized_sql_path, olap_sql_path)

    if ok:
        logger.info(f"All Gold star schema tables are present. {_CHECK}")
    else:
        logger.warning(
            "Some Gold tables do not exist. "
            "Check logs above for details."
        )

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
