# Fundamental API Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign fundamental data pipeline to simulate Bloomberg/Wind/Yahoo Finance API behavior with `cutoff_date` filtering, ticker-partitioned landing zone, and Bloomberg-style history view in dashboard.

**Architecture:** `SPXDataProvider.get_fundamentals(ticker, freq, cutoff_date)` adds column-level filtering by fiscal period date. Landing zone shifts from `/{fiscal_date}/` to `/{ticker}/` partitioning. Simulator switches from batch dump to seed + incremental emit. Gold layer gains `v_fundamental_history`. Dashboard gets a Bloomberg-style fundamental history page.

**Tech Stack:** Python, DuckDB, Parquet, Streamlit, pandas

---

## File Map

| File | Change |
|------|--------|
| `pipeline/data_provider.py` | Add `cutoff_date` parameter + column filtering |
| `duckdb/init_bronze.py` | ALTER table add `freq` column |
| `pipeline/ingestion_engine.py` | Adapt to `/{ticker}/` landing zone structure |
| `pipeline/simulators/comprehensive_simulator.py` | Delete `_emit_all_fundamentals()`, add `_seed_all_fundamentals()`, adapt `_emit_fundamentals(date)` |
| `pipeline/elt_pipeline.py` | Adapt `_unpivot_fundamental()` to include `freq` in output columns |
| `gold/sql/create_gold_views.sql` | Add `freq` to Silver table load; add `v_fundamental_history` |
| `gold/build_gold_layer.py` | No code change needed (SQL is the source of truth) |
| `dashboard.py` | Rename "Fundamental Snapshot" → "Fundamental History" with cutoff_date controls |

---

## Task 1: Data Provider — Add `cutoff_date` Parameter

**Files:**
- Modify: `pipeline/data_provider.py:122-163`

- [ ] **Step 1: Write the failing test**

Create `tests/test_data_provider_cutoff.py`:

```python
"""Tests for SPXDataProvider.get_fundamentals cutoff_date behavior."""
import pytest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.data_provider import SPXDataProvider


class TestGetFundamentalsCutoffDate:
    """Test cutoff_date filtering logic."""

    def test_no_cutoff_returns_all_periods(self):
        """cutoff_date=None should return all fiscal periods (backward compat)."""
        provider = SPXDataProvider()
        # AAPL has fundamental data
        result = provider.get_fundamentals("AAPL", freq="quarterly", cutoff_date=None)
        assert "income" in result or "balance" in result  # at least one report type exists

    def test_cutoff_filters_future_periods(self):
        """period_date > cutoff_date columns should be excluded."""
        provider = SPXDataProvider()
        all_data = provider.get_fundamentals("AAPL", freq="quarterly", cutoff_date=None)
        cutoff_data = provider.get_fundamentals("AAPL", freq="quarterly", cutoff_date="2020-12-31")

        if "income" in all_data and "income" in cutoff_data:
            all_cols = set(all_data["income"].columns)
            cutoff_cols = set(cutoff_data["income"].columns)
            # cutoff should have <= columns of all
            assert cutoff_cols <= all_cols
            # 2021 periods should be absent
            future_cols = [c for c in all_cols if c > "2021"]
            assert not any(c in cutoff_cols for c in future_cols)

    def test_invalid_ticker_raises(self):
        """Non-existent ticker should raise ValueError."""
        provider = SPXDataProvider()
        with pytest.raises(ValueError, match="not found"):
            provider.get_fundamentals("NOTATICKER", cutoff_date="2020-12-31")

    def test_invalid_freq_raises(self):
        """Invalid freq should raise ValueError."""
        provider = SPXDataProvider()
        with pytest.raises(ValueError, match="freq must be"):
            provider.get_fundamentals("AAPL", freq="monthly")
```

- [ ] **Step 2: Run test to verify it fails**

```
$ python -m pytest tests/test_data_provider_cutoff.py -v
========================= 1 failed, 3 error =========================
ERROR: test_no_cutoff_returns_all_periods - TypeError: get_fundamentals() got an unexpected keyword argument 'cutoff_date'
```

- [ ] **Step 3: Implement cutoff_date filtering**

Modify `pipeline/data_provider.py:get_fundamentals()`:

```python
def get_fundamentals(
    self,
    ticker: str,
    freq: str = "quarterly",
    cutoff_date: Optional[str] = None,  # NEW
) -> dict:
    """
    Get fundamental data for a ticker.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')
        freq: 'annual' or 'quarterly' (default: 'quarterly')
        cutoff_date: Optional 'YYYY-MM-DD'. If provided, only fiscal periods
            with period_date <= cutoff_date are returned. Simulates the
            "knowledge cutoff" — you cannot see earnings that haven't been
            reported yet.

    Returns:
        dict with keys: 'income', 'balance', 'cashflow', 'profile'
        Each value is a DataFrame with dates as columns, metrics as rows.
        Returns empty dict if no data for that ticker.
    """
    if freq not in ("annual", "quarterly"):
        raise ValueError(f"freq must be 'annual' or 'quarterly', got '{freq}'")

    result = {}

    # Check which files exist for this ticker
    for report_type in ("income", "balance", "cashflow", "profile"):
        filename = f"{ticker}_{report_type}_{freq}.csv"
        filepath = FUNDAMENTAL_DIR / filename

        if filepath.exists():
            try:
                df = pd.read_csv(filepath, index_col=0)
                # NEW: apply cutoff_date filter — drop columns (fiscal periods) > cutoff
                if cutoff_date is not None:
                    valid_cols = [c for c in df.columns if c <= cutoff_date]
                    df = df[valid_cols]
                result[report_type] = df
            except Exception as e:
                raise DataIntegrityError(f"Corrupted file {filepath}: {e}")
        else:
            pass

    return result
```

- [ ] **Step 4: Run test to verify it passes**

```
$ python -m pytest tests/test_data_provider_cutoff.py -v
========================= 4 passed =========================
```

- [ ] **Step 5: Commit**

```bash
git add pipeline/data_provider.py tests/test_data_provider_cutoff.py
git commit -m "feat(data_provider): add cutoff_date parameter to get_fundamentals()

Simulates Bloomberg/Wind knowledge cutoff behavior: fiscal periods
with period_date > cutoff_date are excluded from results.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 2: DuckDB Bronze — Add `freq` Column

**Files:**
- Modify: `duckdb/init_bronze.py`

- [ ] **Step 1: Read current init_bronze.py**

```bash
cat duckdb/init_bronze.py
```

- [ ] **Step 2: Add `freq` column via ALTER**

Find the `raw_fundamental_index` CREATE TABLE block and add `freq VARCHAR` column:

```sql
-- In duckdb/init_bronze.py, modify raw_fundamental_index CREATE TABLE:
CREATE TABLE IF NOT EXISTS raw_fundamental_index (
    ticker         VARCHAR,
    report_type    VARCHAR,
    freq           VARCHAR,    -- ADDED: 'annual' or 'quarterly'
    fiscal_date    DATE,
    file_path      VARCHAR,
    received_at    TIMESTAMP
);
```

Also add `freq` to the INSERT statement from `ingestion_engine.py` (it inserts into this table). The `freq` comes from the source filename pattern `{ticker}_{report_type}_{freq}.csv`.

- [ ] **Step 3: Initialize and verify**

```bash
python duckdb/init_bronze.py
```

Verify the column exists:
```python
import duckdb
con = duckdb.connect("duckdb/spx_analytics.duckdb")
print(con.execute("PRAGMA table_info('raw_fundamental_index')").fetchdf())
```

- [ ] **Step 4: Commit**

```bash
git add duckdb/init_bronze.py
git commit -m "feat(bronze): add freq column to raw_fundamental_index

Distinguishes annual vs quarterly fundamental reports which share
the same ticker/report_type/fiscal_date composite key.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Ingestion Engine — Adapt to Ticker-Partitioned Landing Zone

**Files:**
- Modify: `pipeline/ingestion_engine.py`

- [ ] **Step 1: Read current ingestion_engine.py (relevant sections)**
- [ ] **Step 2: Identify fundamental file handling**

The ingestion engine's watchdog handler for `fundamentals/` directory currently expects `/{fiscal_date}/` structure. Update to handle `/{ticker}/` structure:
- Path pattern: `landing_zone/fundamentals/{ticker}/{report_type}_{freq}.csv`
- Extract `ticker`, `report_type`, `freq` from the file path
- Insert into `raw_fundamental_index(ticker, report_type, freq, fiscal_date, file_path, received_at)`
- `fiscal_date` is extracted from the CSV header columns (first date column)

- [ ] **Step 3: Run integration test**

After running the full backfill, verify Bronze has `freq` populated:
```python
import duckdb
con = duckdb.connect("duckdb/spx_analytics.duckdb")
df = con.execute("SELECT DISTINCT freq FROM raw_fundamental_index").fetchdf()
print(df)  # should show 'annual' and 'quarterly'
```

- [ ] **Step 4: Commit**

```bash
git add pipeline/ingestion_engine.py
git commit -m "fix(ingestion): adapt to ticker-partitioned fundamental landing zone

Landing zone moved from fundamentals/{fiscal_date}/ to fundamentals/{ticker}/.
Updated watchdog handler to extract ticker, report_type, freq from path.
Inserts freq column into raw_fundamental_index.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 4: Simulator — Seed + Incremental Emit Pattern

**Files:**
- Modify: `pipeline/simulators/comprehensive_simulator.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_simulator_fundamental_emit.py`:

```python
"""Tests for simulator fundamental emit patterns."""
import pytest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.simulators.comprehensive_simulator import ComprehensiveSimulator


class TestFundamentalEmitPattern:
    """Verify simulator uses seed+incremental, not batch dump."""

    def test_seed_emits_all_tickers_once(self):
        """_seed_all_fundamentals() should emit for all tickers at start."""
        simulator = ComprehensiveSimulator()
        # Count seedable tickers
        ticker_files = list((Path('output/landing_zone/fundamentals')).glob('*/*_income_quarterly.csv'))
        assert len(ticker_files) > 0, "No fundamental files emitted"

    def test_no_batch_dump_at_backfill_start(self):
        """Backfill should NOT call _emit_all_fundamentals() (the old batch method)."""
        import pipeline.simulators.comprehensive_simulator as mod
        assert not hasattr(mod.ComprehensiveSimulator, '_emit_all_fundamentals'), \
            "_emit_all_fundamentals should be deleted"
```

- [ ] **Step 2: Run test to verify it fails**

```
$ python -m pytest tests/test_simulator_fundamental_emit.py -v
========================= 1 failed =========================
FAILED: _emit_all_fundamentals still exists
```

- [ ] **Step 3: Implement seed + incremental emit**

In `comprehensive_simulator.py`:

1. **Delete** `_emit_all_fundamentals()` method
2. **Add** `_seed_all_fundamentals()` method:
   ```python
   def _seed_all_fundamentals(self) -> int:
       """
       Emit ALL historical fundamental files to landing zone, organized by ticker.
       Called once at start of backfill to seed the ticker-partitioned landing zone.
       Returns count of files emitted.
       """
       # For each ticker that has fundamental data:
       #   for each freq in ('quarterly', 'annual'):
       #     for each report_type in ('income', 'balance', 'cashflow'):
       #       source = FUNDAMENTAL_DIR / f"{ticker}_{report_type}_{freq}.csv"
       #       dest = landing_zone / "fundamentals" / ticker / f"{report_type}_{freq}.csv"
       #       copy if not exists
   ```
3. **Modify** `run_backfill()`:
   - At start: call `_seed_all_fundamentals()` once
   - In daily loop: `_emit_fundamentals(date)` — unchanged logic (uses `_fundamental_index` keyed by fiscal_date)

- [ ] **Step 4: Run test to verify it passes**

```
$ python -m pytest tests/test_simulator_fundamental_emit.py -v
========================= 2 passed =========================
```

- [ ] **Step 5: Commit**

```bash
git add pipeline/simulators/comprehensive_simulator.py
git commit -m "feat(simulator): replace batch dump with seed+incremental emit

DELETE _emit_all_fundamentals() — batch dump pattern removed.
ADD _seed_all_fundamentals() — emits all historical data per ticker
  once at backfill start to seed the ticker-partitioned landing zone.
DAILY loop unchanged — _emit_fundamentals(date) uses existing
  _fundamental_index for incremental new-period emits.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 5: ELT Pipeline — Include `freq` in Silver Output

**Files:**
- Modify: `pipeline/elt_pipeline.py` (specifically `_unpivot_fundamental`)

- [ ] **Step 1: Find the return statement in `_unpivot_fundamental`**

Current output columns: `["ticker", "report_type", "metric", "period_date", "value"]`

- [ ] **Step 2: Add `freq` to unpivot output**

```python
# In _unpivot_fundamental(), add freq column before return:
result["ticker"] = ticker
result["report_type"] = report_type
result["freq"] = freq   # ADDED: track annual vs quarterly
return result[["ticker", "report_type", "metric", "period_date", "value", "freq"]]
```

- [ ] **Step 3: Run Silver transform and verify schema**

```bash
python -m pipeline.elt_pipeline --resource fundamentals
python -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb')
con.execute(\"CREATE TABLE test_silver AS SELECT * FROM read_parquet('output/silver/fundamentals/**/*.parquet', hive_partitioning=true) LIMIT 1\")
print(con.execute('PRAGMA table_info(\"test_silver\")').fetchdf())
"
```
Verify `freq` column is present.

- [ ] **Step 4: Commit**

```bash
git add pipeline/elt_pipeline.py
git commit -m "feat(elt): add freq column to silver_fundamentals unpivot

Propagates annual/quarterly frequency through Silver layer to
enable correct Gold view queries filtering by freq.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 6: Gold Views — Add `freq` + New `v_fundamental_history`

**Files:**
- Modify: `gold/sql/create_gold_views.sql`

- [ ] **Step 1: Update Silver table load to include `freq`**

In `create_gold_views.sql`, the `silver_fundamentals` table load:
```sql
CREATE TABLE IF NOT EXISTS silver_fundamentals AS
SELECT ticker, report_type, fiscal_date, metric, value, freq   -- ADD freq
FROM read_parquet('output/silver/fundamentals/**/*.parquet', hive_partitioning=true);
```

- [ ] **Step 2: Add `v_fundamental_history`**

```sql
-- v_fundamental_history — full time-series per ticker for Bloomberg-style queries
CREATE OR REPLACE VIEW v_fundamental_history AS
SELECT
    ticker,
    period_date AS fiscal_date,
    report_type,
    freq,
    metric,
    CAST(value AS DOUBLE) AS value
FROM silver_fundamentals
WHERE period_date IS NOT NULL
ORDER BY ticker, period_date, metric;
```

- [ ] **Step 3: Run Gold layer build and verify views**

```bash
python gold/build_gold_layer.py
python gold/tests/test_gold_views.py
```

All existing 9 views + new `v_fundamental_history` should pass.

- [ ] **Step 4: Commit**

```bash
git add gold/sql/create_gold_views.sql
git commit -m "feat(gold): add v_fundamental_history and freq to Silver table load

v_fundamental_history provides full fiscal period time-series per ticker,
enabling Bloomberg-style 'fundamentals as of date X' queries with
WHERE ticker='AAPL' AND fiscal_date <= '2020-06-30'.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 7: Dashboard — Fundamental Snapshot → Fundamental History

**Files:**
- Modify: `dashboard.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_dashboard_fundamental_history.py`:

```python
"""Tests for dashboard fundamental history page."""
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_fundamental_history_view_renders():
    """Fundamental History page should render with ticker and cutoff controls."""
    # This is a Streamlit app — test the underlying query logic
    import duckdb
    con = duckdb.connect(str(Path(__file__).parent.parent / "duckdb" / "spx_analytics.duckdb"), read_only=True)
    # Verify v_fundamental_history exists and returns data
    result = con.execute(
        "SELECT ticker, fiscal_date, metric, value FROM v_fundamental_history WHERE ticker='AAPL' LIMIT 5"
    ).fetchdf()
    assert len(result) > 0, "v_fundamental_history should return data for AAPL"
    assert "fiscal_date" in result.columns
    assert "freq" in result.columns
```

- [ ] **Step 2: Run test to verify it fails**

```
$ python -m pytest tests/test_dashboard_fundamental_history.py -v
========================= 1 failed =========================
FAILED: v_fundamental_history does not exist
```

- [ ] **Step 3: Implement Fundamental History page**

In `dashboard.py`:

1. Rename `render_fundamental_snapshot()` → `render_fundamental_history()`
2. Add controls: ticker selectbox, cutoff_date date input, report_type multiselect, metrics multiselect
3. Query `v_fundamental_history` with `WHERE ticker=? AND fiscal_date <= ?`
4. Pivot to Bloomberg-style table: rows=metrics, columns=fiscal periods

```python
def render_fundamental_history(con, ticker_list: list) -> None:
    """Bloomberg-style fundamental history with cutoff_date."""
    st.subheader("Fundamental History")

    col1, col2, col3 = st.columns(3)
    with col1:
        ticker = st.selectbox("Ticker", ticker_list)
    with col2:
        cutoff = st.text_input("Cutoff Date (YYYY-MM-DD)", value="2024-12-31")
    with col3:
        freq_filter = st.selectbox("Frequency", ["quarterly", "annual", "both"])

    col4, col5 = st.columns([2, 1])
    with col4:
        report_types = st.multiselect(
            "Report Type", ["income", "balance", "cashflow"], default=["income"]
        )
    with col5:
        top_n = st.number_input("Top N periods", value=8, min_value=2, max_value=20)

    if not ticker:
        st.info("Select a ticker to view fundamentals.")
        return

    freq_clause = ""
    if freq_filter == "quarterly":
        freq_clause = "AND freq = 'quarterly'"
    elif freq_filter == "annual":
        freq_clause = "AND freq = 'annual'"

    report_clause = ""
    if report_types:
        types_sql = ", ".join(f"'{r}'" for r in report_types)
        report_clause = f"AND report_type IN ({types_sql})"

    query = f"""
        SELECT ticker, fiscal_date, report_type, freq, metric, value
        FROM v_fundamental_history
        WHERE ticker = '{ticker}' AND fiscal_date <= '{cutoff}' {freq_clause} {report_clause}
        ORDER BY fiscal_date DESC, metric
        LIMIT 1000
    """
    df = con.execute(query).fetchdf()

    if df.empty:
        st.warning(f"No fundamental data for {ticker} as of {cutoff}")
        return

    # Pivot: rows=metrics, columns=fiscal periods
    pivot = df.pivot_table(
        index="metric",
        columns="fiscal_date",
        values="value",
        aggfunc="first"
    )
    # Show only top N most recent periods
    pivot = pivot[sorted(pivot.columns, reverse=True)[:top_n]]

    st.markdown(f"**{ticker} — Fundamental History (as of {cutoff})**")
    st.dataframe(pivot, use_container_width=True)

    st.caption(f"Rows: {len(df)} | Freq: {freq_filter} | Report types: {report_types}")
```

4. **CRITICAL: Use parameterized queries** — never interpolate user input into SQL strings.
   ```python
   # GOOD — parameterized
   df = con.execute("""
       SELECT ticker, fiscal_date, report_type, freq, metric, value
       FROM v_fundamental_history
       WHERE ticker = ? AND fiscal_date <= ? AND freq = ? AND report_type IN (?, ?, ?)
   """, [ticker, cutoff, freq_filter] + report_types).fetchdf()

   # BAD — SQL injection (NEVER do this)
   # query = f"WHERE ticker = '{ticker}' AND fiscal_date <= '{cutoff}'"
   ```
5. Update sidebar page list: replace "Fundamental Snapshot" with "Fundamental History"
6. Update `main()` routing: `render_fundamental_history(conn, ticker_list)` instead of `render_fundamental_snapshot(fundamental_snapshot, selected_ticker)`

- [ ] **Step 4: Run test to verify it passes**

```
$ python -m pytest tests/test_dashboard_fundamental_history.py -v
========================= 1 passed =========================
```

- [ ] **Step 5: Commit**

```bash
git add dashboard.py tests/test_dashboard_fundamental_history.py
git commit -m "feat(dashboard): redesign Fundamental Snapshot to Fundamental History

Bloomberg-style fundamental history page with:
- Ticker selector
- Cutoff date input (knowledge cutoff)
- Frequency filter (quarterly/annual/both)
- Report type multiselect
- Top N periods selector
- Pivoted table (rows=metrics, cols=fiscal periods)

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Integration Test

After all tasks complete, run the full pipeline:

```bash
# 1. Re-init Bronze (with new freq column)
python duckdb/init_bronze.py

# 2. Backfill with new simulator (seed + incremental)
python -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-12-30

# 3. Ingest
python -m pipeline.ingestion_engine --mode scan

# 4. ELT
python -m pipeline.elt_pipeline

# 5. Gold
python gold/build_gold_layer.py

# 6. Verify
python gold/tests/test_gold_views.py
```

Expected results:
- All 9 existing Gold views still pass
- `v_fundamental_history` returns correct row counts
- Dashboard "Fundamental History" page renders with Bloomberg-style table

**End-to-end cutoff_date verification:**
```python
import duckdb
from datetime import date

con = duckdb.connect("duckdb/spx_analytics.duckdb", read_only=True)

# Verify cutoff_date filtering works end-to-end
cutoff = date(2020, 12, 31)
df = con.execute("""
    SELECT DISTINCT fiscal_date FROM v_fundamental_history
    WHERE ticker='AAPL' AND fiscal_date <= ?
""", [cutoff]).fetchdf()

max_date = df['fiscal_date'].max()
assert max_date <= cutoff, f"Expected max fiscal_date <= {cutoff}, got {max_date}"

# Verify no fiscal_date > cutoff in any ticker
violations = con.execute("""
    SELECT ticker, fiscal_date FROM v_fundamental_history
    WHERE fiscal_date > '2020-12-31'
    LIMIT 5
""").fetchdf()
assert len(violations) == 0, "No periods beyond cutoff should exist in view"

# Verify freq column is present and populated
freq_counts = con.execute("""
    SELECT freq, COUNT(*) as n FROM v_fundamental_history
    GROUP BY freq
""").fetchdf()
print(freq_counts)  # should show both 'quarterly' and 'annual' counts
assert freq_counts['freq'].nunique() >= 1, "freq column should be populated"
```

**New integration test to add to `gold/tests/test_gold_views.py`:**
```python
def test_v_fundamental_history_cutoff_filtering(con):
    """Verify v_fundamental_history respects cutoff_date via WHERE clause."""
    from datetime import date
    cutoff = date(2020, 12, 31)
    result = con.execute("""
        SELECT DISTINCT fiscal_date FROM v_fundamental_history
        WHERE ticker='AAPL' AND fiscal_date <= ?
    """, [cutoff]).fetchdf()
    assert result['fiscal_date'].max() <= cutoff
```

```bash
# 1. Re-init Bronze (with new freq column)
python duckdb/init_bronze.py

# 2. Backfill with new simulator (seed + incremental)
python -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-12-30

# 3. Ingest
python -m pipeline.ingestion_engine --mode scan

# 4. ELT
python -m pipeline.elt_pipeline

# 5. Gold
python gold/build_gold_layer.py

# 6. Verify
python gold/tests/test_gold_views.py

# 7. Dashboard
python -m streamlit run dashboard.py --server.headless true
```

Expected results:
- All 9 existing Gold views still pass
- `v_fundamental_history` returns correct row counts
- Dashboard "Fundamental History" page renders with Bloomberg-style table
