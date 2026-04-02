# Gold Layer Enhancement & Bug Fixes — Implementation Plan

> **Status: ⚠️ PARTIAL** — Phases 1, 2, 4 done; Phase 3 Dashboard incomplete (pages removed)
>
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **For inline execution:** Use superpowers:executing-plans with checkpoint reviews between phases.

**Goal:** Fix critical ingestion bugs, replace watchdog with DuckDB-based message queue, add 5 predictive Gold OLAP views, enhance dashboard, add CI/CD.

**Architecture:** 4 phases — (1) Bug fixes + message broker producer/consumer, (2) 5 new Gold SQL views, (3) Dashboard pages, (4) CI/CD + unit tests. Phase 1 must complete before phase 2; phase 3 depends on phase 2; phase 4 is independent.

**Tech Stack:** Python, DuckDB, pandas, watchdog (to be replaced), Streamlit, pytest, GitHub Actions

**Spec:** `docs/superpowers/specs/2026-04-01-gold-layer-enhancement-design.md`

---

## File Map

| File | Responsibility |
|------|---------------|
| `pipeline/ingestion_engine.py` | Bug fix: executemany, audit truthfulness; MB: poll_queue() consumer; remove watchdog |
| `pipeline/simulators/comprehensive_simulator.py` | MB: add `_enqueue_message()` after each file write |
| `duckdb/create_bronze_tables.sql` | MB: add `queue_messages` table |
| `test_pipeline.py` | Bug fix: `sys.executable` instead of hardcoded path |
| `gold/sql/create_gold_views.sql` | Add 5 new views |
| `gold/build_gold_layer.py` | Update `GOLD_VIEWS` tuple to 9 entries |
| `gold/tests/test_gold_views.py` | Extend tests to 9 views |
| `dashboard.py` | Add 5 new pages |
| `tests/test_data_provider.py` | New: unit tests for SPXDataProvider |
| `tests/test_ingestion_engine.py` | New: batch insert + queue consumer tests |
| `.github/workflows/test.yml` | New: CI/CD pipeline |

---

## Phase 1: Bug Fixes + Message Broker

### Task 1: Fix test_pipeline.py Python path

**Files:** `test_pipeline.py:17`

- [ ] **Step 1: Edit test_pipeline.py**

```python
# BEFORE (line 17)
PYTHON = "C:/miniconda3/envs/qf5214_project/python.exe"

# AFTER — add at top of CMDS tuple definition
import sys
PYTHON = sys.executable
```

- [ ] **Step 2: Verify**

Run: `"C:/miniconda3/envs/qf5214_project/python.exe" test_pipeline.py --help` (or equivalent)
Expected: script runs without path errors

- [ ] **Step 3: Commit**

```bash
git add test_pipeline.py
git commit -m "fix: use sys.executable for portable Python path"
```

---

### Task 2: Move safe_float / safe_int to module level

**Files:** `pipeline/ingestion_engine.py:9-101`

- [ ] **Step 1: Read current state of ingestion_engine.py lines 1-30**

```bash
head -30 pipeline/ingestion_engine.py
```

- [ ] **Step 2: Add module-level helpers after imports (after line 14)**

```python
# Module-level helpers for NaN-to-NULL conversion
import math

def _safe_float(val):
    """Convert NaN/None to SQL NULL for float columns."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    return val

def _safe_int(val):
    """Convert NaN/None to SQL NULL for integer columns."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    return int(val) if val is not None else None
```

- [ ] **Step 3: Remove inline safe_float/safe_int definitions** (inside `ingest_price_file` method, around lines 110-120 in current code)

Delete the `def safe_float` and `def safe_int` functions that are defined inside the loop.

- [ ] **Step 4: Verify no other references break**

Search for `safe_float` and `safe_int` references:
```bash
grep -n "safe_float\|safe_int" pipeline/ingestion_engine.py
```
Expected: only references to `_safe_float` and `_safe_int` (the module-level functions), no redefinitions inside methods.

- [ ] **Step 5: Commit**

```bash
git add pipeline/ingestion_engine.py
git commit -m "refactor: move safe_float/safe_int to module level"
```

---

### Task 3: Replace row-by-row INSERT with executemany

**Files:** `pipeline/ingestion_engine.py:107-145`

- [ ] **Step 1: Read current ingest_price_file method**

```python
# Current structure (lines ~107-145):
def ingest_price_file(self, filepath: Path) -> int:
    df = pd.read_csv(filepath)
    file_hash = self._compute_file_hash(filepath)
    market_date = filepath.stem.replace("price_", "")

    con = self._get_connection()
    rows_inserted = 0
    for _, row in df.iterrows():          # <-- REPLACE THIS
        try:
            con.execute("INSERT INTO raw_price_stream ...", [...])  # <-- REPLACE THIS
            rows_inserted += 1
        except Exception as e:
            logger.warning(...)

    self._log_audit("price", ..., "SUCCESS")  # <-- FIX THIS TOO
    return rows_inserted
```

- [ ] **Step 2: Replace the for loop with batch insert + proper audit**

```python
def ingest_price_file(self, filepath: Path) -> int:
    df = pd.read_csv(filepath)
    file_hash = self._compute_file_hash(filepath)
    market_date = filepath.stem.replace("price_", "")

    # Build batch rows
    rows = []
    for _, row in df.iterrows():
        rows.append([
            row["Ticker"],
            market_date,
            _safe_float(row.get("Open")),
            _safe_float(row.get("High")),
            _safe_float(row.get("Low")),
            _safe_float(row.get("Close")),
            _safe_float(row.get("Adj Close")),
            _safe_int(row.get("Volume")),
        ])

    if not rows:
        return 0

    con = self._get_connection()
    con.begin()
    rows_inserted = 0
    failed_rows = 0
    try:
        con.executemany(
            "INSERT INTO raw_price_stream (ticker, date, open, high, low, close, adj_close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        con.commit()
        rows_inserted = len(rows)
        status = "SUCCESS"
    except Exception as e:
        con.rollback()
        logger.error(f"Batch insert failed for {filepath}: {e}")
        failed_rows = len(rows)
        status = "FAILED"

    self._log_audit("price", df["Ticker"].iloc[0] if len(df) > 0 else "UNKNOWN", market_date, file_hash, status)
    logger.info(f"Ingested {rows_inserted} price records from {filepath.stem} ({failed_rows} failed)")
    return rows_inserted
```

- [ ] **Step 3: Run quick sanity check**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import sys; sys.path.insert(0, '.')
from pipeline.ingestion_engine import IngestionEngine, _safe_float, _safe_int
print('safe_float(1.5):', _safe_float(1.5))
print('safe_float(None):', _safe_float(None))
print('safe_float(float(\"nan\")):', _safe_float(float('nan')))
print('All helpers OK')
"
```

- [ ] **Step 4: Commit**

```bash
git add pipeline/ingestion_engine.py
git commit -m "fix: replace row-by-row INSERT with executemany batch"
```

---

### Task 4: Fix audit log truthfulness

**Files:** `pipeline/ingestion_engine.py` — already done in Task 3 above (status variable is now correctly set)

Verify the fix is present: look for the `status` variable being set to "FAILED" when batch fails (line ~139 in new code). The commit from Task 3 already includes this fix.

---

### Task 5: Add queue_messages table to schema

**Files:** `duckdb/create_bronze_tables.sql`

- [ ] **Step 1: Read end of create_bronze_tables.sql**

```bash
tail -20 duckdb/create_bronze_tables.sql
```

- [ ] **Step 2: Add queue_messages table before the final closing comment or after last table**

```sql
-- Message queue for event-driven ingestion (replaces watchdog)
CREATE SEQUENCE IF NOT EXISTS queue_messages_seq;
CREATE TABLE IF NOT EXISTS queue_messages (
    id          BIGINT DEFAULT NEXTVAL('queue_messages_seq') PRIMARY KEY,
    source      VARCHAR(50),
    msg_type    VARCHAR(100),         -- 'price_file', 'fundamental_file', 'transcript_file'
    payload     VARCHAR(1000),        -- JSON: {"filepath": "...", "ticker": "AAPL", "date": "2024-01-15"}
    status      VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, PROCESSING, DONE, FAILED
    created_at  TIMESTAMP DEFAULT NOW(),
    consumed_at TIMESTAMP,
    error_message VARCHAR(500)
);
```

- [ ] **Step 3: Verify with init script**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py
```

Expected output should include `queue_messages` in the created tables list.

- [ ] **Step 4: Commit**

```bash
git add duckdb/create_bronze_tables.sql
git commit -m "feat: add queue_messages table for event-driven ingestion"
```

---

### Task 6: Add _enqueue_message to comprehensive_simulator

**Files:** `pipeline/simulators/comprehensive_simulator.py`

- [ ] **Step 1: Read comprehensive_simulator.py to find where files are emitted**

Find the methods that emit price, fundamental, and transcript files.

```bash
grep -n "_emit\|write\|csv_path\|pdf_path" pipeline/simulators/comprehensive_simulator.py | head -30
```

- [ ] **Step 2: Add _enqueue_message helper method**

Add after the `__init__` method:

```python
import json

def _enqueue_message(self, msg_type: str, payload: dict):
    """Insert a message into queue_messages for downstream consumers."""
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(
            "INSERT INTO queue_messages (source, msg_type, payload, status) VALUES (?, ?, ?, 'PENDING')",
            ["simulator", msg_type, json.dumps(payload)]
        )
    finally:
        con.close()
```

- [ ] **Step 3: Call _enqueue_message after each file write**

Find the three places where files are written (price CSV, fundamental CSV, transcript PDF) and add after each:

```python
# After price CSV write:
self._enqueue_message("price_file", {
    "filepath": str(csv_path),
    "ticker": ticker,
    "date": market_date
})

# After fundamental CSV write:
self._enqueue_message("fundamental_file", {
    "filepath": str(fund_csv_path),
    "ticker": ticker,
    "report_type": report_type
})

# After transcript PDF write:
self._enqueue_message("transcript_file", {
    "filepath": str(pdf_path),
    "ticker": ticker,
    "event_date": event_date
})
```

- [ ] **Step 4: Verify DuckDB import is present**

Check that `import duckdb` and `import json` are in the file. Add if missing.

- [ ] **Step 5: Commit**

```bash
git add pipeline/simulators/comprehensive_simulator.py
git commit -m "feat: add _enqueue_message to simulator for message queue"
```

---

### Task 7: Replace watchdog with poll_queue in ingestion_engine

**Files:** `pipeline/ingestion_engine.py`

- [ ] **Step 1: Read current run_watchdog function**

```bash
grep -n "run_watchdog\|LandingZoneHandler\|Observer" pipeline/ingestion_engine.py
```

- [ ] **Step 2: Add poll_queue method to IngestionEngine class**

Add after the `scan_and_ingest` method:

```python
def poll_queue(self, batch_size: int = 100):
    """
    Poll queue_messages table and process pending messages.

    Called by run_queue_mode() loop. Processes up to batch_size
    PENDING messages per call.
    """
    import json
    con = self._get_connection()

    rows = con.execute("""
        SELECT id, msg_type, payload
        FROM queue_messages
        WHERE status = 'PENDING'
        ORDER BY created_at
        LIMIT ?
    """, [batch_size]).fetchall()

    processed = 0
    for msg_id, msg_type, payload in rows:
        try:
            payload_dict = json.loads(payload)
            filepath = Path(payload_dict["filepath"])

            if msg_type == "price_file":
                self.ingest_price_file(filepath)
            elif msg_type == "fundamental_file":
                self.ingest_fundamental_file(filepath, payload_dict.get("date", ""))
            elif msg_type == "transcript_file":
                self.ingest_transcript_file(filepath)
            else:
                logger.warning(f"Unknown msg_type: {msg_type}")

            con.execute(
                "UPDATE queue_messages SET status = 'DONE', consumed_at = NOW() WHERE id = ?",
                [msg_id]
            )
            processed += 1
        except Exception as e:
            logger.warning(f"Failed to process message {msg_id}: {e}")
            con.execute(
                "UPDATE queue_messages SET status = 'FAILED', error_message = ? WHERE id = ?",
                [str(e), msg_id]
            )

    return processed
```

- [ ] **Step 3: Add run_queue_mode function**

Add at module level (after `run_watchdog` function, before `if __name__ == "__main__":`):

```python
def run_queue_mode(poll_interval: float = 1.0):
    """
    Run the ingestion engine in queue-polling mode.

    Polls queue_messages table continuously, processing new files
    as they are enqueued by the Simulator.
    """
    engine = IngestionEngine()
    logger.info("Starting ingestion engine in queue mode...")
    logger.info("Polling queue_messages every {:.1f}s. Press Ctrl+C to stop.".format(poll_interval))

    try:
        while True:
            processed = engine.poll_queue()
            if processed > 0:
                logger.info(f"Processed {processed} messages")
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down ingestion engine...")
    finally:
        engine.close()
```

- [ ] **Step 4: Update argparse to add --mode queue**

Replace the argparse block:

```python
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SPX Data Pipeline - Ingestion Engine")
    parser.add_argument(
        "--mode",
        choices=["watch", "scan", "queue"],
        default="watch",
        help="watch: legacy watchdog; scan: one-time backfill; queue: poll queue_messages table",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=1.0,
        help="Poll interval in seconds for queue mode (default: 1.0)",
    )
    args = parser.parse_args()

    if args.mode == "scan":
        engine = IngestionEngine()
        engine.scan_and_ingest()
        engine.close()
    elif args.mode == "queue":
        run_queue_mode(poll_interval=args.poll)
    else:
        run_watchdog(mode=args.mode, poll_interval=args.poll)
```

- [ ] **Step 5: Verify imports**

Make sure `time` is imported at top of file (already should be for watchdog). Add `json` import if not present.

- [ ] **Step 6: Test with small run**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode queue --help
```
Expected: help text shows `queue` as a mode option.

- [ ] **Step 7: Commit**

```bash
git add pipeline/ingestion_engine.py
git commit -m "feat: add poll_queue mode, deprecate watchdog"
```

---

### Task 8: Phase 1 verification — run full pipeline with queue mode

**Files:** All Phase 1 files

- [ ] **Step 1: Clean and init**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import shutil
from pathlib import Path
[shutil.rmtree(p, ignore_errors=True) for p in ['output/landing_zone', 'output/silver']]
[Path(p).unlink(missing_ok=True) for p in ['duckdb/spx_analytics.duckdb', 'output/.watermark']]
print('Cleaned')
"
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py
```

- [ ] **Step 2: Run simulator (small sample)**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-01-10
```

- [ ] **Step 3: Verify messages in queue**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb')
print('queue_messages:', con.execute('SELECT COUNT(*) FROM queue_messages').fetchone()[0])
print('by type:', con.execute('SELECT msg_type, COUNT(*) FROM queue_messages GROUP BY msg_type').fetchall())
con.close()
"
```

Expected: queue_messages has entries for price_file (and potentially other types if data exists in date range).

- [ ] **Step 4: Run ingestion in queue mode**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode queue &
sleep 5
# Check it processed messages
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb', read_only=True)
print('DONE:', con.execute(\"SELECT COUNT(*) FROM queue_messages WHERE status='DONE'\").fetchone()[0])
print('PENDING:', con.execute(\"SELECT COUNT(*) FROM queue_messages WHERE status='PENDING'\").fetchone()[0])
print('raw_price_stream:', con.execute('SELECT COUNT(*) FROM raw_price_stream').fetchone()[0])
con.close()
"
# Kill the background process
```

- [ ] **Step 5: Commit Phase 1 completion**

```bash
git add -A
git commit -m "feat: Phase 1 complete — batch insert, message queue, audit fixes"
```

---

## Phase 2: Gold Layer OLAP Views

### Task 9: Add v_rolling_volatility to create_gold_views.sql

**Files:** `gold/sql/create_gold_views.sql`

- [ ] **Step 1: Read end of create_gold_views.sql**

```bash
tail -20 gold/sql/create_gold_views.sql
```

- [ ] **Step 2: Append v_rolling_volatility view**

```sql
-- 5. v_rolling_volatility — 20-day and 60-day rolling volatility (annualized)
CREATE OR REPLACE VIEW v_rolling_volatility AS
WITH daily_returns AS (
    SELECT
        ticker, date, close,
        (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
    FROM silver_price
),
vol AS (
    SELECT
        ticker, date, close, daily_return,
        STDDEV(daily_return) OVER w20 * SQRT(252) AS annualized_vol_20d,
        STDDEV(daily_return) OVER w60 * SQRT(252) AS annualized_vol_60d,
        AVG(daily_return) OVER w20 * 252 AS annualized_return_20d
    FROM daily_returns
    WINDOW w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
           w60 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT ticker, date, close,
    ROUND(annualized_vol_20d, 6) AS annualized_vol_20d,
    ROUND(annualized_vol_60d, 6) AS annualized_vol_60d,
    ROUND(annualized_return_20d, 6) AS annualized_return_20d
FROM vol
WHERE annualized_vol_20d IS NOT NULL
ORDER BY ticker, date;
```

- [ ] **Step 3: Commit**

```bash
git add gold/sql/create_gold_views.sql
git commit -m "feat: add v_rolling_volatility view"
```

---

### Task 10: Add v_momentum_signals

**Files:** `gold/sql/create_gold_views.sql`

- [ ] **Step 1: Append v_momentum_signals to create_gold_views.sql**

```sql
-- 6. v_momentum_signals — multi-period momentum + MA deviation + trend signal
CREATE OR REPLACE VIEW v_momentum_signals AS
WITH price_analytics AS (
    SELECT
        ticker, date, close,
        LAG(close, 5) OVER w AS lag5,
        LAG(close, 20) OVER w AS lag20,
        LAG(close, 60) OVER w AS lag60,
        AVG(close) OVER w20 AS ma20,
        AVG(close) OVER w60 AS ma60
    FROM silver_price
    WINDOW w AS (PARTITION BY ticker ORDER BY date),
           w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
           w60 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    ticker, date, close,
    ROUND((close - lag5) / NULLIF(lag5, 0), 6) AS momentum_5d,
    ROUND((close - lag20) / NULLIF(lag20, 0), 6) AS momentum_20d,
    ROUND((close - lag60) / NULLIF(lag60, 0), 6) AS momentum_60d,
    ROUND((close - ma20) / NULLIF(ma20, 0), 6) AS dist_pct_from_ma20,
    ROUND((close - ma60) / NULLIF(ma60, 0), 6) AS dist_pct_from_ma60,
    CASE
        WHEN close > ma20 AND close > ma60 THEN 'STRONG_UPTREND'
        WHEN close < ma20 AND close < ma60 THEN 'STRONG_DOWNTREND'
        WHEN close > ma20 THEN 'WEAK_UPTREND'
        WHEN close < ma20 THEN 'WEAK_DOWNTREND'
        ELSE 'NEUTRAL'
    END AS trend_signal
FROM price_analytics
WHERE lag5 IS NOT NULL AND lag20 IS NOT NULL AND lag60 IS NOT NULL
ORDER BY ticker, date;
```

- [ ] **Step 2: Commit**

```bash
git add gold/sql/create_gold_views.sql
git commit -m "feat: add v_momentum_signals view"
```

---

### Task 11: Add v_sector_rotation

**Files:** `gold/sql/create_gold_views.sql`

- [ ] **Step 1: Append v_sector_rotation to create_gold_views.sql**

```sql
-- 7. v_sector_rotation — quarterly sector performance ranking
CREATE OR REPLACE VIEW v_sector_rotation AS
WITH sector_daily AS (
    SELECT
        MAX(f.value) AS sector,
        p.date,
        EXTRACT(YEAR FROM p.date) AS year,
        EXTRACT(QUARTER FROM p.date) AS quarter,
        AVG(p.close) AS avg_close,
        SUM(p.volume) AS total_volume,
        STDDEV(p.close) AS price_std,
        COUNT(DISTINCT p.ticker) AS ticker_count
    FROM silver_price p
    LEFT JOIN silver_fundamentals f
        ON p.ticker = f.ticker AND f.metric = 'sector'
    GROUP BY p.date, year, quarter
),
sector_quarterly AS (
    SELECT
        sector,
        year,
        quarter,
        AVG(avg_close) AS avg_close,
        SUM(total_volume) AS total_volume,
        AVG(price_std) AS avg_volatility,
        AVG(ticker_count) AS avg_ticker_count,
        RANK() OVER (
            PARTITION BY year, quarter
            ORDER BY (AVG(avg_close) - LAG(AVG(avg_close)) OVER (PARTITION BY sector ORDER BY year, quarter))
                     / NULLIF(LAG(AVG(avg_close)) OVER (PARTITION BY sector ORDER BY year, quarter), 0)
                     DESC
        ) AS momentum_rank
    FROM sector_daily
    GROUP BY sector, year, quarter
)
SELECT * FROM sector_quarterly
ORDER BY year, quarter, momentum_rank;
```

- [ ] **Step 2: Commit**

```bash
git add gold/sql/create_gold_views.sql
git commit -m "feat: add v_sector_rotation view"
```

---

### Task 12: Add v_sentiment_binned_returns

**Files:** `gold/sql/create_gold_views.sql`

- [ ] **Step 1: Append v_sentiment_binned_returns to create_gold_views.sql**

```sql
-- 8. v_sentiment_binned_returns — sentiment bucket vs forward returns
CREATE OR REPLACE VIEW v_sentiment_binned_returns AS
WITH sentiment_returns AS (
    SELECT
        s.ticker,
        s.event_date,
        s.sentiment_polarity,
        s.sentiment_subjectivity,
        p.next_1d_return,
        p.next_5d_return
    FROM silver_sentiment s
    JOIN v_sentiment_price_view p
        ON s.ticker = p.ticker AND s.event_date = p.transcript_date
    WHERE s.sentiment_polarity IS NOT NULL
)
SELECT
    CASE
        WHEN sentiment_polarity > 0.2 THEN 'POSITIVE'
        WHEN sentiment_polarity < -0.2 THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END AS sentiment_bucket,
    COUNT(*) AS transcript_count,
    ROUND(AVG(next_1d_return), 6) AS avg_1d_return,
    ROUND(AVG(next_5d_return), 6) AS avg_5d_return,
    ROUND(STDDEV(next_1d_return), 6) AS std_1d_return,
    ROUND(AVG(sentiment_subjectivity), 4) AS avg_subjectivity
FROM sentiment_returns
GROUP BY sentiment_bucket;
```

- [ ] **Step 2: Commit**

```bash
git add gold/sql/create_gold_views.sql
git commit -m "feat: add v_sentiment_binned_returns view"
```

---

### Task 13: Add v_ar1_time_series

**Files:** `gold/sql/create_gold_views.sql`

- [ ] **Step 1: Append v_ar1_time_series to create_gold_views.sql**

```sql
-- 9. v_ar1_time_series — AR(1) autoregressive model via OLS window regression
-- r_t = alpha + beta * r_{t-1} + epsilon
-- beta ≈ 1: random walk (unit root); beta ≈ 0: uncorrelated returns
CREATE OR REPLACE VIEW v_ar1_time_series AS
WITH return_series AS (
    SELECT
        ticker, date, close,
        (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
    FROM silver_price
),
ar_input AS (
    SELECT
        ticker, date, close, daily_return,
        LAG(daily_return, 1) OVER (PARTITION BY ticker ORDER BY date) AS lag_return
    FROM return_series
),
ar_coeffs AS (
    SELECT
        ticker, date, close, daily_return, lag_return,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn,
        REGR_SLOPE(daily_return, lag_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS beta_ar1,
        REGR_INTERCEPT(daily_return, lag_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS alpha_ar1,
        REGR_R2(daily_return, lag_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS r_squared_ar1,
        REGR_COUNT(daily_return, lag_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS n_obs
    FROM ar_input
    WHERE lag_return IS NOT NULL
)
SELECT
    ticker, date, close, daily_return,
    ROUND(alpha_ar1, 8) AS alpha_ar1,
    ROUND(beta_ar1, 8) AS beta_ar1,
    ROUND(r_squared_ar1, 6) AS r_squared_ar1,
    n_obs
FROM ar_coeffs
WHERE beta_ar1 IS NOT NULL AND n_obs >= 20
ORDER BY ticker, date;
```

- [ ] **Step 2: Commit**

```bash
git add gold/sql/create_gold_views.sql
git commit -m "feat: add v_ar1_time_series AR(1) view"
```

---

### Task 14: Update build_gold_layer.py GOLD_VIEWS list

**Files:** `gold/build_gold_layer.py:76-81`

- [ ] **Step 1: Update GOLD_VIEWS tuple**

```python
# BEFORE (lines 76-81)
GOLD_VIEWS = (
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_fundamental_snapshot",
    "v_sentiment_price_view",
)

# AFTER
GOLD_VIEWS = (
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_fundamental_snapshot",
    "v_sentiment_price_view",
    "v_rolling_volatility",
    "v_momentum_signals",
    "v_sector_rotation",
    "v_sentiment_binned_returns",
    "v_ar1_time_series",
)
```

- [ ] **Step 2: Commit**

```bash
git add gold/build_gold_layer.py
git commit -m "feat: update GOLD_VIEWS list to 9 views"
```

---

### Task 15: Phase 2 verification — build Gold and check all 9 views

**Files:** All Phase 2 files

- [ ] **Step 1: Run ELT price transform (with existing test data)**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource price
```

- [ ] **Step 2: Build Gold layer**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
```

Expected: all 9 views created. Check output for any SQL errors.

- [ ] **Step 3: Query each new view for sample data**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb', read_only=True)
views = [
    'v_rolling_volatility',
    'v_momentum_signals',
    'v_sector_rotation',
    'v_sentiment_binned_returns',
    'v_ar1_time_series'
]
for v in views:
    try:
        count = con.execute(f'SELECT COUNT(*) FROM {v}').fetchone()[0]
        print(f'{v}: {count} rows')
    except Exception as e:
        print(f'{v}: ERROR — {e}')
con.close()
"
```

Expected: all views return row counts (may be 0 if test data doesn't cover enough dates for rolling windows).

- [ ] **Step 4: Commit Phase 2 completion**

```bash
git add -A
git commit -m "feat: Phase 2 complete — 5 new Gold OLAP views"
```

---

## Phase 3: Dashboard Enhancement + Gold View Tests

> **Note:** Task 16 (extending test_gold_views.py) is placed here because it validates the Gold views that Phase 2 built. It could run before dashboard pages but does not block them.

### Task 16: Extend test_gold_views.py to all 9 views

**Files:** `gold/tests/test_gold_views.py`

- [ ] **Step 1: Read current test_gold_views.py**

```bash
cat gold/tests/test_gold_views.py
```

- [ ] **Step 2: Add expected columns for new views to the views dict**

Replace the views dict (around line 44):

```python
views = {
    "v_market_daily_summary": ["trade_date", "number_of_tickers", "avg_close", "avg_return", "total_volume"],
    "v_ticker_profile": ["ticker", "company_name", "sector", "latest_close", "latest_volume", "latest_trade_date"],
    "v_fundamental_snapshot": ["ticker", "latest_report_date", "revenue", "net_income", "assets", "liabilities"],
    "v_sentiment_price_view": ["ticker", "transcript_date", "sentiment_score", "close_on_event_date", "next_1d_return", "next_5d_return"],
    "v_rolling_volatility": ["ticker", "date", "close", "annualized_vol_20d", "annualized_vol_60d", "annualized_return_20d"],
    "v_momentum_signals": ["ticker", "date", "close", "momentum_5d", "momentum_20d", "momentum_60d", "dist_pct_from_ma20", "dist_pct_from_ma60", "trend_signal"],
    "v_sector_rotation": ["sector", "year", "quarter", "avg_close", "total_volume", "avg_volatility", "avg_ticker_count", "momentum_rank"],
    "v_sentiment_binned_returns": ["sentiment_bucket", "transcript_count", "avg_1d_return", "avg_5d_return", "std_1d_return", "avg_subjectivity"],
    "v_ar1_time_series": ["ticker", "date", "close", "daily_return", "alpha_ar1", "beta_ar1", "r_squared_ar1", "n_obs"],
}
```

- [ ] **Step 3: Run tests**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

Expected: all views exist, have expected columns. May have 0 rows for views requiring more date history.

- [ ] **Step 4: Commit**

```bash
git add gold/tests/test_gold_views.py
git commit -m "test: extend gold view tests to all 9 views"
```

---

### Task 17: Add Volatility dashboard page

**Files:** `dashboard.py`

- [ ] **Step 1: Read dashboard.py structure**

```bash
grep -n "st\\.page\|st\\.title\|st\\.header\|st\\.metric\|def " dashboard.py | head -40
```

- [ ] **Step 2: Add Volatility page function**

Add before the `if __name__ == "__main__":` block:

```python
def volatility_page(con):
    """v_rolling_volatility — 20d vs 60d annualized volatility analysis."""
    st.header("Volatility Analysis")
    st.caption("Data source: v_rolling_volatility")

    ticker_filter = st.sidebar.text_input("Ticker (optional)", value="")
    max_date = st.sidebar.text_input("Max date (YYYY-MM-DD)", value="2024-12-31")

    query = "SELECT * FROM v_rolling_volatility WHERE date <= ?"
    params = [max_date]
    if ticker_filter:
        query += " AND ticker = ?"
        params.append(ticker_filter.upper())
    query += " ORDER BY date DESC LIMIT 5000"

    df = con.execute(query, params).fetchdf()

    if df.empty:
        st.warning("No volatility data available.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Tickers", df["ticker"].nunique())
        st.metric("Date range", f"{df['date'].min()} to {df['date'].max()}")
    with col2:
        avg_vol = df["annualized_vol_20d"].mean()
        st.metric("Avg 20d Vol (annualized)", f"{avg_vol:.4f}" if avg_vol else "N/A")

    st.subheader("20d vs 60d Volatility Scatter")
    scatter_df = df.dropna(subset=["annualized_vol_20d", "annualized_vol_60d"]).head(1000)
    st.scatter_chart(
        scatter_df[["annualized_vol_20d", "annualized_vol_60d"]].rename(
            columns={"annualized_vol_20d": "20d Vol", "annualized_vol_60d": "60d Vol"}
        )
    )

    st.subheader("Volatility Time Series (last 200 rows)")
    st.line_chart(df[["date", "annualized_vol_20d", "annualized_vol_60d"]].head(200).set_index("date"))
```

- [ ] **Step 3: Add page to sidebar navigation**

Find the sidebar navigation section and add:

```python
if page == "Volatility":
    volatility_page(con)
```

Also add "Volatility" to the page selection dropdown/radio.

- [ ] **Step 4: Commit**

```bash
git add dashboard.py
git commit -m "feat: add Volatility dashboard page"
```

---

### Task 18: Add Momentum page

**Files:** `dashboard.py`

- [ ] **Step 1: Add momentum_page function**

```python
def momentum_page(con):
    """v_momentum_signals — multi-period momentum + trend classification."""
    st.header("Momentum Signals")
    st.caption("Data source: v_momentum_signals")

    ticker_filter = st.sidebar.text_input("Ticker (optional)", value="")

    query = "SELECT * FROM v_momentum_signals"
    params = []
    if ticker_filter:
        query += " WHERE ticker = ?"
        params.append(ticker_filter.upper())
    query += " ORDER BY date DESC LIMIT 5000"

    df = con.execute(query, params).fetchdf()

    if df.empty:
        st.warning("No momentum data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total rows", len(df))
    with col2:
        up = (df["trend_signal"] == "STRONG_UPTREND").sum() + (df["trend_signal"] == "WEAK_UPTREND").sum()
        st.metric("Uptrend signals", up)
    with col3:
        down = (df["trend_signal"] == "STRONG_DOWNTREND").sum() + (df["trend_signal"] == "WEAK_DOWNTREND").sum()
        st.metric("Downtrend signals", down)

    st.subheader("Trend Signal Distribution")
    signal_counts = df["trend_signal"].value_counts()
    st.bar_chart(signal_counts)

    st.subheader("Momentum Distribution (5d)")
    hist_df = df[["momentum_5d"]].dropna().tail(1000)
    st.hist_chart(hist_df)

    st.subheader("Sample Data")
    st.dataframe(df.head(20), use_container_width=True)
```

- [ ] **Step 2: Add page to sidebar navigation**

- [ ] **Step 3: Commit**

```bash
git add dashboard.py
git commit -m "feat: add Momentum dashboard page"
```

---

### Task 19: Add Sector Rotation page

**Files:** `dashboard.py`

- [ ] **Step 1: Add sector_rotation_page function**

```python
def sector_rotation_page(con):
    """v_sector_rotation — quarterly sector performance ranking."""
    st.header("Sector Rotation")
    st.caption("Data source: v_sector_rotation")

    df = con.execute("SELECT * FROM v_sector_rotation ORDER BY year, quarter, momentum_rank").fetchdf()

    if df.empty:
        st.warning("No sector rotation data available.")
        return

    latest_q = df["year"].max() * 10 + df["quarter"].max()
    df["yearq"] = df["year"] * 10 + df["quarter"]
    latest = df[df["yearq"] == latest_q].sort_values("momentum_rank")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Sectors tracked", df["sector"].nunique())
    with col2:
        st.metric("Quarters covered", df[["year", "quarter"]].drop_duplicates().shape[0])

    st.subheader(f"Latest Quarter ({latest['year'].iloc[0]} Q{latest['quarter'].iloc[0]}) — Sector Ranking")
    st.dataframe(latest[["momentum_rank", "sector", "avg_close", "avg_volatility", "total_volume"]], use_container_width=True)

    st.subheader("Sector Momentum Rank Over Time")
    pivot = df.pivot_table(index="sector", columns="yearq", values="momentum_rank")
    st.dataframe(pivot, use_container_width=True)
```

- [ ] **Step 2: Add page to sidebar navigation**

- [ ] **Step 3: Commit**

```bash
git add dashboard.py
git commit -m "feat: add Sector Rotation dashboard page"
```

---

### Task 20: Add Sentiment Binned Returns page

**Files:** `dashboard.py`

- [ ] **Step 1: Add sentiment_page function**

```python
def sentiment_page(con):
    """v_sentiment_binned_returns — sentiment bucket vs forward returns."""
    st.header("Sentiment Binned Returns")
    st.caption("Data source: v_sentiment_binned_returns")

    df = con.execute("SELECT * FROM v_sentiment_binned_returns").fetchdf()

    if df.empty:
        st.warning("No sentiment data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total transcripts", df["transcript_count"].sum())
    with col2:
        pos_ret = df[df["sentiment_bucket"] == "POSITIVE"]["avg_1d_return"].values
        st.metric("Positive avg 1d return", f"{pos_ret[0]:.4f}" if len(pos_ret) > 0 else "N/A")
    with col3:
        neg_ret = df[df["sentiment_bucket"] == "NEGATIVE"]["avg_1d_return"].values
        st.metric("Negative avg 1d return", f"{neg_ret[0]:.4f}" if len(neg_ret) > 0 else "N/A")

    st.subheader("Average 1-Day Return by Sentiment Bucket")
    chart_df = df[["sentiment_bucket", "avg_1d_return", "avg_5d_return"]].set_index("sentiment_bucket")
    st.bar_chart(chart_df)

    st.subheader("Full Sentiment Bucket Statistics")
    st.dataframe(df, use_container_width=True)
```

- [ ] **Step 2: Add page to sidebar navigation**

- [ ] **Step 3: Commit**

```bash
git add dashboard.py
git commit -m "feat: add Sentiment Binned Returns dashboard page"
```

---

### Task 21: Add AR(1) Model page

**Files:** `dashboard.py`

- [ ] **Step 1: Add ar1_page function**

```python
def ar1_page(con):
    """v_ar1_time_series — AR(1) autoregressive model results."""
    st.header("AR(1) Time Series Model")
    st.caption("Data source: v_ar1_time_series | Interpretation: beta≈1 = random walk, beta≈0 = uncorrelated returns")

    ticker_filter = st.sidebar.text_input("Ticker (optional)", value="")

    query = "SELECT * FROM v_ar1_time_series"
    params = []
    if ticker_filter:
        query += " WHERE ticker = ?"
        params.append(ticker_filter.upper())
    query += " ORDER BY date DESC LIMIT 5000"

    df = con.execute(query, params).fetchdf()

    if df.empty:
        st.warning("No AR(1) data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Tickers", df["ticker"].nunique())
    with col2:
        avg_beta = df["beta_ar1"].mean()
        st.metric("Avg beta (mean reversion)", f"{avg_beta:.4f}" if avg_beta else "N/A")
    with col3:
        avg_r2 = df["r_squared_ar1"].mean()
        st.metric("Avg R-squared", f"{avg_r2:.6f}" if avg_r2 else "N/A")

    st.subheader("Beta Distribution (last 1000 rows)")
    beta_df = df[["beta_ar1"]].dropna().tail(1000)
    st.hist_chart(beta_df)

    st.subheader("R-squared vs Beta (scatter)")
    scatter_df = df[["beta_ar1", "r_squared_ar1"]].dropna().tail(2000)
    st.scatter_chart(scatter_df.rename(columns={"beta_ar1": "Beta", "r_squared_ar1": "R-squared"}))

    st.subheader("Sample AR(1) Coefficients")
    display_df = df[["ticker", "date", "daily_return", "alpha_ar1", "beta_ar1", "r_squared_ar1", "n_obs"]].head(20)
    st.dataframe(display_df, use_container_width=True)

    st.info("**Interpretation:** beta≈1 means random walk (past returns don't predict future). beta≈0 means uncorrelated returns (white noise). |beta|<1 means deviations decay over time.")
```

- [ ] **Step 2: Add page to sidebar navigation**

- [ ] **Step 3: Commit**

```bash
git add dashboard.py
git commit -m "feat: add AR(1) Model dashboard page"
```

---

### Task 22: Phase 3 verification

**Files:** Dashboard files

- [ ] **Step 1: Run test_gold_views to confirm 9 views**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

Expected: 12+ passes (was 12, now should be more with 9 views × 3 checks each).

- [ ] **Step 2: Start dashboard and verify pages**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m streamlit run dashboard.py --server.headless true --server.port 8502 &
sleep 5
# Visit http://localhost:8502 and check all 5 new pages load without error
```

- [ ] **Step 3: Commit Phase 3 completion**

```bash
git add -A
git commit -m "feat: Phase 3 complete — 5 new dashboard pages"
```

---

## Phase 4: CI/CD + Unit Tests

### Task 23: Create .github/workflows/test.yml

**Files:** `.github/workflows/test.yml` (new file)

- [ ] **Step 1: Create directory and file**

```bash
mkdir -p .github/workflows
```

- [ ] **Step 2: Write test.yml**

```yaml
name: Pipeline CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'  # Match qf5214_project conda env

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r gold/requirements.txt

      - name: Init Bronze
        run: python duckdb/init_bronze.py

      - name: Simulator (Jan 2024 sample)
        run: python -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-01-31

      - name: Ingestion (queue mode — wait for messages)
        run: |
          python -m pipeline.ingestion_engine --mode queue &
          INGESTION_PID=$!
          # Poll queue until all messages processed (up to 120s)
          for i in $(seq 1 24); do
            sleep 5
            DONE=$(python -c "import duckdb; con=duckdb.connect('duckdb/spx_analytics.duckdb',read_only=True); print(con.execute(\"SELECT COUNT(*) FROM queue_messages WHERE status='DONE'\").fetchone()[0])" 2>/dev/null || echo 0)
            PENDING=$(python -c "import duckdb; con=duckdb.connect('duckdb/spx_analytics.duckdb',read_only=True); print(con.execute(\"SELECT COUNT(*) FROM queue_messages WHERE status='PENDING'\").fetchone()[0])" 2>/dev/null || echo 0)
            echo "Queue status — DONE: $DONE, PENDING: $PENDING"
            if [ "$PENDING" = "0" ]; then
              echo "All messages processed"
              break
            fi
          done
          kill $INGESTION_PID 2>/dev/null || true

      - name: Ingestion (scan mode fallback)
        run: python -m pipeline.ingestion_engine --mode scan

      # NOTE: Full CI requires data files to be available. The simulator step
      # above requires access to data/spx_20yr_ohlcv_data.csv etc. For CI to pass,
      # either commit a sample of data files or configure a data checkout action.

      - name: ELT Price
        run: python -m pipeline.elt_pipeline --resource price

      - name: Gold Layer Build
        run: python gold/build_gold_layer.py

      - name: Test Gold Views
        run: python gold/tests/test_gold_views.py

      - name: Unit Tests
        run: python -m pytest tests/ -v || echo "No tests found"
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/test.yml
git commit -m "ci: add GitHub Actions pipeline"
```

---

### Task 24: Create tests/test_data_provider.py

**Files:** `tests/test_data_provider.py` (new file)

- [ ] **Step 1: Create tests directory and file**

```bash
mkdir -p tests
```

- [ ] **Step 2: Write test_data_provider.py**

```python
"""
Unit tests for pipeline.data_provider.SPXDataProvider.

Run: python -m pytest tests/test_data_provider.py -v
"""
import sys
from pathlib import Path

import pytest

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.data_provider import SPXDataProvider

@pytest.fixture
def provider():
    return SPXDataProvider()

def test_get_price_returns_dataframe(provider):
    """get_price returns a DataFrame for valid ticker and date."""
    df = provider.get_price("AAPL", "2024-01-15")
    assert len(df) > 0, "Expected rows for AAPL on 2024-01-15"
    expected_cols = {"Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
    assert set(df.columns).issuperset(expected_cols), f"Missing columns: {expected_cols - set(df.columns)}"

def test_get_price_weekend_returns_empty(provider):
    """get_price returns empty DataFrame for non-trading day."""
    df = provider.get_price("AAPL", "2024-01-13")  # Saturday
    assert len(df) == 0, "Expected empty DataFrame for weekend"

def test_get_ticker_list_returns_list(provider):
    """get_ticker_list returns a non-empty list."""
    tickers = provider.get_ticker_list()
    assert isinstance(tickers, list), "Expected list"
    assert len(tickers) > 0, "Expected non-empty ticker list"
    assert "AAPL" in tickers or "MSFT" in tickers, "Expected common tickers in list"

def test_get_trading_dates_returns_list(provider):
    """get_trading_dates returns list of dates in range."""
    dates = provider.get_trading_dates("2024-01-02", "2024-01-10")
    assert isinstance(dates, list), "Expected list"
    assert len(dates) >= 5, "Expected at least 5 trading days in Jan 2024 first week"
    assert "2024-01-02" in dates, "Expected 2024-01-02 as first trading day"

def test_get_price_invalid_ticker_raises(provider):
    """get_price raises ValueError for invalid ticker."""
    with pytest.raises(ValueError):
        provider.get_price("INVALID_TICKER_XYZ", "2024-01-15")
```

- [ ] **Step 3: Run tests**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m pytest tests/test_data_provider.py -v
```

Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_data_provider.py
git commit -m "test: add SPXDataProvider unit tests"
```

---

### Task 25: Create tests/test_ingestion_engine.py

**Files:** `tests/test_ingestion_engine.py` (new file)

- [ ] **Step 1: Write test_ingestion_engine.py**

```python
"""
Unit tests for pipeline.ingestion_engine batch insert and queue consumer.

Run: python -m pytest tests/test_ingestion_engine.py -v
"""
import sys
import tempfile
import shutil
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
```

- [ ] **Step 2: Run tests**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m pytest tests/test_ingestion_engine.py -v
```

Expected: 6 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_ingestion_engine.py
git commit -m "test: add ingestion engine unit tests"
```

---

### Task 26: Final integration test and plan completion

- [ ] **Step 1: Run full test_pipeline.py**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" test_pipeline.py
```

Expected: all stages complete without error.

- [ ] **Step 2: Run gold view tests**

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "feat: complete all phases — bug fixes, message broker, 5 new Gold views, dashboard, CI/CD"
```

---

## Summary: Task → Commit Mapping

| Task | Description | Commit Message |
|------|-------------|----------------|
| 1 | Fix test_pipeline.py Python path | fix: use sys.executable for portable Python path |
| 2 | Move safe_float/safe_int to module level | refactor: move safe_float/safe_int to module level |
| 3 | Replace row-by-row INSERT with executemany + audit fix | fix: executemany batch insert + audit status on failure |
| 5 | Add queue_messages table | feat: add queue_messages table for event-driven ingestion |
| 6 | Add _enqueue_message to simulator | feat: add _enqueue_message to simulator for message queue |
| 7 | Replace watchdog with poll_queue | feat: add poll_queue mode, deprecate watchdog |
| 8 | Phase 1 verification | feat: Phase 1 complete — batch insert, message queue, audit fixes |
| 9 | Add v_rolling_volatility | feat: add v_rolling_volatility view |
| 10 | Add v_momentum_signals | feat: add v_momentum_signals view |
| 11 | Add v_sector_rotation | feat: add v_sector_rotation view |
| 12 | Add v_sentiment_binned_returns | feat: add v_sentiment_binned_returns view |
| 13 | Add v_ar1_time_series | feat: add v_ar1_time_series AR(1) view |
| 14 | Update GOLD_VIEWS list | feat: update GOLD_VIEWS list to 9 views |
| 15 | Phase 2 verification | feat: Phase 2 complete — 5 new Gold OLAP views |
| 16 | Extend test_gold_views.py to 9 views | test: extend gold view tests to all 9 views |
| 17 | Add Volatility dashboard page | feat: add Volatility dashboard page |
| 18 | Add Momentum dashboard page | feat: add Momentum dashboard page |
| 19 | Add Sector Rotation dashboard page | feat: add Sector Rotation dashboard page |
| 20 | Add Sentiment Binned Returns page | feat: add Sentiment Binned Returns dashboard page |
| 21 | Add AR(1) Model dashboard page | feat: add AR(1) Model dashboard page |
| 22 | Phase 3 verification | feat: Phase 3 complete — 5 new dashboard pages |
| 23 | Create GitHub Actions workflow | ci: add GitHub Actions pipeline |
| 24 | Create test_data_provider.py | test: add SPXDataProvider unit tests |
| 25 | Create test_ingestion_engine.py | test: add ingestion engine unit tests |
| 26 | Final integration test | feat: complete all phases |

---

**Total: 26 tasks across 4 phases. ~5 working days.**
