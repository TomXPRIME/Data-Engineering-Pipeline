# SPX 500 Data Pipeline - Complete Run Guide

> This document describes how to run a complete end-to-end test of the entire project **without modifying any code**.
> Environment: `qf5214_project` (Conda), Python 3.10, using `C:/miniconda3/envs/qf5214_project/python.exe`
>
> **Update Status:** Pipeline-level fundamental API redesign completed (2026-04-02). Dashboard (Phase 6) pending re-implementation.

---

## 1. Data Scale Overview

| Data Type | Source Path | Scale |
|-----------|------------|-------|
| Price OHLCV | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers x 5284 trading days (2004-2024), ~4.32M rows |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/*.csv` | 5726 CSV files (annual + quarterly) |
| Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/*.pdf` | 32,036 PDF files (2005-2025), 1.2 GB |
| Tickers | `data/reference/tickers.csv` | 947 tickers |

---

## 2. Complete Run Steps

### Step 0: Environment Setup

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "import pandas; import duckdb; import watchdog; import textblob; from pypdf import PdfReader; print('All dependencies OK')"
```

---

### Step 1: Initialize DuckDB Bronze Tables

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py
```

Expected output:
```
Bronze tables created: ['ingestion_audit', 'raw_fundamental_index', 'raw_price_stream', 'raw_transcript_index']
```

---

### Step 2: Run Simulator (Generate Landing Zone Data)

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2004-01-02 --end 2024-12-30
```

**Output location:**
- `output/landing_zone/prices/price_YYYY-MM-DD.csv` (5,284 files x 818 tickers)
- `output/landing_zone/fundamentals/{ticker}/` (ticker-partitioned, 8 files per ticker)
- `output/landing_zone/transcripts/*.pdf` (32,036 PDFs)

> **Note (2026-04-02 redesign):** Landing zone fundamentals structure changed from `fundamentals/YYYY-MM-DD/` to `fundamentals/{ticker}/`.
> `freq` column added to `raw_fundamental_index` Bronze table.

**Estimated time:** 20-90 minutes (PDF file copying is the main time consumer)

**Resume from interruption:** Simulator uses `output/.watermark` file to record the last processed date.

---

### Step 3: Run Ingestion Engine (Bronze Layer Ingestion)

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode scan
```

**Expected results:**
- Price: ~4.32M rows ingested into `raw_price_stream`
- Fundamentals: 5,726 file indexes ingested into `raw_fundamental_index` (with `freq` column)
- Transcripts: 32,036 PDF indexes ingested into `raw_transcript_index`

**Verify Bronze layer data:**
```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb', read_only=True)
print('raw_price_stream:', con.execute('SELECT COUNT(*) FROM raw_price_stream').fetchone()[0])
print('raw_fundamental_index:', con.execute('SELECT COUNT(*) FROM raw_fundamental_index').fetchone()[0])
print('raw_transcript_index:', con.execute('SELECT COUNT(*) FROM raw_transcript_index').fetchone()[0])
con.close()
"
```

Expected output (2024 test data):
```
raw_price_stream: 199592 rows
raw_fundamental_index: 2860 rows
raw_transcript_index: 1950 rows
```

---

### Step 4: Run ELT Pipeline (Bronze -> Silver)

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline
```

**Estimated time:**
- Price: 1-3 minutes
- Fundamentals: <1 minute
- Transcripts: 5-15 minutes
- Sentiment: 5-10 minutes

**Verify Silver layer data:**
```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb', read_only=True)

# Price
df = con.execute('''
    SELECT COUNT(*) as total_rows,
           COUNT(DISTINCT ticker) as tickers,
           COUNT(DISTINCT date) as trading_days
    FROM read_parquet(\"output/silver/price/**/*.parquet\", hive_partitioning=true)
''').fetchdf()
print('Silver Price:', df.to_string(index=False))

# Fundamentals
df2 = con.execute('''
    SELECT ticker, COUNT(*) as rows
    FROM read_parquet(\"output/silver/fundamentals/**/*.parquet\", hive_partitioning=true)
    GROUP BY ticker
''').fetchdf()
print('Silver Fundamentals:', df2.head())

# Sentiment
df3 = con.execute('''
    SELECT COUNT(*) as total, COUNT(sentiment_polarity) as with_score
    FROM read_parquet(\"output/silver/transcript_sentiment/**/*.parquet\", hive_partitioning=true)
''').fetchdf()
print('Silver Sentiment:', df3.to_string(index=False))

con.close()
"
```

---

### Step 5: Build Gold Layer

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
```

**Expected result:** 10 Gold views created successfully

**View Gold view results:**
```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb', read_only=True)

views = [
    'v_market_daily_summary', 'v_ticker_profile', 'v_fundamental_snapshot',
    'v_fundamental_history', 'v_sentiment_price_view', 'v_rolling_volatility',
    'v_momentum_signals', 'v_sector_rotation', 'v_sentiment_binned_returns',
    'v_ar1_time_series'
]
for v in views:
    count = con.execute(f'SELECT COUNT(*) FROM {v}').fetchone()[0]
    print(f'{v}: {count:,} rows')
con.close()
"
```

Expected output (2024 test):
```
v_market_daily_summary: 251 rows
v_ticker_profile: 818 rows
v_fundamental_snapshot: 595 rows
v_fundamental_history: 735,163 rows
v_sentiment_price_view: 1,954 rows
v_rolling_volatility: 147,003 rows
v_momentum_signals: 112,705 rows
v_sector_rotation: 4 rows
v_sentiment_binned_returns: 2 rows
v_ar1_time_series: 135,160 rows
```

---

### Step 6: Verify Gold Views

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

Expected output:
```
--- v_market_daily_summary ---
  [PASS] View exists
  [PASS] Has 251 rows
  [PASS] All expected columns present
--- v_ticker_profile ---
  [PASS] View exists
  [PASS] Has 818 rows
  [PASS] All expected columns present
--- v_fundamental_snapshot ---
  [PASS] View exists
  [PASS] Has 595 rows
  [PASS] All expected columns present
--- v_sentiment_price_view ---
  [PASS] View exists
  [PASS] Has 1,954 rows
  [PASS] All expected columns present
--- v_rolling_volatility ---
  [PASS] View exists
  [PASS] Has 147,003 rows
  [PASS] All expected columns present
--- v_momentum_signals ---
  [PASS] View exists
  [PASS] Has 112,705 rows
  [PASS] All expected columns present
--- v_sector_rotation ---
  [PASS] View exists
  [PASS] Has 4 rows
  [PASS] All expected columns present
--- v_sentiment_binned_returns ---
  [PASS] View exists
  [PASS] Has 2 rows
  [PASS] All expected columns present
--- v_ar1_time_series ---
  [PASS] View exists
  [PASS] Has 135,160 rows
  [PASS] All expected columns present
========================================
Results: 27 passed, 0 failed
```

---

## 3. Dashboard (Phase 6 — INCOMPLETE)

Dashboard was removed from the repository (`st.hist_chart` bug + scope issues).

**Pending:** Bloomberg-style `Fundamental History` page using `v_fundamental_history` view with `cutoff_date` filtering.

Restore when ready:
```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m streamlit run dashboard.py --server.headless true
```

---

## 4. Data Flow Summary (Pipeline Complete)

```
data/ (raw CSV/PDF)
    ↓ [Simulator - Step 2]
output/landing_zone/
    ├── prices/                    5,284 CSVs ✅
    ├── fundamentals/{ticker}/      ticker-partitioned ✅
    └── transcripts/               32,036 PDFs ✅
    ↓ [Ingestion Engine - Step 3]
duckdb/spx_analytics.duckdb (Bronze)
    ├── raw_price_stream             4,322,232 rows ✅
    ├── raw_fundamental_index (with freq)  5,726 rows ✅
    └── raw_transcript_index        32,036 rows ✅
    ↓ [ELT Pipeline - Step 4]
output/silver/
    ├── price/                      Date-partitioned Parquet ✅
    ├── fundamentals/               Ticker-partitioned Parquet (with freq) ✅
    ├── transcript_text/             PDF text extraction ✅
    └── transcript_sentiment/       Sentiment analysis Parquet ✅
    ↓ [Gold Layer - Step 5]
duckdb/spx_analytics.duckdb (Gold Views, 10 total)
    ├── v_market_daily_summary       ✅
    ├── v_ticker_profile             ✅
    ├── v_fundamental_snapshot       ✅
    ├── v_fundamental_history       ✅ (new)
    ├── v_sentiment_price_view       ✅
    ├── v_rolling_volatility         ✅
    ├── v_momentum_signals           ✅
    ├── v_sector_rotation            ✅
    ├── v_sentiment_binned_returns   ✅
    └── v_ar1_time_series            ✅
```

---

## 5. Time Estimates (Full 20-Year Data)

| Phase | Estimated Time | Notes |
|-------|---------------|-------|
| Step 2 Simulator | 20-90 minutes | 5284 price files + 5726 fundamental files + 32036 PDFs |
| Step 3 Ingestion | 5-15 minutes | 4.32M CSV rows + indexes |
| Step 4 ELT Price | 1-3 minutes | DuckDB deduplication + Parquet export |
| Step 4 ELT Fundamentals | 1-2 minutes | CSV unpivot + Parquet export |
| Step 4 ELT Transcripts | 5-15 minutes | PDF text extraction |
| Step 4 ELT Sentiment | 5-10 minutes | TextBlob sentiment analysis |
| Step 5 Gold | 10-30 seconds | Parquet read + view creation |
| **Total** | **~40 minutes - 2.5 hours** | |

---

## 6. DuckDB Direct Query Examples

```sql
-- Bloomberg-style: query historical fundamentals with cutoff_date
-- Equivalent to DataProvider.get_fundamentals(ticker, freq, cutoff_date)
SELECT ticker, fiscal_date, report_type, freq, metric, value
FROM v_fundamental_history
WHERE ticker = 'AAPL'
  AND fiscal_date <= '2020-12-31'   -- cutoff_date
  AND freq = 'quarterly'
ORDER BY fiscal_date DESC, metric
LIMIT 50;

-- Market daily summary
SELECT trade_date, number_of_tickers, avg_close, avg_return, total_volume
FROM v_market_daily_summary
WHERE trade_date BETWEEN '2020-01-01' AND '2024-12-31'
ORDER BY trade_date;

-- Silver layer price data (direct Parquet query)
SELECT ticker, date, close, volume
FROM read_parquet('output/silver/price/*/*.parquet', hive_partitioning=true)
WHERE ticker = 'AAPL'
ORDER BY date
LIMIT 10;
```

---

## 7. Pipeline Update Log (2026-04-02)

| Change | Description | Status |
|--------|-------------|--------|
| `SPXDataProvider.get_fundamentals(cutoff_date)` | Added `cutoff_date` param, Bloomberg-style history filtering | ✅ Done |
| Landing Zone `fundamentals/{ticker}/` | Changed from `YYYY-MM-DD/` to `ticker/` partition | ✅ Done |
| Bronze `raw_fundamental_index.freq` | Added `freq` VARCHAR column | ✅ Done |
| Simulator `_seed_all_fundamentals()` | Replaced batch dump with ticker-partitioned seed | ✅ Done |
| ELT `freq` column | Silver fundamentals parquet includes `freq` field | ✅ Done |
| `v_fundamental_history` | New Gold view (735,163 rows, supports fiscal_date <= cutoff) | ✅ Done |
| Dashboard Bloomberg page | `render_fundamental_history` with cutoff_date selector | ❌ Pending |

---

## 8. Quick Verification Commands (Small Sample)

```bash
# 1. Initialize
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py

# 2. Simulator (244 trading days)
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-12-30

# 3. Ingestion
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode scan

# 4. ELT
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline

# 5. Gold Build & Test
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

---

*Document updated: 2026-04-02*
*Pipeline version: Phase 1-5 complete, Phase 6 Dashboard pending*
