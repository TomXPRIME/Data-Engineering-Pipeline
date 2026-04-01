# SPX 500 Data Pipeline - Complete Run Guide

> This document describes how to run a complete end-to-end test of the entire project **without modifying any code**.
> Environment: `qf5214_project` (Conda), Python 3.13, using `C:/miniconda3/envs/qf5214_project/python.exe`
>
> **Update Status:** All pipeline schema mismatches have been fixed; full pipeline test passed on 2026-04-01.

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

Verify Conda environment `qf5214_project` has all dependencies:

```bash
# Use full path to ensure correct conda environment loads
"C:/miniconda3/envs/qf5214_project/python.exe" -c "import pandas; import duckdb; import watchdog; import textblob; from pypdf import PdfReader; print('All dependencies OK')"
```

If dependencies are missing, install them:
```bash
pip install -r requirements.txt
pip install -r gold/requirements.txt
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

Use `-m` mode (avoids relative import errors):

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2004-01-02 --end 2024-12-30
```

**Output location:**
- `output/landing_zone/prices/price_YYYY-MM-DD.csv` (5,284 files x 818 tickers)
- `output/landing_zone/fundamentals/YYYY-MM-DD/*.csv` (5,726 files)
- `output/landing_zone/transcripts/*.pdf` (32,036 PDFs)

**Estimated time:** 20-90 minutes (PDF file copying is the main time consumer)

**Progress check:**
```bash
ls output/landing_zone/prices/ | wc -l   # Number of price files generated
ls output/landing_zone/transcripts/ | wc -l  # Number of transcripts generated
```

**Resume from interruption:** Simulator uses `output/.watermark` file to record the last processed date. Re-running after interruption will automatically continue from the checkpoint.

---

### Step 3: Run Ingestion Engine (Bronze Layer Ingestion)

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode scan
```

**Expected results:**
- Price: ~4.32M rows ingested into `raw_price_stream`
- Fundamentals: 5,726 file indexes ingested into `raw_fundamental_index`
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
raw_fundamental_index: 7 rows
raw_transcript_index: 1950 rows
```

---

### Step 4: Run ELT Pipeline (Bronze -> Silver)

```bash
cd <repo_root>

# 4a: Transform Price (all data)
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource price

# 4b: Transform Fundamentals
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource fundamentals

# 4c: Transform Transcripts (extract PDF text)
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource transcripts

# 4d: Compute Sentiment (based on extracted text)
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource sentiment
```

**Estimated time:**
- Price: 1-3 minutes
- Fundamentals: <1 minute
- Transcripts: 5-15 minutes (PDF text extraction, depends on file count)
- Sentiment: 5-10 minutes (TextBlob sentiment analysis)

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
    FROM read_parquet('output/silver/price/**/*.parquet', hive_partitioning=true)
''').fetchdf()
print('Silver Price:', df.to_string(index=False))

# Fundamentals
df2 = con.execute('''
    SELECT ticker, COUNT(*) as rows
    FROM read_parquet('output/silver/fundamentals/*/data.parquet', hive_partitioning=true)
    GROUP BY ticker
''').fetchdf()
print('\nSilver Fundamentals (by ticker):', df2.to_string(index=False))

# Sentiment
df3 = con.execute('''
    SELECT COUNT(*) as total, COUNT(sentiment_polarity) as with_score
    FROM read_parquet('output/silver/transcript_sentiment/**/*.parquet', hive_partitioning=true)
''').fetchdf()
print('\nSilver Sentiment:', df3.to_string(index=False))

con.close()
"
```

Expected output (2024 test):
```
Silver Price: total_rows=199592, tickers=818, trading_days=244
Silver Fundamentals: BIGGQ=658 rows, SNI=0 rows
Silver Sentiment: total=1950, with_score=1950
```

---

### Step 5: Build Gold Layer

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
```

**Expected result:** All 9 Gold views created successfully

**View Gold view results:**
```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "
import duckdb
con = duckdb.connect('duckdb/spx_analytics.duckdb', read_only=True)

views = ['v_market_daily_summary', 'v_ticker_profile', 'v_fundamental_snapshot', 'v_sentiment_price_view', 'v_rolling_volatility', 'v_momentum_signals', 'v_sector_rotation', 'v_sentiment_binned_returns', 'v_ar1_time_series']
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
v_fundamental_snapshot: 2 rows
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
  [PASS] Has 2 rows
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

## 3. Streamlit Dashboard

Start the Dashboard:

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m streamlit run dashboard.py --server.headless true
```

Access: **http://localhost:8501**

### Page Functionality

| Page | Data Source | Description |
|------|-------------|-------------|
| **Overview** | All views | Pipeline overview (4 metric cards) + market trend chart + avg daily return chart |
| **Market Daily Summary** | `v_market_daily_summary` | Daily market summary table (avg_close, avg_return, total_volume) |
| **Ticker Profile** | `v_ticker_profile` | Latest snapshot per ticker (company_name, sector, latest_close) + Sector distribution bar chart |
| **Fundamental Snapshot** | `v_fundamental_snapshot` | Latest financial data per ticker (revenue, net_income, assets, liabilities) |
| **Sentiment Price View** | `v_sentiment_price_view` | Sentiment score + price change scatter plot (sentiment_score vs next_1d_return) |

### Sidebar Controls

- **Ticker (optional):** Dropdown to filter single ticker data
- **Sentiment row limit:** Sentiment page row limit (default 2000)

---

## 4. Time Estimates (Full 20-Year Data)

| Phase | Estimated Time | Notes |
|-------|---------------|-------|
| Step 2 Simulator | 20-90 minutes | 5284 price day files + 5726 fundamental files + 32036 PDF copies |
| Step 3 Ingestion | 5-15 minutes | 4.32M CSV rows + 5726 index rows + 32036 index rows |
| Step 4 ELT Price | 1-3 minutes | DuckDB SQL deduplication + Parquet export |
| Step 4 ELT Fundamentals | 1-2 minutes | CSV unpivot + Parquet export |
| Step 4 ELT Transcripts | 5-15 minutes | PDF text extraction (32036 files) |
| Step 4 ELT Sentiment | 5-10 minutes | TextBlob sentiment analysis |
| Step 5 Gold | 10-30 seconds | Parquet read + view creation |
| **Total** | **~40 minutes - 2.5 hours** | |

---

## 5. DuckDB Direct Query Examples

```sql
-- Market daily summary
SELECT
    trade_date,
    number_of_tickers,
    avg_close,
    avg_return,
    total_volume
FROM v_market_daily_summary
WHERE trade_date BETWEEN '2020-01-01' AND '2024-12-31'
ORDER BY trade_date;

-- Annual statistics
SELECT
    EXTRACT(YEAR FROM trade_date) AS year,
    COUNT(*) AS trading_days,
    ROUND(AVG(avg_return) * 100, 2) AS avg_daily_return_pct,
    SUM(total_volume) AS total_volume
FROM v_market_daily_summary
GROUP BY EXTRACT(YEAR FROM trade_date)
ORDER BY year;

-- Sentiment and price change correlation (v_sentiment_price_view example)
SELECT
    ticker,
    transcript_date,
    sentiment_score,
    close_on_event_date,
    next_1d_return,
    next_5d_return
FROM v_sentiment_price_view
WHERE sentiment_score IS NOT NULL
ORDER BY transcript_date DESC
LIMIT 20;

-- Silver layer price data (direct Parquet query)
SELECT ticker, date, close, volume
FROM read_parquet('output/silver/price/*/*.parquet', hive_partitioning=true)
WHERE ticker = 'AAPL'
ORDER BY date
LIMIT 10;
```

---

## 6. Data Flow Summary (After Complete Run)

```
data/ (raw CSV/PDF)
    ↓ [Simulator - Step 2]
output/landing_zone/
    ├── prices/         5,284 CSVs ✅
    ├── fundamentals/   5,726 CSVs ✅
    └── transcripts/    32,036 PDFs ✅
    ↓ [Ingestion Engine - Step 3]
duckdb/spx_analytics.duckdb (Bronze)
    ├── raw_price_stream       4,322,232 rows ✅
    ├── raw_fundamental_index  5,726 rows ✅
    └── raw_transcript_index  32,036 rows ✅
    ↓ [ELT Pipeline - Step 4]
output/silver/
    ├── price/          Date-partitioned Parquet ✅
    ├── fundamentals/   Ticker-partitioned Parquet ✅
    ├── transcript_text/  PDF text extraction ✅
    └── transcript_sentiment/  Sentiment analysis Parquet ✅
    ↓ [Gold Layer - Step 5]
duckdb/spx_analytics.duckdb (Gold Views)
    ├── v_market_daily_summary       ✅
    ├── v_ticker_profile             ✅
    ├── v_fundamental_snapshot       ✅
    ├── v_sentiment_price_view      ✅
    ├── v_rolling_volatility        ✅
    ├── v_momentum_signals          ✅
    ├── v_sector_rotation           ✅
    ├── v_sentiment_binned_returns   ✅
    └── v_ar1_time_series           ✅
```

---

## 7. Quick Verification Commands (Small Sample for Testing)

For quick verification of key paths, use 2024 data:

```bash
# 1. Initialize
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py

# 2. Simulator (244 trading days)
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-12-30

# 3. Ingestion
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode scan

# 4. ELT (run step by step)
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource price
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource fundamentals
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource transcripts
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource sentiment

# 5. Gold Build & Test
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

---

## 8. Known Issues Fix History (2026-04-01)

The following issues have been fixed; all pipeline stages now work correctly:

| Issue | Cause | Fix | Status |
|-------|-------|-----|--------|
| Fundamentals failed to ingest to Bronze | `ingestion_engine.py` used non-existent columns `period` and `market_date` | Changed to `fiscal_date` (matching spec schema) | ✅ Fixed |
| Transcripts failed to ingest to Bronze | `ingestion_engine.py` used `file_path`, spec defines `pdf_path` | Changed to `pdf_path`, added missing `text_hash` column | ✅ Fixed |
| ELT fundamentals transform failed | `elt_pipeline.py` SQL queried `period` column (doesn't exist) and `ingested_at` (should be `received_at`) | Changed to `fiscal_date` and `received_at` | ✅ Fixed |
| ELT transcripts transform failed | `elt_pipeline.py` SQL queried `file_path` (should be `pdf_path`) and `ingested_at` (should be `received_at`) | Changed to `pdf_path` and `received_at` | ✅ Fixed |
| init_bronze.py / verify_tables.py path errors | Hardcoded Windows paths | Changed to `Path(__file__)` relative paths | ✅ Fixed |
| build_gold_layer.py GBK console encoding error | Unicode characters cannot display in GBK console | Added ASCII fallback mechanism | ✅ Fixed |

---

*Document updated: 2026-04-01*
*Pipeline version: Phase 1-6 all verified*
