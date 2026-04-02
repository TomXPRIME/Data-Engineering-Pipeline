# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NUS MQF (Master of Quantitative Finance) course project — a production-like SPX 500 data pipeline with Medallion architecture (Bronze → Silver → Gold) and a simulated financial data API.

## Project Structure

```
5214_Project_SPX_Index_Raw_Data/
├── data/                              # Raw data (read-only)
│   ├── price/spx_20yr_ohlcv_data.csv
│   ├── fundamental/SPX_Fundamental_History/
│   ├── transcript/SPX_20yr_PDF_Library_10GB/
│   └── reference/tickers.csv
├── pipeline/                           # Pipeline source code
│   ├── data_provider.py               # Simulated financial API
│   ├── ingestion_engine.py            # Bronze layer (watchdog-based)
│   ├── elt_pipeline.py                # Bronze → Silver transform
│   └── simulators/                    # Virtual clock simulators
├── output/
│   ├── landing_zone/                  # Simulator output
│   │   ├── prices/price_YYYY-MM-DD.csv
│   │   ├── fundamentals/{ticker}/   # ticker-partitioned (2026-04 redesign)
│   │   └── transcripts/
│   └── silver/                       # Silver layer Parquet
├── duckdb/                            # Gold layer SQL + DB file
├── gold/                              # Gold layer
│   ├── build_gold_layer.py           # Gold layer builder
│   ├── sql/                          # Gold view DDL
│   └── tests/test_gold_views.py      # 27-check validation test
├── docs/
│   ├── RUN_GUIDE.md                  # Detailed run guide
│   ├── ARCHIVE/                      # Archived docs
│   └── superpowers/specs/           # Technical design specs
├── test_pipeline.py                   # One-click pipeline test
├── dashboard.py                      # Streamlit dashboard (INCOMPLETE - Phase 6 pending)
├── STANDARDS.md                      # Development standards
└── README.md                         # Project overview
```

## Data Architecture

```
Existing Dataset (CSV/PDF)
    ↓
DataProvider API (simulates Yahoo Finance, with cutoff_date filtering)
    ↓
Bronze Layer (OLTP - watchdog ingestion)
    ↓
ELT Pipeline (Bronze → Silver transform)
    ↓
Silver Layer (clean Parquet + sentiment)
    ↓
Gold Layer (OLAP views + Streamlit)
```

## Available Data

All data is already present locally — no downloads needed.

| Data | Path | Scale |
|------|------|-------|
| Price (OHLCV) | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers, 5284 trading days (2004-2024) |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/*.csv` | 5726 files, annual + quarterly |
| PDF Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/*.pdf` | 32,036 files |
| Tickers | `data/reference/tickers.csv` | 947 entries |

## Running the Pipeline

**Python interpreter (conda environment):**
```
C:/miniconda3/envs/qf5214_project/python.exe
```

**One-click test (recommended for verification):**
```bash
python test_pipeline.py
```

**Manual pipeline (step by step):**
```bash
# 1. Initialize Bronze tables
python duckdb/init_bronze.py

# 2. Simulator - emit landing zone files
python -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2004-01-02 --end 2024-12-30

# 3. Ingestion Engine - ingest landing zone to Bronze
python -m pipeline.ingestion_engine --mode scan

# 4. ELT Pipeline - Bronze to Silver
python -m pipeline.elt_pipeline

# 5. Gold Layer - build OLAP views
python gold/build_gold_layer.py

# 6. Verify Gold views
python gold/tests/test_gold_views.py
```

## Key Scripts

| Script | Purpose |
|--------|---------|
| `test_pipeline.py` | One-click pipeline test (2024-01, ~5-10 min) |
| `duckdb/init_bronze.py` | Initialize DuckDB Bronze tables |
| `gold/build_gold_layer.py` | Build Gold OLAP views from Silver Parquet |
| `gold/tests/test_gold_views.py` | Verify all 10 Gold views (27 checks) |

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/RUN_GUIDE.md` | Detailed run guide with full-year data instructions |
| `docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md` | Technical design (source of truth) |
| `STANDARDS.md` | Development standards — code style, naming, testing, logging |
| `README.md` | Project overview — quick start, architecture, structure |

## Dependencies

Core packages (conda environment `qf5214_project`):
- `pandas`, `duckdb`, `watchdog`, `streamlit`
- `pypdf` for PDF text extraction
- `textblob` for sentiment analysis

## Gold Views (10 total)

| View | Description | Rows (2024 test) |
|------|-------------|-------------|
| `v_market_daily_summary` | Daily market aggregates | 251 |
| `v_ticker_profile` | Latest ticker snapshot | 818 |
| `v_fundamental_snapshot` | Latest financials per ticker | 595 |
| `v_fundamental_history` | Full history with fiscal_date filtering | 735,163 |
| `v_sentiment_price_view` | Sentiment + price reaction | 1,954 |
| `v_rolling_volatility` | 20d/60d annualized volatility | 147,003 |
| `v_momentum_signals` | Multi-period momentum + trend | 112,705 |
| `v_sector_rotation` | Quarterly sector ranking | 4 |
| `v_sentiment_binned_returns` | Sentiment bucket vs forward returns | 2 |
| `v_ar1_time_series` | AR(1) OLS regression | 135,160 |

## Dashboard (Phase 6 — INCOMPLETE)

Dashboard was removed due to `st.hist_chart` bug and scope issues.
**Bloomberg-style Fundamental History page** (with `cutoff_date` filtering via `v_fundamental_history`)
needs to be re-implemented as a separate page on top of the restored dashboard.

## Implementation Phases

| Phase | Task | Status |
|-------|------|--------|
| 1 | DataProvider API (`cutoff_date` parameter) | ✅ Completed |
| 2 | Bronze Layer (Ingestion Engine, ticker-partitioned fundamentals) | ✅ Completed |
| 3 | ELT Pipeline (Bronze → Silver, freq propagation) | ✅ Completed |
| 4 | Silver Layer (Parquet + Sentiment) | ✅ Completed |
| 5 | Gold Layer (10 OLAP views including `v_fundamental_history`) | ✅ Completed |
| 6 | Streamlit Dashboard | ❌ Incomplete |
