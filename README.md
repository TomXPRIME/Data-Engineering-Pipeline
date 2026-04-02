# SPX 500 Data Pipeline

NUS MQF (Master of Quantitative Finance) QF5214 Data Engineering Course Project.

A production-like SPX 500 data pipeline with Medallion architecture (Bronze → Silver → Gold) and a simulated financial data API.

## Architecture

```
Existing Dataset (CSV/PDF)
    ↓
DataProvider API (simulates Bloomberg/Yahoo Finance with cutoff_date filtering)
    ↓
Bronze Layer (OLTP - watchdog ingestion, ticker-partitioned fundamentals)
    ↓
ELT Pipeline (Bronze → Silver transform)
    ↓
Silver Layer (clean Parquet + sentiment)
    ↓
Gold Layer (10 OLAP views — DuckDB)
```

## Data Scale

| Data | Path | Scale |
|------|------|-------|
| Price (OHLCV) | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers, 5284 trading days (2004-2024) |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/` | 5726 files, annual + quarterly |
| PDF Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/` | 32,036 files |
| Tickers | `data/reference/tickers.csv` | 947 entries |

## Quick Start

### One-click test (recommended)

```bash
python test_pipeline.py
```

Auto: cleanup → Simulator → Ingestion → ELT → Gold Build → verify
Test range: 2024-01 (~20 trading days), ~5-10 min

### Full run (20-year data)

```bash
# 1. Initialize Bronze tables
python duckdb/init_bronze.py

# 2. Simulator (20-year history, ~20-90 min)
python -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2004-01-02 --end 2024-12-30

# 3. Ingestion Engine
python -m pipeline.ingestion_engine --mode scan

# 4. ELT Pipeline
python -m pipeline.elt_pipeline

# 5. Gold Layer
python gold/build_gold_layer.py

# 6. Verify
python gold/tests/test_gold_views.py
```

## Implementation Phases

| Phase | Task | Status |
|-------|------|--------|
| 1 | DataProvider API (`cutoff_date` Bloomberg-style filtering) | ✅ Completed |
| 2 | Bronze Layer (ticker-partitioned landing zone, `freq` column) | ✅ Completed |
| 3 | ELT Pipeline (Bronze → Silver, `freq` propagation) | ✅ Completed |
| 4 | Silver Layer (Parquet + Sentiment) | ✅ Completed |
| 5 | Gold Layer (10 OLAP views including `v_fundamental_history`) | ✅ Completed |
| 6 | Streamlit Dashboard | ❌ Incomplete |

## Project Structure

```
5214_Project_SPX_Index_Raw_Data/
├── data/                              # Raw data (read-only)
│   ├── price/spx_20yr_ohlcv_data.csv
│   ├── fundamental/SPX_Fundamental_History/
│   ├── transcript/SPX_20yr_PDF_Library_10GB/
│   └── reference/tickers.csv
├── pipeline/                          # Pipeline source code
│   ├── data_provider.py              # Simulated financial API
│   ├── ingestion_engine.py            # Bronze layer
│   ├── elt_pipeline.py                # Bronze → Silver transform
│   └── simulators/                    # Virtual clock simulators
├── output/
│   ├── landing_zone/                 # Simulator output
│   │   ├── prices/price_YYYY-MM-DD.csv
│   │   ├── fundamentals/{ticker}/   # ticker-partitioned (2026-04 redesign)
│   │   └── transcripts/
│   └── silver/                       # Silver layer Parquet
├── duckdb/                            # Gold layer SQL + DuckDB file
├── gold/                              # Gold layer
│   ├── build_gold_layer.py           # Gold layer builder
│   ├── sql/                          # Gold view DDL
│   └── tests/test_gold_views.py      # 27-check validation test
├── docs/                              # Documentation
│   ├── RUN_GUIDE.md                  # Detailed run guide
│   └── superpowers/specs/           # Technical design specs
├── test_pipeline.py                   # One-click pipeline test
├── dashboard.py                      # Streamlit Dashboard (INCOMPLETE)
├── STANDARDS.md                      # Development standards
└── README.md                         # This file
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Access | Python class (DataProvider with `cutoff_date`) |
| Ingestion | pandas + DuckDB + watchdog |
| Database | DuckDB (OLAP optimized) |
| ELT | DuckDB SQL + Python |
| Sentiment | TextBlob |
| Monitoring | Streamlit (pending re-implementation) |
| Environment | conda (`qf5214_project`) |

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/RUN_GUIDE.md` | Detailed run guide (Chinese) |
| `docs/RUN_GUIDE_en.md` | Detailed run guide (English) |
| `docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md` | Technical design (source of truth) |
| `docs/superpowers/specs/2026-04-02-fundamental-api-redesign-design.md` | Fundamental API redesign spec |
| `STANDARDS.md` | Development standards |
| `CLAUDE.md` | Claude Code instructions |

## Gold Views (10 total)

| View | Description |
|------|-------------|
| `v_market_daily_summary` | Daily market aggregates |
| `v_ticker_profile` | Latest ticker snapshot |
| `v_fundamental_snapshot` | Latest financials per ticker |
| `v_fundamental_history` | Full history with fiscal_date filtering (Bloomberg-style) |
| `v_sentiment_price_view` | Sentiment + price reaction |
| `v_rolling_volatility` | 20d/60d annualized volatility |
| `v_momentum_signals` | Multi-period momentum + trend |
| `v_sector_rotation` | Quarterly sector ranking |
| `v_sentiment_binned_returns` | Sentiment bucket vs forward returns |
| `v_ar1_time_series` | AR(1) OLS regression |
