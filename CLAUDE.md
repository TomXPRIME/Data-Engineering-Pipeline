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
│   ├── sql/                          # Gold SQL DDL (star schema + materialized + OLAP views)
│   ├── query/                        # Python Query layer (7 query classes)
│   ├── dim_date_generator.py         # dim_date parquet generator
│   ├── dim_ticker_generator.py       # dim_ticker SCD Type 2 generator
│   └── tests/test_gold_views.py      # 45-check validation test
├── docs/
│   ├── RUN_GUIDE.md                  # Detailed run guide
│   ├── ARCHIVE/                      # Archived docs
│   └── superpowers/specs/           # Technical design specs
├── test_pipeline.py                   # One-click pipeline test
├── dashboard.py                      # 6-tab Streamlit dashboard (✅ COMPLETE)
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
| `gold/build_gold_layer.py` | Build Gold Star Schema (tables + materialized + views) |
| `gold/tests/test_gold_views.py` | Verify all Gold tables/views (45 checks) |

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/RUN_GUIDE.md` | Detailed run guide with full-year data instructions |
| `docs/CONSOLIDATED.md` | **Consolidated docs** — all specs integrated, contradictions resolved (code is source of truth) |
| `docs/superpowers/specs/2026-04-02-fundamental-api-redesign-design.md` | Technical design for fundamental API redesign |
| `STANDARDS.md` | Development standards — code style, naming, testing, logging |
| `README.md` | Project overview — quick start, architecture, structure |

## Dependencies

Core packages (conda environment `qf5214_project`):
- `pandas`, `duckdb`, `watchdog`, `streamlit`
- `pypdf` for PDF text extraction
- `textblob` for sentiment analysis

## Gold Layer — Star Schema (5 tables + 3 materialized + 7 views)

**Physical Dimension Tables:**
| Table | Description | Rows |
|-------|-------------|------|
| `dim_date` | Physical date dimension (US trading calendar) | 36,890 |
| `dim_ticker` | SCD Type 2 ticker dimension | 595 |

**Fact Tables:**
| Table | Description | Rows |
|-------|-------------|------|
| `fact_daily_price` | Daily OHLCV + precomputed returns | 205,318 |
| `fact_quarterly_fundamentals` | Pivoted quarterly financials | 4,504 |
| `fact_earnings_transcript` | Sentiment + price reaction | 1,954 |

**Materialized Fact Tables (pre-computed heavy window functions):**
| Table | Description | Rows |
|-------|-------------|------|
| `fact_rolling_volatility` | 20d/60d annualized volatility | 147,003 |
| `fact_momentum_signals` | Multi-period momentum + MA deviation | 112,705 |
| `fact_ar1_results` | AR(1) OLS regression per ticker | 135,159 |

**OLAP Views (lightweight aggregations):**
| View | Description | Rows |
|------|-------------|------|
| `v_market_daily_summary` | Daily market aggregates | 251 |
| `v_ticker_profile` | Latest ticker snapshot | 818 |
| `v_fundamental_snapshot` | Latest financials per ticker | 595 |
| `v_fundamental_history` | Bloomberg-style AS OF join | 205,318 |
| `v_sentiment_price_view` | Sentiment + price reaction | 1,954 |
| `v_sentiment_binned_returns` | Sentiment bucket vs forward returns | 2 |
| `v_sector_rotation` | Quarterly sector ranking | 52 |

## Python Query Layer

7 query classes in `gold/query/` with `@st.cache_data(ttl=3600)`:
- `PriceQuery` — market overview, ticker price, trading dates
- `FundamentalsQuery` — snapshot, history (Bloomberg AS OF), quarterly
- `SentimentQuery` — sentiment view, binned returns
- `RiskQuery` — rolling volatility, AR1 results
- `SectorQuery` — sector rotation
- `DimensionQuery` — tickers, trading calendar

All queries use **parameterized SQL** to prevent injection.

## Dashboard (Phase 6 — COMPLETE)

6-tab Bloomberg-style Streamlit dashboard at `dashboard.py`:
- **Tab1 Market Overview** — avg_close, avg_return, volume time series
- **Tab2 Stock Analysis** — OHLCV chart + data table
- **Tab3 Fundamental History** — Bloomberg-style `cutoff_date` filter via `v_fundamental_history`
- **Tab4 Sentiment Analytics** — sentiment time series + binned returns
- **Tab5 Sector Rotation** — quarterly sector rankings with momentum rank
- **Tab6 Risk & Performance** — 20d/60d volatility, AR1 alpha/beta

## Implementation Phases

| Phase | Task | Status |
|-------|------|--------|
| 1 | DataProvider API (`cutoff_date` parameter) | ✅ Completed |
| 2 | Bronze Layer (Ingestion Engine, ticker-partitioned fundamentals) | ✅ Completed |
| 3 | ELT Pipeline (Bronze → Silver, freq propagation) | ✅ Completed |
| 4 | Silver Layer (Parquet + Sentiment) | ✅ Completed |
| 5 | Gold Layer Star Schema (5 tables + 3 materialized + 7 views) | ✅ Completed |
| 6 | Streamlit 6-tab Dashboard | ✅ Completed |
