# SPX 500 Data Pipeline — Consolidated Documentation

> **Source of Truth Rule:** When docs contradict code, **code wins**.
> This document integrates all specs and corrects known contradictions.
> Last updated: 2026-04-02

---

## 1. Architecture

```
Raw Data (CSV/PDF)
    ↓
DataProvider API (SPXDataProvider)
    ↓
Bronze Layer (OLTP — DuckDB message queue)
    ↓ ELT Pipeline
Silver Layer (clean Parquet + sentiment)
    ↓ Gold Build
Gold Layer (Medallion + Star Schema)
    ├── dim_ticker (SCD Type 2)
    ├── dim_date (物理化)
    ├── fact_daily_price
    ├── fact_quarterly_fundamentals
    ├── fact_earnings_transcript
    ├── fact_rolling_volatility (物化)
    ├── fact_momentum_signals (物化)
    ├── fact_ar1_results (物化)
    └── [7 OLAP views]
        ↓ Python Query Layer
Streamlit Dashboard (6 Tab)
```

---

## 2. Implementation Phases

| Phase | Task | Status | Notes |
|-------|------|--------|-------|
| 1 | DataProvider API (`cutoff_date` parameter) | ✅ Completed | `get_fundamentals(ticker, freq, cutoff_date)` |
| 2 | Bronze Layer (ticker-partitioned landing zone, `freq` column) | ✅ Completed | `fundamentals/{ticker}/` |
| 3 | ELT Pipeline (Bronze → Silver, `freq` propagation) | ✅ Completed | |
| 4 | Silver Layer (Parquet + Sentiment) | ✅ Completed | |
| 5 | Gold Layer (Star Schema) | 🔄 In Progress | New design: medallion + star schema fusion |
| 6 | Streamlit Dashboard (6 Tab) | 🔄 In Progress | Part of Phase 5 |

**Phase 5 & 6 详细设计：** `docs/superpowers/specs/2026-04-02-medallion-star-schema-design.md`
**执行计划：** `docs/superpowers/plans/2026-04-02-medallion-star-schema-plan.md`

---

## 3. Project Structure

```
5214_Project_SPX_Index_Raw_Data/
├── data/                              # Raw data (read-only)
│   ├── price/spx_20yr_ohlcv_data.csv
│   ├── fundamental/SPX_Fundamental_History/
│   ├── transcript/SPX_20yr_PDF_Library_10GB/
│   └── reference/tickers.csv
├── pipeline/
│   ├── data_provider.py               # SPXDataProvider (cutoff_date ✅)
│   ├── ingestion_engine.py
│   ├── elt_pipeline.py
│   └── simulators/
│       └── comprehensive_simulator.py
├── output/
│   ├── landing_zone/
│   │   ├── prices/price_YYYY-MM-DD.csv
│   │   ├── fundamentals/{ticker}/      # ticker-partitioned
│   │   └── transcripts/
│   └── silver/                         # Parquet
├── duckdb/
│   ├── spx_analytics.duckdb
│   ├── init_bronze.py
│   └── create_bronze_tables.sql
├── gold/
│   ├── build_gold_layer.py            # 重建为 Star Schema 构建器
│   ├── dim_date_generator.py           # 新增: dim_date 生成器
│   ├── dim_ticker_generator.py         # 新增: dim_ticker SCD Type 2
│   ├── sql/
│   │   ├── create_star_schema.sql      # 新增: 5张核心表
│   │   ├── create_materialized.sql     # 新增: 3张物化表
│   │   └── create_olap_views.sql        # 新增: 7个轻量视图
│   ├── query/                          # 新增: Python Query 层
│   │   ├── gold_data_provider.py
│   │   ├── price_query.py
│   │   ├── fundamentals_query.py
│   │   ├── sentiment_query.py
│   │   ├── risk_query.py
│   │   ├── sector_query.py
│   │   └── dimension_query.py
│   └── tests/
│       ├── test_dim_date_generator.py  # 新增
│       ├── test_dim_ticker_generator.py # 新增
│       └── test_gold_views.py           # 旧9视图测试，待更新
├── docs/
│   ├── RUN_GUIDE.md
│   ├── RUN_GUIDE_en.md
│   ├── ARCHIVE/
│   └── superpowers/
│       └── specs/
│           └── 2026-04-02-medallion-star-schema-design.md  # 当前设计
├── dashboard.py                        # 待重建: 6 Tab Streamlit
├── STANDARDS.md
└── README.md
```

---

## 4. Star Schema 设计（Phase 5 目标）

### Gold Layer 目标结构

| 表类型 | 表名 | 粒度 |
|--------|------|------|
| Dim | `dim_ticker` | ticker, SCD Type 2 |
| Dim | `dim_date` | date, 物理化 |
| Fact | `fact_daily_price` | ticker × date |
| Fact | `fact_quarterly_fundamentals` | ticker × fiscal_date |
| Fact | `fact_earnings_transcript` | ticker × event_date |
| Fact (物化) | `fact_rolling_volatility` | ticker × date |
| Fact (物化) | `fact_momentum_signals` | ticker × date |
| Fact (物化) | `fact_ar1_results` | ticker × date |

### OLAP 视图（7个）

| 视图 | 依赖表 |
|------|--------|
| v_market_daily_summary | fact_daily_price |
| v_ticker_profile | fact_daily_price + dim_ticker |
| v_fundamental_snapshot | fact_quarterly_fundamentals |
| v_fundamental_history | fact_quarterly_fundamentals (AS OF join) |
| v_sentiment_price_view | fact_earnings_transcript |
| v_sentiment_binned_returns | fact_earnings_transcript |
| v_sector_rotation | fact_daily_price + dim_ticker |

---

## 5. 数据规模

| 数据 | 路径 | 规模 |
|------|------|------|
| Price (OHLCV) | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers, 5284 trading days (2004-2024) |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/*.csv` | 5726 files, annual + quarterly |
| PDF Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/*.pdf` | 32,036 files |
| Tickers | `data/reference/tickers.csv` | 947 entries |

---

## 6. 已删除/过期的文档

以下文档已被删除（见 git commit history）：

| 文件 | 删除原因 |
|------|---------|
| `docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md` | 旧版 landing zone 设计（`{YYYY-MM-DD}/TICKER` 结构） |
| `docs/superpowers/specs/2026-04-01-gold-layer-enhancement-design.md` | 描述9视图+计划中dashboard，已过期 |
| `docs/superpowers/plans/2026-04-01-gold-layer-enhancement-plan.md` | 旧计划，dashboard 从该计划中移除 |
| `docs/superpowers/plans/2026-04-02-fundamental-api-redesign-plan.md` | Fundamental API 重构已完成 |
| `gold/sql/person_a_views.sql` | 孤儿文件，未被引用 |
| `gold/sql/person_b_views.sql` | 孤儿文件，未被引用 |
| `gold/sql/create_gold_views.sql` | 被新的3个 SQL 文件替代 |

---

*文档更新日期：2026-04-02*
*当前架构：Medallion + Star Schema 设计阶段（Phase 5 执行中）*
