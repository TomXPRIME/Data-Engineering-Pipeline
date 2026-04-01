# SPX 500 Data Pipeline

NUS MQF (Master of Quantitative Finance) QF5214 Data Engineering Course Project.

A production-like SPX 500 data pipeline with Medallion architecture (Bronze → Silver → Gold) and a simulated financial data API.

## Architecture

```
Existing Dataset (CSV/PDF)
    ↓
DataProvider API (simulates Yahoo Finance)
    ↓
Bronze Layer (OLTP - watchdog ingestion)
    ↓
ELT Pipeline (Bronze → Silver transform)
    ↓
Silver Layer (clean Parquet + sentiment)
    ↓
Gold Layer (OLAP views + Streamlit)
```

## Data Scale

| Data | Path | Scale |
|------|------|-------|
| Price (OHLCV) | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers, 5284 trading days (2004-2024) |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/` | 5726 files, annual + quarterly |
| PDF Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/` | 32,036 files |
| Tickers | `data/reference/tickers.csv` | 947 entries |

## Quick Start

### 一键测试（推荐）

```bash
python test_pipeline.py
```

自动执行：清理 → Simulator → Ingestion → ELT → Gold Build → 验证
测试范围：2024-01-02 ~ 2024-01-31（约20个交易日），预计 5-10 分钟

### 完整运行（20年数据）

```bash
# 1. 初始化 Bronze 表
python duckdb/init_bronze.py

# 2. Simulator（20年历史数据，约20-90分钟）
python -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2004-01-02 --end 2024-12-30

# 3. Ingestion Engine
python -m pipeline.ingestion_engine --mode scan

# 4. ELT Pipeline
python -m pipeline.elt_pipeline

# 5. Gold Layer
python gold/build_gold_layer.py

# 6. 验证
python gold/tests/test_gold_views.py
```

### Streamlit Dashboard

```bash
python -m streamlit run dashboard.py --server.headless true
```

访问 http://localhost:8501

## Implementation Phases

| Phase | Task | Status |
|-------|------|--------|
| 1 | DataProvider API | ✅ Completed |
| 2 | Bronze Layer (Ingestion Engine) | ✅ Completed |
| 3 | ELT Pipeline (Transform Jobs) | ✅ Completed |
| 4 | Silver Layer (Parquet + Sentiment) | ✅ Completed |
| 5 | Gold Layer (OLAP Views) | ✅ Completed |
| 6 | Streamlit Dashboard | ✅ Completed |

## Project Structure

```
5214_Project_SPX_Index_Raw_Data/
├── data/                              # 原始数据（只读）
│   ├── price/spx_20yr_ohlcv_data.csv
│   ├── fundamental/SPX_Fundamental_History/
│   ├── transcript/SPX_20yr_PDF_Library_10GB/
│   └── reference/tickers.csv
├── pipeline/                          # Pipeline 源代码
│   ├── data_provider.py              # 模拟金融 API
│   ├── ingestion_engine.py            # Bronze 层（watchdog）
│   ├── elt_pipeline.py                # Bronze → Silver 转换
│   └── simulators/                    # 虚拟时钟模拟器
├── output/
│   ├── landing_zone/                 # Simulator 输出
│   │   ├── prices/price_YYYY-MM-DD.csv
│   │   ├── fundamentals/YYYY-MM-DD/
│   │   └── transcripts/
│   └── silver/                       # Silver 层 Parquet
├── duckdb/                            # Gold 层 SQL + DuckDB 文件
├── gold/                              # Gold 层
│   ├── build_gold_layer.py           # Gold 层构建脚本
│   ├── sql/                          # Gold 视图 DDL
│   └── tests/test_gold_views.py      # Gold 视图测试
├── docs/                              # 文档
│   ├── RUN_GUIDE.md                  # 详细运行指南
│   └── ARCHIVE/                      # 已归档文档
├── test_pipeline.py                   # 一键测试脚本
├── dashboard.py                      # Streamlit Dashboard
├── STANDARDS.md                      # 开发规范
└── README.md                         # 本文件
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Access | Python class (DataProvider) |
| Ingestion | pandas + DuckDB + watchdog |
| Database | DuckDB (OLAP optimized) |
| ELT | DuckDB SQL + Python |
| Sentiment | TextBlob |
| Monitoring | Streamlit |
| Environment | conda (`qf5214_project`) |

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/RUN_GUIDE.md` | 详细运行指南（完整流水线步骤、数据规模估算） |
| `docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md` | 技术设计规范（架构、API、schema） |
| `STANDARDS.md` | 开发规范（代码风格、命名、测试、日志） |
| `CLAUDE.md` | Claude Code 提示词配置 |
