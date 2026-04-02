# Medallion + Star Schema 融合架构设计

> **STATUS: ✅ COMPLETED (2026-04-02)** — 所有设计已实现并验证通过。本文档保留供历史参考。
>
> **Source of Truth Rule:** 当本文档与代码矛盾时，**以代码为准**。
> Last updated: 2026-04-02

---

## 1. 背景与目标

当前 pipeline 采用 Medallion 架构 (Bronze → Silver → Gold)，但存在以下问题：

1. Silver 层使用 EAV (Entity-Attribute-Value) 模式存储基本面数据，查询时需要大量 `CASE WHEN` pivot 操作
2. Gold 层仅有 OLAP 视图，无物理 star schema 事实表，BI 查询性能差
3. 缺少共享维度表，跨表分析（如 price × fundamentals）没有 AS OF 语义，存在 look-ahead bias 风险
4. Streamlit Dashboard 每次交互都重新运行全量查询，响应慢

**融合目标：**

- Medallion 保证数据质量分层
- Star Schema 保证查询性能和数据组织清晰
- Python Query 层封装 + Streamlit 缓存保证交互性能

---

## 2. 整体架构

```
Raw Data (CSV/PDF)
       ↓
DataProvider API (SPXDataProvider)
    ├── get_price(ticker, date)
    ├── get_fundamentals(ticker, freq, cutoff_date)
    ├── get_transcript(ticker, date)
    ├── get_trading_dates(start, end)
    └── get_ticker_list()
       ↓
Bronze Layer (OLTP - DuckDB)
    ├── raw_price_stream
    ├── raw_fundamental_index
    ├── raw_transcript_index
    └── ingestion_audit
       ↓ ELT Pipeline
Silver Layer (清洗 + 打宽)
    ├── silver_price (ticker, date, OHLCV)
    ├── silver_fundamentals (打宽: ticker, fiscal_date, period_date, revenue, net_income, ...)
    └── silver_sentiment (ticker, event_date, polarity, subjectivity)
       ↓ Gold Build (build_gold_layer.py)
Gold Layer (Star Schema)
    ├── dim_ticker (SCD Type 2)
    ├── dim_date (物理化)
    ├── fact_daily_price
    ├── fact_quarterly_fundamentals
    ├── fact_earnings_transcript
    ├── fact_rolling_volatility (物化)
    ├── fact_momentum_signals (物化)
    ├── fact_ar1_results (物化)
    └── [OLAP 视图 7个]
       ↓ Python Query Layer
Streamlit Dashboard (6 Tab)
```

---

## 3. DataProvider 衔接设计

### 3.1 `dim_date` 生成 → DataProvider

```
DataProvider.get_trading_dates('2000-01-01', '2100-12-31')
    → 遍历所有日期，标记 is_trading_day = (date in trading_dates)
    → 美股假期硬编码: NewYear, MLKDay, PresidentsDay, GoodFriday, MemorialDay,
                      IndependenceDay, LaborDay, Thanksgiving, Christmas
    → 写入 output/gold/dim_date.parquet
```

**dim_date 字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| date | DATE PK | 日期 |
| year | INT | 年 |
| quarter | INT | 日历季度 |
| month | INT | 月 |
| day | INT | 日 |
| day_of_week | INT | 0=周一, 4=周五 |
| is_trading_day | BOOLEAN | 是否交易日 |
| is_holiday | BOOLEAN | 是否假期 |
| holiday_name | VARCHAR | 假期名称 |
| trading_day_offset | INT | 距最近交易日偏移 |

### 3.2 `dim_ticker` 生成 → DataProvider + Silver 层

```
DataProvider.get_ticker_list()
    → 获取所有 ticker 列表
Silver_fundamentals (profile report_type)
    → 获取 company_name, sector, industry
    → 写入 dim_ticker (SCD Type 2)
```

### 3.3 AS OF Join 语义 → `cutoff_date` 参数

`DataProvider.get_fundamentals(cutoff_date)` 保证了"截至某日期只能看到当时已公开的财务数据"。

Gold 层跨粒度视图使用相同语义：

```sql
-- AS OF join: 某日期看到的是该日期之前最新发布的财报
LEFT JOIN fact_quarterly_fundamentals f
    ON p.ticker = f.ticker
    AND f.period_date = (
        SELECT MAX(period_date)
        FROM fact_quarterly_fundamentals
        WHERE ticker = p.ticker
          AND period_date <= p.date  -- AS OF 语义
    )
```

---

## 4. Star Schema 物理表设计

### 4.1 dim_ticker (SCD Type 2)

| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR PK | 股票代码 |
| company_name | VARCHAR | 公司名称 |
| sector | VARCHAR | 板块 |
| industry | VARCHAR | 行业 |
| valid_from | DATE | 生效日期 |
| valid_to | DATE | 失效日期 |
| is_current | BOOLEAN | 是否当前版本 |

**SCD Type 2 策略：** sector/industry 变化时，关闭旧行（valid_to = 变化日期-1），插入新行（valid_from = 变化日期）。
变更触发条件：同一 ticker 的 sector 或 industry 字段值与上一版本不同时，插入新行。

### 4.2 dim_date (物理化)

| 字段 | 类型 | 说明 |
|------|------|------|
| date | DATE PK | 日期 |
| year | INT | 年 |
| quarter | INT | 日历季度 |
| month | INT | 月 |
| day | INT | 日 |
| day_of_week | INT | 0=周一...6=周日 |
| is_trading_day | BOOLEAN | 是否交易日 |
| is_holiday | BOOLEAN | 是否假期 |
| holiday_name | VARCHAR | 假期名称 |
| trading_day_offset | INT | 距最近交易日偏移 |

### 4.3 fact_daily_price

| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR FK→dim_ticker | 股票代码 |
| date | DATE FK→dim_date | 交易日期 |
| open | DECIMAL(18,6) | 开盘价 |
| high | DECIMAL(18,6) | 最高价 |
| low | DECIMAL(18,6) | 最低价 |
| close | DECIMAL(18,6) | 收盘价 |
| adj_close | DECIMAL(18,6) | 复权收盘价 |
| volume | BIGINT | 成交量 |
| prev_close | DECIMAL(18,6) | 前一交易日收盘价 |
| daily_return | DECIMAL(18,8) | 日收益率 |
| next_1d_return | DECIMAL(18,8) | 前向1日收益率（最后1-2行无数据，Gold Build时置NULL） |
| next_5d_return | DECIMAL(18,8) | 前向5日收益率（最后5行无数据，Gold Build时置NULL） |

### 4.4 fact_quarterly_fundamentals

| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR FK→dim_ticker | 股票代码 |
| fiscal_date | DATE | 财年/季结束日 |
| period_date | DATE | 实际发布日期（用于AS OF join） |
| report_type | VARCHAR | income/balance/cashflow/profile |
| freq | VARCHAR | annual/quarterly |
| revenue | DECIMAL(18,2) | 营收 |
| net_income | DECIMAL(18,2) | 净利润 |
| total_assets | DECIMAL(18,2) | 总资产 |
| total_liabilities | DECIMAL(18,2) | 总负债 |
| eps | DECIMAL(18,6) | 每股收益 |
| book_value_per_share | DECIMAL(18,6) | 每股账面价值 |

> **period_date vs fiscal_date：** `fiscal_date` 是财报覆盖的期间（如 2020Q1），`period_date` 是该财报实际发布的日期。AS OF join 使用 `period_date` 确保只看截至当前日期已发布的财报。

### 4.5 fact_earnings_transcript

| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR FK→dim_ticker | 股票代码 |
| event_date | DATE FK→dim_date | 财报会议日期 |
| transcript_date | DATE | 实际发布日期 |
| sentiment_polarity | DECIMAL(8,6) | 情感极性 [-1, 1] |
| sentiment_subjectivity | DECIMAL(8,6) | 主观性 [0, 1] |
| next_1d_return | DECIMAL(18,8) | 前向1日收益 |
| next_5d_return | DECIMAL(18,8) | 前向5日收益 |

---

## 5. 物化表设计

重度计算窗口函数预物化为物理表：

### 5.1 fact_rolling_volatility

基于 `fact_daily_price` 预计算：
- `annualized_vol_20d` — 20天年化波动率
- `annualized_vol_60d` — 60天年化波动率
- `annualized_return_20d` — 20天年化收益率

### 5.2 fact_momentum_signals

基于 `fact_daily_price` 预计算：
- `momentum_5d`, `momentum_20d`, `momentum_60d`
- `dist_pct_from_ma20`, `dist_pct_from_ma60`
- `trend_signal` — STRONG_UPTREND/WEAK_UPTREND/NEUTRAL/WEAK_DOWNTREND/STRONG_DOWNTREND

### 5.3 fact_ar1_results

基于 `fact_daily_price` 预计算：
- `alpha_ar1`, `beta_ar1`, `r_squared_ar1`, `n_obs`
- AR(1) 模型: r_t = α + β * r_{t-1} + ε

---

## 6. Gold OLAP 视图设计（7个轻量视图）

物化表承担重度计算，剩余7个轻量视图：

| 视图 | 依赖表 | 查询频率 |
|------|--------|---------|
| v_market_daily_summary | fact_daily_price | 高 |
| v_ticker_profile | fact_daily_price + dim_ticker | 高 |
| v_fundamental_snapshot | fact_quarterly_fundamentals | 中 |
| v_fundamental_history | fact_quarterly_fundamentals | 中 |
| v_sentiment_price_view | fact_earnings_transcript + fact_daily_price | 中 |
| v_sentiment_binned_returns | fact_earnings_transcript | 低 |
| v_sector_rotation | fact_daily_price + dim_ticker | 低 |

---

## 7. Python Query 层设计

### 7.1 类结构

| 类 | 职责 | 核心方法 |
|----|------|---------|
| `PriceQuery` | 价格/市场/交易日历 | `get_daily_summary()`, `get_ticker_price()`, `get_trading_dates()`, `get_market_overview()` （market_overview 在 PriceQuery 内，非独立类） |
| `FundamentalsQuery` | 基本面 ASOF 查询 | `get_snapshot(ticker)`, `get_history(ticker, cutoff_date)`, `get_quarterly(ticker)` |
| `SentimentQuery` | 情感分析 | `get_sentiment_price(ticker)`, `get_binned_returns()`, `get_sentiment_series(ticker)` |
| `RiskQuery` | 风险指标 | `get_rolling_volatility(ticker)`, `get_momentum_signals(ticker)`, `get_ar1(ticker)` |
| `SectorQuery` | 板块轮动 | `get_sector_rotation()` |
| `DimensionQuery` | 维度表查询 | `get_ticker(ticker)`, `get_date_dim(start, end)` |

### 7.2 缓存策略

```python
import streamlit as st

class PriceQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_daily_summary(start_date: str, end_date: str) -> pd.DataFrame:
        """市场日汇总，缓存1小时"""
        ...

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_ticker_price(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """单只股票价格序列，缓存1小时"""
        ...
```

### 7.3 DataProvider 封装

```python
class GoldDataProvider:
    """Gold 层的 DataProvider 封装，屏蔽底层存储细节"""

    def __init__(self, duckdb_path: str = "duckdb/spx_analytics.duckdb"):
        self._conn = duckdb.connect(duckdb_path)

    def execute(self, query: str) -> pd.DataFrame:
        return self._conn.execute(query).fetchdf()
```

---

## 8. Streamlit Dashboard 设计

### 8.1 页面结构（6 Tab）

| Tab | 名称 | 核心功能 | Query 类 |
|-----|------|---------|---------|
| 1 | Market Overview | 市场指数、涨跌幅、热力图 | `PriceQuery`, `MarketQuery` |
| 2 | Stock Analysis | 个股OHLCV、技术指标叠加 | `PriceQuery`, `RiskQuery` |
| 3 | Fundamental History | Bloomberg风格基本面历史，cutoff_date过滤 | `FundamentalsQuery` |
| 4 | Sentiment Analytics | 情感时序、情感桶收益分析 | `SentimentQuery` |
| 5 | Sector Rotation | 季度板块轮动排名 | `SectorQuery` |
| 6 | Risk & Performance | 波动率、AR1、动量信号 | `RiskQuery` |

### 8.2 Tab3: Fundamental History（Bloomberg风格）

```python
# 核心交互：cutoff_date 过滤
cutoff_date = st.date_input(
    "As-of Date (cutoff_date)",
    value=datetime(2020, 6, 30),
    max_value=datetime(2024, 12, 31)
)

# 查询：只返回 cutoff_date 之前发布的财报
df = FundamentalsQuery.get_history(ticker, cutoff_date=str(cutoff_date))
```

### 8.3 性能保障

- 所有 Query 方法加 `@st.cache_data(ttl=3600)`
- 大数据量查询使用 `st.progress` 提示用户
- Sidebar 提供全局日期范围过滤，影响所有 Tab

---

## 9. 数据流总结

```
DataProvider API
    │
    ├─ get_ticker_list() ──────────────────────────→ dim_ticker (SCD Type 2)
    ├─ get_trading_dates() ────────────────────────→ dim_date (is_trading_day)
    ├─ get_price() ────────────────────────────────→ Bronze(raw_price_stream)
    ├─ get_fundamentals(cutoff_date) ─────────────→ Bronze(raw_fundamental_index)
    │       │ ELT: 打宽 + period_date 追踪
    │       ↓
    │   Silver: silver_fundamentals (打宽)
    │       │ Gold Build: AS OF join + 预计算
    │       ↓
    │   Gold: fact_quarterly_fundamentals (period_date for AS OF)
    │
    └─ get_transcript() ───────────────────────────→ Bronze(raw_transcript_index)
            │ ELT: PDF文本提取 + TextBlob情感
            ↓
        Silver: silver_sentiment
            │ Gold Build
            ↓
        Gold: fact_earnings_transcript
```

---

## 10. 实现优先级

| 优先级 | 任务 | 说明 |
|--------|------|------|
| P0 | `dim_date` 物理化 | 依赖 DataProvider.get_trading_dates() |
| P0 | `dim_ticker` SCD Type 2 | 依赖 Silver fundamentals profile 数据 |
| P0 | `fact_daily_price` | Silver price → Gold 打宽 + 预计算收益 |
| P1 | `fact_quarterly_fundamentals` | Silver fundamentals 打宽 + period_date |
| P1 | `fact_earnings_transcript` | Silver sentiment + 价格收益 |
| P1 | 物化3表 | fact_rolling_volatility, fact_momentum_signals, fact_ar1_results |
| P2 | 7个 OLAP 视图 | 基于 Star 表重建 |
| P2 | Python Query 层 | 6个类 + @st.cache_data |
| P3 | Streamlit Dashboard | 6 Tab |

---

## 11. 文件变更清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `gold/build_gold_layer.py` | 重写 | Star Schema + 物化表构建逻辑 |
| `gold/sql/create_star_schema.sql` | 新增 | Star Schema DDL |
| `gold/sql/create_materialized.sql` | 新增 | 物化表 DDL |
| `gold/sql/create_olap_views.sql` | 新增 | 轻量 OLAP 视图（原10个视图中的7个保留，3个重度视图转为物化物理表） |
| `gold/dim_date_generator.py` | 新增 | dim_date 生成器 |
| `gold/dim_ticker_generator.py` | 新增 | dim_ticker SCD Type 2 生成器 |
| `gold/query/` | 新增 | Python Query 类目录 |
| `gold/query/__init__.py` | 新增 | Query 类导出 |
| `gold/query/price_query.py` | 新增 | PriceQuery |
| `gold/query/fundamentals_query.py` | 新增 | FundamentalsQuery |
| `gold/query/sentiment_query.py` | 新增 | SentimentQuery |
| `gold/query/risk_query.py` | 新增 | RiskQuery |
| `gold/query/sector_query.py` | 新增 | SectorQuery |
| `gold/query/dimension_query.py` | 新增 | DimensionQuery |
| `gold/query/gold_data_provider.py` | 新增 | GoldDataProvider DuckDB封装 |
| `dashboard.py` | 重写 | 6 Tab Streamlit Dashboard |

---

## 12. 已知约束

1. **数据集时间范围:** 2004-2024 (20年)，dim_date 需要向前补充到2000年，向后扩展到2100年
2. **美股假期:** 硬编码在 dim_date_generator.py 中（10个年度假期）
3. **DuckDB 物化视图:** DuckDB 0.10+ 支持物化视图，但本项目使用 Parquet 文件存储物化结果，由 Python 脚本控制刷新
4. **SCD Type 2 历史数据量:** 预计 ~5000 行（947 tickers × 平均5年变更周期），存储成本可忽略
