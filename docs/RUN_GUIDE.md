# SPX 500 数据管道 - 完整运行指南

> 本文档记录在**不修改任何代码**的前提下，如何对整个项目进行完整一次性测试。
> 环境：`qf5214_project`（Conda），Python 3.10，使用 `C:/miniconda3/envs/qf5214_project/python.exe`
>
> **更新状态：** Pipeline 层 fundamental API redesign 已完成（2026-04-02），Dashboard（Phase 6）待重新实现。
>
> **English version available:** [`RUN_GUIDE_en.md`](./RUN_GUIDE_en.md)

---

## 一、数据规模总览

| 数据类型 | 源文件路径 | 规模 |
|----------|-----------|------|
| Price OHLCV | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers × 5284 交易日（2004-2024），约 432 万行 |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/*.csv` | 5726 个 CSV 文件（annual + quarterly） |
| Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/*.pdf` | 32,036 个 PDF 文件（2005-2025），1.2 GB |
| Tickers | `data/reference/tickers.csv` | 947 个 ticker |

---

## 二、完整运行步骤

### Step 0：环境准备

确认 Conda 环境 `qf5214_project` 已安装所有依赖：

```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -c "import pandas; import duckdb; import watchdog; import textblob; from pypdf import PdfReader; print('All dependencies OK')"
```

---

### Step 1：初始化 DuckDB Bronze 表

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py
```

预期输出：
```
Bronze tables created: ['ingestion_audit', 'raw_fundamental_index', 'raw_price_stream', 'raw_transcript_index']
```

---

### Step 2：运行 Simulator（生成 Landing Zone 数据）

使用 `-m` 方式运行（避免相对导入错误）：

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2004-01-02 --end 2024-12-30
```

**输出位置：**
- `output/landing_zone/prices/price_YYYY-MM-DD.csv`（5284 个文件 × 818 tickers）
- `output/landing_zone/fundamentals/{ticker}/`（ticker 分区，每个 ticker 下 4 个 report × 2 freq = 8 个 CSV）
- `output/landing_zone/transcripts/*.pdf`（32036 个 PDF）

> **注意（2026-04-02 redesign）：** Landing zone 的 fundamentals 目录结构已从 `fundamentals/YYYY-MM-DD/` 改为 `fundamentals/{ticker}/`。
> `freq` 列已添加到 `raw_fundamental_index` Bronze 表。

**预估耗时：** 20-90 分钟（主要时间消耗在 PDF 文件复制）

**断点续传：** Simulator 使用 `output/.watermark` 文件记录最后处理日期，中断后重新运行会自动从断点继续。

---

### Step 3：运行 Ingestion Engine（Bronze 层摄入）

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode scan
```

**预期结果：**
- ✅ Price: 约 432 万行成功摄入到 `raw_price_stream`
- ✅ Fundamentals: 5726 个文件索引摄入到 `raw_fundamental_index`（含 `freq` 字段）
- ✅ Transcripts: 32036 个 PDF 索引摄入到 `raw_transcript_index`

**验证 Bronze 层数据：**
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

预期输出（2024 年测试数据）：
```
raw_price_stream: 199592 行
raw_fundamental_index: 2860 行（约 595 tickers × ~4-8 文件）
raw_transcript_index: 1950 行
```

---

### Step 4：运行 ELT Pipeline（Bronze → Silver）

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline
```

**预估耗时：**
- Price: 1-3 分钟
- Fundamentals: <1 分钟
- Transcripts: 5-15 分钟（PDF 文本提取，取决于文件数量）
- Sentiment: 5-10 分钟（TextBlob 情感分析）

**验证 Silver 层数据：**
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
print('\nSilver Fundamentals (by ticker):', df2.head().to_string(index=False))

# Sentiment
df3 = con.execute('''
    SELECT COUNT(*) as total, COUNT(sentiment_polarity) as with_score
    FROM read_parquet(\"output/silver/transcript_sentiment/**/*.parquet\", hive_partitioning=true)
''').fetchdf()
print('\nSilver Sentiment:', df3.to_string(index=False))

con.close()
"
```

---

### Step 5：构建 Gold 层

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
```

**预期结果：** 10 个 Gold 视图创建成功

**查看 Gold 视图结果：**
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
    print(f'{v}: {count:,} 行')
con.close()
"
```

预期输出（2024 年测试）：
```
v_market_daily_summary: 251 行
v_ticker_profile: 818 行
v_fundamental_snapshot: 595 行
v_fundamental_history: 735,163 行
v_sentiment_price_view: 1,954 行
v_rolling_volatility: 147,003 行
v_momentum_signals: 112,705 行
v_sector_rotation: 4 行
v_sentiment_binned_returns: 2 行
v_ar1_time_series: 135,160 行
```

---

### Step 6：验证 Gold 视图

```bash
cd <repo_root>
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

预期输出：
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

## 三、Dashboard（Phase 6 — 待完成）

Dashboard 已从仓库中移除（`st.hist_chart` bug + 范围问题）。

**待实现：** Bloomberg 风格的 `Fundamental History` 页面，使用 `v_fundamental_history` 视图，支持 `cutoff_date` 参数过滤。

实现完成后恢复：
```bash
"C:/miniconda3/envs/qf5214_project/python.exe" -m streamlit run dashboard.py --server.headless true
```

---

## 四、数据流汇总（Pipeline 完成后）

```
data/ (原始 CSV/PDF)
    ↓ [Simulator - Step 2]
output/landing_zone/
    ├── prices/              5,284 个 CSV ✅
    ├── fundamentals/{ticker}/  ticker 分区 CSV ✅
    └── transcripts/          32,036 个 PDF ✅
    ↓ [Ingestion Engine - Step 3]
duckdb/spx_analytics.duckdb (Bronze)
    ├── raw_price_stream              4,322,232 行 ✅
    ├── raw_fundamental_index (含 freq 列)  5,726 行 ✅
    └── raw_transcript_index          32,036 行 ✅
    ↓ [ELT Pipeline - Step 4]
output/silver/
    ├── price/                    日期分区 Parquet ✅
    ├── fundamentals/            ticker 分区 Parquet（含 freq） ✅
    ├── transcript_text/          PDF 文本提取 ✅
    └── transcript_sentiment/     情感分析 Parquet ✅
    ↓ [Gold Layer - Step 5]
duckdb/spx_analytics.duckdb (Gold Views, 10 个)
    ├── v_market_daily_summary       ✅
    ├── v_ticker_profile            ✅
    ├── v_fundamental_snapshot      ✅
    ├── v_fundamental_history       ✅ (新增)
    ├── v_sentiment_price_view      ✅
    ├── v_rolling_volatility        ✅
    ├── v_momentum_signals         ✅
    ├── v_sector_rotation          ✅
    ├── v_sentiment_binned_returns  ✅
    └── v_ar1_time_series          ✅
```

---

## 五、运行时间估算（完整 20 年数据）

| 阶段 | 预估时间 | 说明 |
|------|----------|------|
| Step 2 Simulator | 20-90 分钟 | 5284 个价格日文件 + 5726 个 fundamental 文件 + 32036 个 PDF 复制 |
| Step 3 Ingestion | 5-15 分钟 | 432 万行 CSV + 5726 索引行 + 32036 索引行 |
| Step 4 ELT Price | 1-3 分钟 | DuckDB SQL 去重 + Parquet 导出 |
| Step 4 ELT Fundamentals | 1-2 分钟 | CSV unpivot + Parquet 导出 |
| Step 4 ELT Transcripts | 5-15 分钟 | PDF 文本提取（32036 个文件） |
| Step 4 ELT Sentiment | 5-10 分钟 | TextBlob 情感分析 |
| Step 5 Gold | 10-30 秒 | Parquet 读入 + 视图创建 |
| **总计** | **约 40 分钟 - 2.5 小时** | |

---

## 六、快速验证命令（测试用小样本）

```bash
# 1. 初始化
"C:/miniconda3/envs/qf5214_project/python.exe" duckdb/init_bronze.py

# 2. Simulator（244 交易日）
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.simulators.comprehensive_simulator --mode backfill --start 2024-01-02 --end 2024-12-30

# 3. Ingestion
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.ingestion_engine --mode scan

# 4. ELT（分步骤运行）
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource price
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource fundamentals
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource transcripts
"C:/miniconda3/envs/qf5214_project/python.exe" -m pipeline.elt_pipeline --resource sentiment

# 5. Gold Build & Test
"C:/miniconda3/envs/qf5214_project/python.exe" gold/build_gold_layer.py
"C:/miniconda3/envs/qf5214_project/python.exe" gold/tests/test_gold_views.py
```

---

## 七、DuckDB 直接查询示例

```sql
-- Bloomberg 风格：查询历史财务数据（cutoff_date 过滤）
-- 等价于 DataProvider.get_fundamentals(ticker, freq, cutoff_date)
SELECT ticker, fiscal_date, report_type, freq, metric, value
FROM v_fundamental_history
WHERE ticker = 'AAPL'
  AND fiscal_date <= '2020-12-31'   -- cutoff_date 知识截止
  AND freq = 'quarterly'
ORDER BY fiscal_date DESC, metric
LIMIT 50;

-- 市场日汇总
SELECT trade_date, number_of_tickers, avg_close, avg_return, total_volume
FROM v_market_daily_summary
WHERE trade_date BETWEEN '2020-01-01' AND '2024-12-31'
ORDER BY trade_date;

-- Silver 层价格数据（直接查 Parquet）
SELECT ticker, date, close, volume
FROM read_parquet('output/silver/price/*/*.parquet', hive_partitioning=true)
WHERE ticker = 'AAPL'
ORDER BY date
LIMIT 10;
```

---

## 八、Pipeline 层更新记录（2026-04-02）

以下为 fundamental API redesign 完成后的 pipeline 变更：

| 变更 | 说明 | 状态 |
|------|------|------|
| `SPXDataProvider.get_fundamentals(cutoff_date)` | 新增 `cutoff_date` 参数，支持 Bloomberg 风格历史数据过滤 | ✅ 已完成 |
| Landing Zone `fundamentals/{ticker}/` | 从 `YYYY-MM-DD/` 分区改为 `ticker/` 分区 | ✅ 已完成 |
| Bronze `raw_fundamental_index.freq` | 新增 `freq` 列（VARCHAR） | ✅ 已完成 |
| Simulator `_seed_all_fundamentals()` | 替代 `_emit_all_fundamentals()`，ticker 分区存储 | ✅ 已完成 |
| ELT `freq` 列 | Silver fundamentals parquet 包含 `freq` 字段 | ✅ 已完成 |
| `v_fundamental_history` | 新增 Gold 视图（735,163 行，支持 fiscal_date <= cutoff 过滤） | ✅ 已完成 |
| Dashboard Bloomberg 风格 | `render_fundamental_history` 页面（cutoff_date + ticker 选择） | ❌ 待实现 |

---

*文档更新日期：2026-04-02*
*Pipeline 版本：Phase 1-5 完成，Phase 6 Dashboard 待实现*
