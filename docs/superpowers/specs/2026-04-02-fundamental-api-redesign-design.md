# Fundamental Data API Redesign — 2026-04-02

## Status
Draft — awaiting spec review

## 1. Problem Statement

The current fundamental data pipeline dumps ALL 5726 CSV files in one batch at backfill start
(`_emit_all_fundamentals()`). This does not simulate real financial API behavior (Bloomberg/Wind/Yahoo Finance).

**Correct behavior**: fundamentals should be queried per-ticker with a `knowledge_cutoff_date`,
returning only fiscal periods that would have been publicly available at that point in time.
The architecture should follow Star Schema.

---

## 2. Design Principle

- Bloomberg/Wind/Yahoo Finance API behavior: `get_fundamentals(ticker, freq, cutoff_date)`
  - Returns full historical record set for that ticker filtered by `fiscal_date <= cutoff_date`
  - No batching — on-demand per ticker
- Star Schema: `fact_fundamentals` as central fact table, dimension tables for ticker/period/metric
- `fiscal_date` is the knowledge cutoff key (as agreed: `fiscal_date` itself as cutoff, not `fiscal_date+60`)

---

## 3. Component Changes

### 3.1 Data Provider — `pipeline/data_provider.py`

**New signature:**
```python
def get_fundamentals(
    self,
    ticker: str,
    freq: str = "quarterly",
    cutoff_date: Optional[str] = None,  # NEW: knowledge cutoff (YYYY-MM-DD)
) -> dict:
```

**Behavior:**
- `cutoff_date=None` → return all fiscal periods (backward-compatible, used by tests)
- `cutoff_date='2020-06-30'` → filter out columns where `period_date > cutoff_date`

**Returns:**
```python
{
    "income":   DataFrame,   # rows=metrics, cols=fiscal periods, value=cells
    "balance":  DataFrame,
    "cashflow": DataFrame,
    "profile":  DataFrame,
}
```

### 3.2 Landing Zone — Structure Change

**Current:**
```
landing_zone/fundamentals/{fiscal_date}/{ticker}_{report_type}_{freq}.csv
```

**New:**
```
landing_zone/fundamentals/{ticker}/{report_type}_{freq}.csv
```
- One file per ticker per report_type per freq
- File contains ALL historical fiscal periods (unfiltered at this layer)
- Filtering by cutoff_date happens at query time in Data Provider

### 3.3 Simulator — `pipeline/simulators/comprehensive_simulator.py`

**Changes:**
1. **DELETE** `_emit_all_fundamentals()` — the batch dump pattern
2. **MODIFY** `run_backfill()` daily loop:
   - For each trading date `d`, for each ticker `t`:
     - Call `provider.get_fundamentals(t, freq='quarterly', cutoff_date=d)`
     - If non-empty, emit to `landing_zone/fundamentals/{t}/{report_type}_{freq}.csv`
3. **ADD** `_emit_fundamentals_for_ticker(ticker, date)` — emit one ticker's data for a given cutoff date

**Performance approach:**
- At backfill start: call `provider.get_fundamentals(ticker, freq, cutoff_date=None)` once per ticker (returns all history). Emit to `landing_zone/fundamentals/{ticker}/`. This is the single "seed" of all historical data.
- At each trading date `d`: check `_fundamental_index` for any new fiscal periods that become known on date `d`. Only emit files for tickers whose next fiscal period equals `d`. This is NOT a re-emit of all data — only incremental new data.
- Result: O(tickers × freqs × reports) seed emits at start, then O(new_fiscal_periods_per_day) incremental emits.
- This matches real Bloomberg/Wind behavior: all historical data is available immediately, new quarters appear when reported.

### 3.4 Ingestion Engine — `pipeline/ingestion_engine.py`

**Changes:**
- Watch `landing_zone/fundamentals/{ticker}/` directories (ticker-partitioned)
- For each new/modified CSV, read and insert into Bronze table
- Schema unchanged (raw_fundamental_index), but `fiscal_date` column in each file now covers all periods

### 3.5 Bronze Table — DuckDB Schema (Revised)

**Current:**
```sql
raw_fundamental_index(ticker, report_type, fiscal_date, file_path, received_at)
```

**Key insight:** `cutoff_date` is a **query-time parameter** to `DataProvider.get_fundamentals()`, NOT a column stored in Bronze or Silver. The Bronze table stores raw data pointers, not the filtered results. Filtering by `cutoff_date` happens only when the DataProvider reads landing zone files.

**Revised Bronze table:**
```sql
raw_fundamental_index(
    ticker         VARCHAR,
    report_type    VARCHAR,    -- income / balance / cashflow / profile
    freq           VARCHAR,    -- annual / quarterly  ← ADDED (needed to distinguish duplicate keys)
    fiscal_date    DATE,       -- first fiscal date in the source file (used for landing zone dir)
    file_path      VARCHAR,    -- pointer to landing zone file
    received_at    TIMESTAMP
)
```

**Migration strategy:**
1. Add `freq` column to existing `raw_fundamental_index` (non-breaking ALTER)
2. Landing zone moves to `landing_zone/fundamentals/{ticker}/` structure (file paths update)
3. Old date-partitioned files are deprecated but can coexist during transition
4. `cutoff_date` logic lives exclusively in `SPXDataProvider.get_fundamentals()` — never in Bronze/Silver

### 3.6 ELT Pipeline — `pipeline/elt_pipeline.py`

**Changes:**
- `transform_fundamentals()` reads from new ticker-partitioned landing zone via Bronze index
  - Bronze `raw_fundamental_index.file_path` now points to `landing_zone/fundamentals/{ticker}/...csv`
  - The ELT pipeline unchanged in structure: read index → read CSV → unpivot → write Silver
- Unpivot wide CSV → long format with `ticker`, `report_type`, `metric`, `period_date` (=fiscal_date), `value`, `freq`
- Write to Silver: `silver/fundamentals/ticker=XXX/data.parquet`
- `cutoff_date` is NOT in Bronze or Silver — it is a DataProvider query parameter only

### 3.7 Silver Parquet Structure

```
silver/fundamentals/
  ticker=AAPL/
    data.parquet    -- ticker, report_type, metric, period_date, value, freq
```

Note: `freq` is stored as a column in the Parquet, not as a directory partition. This matches the existing behavior.

### 3.8 Gold Views — `gold/sql/create_gold_views.sql`

**Changes to existing views:**

`v_fundamental_snapshot` — **no structural change**; it already shows the latest period per ticker. Callers apply `cutoff_date` at query time via `WHERE` clause on `period_date`.

**New Gold Views:**

`v_fundamental_history` — full time-series per ticker (unfiltered, for Bloomberg-style queries):
```sql
CREATE OR REPLACE VIEW v_fundamental_history AS
SELECT
    ticker,
    period_date AS fiscal_date,
    report_type,
    freq,
    metric,
    CAST(value AS DOUBLE) AS value
FROM silver_fundamentals
WHERE period_date IS NOT NULL
ORDER BY ticker, period_date, metric;
```

Usage — Bloomberg-style "fundamentals as of date X":
```sql
SELECT * FROM v_fundamental_history
WHERE ticker = 'AAPL' AND fiscal_date <= '2020-06-30'
PIVOT ...  -- or use app-layer pivot for Bloomberg-style table
```

### 3.9 Dashboard — `dashboard.py`

**Changes to "Fundamental Snapshot" page:**

Rename to **"Fundamental History"** and redesign:

| Control | Type | Description |
|---------|------|-------------|
| Ticker | Selectbox | Single ticker selection |
| Cutoff Date | Date input | Knowledge cutoff (YYYY-MM-DD) |
| Report Type | Multiselect | income / balance / cashflow |
| Metrics | Multiselect | TotalRevenue, NetIncome, etc. |

**Display:**
- Time-series table: rows=metrics, columns=fiscal periods, cells=values
- (类似 Bloomberg Financial Tables 的横向时间轴格式)
- 支持选择特定 fiscal quarter/year 范围

---

## 4. Data Flow (New)

```
DataProvider.get_fundamentals(ticker, freq, cutoff_date)
    │
    ├─► Read CSV from FUNDAMENTAL_DIR
    ├─► Filter columns: keep only period_date <= cutoff_date
    └─► Return dict of DataFrames

Simulator.run_backfill()
    │
    ├─► For each trading date:
    │       └─► For each ticker:
    │               ├─► provider.get_fundamentals(ticker, 'quarterly', date)
    │               └─► emit to landing_zone/fundamentals/{ticker}/
    │
    ▼
IngestionEngine (watchdog)
    │
    └─► Bronze table: raw_fundamental_index (with freq column added)
    │
    ▼
ELT Pipeline
    │
    └─► Silver Parquet: silver/fundamentals/ticker=XXX/
    │
    ▼
Gold Views
    │
    ├─► v_fundamental_history (full time-series)
    ├─► v_fundamental_snapshot (latest period)
    └─► v_ticker_profile (unchanged)
    │
    ▼
Dashboard — Fundamental History Page
    │
    └─► Query: SELECT * FROM v_fundamental_history
            WHERE ticker=? AND fiscal_date<=?
            PIVOT to Bloomberg-style table
```

---

## 5. Backward Compatibility

- `SPXDataProvider.get_fundamentals(ticker, freq)` with no `cutoff_date` behaves identically to before (returns all history)
- All existing Gold views unchanged except `v_fundamental_snapshot` (which already shows latest only)
- `v_fundamental_history` is additive — no existing view is broken

---

## 6. Implementation Order

1. `data_provider.py` — add `cutoff_date` filtering logic (backward-compatible, `cutoff_date=None` returns all)
2. `duckdb/init_bronze.py` — add `freq` column to `raw_fundamental_index` (non-breaking ALTER)
3. `ingestion_engine.py` — adapt to ticker-partitioned landing zone (file path changes only)
4. `comprehensive_simulator.py` — replace `_emit_all_fundamentals()` with:
   - `_seed_all_fundamentals()` — emit all historical data per ticker once at backfill start
   - `_emit_fundamentals(date)` — only emit if fiscal date matches current date (incremental)
5. `elt_pipeline.py` — adapt `transform_fundamentals()` to ticker-partitioned landing zone (minimal changes)
6. `gold/sql/create_gold_views.sql` — add `v_fundamental_history`, add `freq` to Silver table load
7. `gold/build_gold_layer.py` — verify Silver table load includes `freq`
8. `dashboard.py` — redesign "Fundamental Snapshot" page → "Fundamental History" with cutoff_date selector

---

## 7. Testing Strategy

- `test_data_provider.py`: verify `cutoff_date` filtering for all freq/ticker combinations
- `test_simulator.py`: verify no batch fundamental dump at backfill start
- `test_ingestion.py`: verify ticker-partitioned files ingested correctly
- `test_elt_pipeline.py`: verify Silver output schema
- `test_gold_views.py`: verify `v_fundamental_history` row counts and `v_fundamental_snapshot` unchanged
