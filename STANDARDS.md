# QF5214 Project Implementation Standards

This document defines development, testing, and deployment standards for the data engineering pipeline.

---

## 1. Environment Standards

### 1.1 Conda Environments

| Environment | Purpose |
|-------------|---------|
| `qf5214_project` | Main development environment |
| `data_analysis` | Temporary verification environment (has pandas, yfinance) |

**Activation:**
```bash
conda activate qf5214_project
```

### 1.2 Dependency Management

When adding new dependencies, sync documentation:

```bash
# Install new package
conda install -n qf5214_project <package>

# Export environment
conda env export -n qf5214_project > environment.yml
```

---

## 2. Code Standards

### 2.1 Python Code Style

- Follow **PEP 8** style guide
- Use `black` to format code
- Use `ruff` for linting

```bash
# Format
black .

# Check
ruff check .
```

### 2.2 Module Naming

| Module Type | Path | Example |
|------------|------|---------|
| Data access layer | `pipeline/data_provider.py` | `SPXDataProvider` |
| Ingestion engine | `pipeline/ingestion_engine.py` | `IngestionEngine` |
| ELT pipeline | `pipeline/elt_pipeline.py` | `ELTPipeline` |
| Simulator | `pipeline/simulators/*.py` | `ComprehensiveSimulator` |
| Utilities | `pipeline/utils.py` | `calculate_returns()` |
| Config | `pipeline/config.py` | `DATA_DIR`, `DB_PATH` |
| Dashboard | `dashboard.py` (root) | Streamlit 6-tab app |

### 2.3 Function Naming

| Operation | Naming | Example |
|----------|--------|---------|
| Fetch data | `get_<resource>` | `get_price()`, `get_fundamentals()` |
| Batch write | `ingest_<resource>` | `ingest_price_batch()` |
| Query | `query_<view>` | `query_earnings_surprise()` |
| Transform | `transform_<resource>` | `unpivot_financials()` |

### 2.4 Data Types

- **Date format**:统一使用 `YYYY-MM-DD` strings
- **Timestamp**:统一使用 `YYYY-MM-DD HH:MM:SS`
- **Currency**:统一使用 `DECIMAL(18, 6)`
- **Volume**:统一使用 `BIGINT`

---

## 3. Data Standards

### 3.1 Landing Zone File Naming

| Data Type | Format | Example |
|-----------|--------|---------|
| Price | `landing_zone/prices/price_YYYY-MM-DD.csv` | `price_2024-01-15.csv` |
| Fundamental | `landing_zone/fundamentals/{ticker}/{report_type}_{freq}.csv` | `fundamentals/AAPL/income_quarterly.csv` |
| Transcript PDF | `landing_zone/transcripts/TICKER_YYYY-MM-DD.pdf` | `transcripts/AAPL_2024-02-01.pdf` |
| Silver Parquet | `silver/price/date=YYYY-MM-DD/data.parquet` | `silver/price/date=2024-01-15/data.parquet` |
| Silver Fundamentals | `silver/fundamentals/ticker=XXX/data.parquet` | `silver/fundamentals/ticker=AAPL/data.parquet` |

### 3.2 DuckDB Table Naming

| Layer | Table Prefix | Example |
|-------|-------------|---------|
| Bronze | `raw_` | `raw_price_stream` |
| Silver | `silver_` | `silver_price` |
| Gold Dim/Fact | `dim_` / `fact_` | `dim_ticker`, `fact_daily_price` |
| Gold Views | `v_` | `v_market_daily_summary` |

### 3.3 Gold Layer (Star Schema)

Medallion + Star Schema 融合设计。见 `docs/superpowers/specs/2026-04-02-medallion-star-schema-design.md`。

### 3.4 Schema Changes

All table schema changes must:
1. Create migration script (`migrations/001_add_xxx.sql`)
2. Register in migration record table
3. Avoid direct ALTER on production tables

---

## 4. API Design Standards

### 4.1 DataProvider Interface Contract

```python
class SPXDataProvider:
    """All data access must go through this class"""

    def get_price(self, ticker: str, date: str) -> pd.DataFrame:
        """
        Args:
            ticker: Stock symbol (e.g., 'AAPL')
            date: Date in 'YYYY-MM-DD'
        Returns:
            DataFrame with columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
            Returns DataFrame if data exists, empty DataFrame if not
        Raises:
            ValueError: if ticker not found
        """
        ...

    def get_fundamentals(self, ticker: str, freq: str = "quarterly") -> dict:
        """
        Returns:
            dict with keys: 'income', 'balance', 'cashflow', 'profile'
            Returns dict if data exists, empty dict if not
        """
        ...

    def get_transcript(self, ticker: str, date: str) -> bytes:
        """Returns raw PDF bytes. Raises FileNotFoundError if not found"""
        ...

    def list_transcripts(self, ticker: str = None, year: int = None) -> list:
        """
        Filter transcripts by ticker and/or year.
        Returns: [{'ticker': 'AAPL', 'date': '2024-02-01'}, ...]
        """
        ...

    def get_trading_dates(self, start_date: str, end_date: str) -> list:
        """Returns all trading dates in the specified range"""
        ...

    def get_ticker_list(self) -> list:
        """Returns all available tickers"""
        ...
```

### 4.2 Error Handling

| Error Type | Handling |
|------------|---------|
| Data not found | Raise `FileNotFoundError` or return empty DataFrame |
| Data corrupted | Raise `ValueError` and log |
| Format mismatch | Raise `DataIntegrityError` |

---

## 5. Testing Standards

### 5.1 Unit Tests

```python
# tests/test_data_provider.py
import pytest
from pipeline.data_provider import SPXDataProvider

def test_get_price_returns_correct_columns():
    provider = SPXDataProvider()
    df = provider.get_price('AAPL', '2024-01-15')
    assert list(df.columns) == ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    assert len(df) > 0

def test_get_price_invalid_ticker_raises():
    provider = SPXDataProvider()
    with pytest.raises(ValueError):
        provider.get_price('INVALID_TICKER', '2024-01-15')

def test_get_price_no_data_returns_empty():
    provider = SPXDataProvider()
    df = provider.get_price('AAPL', '2024-01-16')  # Weekend, no data
    assert len(df) == 0
```

### 5.2 Test Data

- Use `tests/fixtures/` for small test datasets
- Do not use full 20-year data in tests (performance issues)
- Each interface must have at least one positive test and one exception test

---

## 6. Security Standards

### 6.1 Prohibited

- **Never** commit API keys or tokens to the repository
- **Never** print sensitive data in logs (passwords, tokens)
- **Never** download data from non-HTTPS sources

### 6.2 Sensitive Information

For API key configuration, use environment variables:

```python
import os
API_KEY = os.environ.get('YFINANCE_API_KEY', '')  # No hardcoding
```

---

## 7. Logging Standards

### 7.1 Log Levels

| Level | Use | Example |
|-------|-----|---------|
| DEBUG | Development debugging | `logger.debug(f"Loaded {len(df)} rows")` |
| INFO | Normal flow | `logger.info("Ingestion completed")` |
| WARNING | Abnormal but handleable | `logger.warning(f"Missing data for {ticker}")` |
| ERROR | Error needs investigation | `logger.error(f"Failed to connect to DB")` |

### 7.2 Log Format

```
%(asctime)s - %(name)s - %(levelname)s - %(message)s
2024-01-15 10:30:00 - ingestion_engine - INFO - Ingested 500 price records
```

---

## 8. Git Commit Standards

### 8.1 Commit Message Format

```
<type>: <short summary>

<body (optional)>

<footer (optional)>
```

Type types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation update
- `refactor`: Refactoring (no functional change)
- `test`: Test-related
- `chore`: Build/tool change

### 8.2 Example

```
feat: add SPXDataProvider.get_transcript method

- Returns raw PDF bytes for given ticker and date
- Raises FileNotFoundError if PDF not found
- Added unit tests for valid and invalid inputs

Closes #12
```

---

## 9. Performance Standards

### 9.1 Data Reading

- **Never** read large files in loops (use vectorized operations)
- **Never** scan all file headers every day (use pre-built indexes)
- When price data exceeds 100MB, use chunked reading

### 9.2 Database Writing

- Batch write > single write (use `executemany` or DataFrame `to_sql(if_exists='append')`)
- Create indexes before importing large data
- Enable WAL mode for concurrent write performance

```python
con.execute("PRAGMA synchronous=NORMAL")  # Improve write performance
con.execute("PRAGMA journal_mode=WAL")    # Concurrent read/write
```

---

## 10. Deployment Standards

### 10.1 Development Flow

```
1. Create feature branch from main
2. Develop + unit tests
3. Commit PR + Code Review
4. Merge to main
5. Deploy to production (if applicable)
```

### 10.2 Pre-launch Checklist

- [ ] All unit tests pass
- [ ] New data interfaces are documented
- [ ] Environment changes updated in `environment.yml`
- [ ] No hardcoded sensitive information
- [ ] Log levels set correctly (INFO for production)

---

## 11. Documentation Maintenance

### 11.1 Required Documentation

| Document | Update Timing |
|---------|-------------|
| `CLAUDE.md` | New components, architecture changes |
| `README.md` | Project overview, quick start |
| `docs/RUN_GUIDE.md` | Detailed run guide |
| `STANDARDS.md` | Standards changes |
| Code docstrings | Function signature changes |

### 11.2 Docstring Format

```python
def get_price(self, ticker: str, date: str) -> pd.DataFrame:
    """
    Get OHLCV price data for a ticker on a specific date.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')
        date: Date in 'YYYY-MM-DD' format

    Returns:
        DataFrame with columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
        Returns empty DataFrame if no data for that date (e.g., weekend/holiday)

    Raises:
        ValueError: If ticker is not found

    Example:
        >>> provider = SPXDataProvider()
        >>> df = provider.get_price('AAPL', '2024-01-15')
        >>> print(df.head())
    """
    ...
```

---

Technical design documents: see `docs/superpowers/specs/` for completed design specs (fundamental API redesign, dashboard UX redesign, medallion star schema).
