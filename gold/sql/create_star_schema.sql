-- =============================================================================
-- Star Schema DDL — Phase 5 Gold Layer
-- Creates staging tables from Silver Parquet, then builds 5 star schema tables.
-- Run via: python gold/build_gold_layer.py
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Staging tables — loaded from Silver Parquet (dropped after star table creation)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_price_staging AS
SELECT * FROM read_parquet('output/silver/price/**/*.parquet', hive_partitioning=true);

CREATE TABLE IF NOT EXISTS silver_fundamentals_staging AS
SELECT ticker, report_type, period_date, period_date AS fiscal_date, metric, value, freq
FROM read_parquet('output/silver/fundamentals/**/*.parquet', hive_partitioning=true);

CREATE TABLE IF NOT EXISTS silver_sentiment_staging AS
SELECT * FROM read_parquet('output/silver/transcript_sentiment/**/*.parquet', hive_partitioning=true);

-- -----------------------------------------------------------------------------
-- dim_ticker: SCD Type 2 from Gold dim_ticker parquet (Task 2 output)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_ticker AS
SELECT * FROM read_parquet('output/gold/dim_ticker.parquet');

-- -----------------------------------------------------------------------------
-- dim_date: physical date dimension (references existing Gold parquet)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_date AS
SELECT * FROM read_parquet('output/gold/dim_date.parquet');

-- -----------------------------------------------------------------------------
-- fact_daily_price: daily OHLCV + precomputed returns
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_daily_price AS
WITH price_with_lags AS (
    SELECT
        ticker, date, open, high, low, close, adj_close, volume,
        LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
        LEAD(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS close_next_1d,
        LEAD(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_next_5d
    FROM silver_price_staging
),
price_with_returns AS (
    SELECT
        ticker, date, open, high, low, close, adj_close, volume, prev_close,
        CASE WHEN prev_close IS NOT NULL AND prev_close != 0
             THEN (close - prev_close) / prev_close ELSE NULL END AS daily_return,
        CASE WHEN close_next_1d IS NOT NULL AND close != 0
             THEN (close_next_1d - close) / NULLIF(close, 0) ELSE NULL END AS next_1d_return,
        CASE WHEN close_next_5d IS NOT NULL AND close != 0
             THEN (close_next_5d - close) / NULLIF(close, 0) ELSE NULL END AS next_5d_return
    FROM price_with_lags
)
SELECT * FROM price_with_returns;

-- -----------------------------------------------------------------------------
-- fact_quarterly_fundamentals: pivoted wide-form financials
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_quarterly_fundamentals AS
WITH pivoted AS (
    SELECT
        ticker, fiscal_date, report_type, freq,
        MAX(CASE WHEN metric = 'TotalRevenue' THEN CAST(value AS DOUBLE) END) AS revenue,
        MAX(CASE WHEN metric = 'NetIncome' THEN CAST(value AS DOUBLE) END) AS net_income,
        MAX(CASE WHEN metric = 'TotalAssets' THEN CAST(value AS DOUBLE) END) AS total_assets,
        MAX(CASE WHEN metric = 'TotalLiabilitiesNetMinorityInterest' THEN CAST(value AS DOUBLE) END) AS total_liabilities,
        MAX(CASE WHEN metric = 'BasicEPS' OR metric = 'DilutedEPS' THEN CAST(value AS DOUBLE) END) AS eps,
        MAX(CASE WHEN metric = 'BookValuePerShare' THEN CAST(value AS DOUBLE) END) AS book_value_per_share,
        period_date
    FROM silver_fundamentals_staging
    WHERE fiscal_date IS NOT NULL
    GROUP BY ticker, fiscal_date, report_type, freq, period_date
)
SELECT ticker, fiscal_date, MAX(period_date) AS period_date, MAX(report_type) AS report_type,
    MAX(freq) AS freq, MAX(revenue) AS revenue, MAX(net_income) AS net_income,
    MAX(total_assets) AS total_assets, MAX(total_liabilities) AS total_liabilities,
    MAX(eps) AS eps, MAX(book_value_per_share) AS book_value_per_share
FROM pivoted GROUP BY ticker, fiscal_date ORDER BY ticker, fiscal_date;

-- -----------------------------------------------------------------------------
-- fact_earnings_transcript: sentiment + price reaction
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_earnings_transcript AS
WITH sentiment_ranked AS (
    SELECT ticker, event_date, sentiment_polarity, sentiment_subjectivity,
        ROW_NUMBER() OVER (PARTITION BY ticker, event_date ORDER BY event_date DESC) AS rn
    FROM silver_sentiment_staging
),
price_for_return AS (
    SELECT ticker, date, close,
        LEAD(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS close_next_1d,
        LEAD(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_next_5d
    FROM silver_price_staging
)
SELECT s.ticker, s.event_date, s.sentiment_polarity, s.sentiment_subjectivity,
    p.close AS close_on_event,
    CASE WHEN p.close_next_1d IS NOT NULL AND p.close != 0
         THEN (p.close_next_1d - p.close) / NULLIF(p.close, 0) ELSE NULL END AS next_1d_return,
    CASE WHEN p.close_next_5d IS NOT NULL AND p.close != 0
         THEN (p.close_next_5d - p.close) / NULLIF(p.close, 0) ELSE NULL END AS next_5d_return
FROM sentiment_ranked s
LEFT JOIN price_for_return p ON s.ticker = p.ticker AND p.date = s.event_date
WHERE s.rn = 1 ORDER BY s.ticker, s.event_date;

-- -----------------------------------------------------------------------------
-- Cleanup: drop staging tables
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver_price_staging;
DROP TABLE IF EXISTS silver_fundamentals_staging;
DROP TABLE IF EXISTS silver_sentiment_staging;
