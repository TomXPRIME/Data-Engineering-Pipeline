-- =============================================================================
-- OLAP Views — Phase 5 Gold Layer (7 lightweight views on Star Schema)
-- Executes after create_star_schema.sql and create_materialized.sql.
-- Run via: python gold/build_gold_layer.py
-- =============================================================================

-- ============================================================================
-- v_market_daily_summary: Daily market aggregates
-- ============================================================================
CREATE OR REPLACE VIEW v_market_daily_summary AS
SELECT
    date AS trade_date,
    COUNT(DISTINCT ticker) AS number_of_tickers,
    ROUND(AVG(close), 4) AS avg_close,
    ROUND(AVG(daily_return), 6) AS avg_return,
    SUM(volume) AS total_volume
FROM fact_daily_price
GROUP BY date
ORDER BY date;

-- ============================================================================
-- v_ticker_profile: Latest ticker snapshot
-- ============================================================================
CREATE OR REPLACE VIEW v_ticker_profile AS
WITH latest_price AS (
    SELECT ticker, date AS latest_trade_date, close AS latest_close, volume AS latest_volume,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
    FROM fact_daily_price
),
ticker_dims AS (
    SELECT ticker, MAX(company_name) AS company_name, MAX(sector) AS sector
    FROM dim_ticker WHERE is_current = True GROUP BY ticker
)
SELECT lp.ticker, td.company_name, td.sector, lp.latest_close, lp.latest_volume, lp.latest_trade_date
FROM latest_price lp
LEFT JOIN ticker_dims td ON lp.ticker = td.ticker
WHERE lp.rn = 1 ORDER BY lp.ticker;

-- ============================================================================
-- v_fundamental_snapshot: Latest financials per ticker (AS OF latest period_date)
-- ============================================================================
CREATE OR REPLACE VIEW v_fundamental_snapshot AS
WITH latest_fundamentals AS (
    SELECT ticker, fiscal_date, period_date, report_type, revenue, net_income, total_assets, total_liabilities,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_date DESC) AS rn
    FROM fact_quarterly_fundamentals
)
SELECT ticker, fiscal_date, MAX(period_date) AS latest_report_date,
    MAX(revenue) AS revenue, MAX(net_income) AS net_income,
    MAX(total_assets) AS assets, MAX(total_liabilities) AS liabilities
FROM latest_fundamentals WHERE rn = 1
GROUP BY ticker, fiscal_date ORDER BY ticker;

-- ============================================================================
-- v_fundamental_history: Bloomberg-style AS OF join (point-in-time financials)
-- ============================================================================
CREATE OR REPLACE VIEW v_fundamental_history AS
SELECT p.ticker, p.date AS price_date, f.fiscal_date, f.report_type, f.freq,
    f.revenue, f.net_income, f.total_assets, f.total_liabilities
FROM fact_daily_price p
LEFT JOIN (
    SELECT ticker, fiscal_date, report_type, freq, revenue, net_income, total_assets, total_liabilities, period_date
    FROM fact_quarterly_fundamentals
) f ON p.ticker = f.ticker
    AND CAST(f.period_date AS DATE) = (
        SELECT MAX(CAST(f2.period_date AS DATE)) FROM fact_quarterly_fundamentals f2
        WHERE f2.ticker = p.ticker AND CAST(f2.period_date AS DATE) <= p.date
    )
ORDER BY p.ticker, p.date, f.fiscal_date;

-- ============================================================================
-- v_sentiment_price_view: Sentiment + price reaction
-- ============================================================================
CREATE OR REPLACE VIEW v_sentiment_price_view AS
SELECT ticker, event_date AS transcript_date, sentiment_polarity AS sentiment_score,
    close_on_event, next_1d_return, next_5d_return
FROM fact_earnings_transcript
ORDER BY ticker, event_date;

-- ============================================================================
-- v_sentiment_binned_returns: Sentiment bucket vs forward returns
-- ============================================================================
CREATE OR REPLACE VIEW v_sentiment_binned_returns AS
SELECT
    CASE WHEN sentiment_polarity > 0.2 THEN 'POSITIVE'
         WHEN sentiment_polarity < -0.2 THEN 'NEGATIVE'
         ELSE 'NEUTRAL' END AS sentiment_bucket,
    COUNT(*) AS transcript_count,
    ROUND(AVG(next_1d_return), 6) AS avg_1d_return,
    ROUND(AVG(next_5d_return), 6) AS avg_5d_return,
    ROUND(STDDEV(next_1d_return), 6) AS std_1d_return,
    ROUND(AVG(sentiment_subjectivity), 4) AS avg_subjectivity
FROM fact_earnings_transcript
WHERE sentiment_polarity IS NOT NULL
GROUP BY sentiment_bucket;

-- ============================================================================
-- v_sector_rotation: Quarterly sector ranking by momentum
-- ============================================================================
CREATE OR REPLACE VIEW v_sector_rotation AS
WITH sector_daily AS (
    SELECT p.date, td.sector, EXTRACT(YEAR FROM p.date) AS year,
        EXTRACT(QUARTER FROM p.date) AS quarter,
        AVG(p.close) AS avg_close, SUM(p.volume) AS total_volume,
        STDDEV(p.close) AS price_std, COUNT(DISTINCT p.ticker) AS ticker_count
    FROM fact_daily_price p
    LEFT JOIN dim_ticker td ON p.ticker = td.ticker AND td.is_current = True
    GROUP BY p.date, td.sector, year, quarter
),
sector_quarterly AS (
    SELECT sector, year, quarter, AVG(avg_close) AS avg_close, SUM(total_volume) AS total_volume,
        AVG(price_std) AS avg_volatility, AVG(ticker_count) AS avg_ticker_count,
        LAG(AVG(avg_close)) OVER (PARTITION BY sector ORDER BY year, quarter) AS prev_avg_close
    FROM sector_daily GROUP BY sector, year, quarter
)
SELECT sector, year, quarter, avg_close, total_volume, avg_volatility, avg_ticker_count,
    CASE WHEN prev_avg_close IS NOT NULL AND prev_avg_close != 0
         THEN (avg_close - prev_avg_close) / prev_avg_close ELSE NULL END AS qoq_return,
    RANK() OVER (PARTITION BY year, quarter ORDER BY
        CASE WHEN prev_avg_close IS NOT NULL AND prev_avg_close != 0
        THEN (avg_close - prev_avg_close) / prev_avg_close ELSE NULL END DESC NULLS LAST) AS momentum_rank
FROM sector_quarterly
ORDER BY year, quarter, momentum_rank;
