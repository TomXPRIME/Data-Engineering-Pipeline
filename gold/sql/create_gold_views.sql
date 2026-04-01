-- ============================================================
-- Phase 5: Gold Layer (OLAP Views)
-- All Gold views created on top of Silver layer data in DuckDB
-- ============================================================

-- Load Silver Parquet data into DuckDB tables (if not already loaded)
-- Price
CREATE TABLE IF NOT EXISTS silver_price AS
SELECT * FROM read_parquet('output/silver/price/**/*.parquet', hive_partitioning=true);

-- Fundamentals
CREATE TABLE IF NOT EXISTS silver_fundamentals AS
SELECT * FROM read_parquet('output/silver/fundamentals/**/*.parquet', hive_partitioning=true);

-- Sentiment
CREATE TABLE IF NOT EXISTS silver_sentiment AS
SELECT * FROM read_parquet('output/silver/transcript_sentiment/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- Person A Views: Price & Profile
-- ============================================================

-- 1. Market Daily Summary — per-day market aggregate
CREATE OR REPLACE VIEW v_market_daily_summary AS
SELECT
    date AS trade_date,
    COUNT(DISTINCT ticker) AS number_of_tickers,
    ROUND(AVG(close), 4) AS avg_close,
    ROUND(AVG(
        CASE WHEN prev_close IS NOT NULL AND prev_close > 0
             THEN (close - prev_close) / prev_close
             ELSE NULL
        END
    ), 6) AS avg_return,
    SUM(volume) AS total_volume
FROM (
    SELECT
        ticker, date, close, volume,
        LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM silver_price
) sub
GROUP BY date
ORDER BY date;

-- 2. Ticker Profile — latest snapshot per ticker (with company_name & sector from fundamentals)
CREATE OR REPLACE VIEW v_ticker_profile AS
WITH latest_price AS (
    SELECT
        ticker,
        date AS latest_trade_date,
        close AS latest_close,
        volume AS latest_volume,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
    FROM silver_price
),
profile_data AS (
    SELECT
        ticker,
        COALESCE(
            MAX(CASE WHEN metric = 'longName' THEN value END),
            MAX(CASE WHEN metric = 'shortName' THEN value END),
            MAX(CASE WHEN metric ILIKE '%company%name%' THEN value END)
        ) AS company_name,
        MAX(CASE WHEN metric = 'sector' THEN value END) AS sector
    FROM silver_fundamentals
    WHERE report_type LIKE '%profile%'
    GROUP BY ticker
)
SELECT
    lp.ticker,
    pd.company_name,
    pd.sector,
    lp.latest_close,
    lp.latest_volume,
    lp.latest_trade_date
FROM latest_price lp
LEFT JOIN profile_data pd
    ON lp.ticker = pd.ticker
WHERE lp.rn = 1
ORDER BY lp.ticker;

-- ============================================================
-- Person B Views: Text & Financial Fusion
-- ============================================================

-- 3. Fundamental Snapshot — latest financials per ticker
CREATE OR REPLACE VIEW v_fundamental_snapshot AS
WITH latest_fundamentals AS (
    SELECT
        ticker,
        report_type,
        metric,
        period_date,
        value,
        ROW_NUMBER() OVER (PARTITION BY ticker, report_type, metric ORDER BY period_date DESC) AS rn
    FROM silver_fundamentals
    WHERE period_date IS NOT NULL
)
SELECT
    ticker,
    MAX(period_date) AS latest_report_date,
    MAX(CASE WHEN metric = 'TotalRevenue' AND report_type LIKE '%income%' THEN CAST(value AS DOUBLE) END) AS revenue,
    MAX(CASE WHEN metric = 'NetIncome' AND report_type LIKE '%income%' THEN CAST(value AS DOUBLE) END) AS net_income,
    MAX(CASE WHEN metric = 'TotalAssets' AND report_type LIKE '%balance%' THEN CAST(value AS DOUBLE) END) AS assets,
    MAX(CASE WHEN metric = 'TotalLiabilitiesNetMinorityInterest' AND report_type LIKE '%balance%' THEN CAST(value AS DOUBLE) END) AS liabilities
FROM latest_fundamentals
WHERE rn = 1
GROUP BY ticker
ORDER BY ticker;

-- 4. Sentiment-Price View — transcript sentiment + price reaction
-- Uses ASOF join logic: if transcript falls on non-trading day, match to nearest prior trading day
CREATE OR REPLACE VIEW v_sentiment_price_view AS
WITH price_with_future AS (
    SELECT
        ticker,
        date,
        close,
        LEAD(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS close_next_1d,
        LEAD(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_next_5d
    FROM silver_price
),
sentiment_with_date AS (
    SELECT
        ticker,
        event_date,
        sentiment_polarity,
        CAST(event_date AS DATE) AS event_dt
    FROM silver_sentiment
    WHERE sentiment_polarity IS NOT NULL
)
SELECT
    s.ticker,
    s.event_date AS transcript_date,
    s.sentiment_polarity AS sentiment_score,
    p.close AS close_on_event_date,
    ROUND((p.close_next_1d - p.close) / NULLIF(p.close, 0), 6) AS next_1d_return,
    ROUND((p.close_next_5d - p.close) / NULLIF(p.close, 0), 6) AS next_5d_return
FROM sentiment_with_date s
ASOF LEFT JOIN price_with_future p
    ON s.ticker = p.ticker
    AND p.date <= s.event_dt
ORDER BY s.ticker, s.event_date;

-- ============================================================
-- Person C Views: Sentiment Analytics
-- ============================================================

-- 8. v_sentiment_binned_returns — sentiment bucket vs forward returns
CREATE OR REPLACE VIEW v_sentiment_binned_returns AS
WITH sentiment_returns AS (
    SELECT
        s.ticker,
        s.event_date,
        s.sentiment_polarity,
        s.sentiment_subjectivity,
        p.next_1d_return,
        p.next_5d_return
    FROM silver_sentiment s
    JOIN v_sentiment_price_view p
        ON s.ticker = p.ticker AND s.event_date = p.transcript_date
    WHERE s.sentiment_polarity IS NOT NULL
)
SELECT
    CASE
        WHEN sentiment_polarity > 0.2 THEN 'POSITIVE'
        WHEN sentiment_polarity < -0.2 THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END AS sentiment_bucket,
    COUNT(*) AS transcript_count,
    ROUND(AVG(next_1d_return), 6) AS avg_1d_return,
    ROUND(AVG(next_5d_return), 6) AS avg_5d_return,
    ROUND(STDDEV(next_1d_return), 6) AS std_1d_return,
    ROUND(AVG(sentiment_subjectivity), 4) AS avg_subjectivity
FROM sentiment_returns
GROUP BY sentiment_bucket;

-- ============================================================
-- Person C Views: Sector Analysis
-- ============================================================

-- 7. v_sector_rotation — quarterly sector performance ranking
CREATE OR REPLACE VIEW v_sector_rotation AS
WITH sector_daily AS (
    SELECT
        MAX(f.value) AS sector,
        p.date,
        EXTRACT(YEAR FROM p.date) AS year,
        EXTRACT(QUARTER FROM p.date) AS quarter,
        AVG(p.close) AS avg_close,
        SUM(p.volume) AS total_volume,
        STDDEV(p.close) AS price_std,
        COUNT(DISTINCT p.ticker) AS ticker_count
    FROM silver_price p
    LEFT JOIN silver_fundamentals f
        ON p.ticker = f.ticker AND f.metric = 'sector'
    GROUP BY p.date, year, quarter
),
sector_quarterly AS (
    SELECT
        sector,
        year,
        quarter,
        AVG(avg_close) AS avg_close,
        SUM(total_volume) AS total_volume,
        AVG(price_std) AS avg_volatility,
        AVG(ticker_count) AS avg_ticker_count,
        RANK() OVER (
            PARTITION BY year, quarter
            ORDER BY (AVG(avg_close) - LAG(AVG(avg_close)) OVER (PARTITION BY sector ORDER BY year, quarter))
                     / NULLIF(LAG(AVG(avg_close)) OVER (PARTITION BY sector ORDER BY year, quarter), 0)
                     DESC
        ) AS momentum_rank
    FROM sector_daily
    GROUP BY sector, year, quarter
)
SELECT * FROM sector_quarterly
ORDER BY year, quarter, momentum_rank;

-- ============================================================
-- Person C Views: Risk & Performance
-- ============================================================

-- 5. v_rolling_volatility — 20-day and 60-day rolling volatility (annualized)
CREATE OR REPLACE VIEW v_rolling_volatility AS
WITH daily_returns AS (
    SELECT
        ticker, date, close,
        (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
    FROM silver_price
),
vol AS (
    SELECT
        ticker, date, close, daily_return,
        STDDEV(daily_return) OVER w20 * SQRT(252) AS annualized_vol_20d,
        STDDEV(daily_return) OVER w60 * SQRT(252) AS annualized_vol_60d,
        AVG(daily_return) OVER w20 * 252 AS annualized_return_20d
    FROM daily_returns
    WINDOW w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
           w60 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT ticker, date, close,
    ROUND(annualized_vol_20d, 6) AS annualized_vol_20d,
    ROUND(annualized_vol_60d, 6) AS annualized_vol_60d,
    ROUND(annualized_return_20d, 6) AS annualized_return_20d
FROM vol
WHERE annualized_vol_20d IS NOT NULL
ORDER BY ticker, date;

-- ============================================================
-- Person D Views: Momentum & Trend Analytics
-- ============================================================

-- 6. v_momentum_signals — multi-period momentum + MA deviation + trend signal
CREATE OR REPLACE VIEW v_momentum_signals AS
WITH price_analytics AS (
    SELECT
        ticker, date, close,
        LAG(close, 5) OVER w AS lag5,
        LAG(close, 20) OVER w AS lag20,
        LAG(close, 60) OVER w AS lag60,
        AVG(close) OVER w20 AS ma20,
        AVG(close) OVER w60 AS ma60
    FROM silver_price
    WINDOW w AS (PARTITION BY ticker ORDER BY date),
           w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
           w60 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    ticker, date, close,
    ROUND((close - lag5) / NULLIF(lag5, 0), 6) AS momentum_5d,
    ROUND((close - lag20) / NULLIF(lag20, 0), 6) AS momentum_20d,
    ROUND((close - lag60) / NULLIF(lag60, 0), 6) AS momentum_60d,
    ROUND((close - ma20) / NULLIF(ma20, 0), 6) AS dist_pct_from_ma20,
    ROUND((close - ma60) / NULLIF(ma60, 0), 6) AS dist_pct_from_ma60,
    CASE
        WHEN close > ma20 AND close > ma60 THEN 'STRONG_UPTREND'
        WHEN close < ma20 AND close < ma60 THEN 'STRONG_DOWNTREND'
        WHEN close > ma20 THEN 'WEAK_UPTREND'
        WHEN close < ma20 THEN 'WEAK_DOWNTREND'
        ELSE 'NEUTRAL'
    END AS trend_signal
FROM price_analytics
WHERE lag5 IS NOT NULL AND lag20 IS NOT NULL AND lag60 IS NOT NULL
ORDER BY ticker, date;

-- ============================================================
-- Person E Views: Time-Series & Autoregressive Models
-- ============================================================

-- 9. v_ar1_time_series — AR(1) autoregressive model via OLS window regression
-- r_t = alpha + beta * r_{t-1} + epsilon
-- beta ≈ 1: random walk (unit root); beta ≈ 0: uncorrelated returns
CREATE OR REPLACE VIEW v_ar1_time_series AS
WITH return_series AS (
    SELECT
        ticker, date, close,
        (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
    FROM silver_price
),
ar_input AS (
    SELECT
        ticker, date, close, daily_return,
        LAG(daily_return, 1) OVER (PARTITION BY ticker ORDER BY date) AS lag_return
    FROM return_series
),
ar_coeffs AS (
    SELECT
        ticker, date, close, daily_return, lag_return,
        COUNT(*) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS n_obs,
        SUM(lag_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS sum_lag,
        SUM(daily_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS sum_ret,
        SUM(lag_return * daily_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS sum_lag_ret,
        SUM(lag_return * lag_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS sum_lag_sq,
        SUM(daily_return * daily_return) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS sum_ret_sq
    FROM ar_input
    WHERE lag_return IS NOT NULL
),
ar_ols AS (
    SELECT
        ticker, date, close, daily_return, lag_return, n_obs,
        sum_lag, sum_ret, sum_lag_ret, sum_lag_sq,
        CASE
            WHEN n_obs >= 20 AND (n_obs * sum_lag_sq - sum_lag * sum_lag) != 0
            THEN (n_obs * sum_lag_ret - sum_lag * sum_ret) * 1.0 / (n_obs * sum_lag_sq - sum_lag * sum_lag)
            ELSE NULL
        END AS beta_ar1,
        CASE
            WHEN n_obs >= 20 AND (n_obs * sum_lag_sq - sum_lag * sum_lag) != 0
            THEN sum_ret * 1.0 / n_obs - ((n_obs * sum_lag_ret - sum_lag * sum_ret) * 1.0 / (n_obs * sum_lag_sq - sum_lag * sum_lag)) * (sum_lag * 1.0 / n_obs)
            ELSE NULL
        END AS alpha_ar1,
        CASE
            WHEN n_obs >= 20 AND (n_obs * sum_lag_sq - sum_lag * sum_lag) != 0
            THEN
                -- R-squared = 1 - SS_res / SS_tot
                -- SS_res = sum(y - alpha - beta*x)^2 = sum(y^2) - 2*alpha*sum(y) - 2*beta*sum(x*y) + alpha^2*n + 2*alpha*beta*sum(x) + beta^2*sum(x^2)
                -- SS_tot = sum((y - mean_y)^2) = sum(y^2) - sum(y)^2/n
                -- Simplified using the identity: beta = cov(x,y)/var(x) and R² = corr(x,y)^2
                -- For AR(1) with OLS: R² = (beta * cov(x,y) * n) / (n*var(y))
                -- We use the correlation approach: R² = (cov(x,y) / (stddev(x)*stddev(y)))^2
                POWER(
                    ((n_obs * sum_lag_ret - sum_lag * sum_ret) * 1.0 /
                     (n_obs * sum_lag_sq - sum_lag * sum_lag) *
                     (n_obs * sum_lag_sq - sum_lag * sum_lag)) * 1.0 /
                    NULLIF(n_obs * sum_ret_sq - sum_ret * sum_ret, 0),
                    0.5
                )
            ELSE NULL
        END AS r_squared_ar1
    FROM ar_coeffs
)
SELECT
    ticker, date, close, daily_return,
    ROUND(alpha_ar1, 8) AS alpha_ar1,
    ROUND(beta_ar1, 8) AS beta_ar1,
    ROUND(r_squared_ar1, 6) AS r_squared_ar1,
    n_obs
FROM ar_ols
WHERE beta_ar1 IS NOT NULL AND n_obs >= 20
ORDER BY ticker, date;
