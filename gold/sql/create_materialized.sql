-- Materialized fact tables for analytics workloads
-- These CTAS tables pre-compute expensive window functions and regressions.

-- ============================================================================
-- fact_rolling_volatility: 20d/60d annualized volatility
-- ============================================================================
CREATE OR REPLACE TABLE fact_rolling_volatility AS
WITH daily_returns AS (
    SELECT
        ticker, date, close,
        (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
    FROM fact_daily_price
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

-- ============================================================================
-- fact_momentum_signals: multi-period momentum + MA deviation
-- ============================================================================
CREATE OR REPLACE TABLE fact_momentum_signals AS
WITH price_analytics AS (
    SELECT
        ticker, date, close,
        LAG(close, 5) OVER w AS lag5,
        LAG(close, 20) OVER w AS lag20,
        LAG(close, 60) OVER w AS lag60,
        AVG(close) OVER w20 AS ma20,
        AVG(close) OVER w60 AS ma60
    FROM fact_daily_price
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

-- ============================================================================
-- fact_ar1_results: AR(1) OLS regression per ticker window
-- ============================================================================
CREATE OR REPLACE TABLE fact_ar1_results AS
WITH return_series AS (
    SELECT
        ticker, date, close,
        (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
    FROM fact_daily_price
),
ar_input AS (
    SELECT
        ticker, date, close, daily_return,
        LAG(daily_return, 1) OVER (PARTITION BY ticker ORDER BY date) AS lag_return
    FROM return_series
    WHERE daily_return IS NOT NULL
),
ar_window AS (
    SELECT
        ticker, date, close, daily_return, lag_return,
        COUNT(*) OVER w AS n_obs,
        SUM(lag_return) OVER w AS sum_lag,
        SUM(daily_return) OVER w AS sum_ret,
        SUM(lag_return * daily_return) OVER w AS sum_lag_ret,
        SUM(lag_return * lag_return) OVER w AS sum_lag_sq,
        CORR(daily_return, lag_return) OVER w AS corr_ar1
    FROM ar_input
    WHERE lag_return IS NOT NULL
    WINDOW w AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
)
SELECT
    ticker, date, close, daily_return,
    CASE
        WHEN n_obs >= 20 AND (n_obs * sum_lag_sq - sum_lag * sum_lag) != 0
        THEN ROUND(sum_ret * 1.0 / n_obs - ((n_obs * sum_lag_ret - sum_lag * sum_ret) * 1.0
                  / (n_obs * sum_lag_sq - sum_lag * sum_lag)) * (sum_lag * 1.0 / n_obs), 8)
        ELSE NULL
    END AS alpha_ar1,
    CASE
        WHEN n_obs >= 20 AND (n_obs * sum_lag_sq - sum_lag * sum_lag) != 0
        THEN ROUND((n_obs * sum_lag_ret - sum_lag * sum_ret) * 1.0
                  / (n_obs * sum_lag_sq - sum_lag * sum_lag), 8)
        ELSE NULL
    END AS beta_ar1,
    CASE WHEN corr_ar1 IS NOT NULL THEN ROUND(corr_ar1 * corr_ar1, 6) ELSE NULL END AS r_squared_ar1,
    n_obs
FROM ar_window
WHERE n_obs >= 20
ORDER BY ticker, date;
