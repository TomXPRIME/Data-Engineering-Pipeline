# Dashboard UX Redesign Implementation Plan

> **STATUS: ✅ COMPLETED (2026-04-02)** — All tasks implemented and verified.
> This document is kept for historical reference only.
>
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the Streamlit dashboard with searchable ticker dropdown, unified ticker state, professional column labels, and improved layout.

**Architecture:** Rewrite `dashboard.py` with `st.session_state.selected_ticker` as single source of truth, per-tab `st.selectbox` for ticker selection, a `COLUMN_MAPPING` dictionary for professional display names, and improved layout with dividers and containers.

**Tech Stack:** Python, Streamlit, pandas

---

### Task 1: Rewrite dashboard.py with all improvements

**Files:**
- Modify: `dashboard.py` (complete rewrite of `main()` function and additions)

This is the only file that changes. All modifications are in `dashboard.py`. The plan breaks into sequential steps within one commit because the change is to a single ~184-line file and all pieces are interdependent.

- [ ] **Step 1: Write the complete new `dashboard.py`**

Replace the entire content of `dashboard.py` with the following. Key changes:
1. `COLUMN_MAPPING` dict at module level for all column renaming
2. `rename_columns()` helper function
3. `get_available_tickers()` to fetch ticker list from `DimensionQuery`
4. Sidebar simplified: title + date range only, no ticker input
5. Per-tab ticker `st.selectbox` in Tabs 2, 3, 4, 6 with session state sync
6. All `st.dataframe()` calls use `rename_columns()`
7. All metric labels use professional names
8. `st.divider()` between sections
9. Proper heading hierarchy

```python
"""SPX Analytics Dashboard - 6-tab Streamlit application."""
import streamlit as st
import pandas as pd
from datetime import datetime

from gold.query import PriceQuery, FundamentalsQuery, SentimentQuery, RiskQuery, SectorQuery, DimensionQuery

st.set_page_config(page_title="SPX Analytics Dashboard", layout="wide")

# ── Column Name Mapping ──────────────────────────────────────────────
COLUMN_MAPPING = {
    # Market Overview
    "trade_date": "Trading Date",
    "number_of_tickers": "Number of Tickers",
    "avg_close": "Average Close Price",
    "avg_return": "Average Return",
    "total_volume": "Total Volume",
    # Stock Analysis
    "date": "Date",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "adj_close": "Adjusted Close",
    "volume": "Volume",
    "daily_return": "Daily Return",
    "next_1d_return": "Next 1-Day Return",
    "next_5d_return": "Next 5-Day Return",
    # Fundamental History
    "ticker": "Ticker",
    "price_date": "Report Date",
    "fiscal_date": "Fiscal Period End",
    "report_type": "Report Type",
    "freq": "Frequency",
    "revenue": "Revenue",
    "net_income": "Net Income",
    "total_assets": "Total Assets",
    "total_liabilities": "Total Liabilities",
    # Sentiment Analytics
    "transcript_date": "Transcript Date",
    "sentiment_score": "Sentiment Score",
    "close_on_event": "Close Price on Event Date",
    "sentiment_bucket": "Sentiment Category",
    "avg_1d_return": "Average 1-Day Forward Return",
    # Sector Rotation
    "sector": "Sector",
    "year": "Year",
    "quarter": "Quarter",
    "avg_volatility": "Average Volatility",
    "avg_ticker_count": "Number of Constituents",
    "qoq_return": "Quarter-over-Quarter Return",
    "momentum_rank": "Momentum Rank",
    # Risk & Performance
    "annualized_vol_20d": "20-Day Annualized Volatility",
    "annualized_vol_60d": "60-Day Annualized Volatility",
    "annualized_return_20d": "20-Day Annualized Return",
    "alpha_ar1": "AR(1) Alpha",
    "beta_ar1": "AR(1) Beta",
    "r_squared_ar1": "AR(1) R-Squared",
    "n_obs": "Observations",
}

# Metric label mapping
METRIC_LABELS = {
    "Latest Avg Close": "Average Close Price",
    "Trading Days": "Trading Days",
    "Period Change": "Period Return",
    "Latest Close": "Close Price",
    "Volume": "Trading Volume",
    "Daily Return": "Daily Return",
}


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename DataFrame columns using COLUMN_MAPPING."""
    return df.rename(columns={col: COLUMN_MAPPING.get(col, col) for col in df.columns})


@st.cache_data(ttl=3600)
def get_available_tickers():
    """Fetch sorted list of available tickers from the database."""
    tickers_df = DimensionQuery.get_tickers()
    if tickers_df.empty:
        return ["AAPL"]
    return sorted(tickers_df["ticker"].tolist())


def main():
    # ── Sidebar: Global Filters ──────────────────────────────────────
    st.sidebar.title("SPX Analytics")
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(datetime(2023, 1, 1), datetime(2024, 12, 31)),
        max_value=datetime(2024, 12, 31),
    )
    if len(date_range) == 2:
        start_date, end_date = date_range[0].strftime("%Y-%m-%d"), date_range[1].strftime("%Y-%m-%d")
    else:
        start_date, end_date = "2023-01-01", "2024-12-31"

    # ── Global Ticker State ─────────────────────────────────────────
    available_tickers = get_available_tickers()
    if "selected_ticker" not in st.session_state:
        st.session_state.selected_ticker = "AAPL" if "AAPL" in available_tickers else available_tickers[0]

    # ── Tab Layout ──────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Market Overview",
        "Stock Analysis",
        "Fundamental History",
        "Sentiment Analytics",
        "Sector Rotation",
        "Risk & Performance",
    ])

    # ── Tab 1: Market Overview ──────────────────────────────────────
    with tab1:
        st.header("Market Overview")
        try:
            df_summary = PriceQuery.get_daily_summary(start_date, end_date)
            if not df_summary.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    latest = df_summary.iloc[-1]
                    st.metric(METRIC_LABELS["Latest Avg Close"], f"${latest['avg_close']:.2f}")
                with col2:
                    st.metric(METRIC_LABELS["Trading Days"], f"{len(df_summary):,}")
                with col3:
                    if len(df_summary) > 1:
                        pct_change = ((df_summary.iloc[-1]["avg_close"] / df_summary.iloc[0]["avg_close"]) - 1) * 100
                        st.metric(METRIC_LABELS["Period Change"], f"{pct_change:.2f}%")

                st.line_chart(df_summary.set_index("trade_date")["avg_close"])
                st.divider()
                st.subheader("Daily Summary Data")
                st.dataframe(rename_columns(df_summary), use_container_width=True)
            else:
                st.info("No market data available for the selected date range.")
        except Exception as e:
            st.error(f"Error loading market overview: {e}")

    # ── Tab 2: Stock Analysis ───────────────────────────────────────
    with tab2:
        st.header("Stock Analysis")
        selected_ticker = st.selectbox(
            "Select Ticker",
            options=available_tickers,
            index=available_tickers.index(st.session_state.selected_ticker) if st.session_state.selected_ticker in available_tickers else 0,
            key="stock_analysis_ticker",
        )
        st.session_state.selected_ticker = selected_ticker
        st.divider()
        st.subheader(f"{selected_ticker}")
        try:
            df_price = PriceQuery.get_ticker_price(selected_ticker, start_date, end_date)
            if not df_price.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(METRIC_LABELS["Latest Close"], f"${df_price.iloc[-1]['close']:.2f}")
                with col2:
                    st.metric(METRIC_LABELS["Volume"], f"{df_price.iloc[-1]['volume']:,.0f}")
                with col3:
                    ret = df_price.iloc[-1]["daily_return"]
                    st.metric(METRIC_LABELS["Daily Return"], f"{ret:.2%}" if pd.notna(ret) else "N/A")

                st.line_chart(df_price.set_index("date")["close"])
                st.divider()
                st.subheader("Price and Volume Data")
                display_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
                st.dataframe(rename_columns(df_price[display_cols]), use_container_width=True)
            else:
                st.info(f"No price data available for {selected_ticker} in the selected date range.")
        except Exception as e:
            st.error(f"Error loading stock analysis: {e}")

    # ── Tab 3: Fundamental History ──────────────────────────────────
    with tab3:
        st.header("Fundamental History")
        col1, col2 = st.columns([1, 3])
        with col1:
            cutoff_date = st.date_input(
                "Report As-of Date",
                value=datetime(2020, 6, 30),
                max_value=datetime(2024, 12, 31),
                key="cutoff_date",
            )
        with col2:
            ticker_select = st.selectbox(
                "Select Ticker",
                options=available_tickers,
                index=available_tickers.index(st.session_state.selected_ticker) if st.session_state.selected_ticker in available_tickers else 0,
                key="fundamental_ticker",
            )
            st.session_state.selected_ticker = ticker_select
        try:
            df = FundamentalsQuery.get_history(ticker_select, cutoff_date=str(cutoff_date))
            if not df.empty:
                st.dataframe(rename_columns(df), use_container_width=True)
                st.caption(f"Showing {len(df)} rows of financial data as of {cutoff_date.strftime('%Y-%m-%d')}")
            else:
                st.info(f"No fundamental history available for {ticker_select} as of {cutoff_date.strftime('%Y-%m-%d')}.")
        except Exception as e:
            st.error(f"Error loading fundamental history: {e}")

    # ── Tab 4: Sentiment Analytics ──────────────────────────────────
    with tab4:
        st.header("Sentiment Analytics")
        selected_ticker = st.selectbox(
            "Select Ticker",
            options=available_tickers,
            index=available_tickers.index(st.session_state.selected_ticker) if st.session_state.selected_ticker in available_tickers else 0,
            key="sentiment_ticker",
        )
        st.session_state.selected_ticker = selected_ticker
        st.divider()
        try:
            df_sentiment = SentimentQuery.get_sentiment_view(selected_ticker)
            if not df_sentiment.empty:
                st.subheader(f"Sentiment Scores for {selected_ticker}")
                st.line_chart(df_sentiment.set_index("transcript_date")["sentiment_score"])
                st.dataframe(rename_columns(df_sentiment), use_container_width=True)
            else:
                st.info(f"No sentiment data available for {selected_ticker}.")

            st.divider()
            st.subheader("Sentiment Category vs Forward Returns")
            df_binned = SentimentQuery.get_binned_returns()
            if not df_binned.empty:
                st.bar_chart(df_binned.set_index("sentiment_bucket")["avg_1d_return"])
                st.dataframe(rename_columns(df_binned), use_container_width=True)
        except Exception as e:
            st.error(f"Error loading sentiment analytics: {e}")

    # ── Tab 5: Sector Rotation ──────────────────────────────────────
    with tab5:
        st.header("Sector Rotation")
        try:
            df_sector = SectorQuery.get_sector_rotation()
            if not df_sector.empty:
                latest_yq = df_sector[["year", "quarter"]].drop_duplicates().sort_values(["year", "quarter"]).iloc[-1]
                latest_df = df_sector[(df_sector["year"] == latest_yq["year"]) & (df_sector["quarter"] == latest_yq["quarter"])]
                st.subheader("Latest Quarter Sector Rankings")
                st.dataframe(rename_columns(latest_df.sort_values("momentum_rank")), use_container_width=True)

                st.divider()
                st.subheader("Momentum Ranking by Quarter")
                for year in sorted(df_sector["year"].unique(), reverse=True):
                    for quarter in sorted(df_sector["quarter"].unique(), reverse=True):
                        quarter_data = df_sector[
                            (df_sector["year"] == year) & (df_sector["quarter"] == quarter)
                        ]
                        if not quarter_data.empty:
                            st.write(f"**{year} Q{quarter}**")
                            st.dataframe(rename_columns(quarter_data), use_container_width=True)
            else:
                st.info("No sector rotation data available.")
        except Exception as e:
            st.error(f"Error loading sector rotation: {e}")

    # ── Tab 6: Risk & Performance ───────────────────────────────────
    with tab6:
        st.header("Risk & Performance")
        selected_ticker = st.selectbox(
            "Select Ticker",
            options=available_tickers,
            index=available_tickers.index(st.session_state.selected_ticker) if st.session_state.selected_ticker in available_tickers else 0,
            key="risk_ticker",
        )
        st.session_state.selected_ticker = selected_ticker
        st.divider()
        try:
            df_vol = RiskQuery.get_rolling_volatility(selected_ticker, start_date, end_date)
            if not df_vol.empty:
                st.subheader("Rolling Volatility")
                vol_chart_data = df_vol.set_index("date")[["annualized_vol_20d", "annualized_vol_60d"]]
                st.line_chart(vol_chart_data)
                st.dataframe(rename_columns(df_vol), use_container_width=True)
            else:
                st.info(f"No volatility data available for {selected_ticker}.")

            st.divider()
            st.subheader("AR(1) Regression Results")
            try:
                df_ar1 = RiskQuery.get_ar1_results(selected_ticker, start_date, end_date)
                if not df_ar1.empty:
                    ar1_chart_data = df_ar1.set_index("date")[["alpha_ar1", "beta_ar1"]]
                    st.line_chart(ar1_chart_data)
                    st.dataframe(rename_columns(df_ar1), use_container_width=True)
                else:
                    st.info(f"No AR(1) data available for {selected_ticker}.")
            except Exception:
                st.info(f"No AR(1) data available for {selected_ticker}.")
        except Exception as e:
            st.error(f"Error loading risk and performance: {e}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the dashboard launches without errors**

Run:
```bash
cd D:/NUS_MQF/QF5214/5214_Project_SPX_Index_Raw_Data/Data-Engineering-Pipeline
C:/miniconda3/envs/qf5214_project/python.exe -c "import dashboard; print('Import OK')"
```
Expected: `Import OK` with no errors.

- [ ] **Step 3: Run existing dashboard tests**

Run:
```bash
cd D:/NUS_MQF/QF5214/5214_Project_SPX_Index_Raw_Data/Data-Engineering-Pipeline
C:/miniconda3/envs/qf5214_project/python.exe -m pytest tests/test_dashboard_fundamental_history.py -v
```
Expected: Tests pass (verifies the underlying query layer is unchanged).

- [ ] **Step 4: Commit**

```bash
git add dashboard.py
git commit -m "feat: redesign dashboard UX with searchable ticker dropdown, professional column labels, and unified ticker state"
```

---

## Self-Review

**1. Spec coverage:**
- Ticker display bug (unified ticker) → `st.session_state.selected_ticker` + per-tab selectbox ✓
- Searchable dropdown → `st.selectbox` with all tickers from `DimensionQuery.get_tickers()` ✓
- Column name mapping → `COLUMN_MAPPING` dict + `rename_columns()` helper ✓
- Professional financial English → all labels reviewed ✓
- No underscores in display names → verified ✓
- Layout improvements → `st.divider()`, `st.header()`, metric label updates ✓
- Sidebar simplified → only title + date range ✓

**2. Placeholder scan:** No TBD/TODO, no "add validation later", all code shown in full ✓

**3. Type consistency:** `COLUMN_MAPPING` keys match all column names returned by query methods in PriceQuery, FundamentalsQuery, SentimentQuery, RiskQuery, SectorQuery ✓
