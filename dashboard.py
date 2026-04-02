"""SPX Analytics Dashboard - 6-tab Streamlit application."""
import streamlit as st
import pandas as pd
from datetime import datetime

from gold.query import PriceQuery, FundamentalsQuery, SentimentQuery, RiskQuery, SectorQuery

st.set_page_config(page_title="SPX Analytics Dashboard", layout="wide")


def main():
    # Sidebar: global filters
    st.sidebar.title("SPX Analytics")
    ticker_input = st.sidebar.text_input("Ticker (e.g. AAPL)", value="AAPL")
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(datetime(2023, 1, 1), datetime(2024, 12, 31)),
        max_value=datetime(2024, 12, 31),
    )
    if len(date_range) == 2:
        start_date, end_date = date_range[0].strftime("%Y-%m-%d"), date_range[1].strftime("%Y-%m-%d")
    else:
        start_date, end_date = "2023-01-01", "2024-12-31"

    # Tab layout
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Market Overview",
        "Stock Analysis",
        "Fundamental History",
        "Sentiment Analytics",
        "Sector Rotation",
        "Risk & Performance",
    ])

    # Tab 1: Market Overview
    with tab1:
        st.subheader("Market Overview")
        try:
            df_summary = PriceQuery.get_daily_summary(start_date, end_date)
            if not df_summary.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    latest = df_summary.iloc[-1]
                    st.metric("Latest Avg Close", f"${latest['avg_close']:.2f}")
                with col2:
                    st.metric("Trading Days", len(df_summary))
                with col3:
                    if len(df_summary) > 1:
                        pct_change = ((df_summary.iloc[-1]['avg_close'] / df_summary.iloc[0]['avg_close']) - 1) * 100
                        st.metric("Period Change", f"{pct_change:.2f}%")

                st.line_chart(df_summary.set_index("trade_date")["avg_close"])
                st.subheader("Daily Summary Data")
                st.dataframe(df_summary, use_container_width=True)
            else:
                st.info("No market data available for the selected date range.")
        except Exception as e:
            st.error(f"Error loading market overview: {e}")

    # Tab 2: Stock Analysis
    with tab2:
        st.subheader(f"Stock Analysis: {ticker_input}")
        try:
            df_price = PriceQuery.get_ticker_price(ticker_input, start_date, end_date)
            if not df_price.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Latest Close", f"${df_price.iloc[-1]['close']:.2f}")
                with col2:
                    st.metric("Volume", f"{df_price.iloc[-1]['volume']:,.0f}")
                with col3:
                    ret = df_price.iloc[-1]['daily_return']
                    st.metric("Daily Return", f"{ret:.2%}" if pd.notna(ret) else "N/A")

                st.line_chart(df_price.set_index("date")["close"])
                st.subheader("OHLCV Data")
                display_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
                st.dataframe(df_price[display_cols], use_container_width=True)
            else:
                st.info(f"No price data available for {ticker_input} in the selected date range.")
        except Exception as e:
            st.error(f"Error loading stock analysis: {e}")

    # Tab 3: Fundamental History (Bloomberg-style)
    with tab3:
        st.subheader("Fundamental History (Bloomberg-style)")
        col1, col2 = st.columns([1, 3])
        with col1:
            cutoff_date = st.date_input(
                "As-of Date (cutoff_date)",
                value=datetime(2020, 6, 30),
                max_value=datetime(2024, 12, 31),
                key="cutoff_date",
            )
        with col2:
            ticker_for_history = st.text_input("Ticker", value=ticker_input, key="ticker_for_history")

        try:
            df = FundamentalsQuery.get_history(ticker_for_history, cutoff_date=str(cutoff_date))
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                st.caption(f"Showing {len(df)} rows of financials available as of {cutoff_date}")
            else:
                st.info(f"No fundamental history available for {ticker_for_history} as of {cutoff_date}.")
        except Exception as e:
            st.error(f"Error loading fundamental history: {e}")

    # Tab 4: Sentiment Analytics
    with tab4:
        st.subheader("Sentiment Analytics")
        try:
            df_sentiment = SentimentQuery.get_sentiment_view(ticker_input)
            if not df_sentiment.empty:
                st.subheader(f"Sentiment Scores for {ticker_input}")
                st.line_chart(df_sentiment.set_index("transcript_date")["sentiment_score"])
                st.dataframe(df_sentiment, use_container_width=True)
            else:
                st.info(f"No sentiment data available for {ticker_input}.")

            st.subheader("Sentiment vs Forward Returns")
            df_binned = SentimentQuery.get_binned_returns()
            if not df_binned.empty:
                st.bar_chart(df_binned.set_index("sentiment_bucket")["avg_forward_return"])
                st.dataframe(df_binned, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading sentiment analytics: {e}")

    # Tab 5: Sector Rotation
    with tab5:
        st.subheader("Sector Rotation")
        try:
            df_sector = SectorQuery.get_sector_rotation()
            if not df_sector.empty:
                # Filter to most recent quarter
                latest_data = df_sector.groupby(["year", "quarter"]).first().reset_index()
                st.subheader("Latest Quarter Sector Rankings")
                st.dataframe(latest_data, use_container_width=True)

                st.subheader("Momentum Ranking by Quarter")
                for year in sorted(df_sector["year"].unique(), reverse=True):
                    for quarter in sorted(df_sector["quarter"].unique(), reverse=True):
                        quarter_data = df_sector[
                            (df_sector["year"] == year) & (df_sector["quarter"] == quarter)
                        ]
                        if not quarter_data.empty:
                            st.write(f"**{year} Q{quarter}**")
                            st.dataframe(quarter_data, use_container_width=True)
            else:
                st.info("No sector rotation data available.")
        except Exception as e:
            st.error(f"Error loading sector rotation: {e}")

    # Tab 6: Risk & Performance
    with tab6:
        st.subheader(f"Risk & Performance: {ticker_input}")
        try:
            df_vol = RiskQuery.get_rolling_volatility(ticker_input, start_date, end_date)
            if not df_vol.empty:
                st.subheader("Rolling Volatility")
                vol_chart_data = df_vol.set_index("date")[["annualized_vol_20d", "annualized_vol_60d"]]
                st.line_chart(vol_chart_data)
                st.dataframe(df_vol, use_container_width=True)
            else:
                st.info(f"No volatility data available for {ticker_input}.")

            st.subheader("AR(1) Regression Results")
            try:
                df_ar1 = RiskQuery.get_ar1_results(ticker_input, start_date, end_date)
                if not df_ar1.empty:
                    ar1_chart_data = df_ar1.set_index("date")[["alpha_ar1", "beta_ar1"]]
                    st.line_chart(ar1_chart_data)
                    st.dataframe(df_ar1, use_container_width=True)
                else:
                    st.info(f"No AR(1) data available for {ticker_input}.")
            except Exception:
                st.info(f"No AR(1) data available for {ticker_input}.")
        except Exception as e:
            st.error(f"Error loading risk & performance: {e}")


if __name__ == "__main__":
    main()
