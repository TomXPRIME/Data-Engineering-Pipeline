"""Price and market data queries."""
import streamlit as st
import pandas as pd
from gold.query.gold_data_provider import GoldDataProvider


class PriceQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_daily_summary(start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT trade_date, number_of_tickers, avg_close,
                       avg_return, total_volume
                FROM v_market_daily_summary
                WHERE trade_date BETWEEN ? AND ?
                ORDER BY trade_date
            """, (start_date, end_date))

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_ticker_price(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT ticker, date, open, high, low, close, adj_close, volume,
                       daily_return, next_1d_return, next_5d_return
                FROM fact_daily_price
                WHERE ticker = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date
            """, (ticker, start_date, end_date))

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_trading_dates(start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT date
                FROM dim_date
                WHERE date BETWEEN ? AND ?
                  AND is_trading_day = True
                ORDER BY date
            """, (start_date, end_date))

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_market_overview(date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT trade_date, number_of_tickers, avg_close, avg_return, total_volume
                FROM v_market_daily_summary
                WHERE trade_date = ?
                ORDER BY trade_date
            """, (date,))
