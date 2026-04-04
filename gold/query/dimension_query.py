"""Dimension table queries."""
import streamlit as st
import pandas as pd
from gold.query.gold_data_provider import GoldDataProvider


class DimensionQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_tickers(sector: str = None) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            if sector:
                return gdp.execute("""
                    SELECT ticker, company_name, sector, industry, valid_from, valid_to, is_current
                    FROM dim_ticker
                    WHERE sector = ? AND is_current = True
                    ORDER BY ticker
                """, (sector,))
            return gdp.execute("""
                SELECT ticker, company_name, sector, industry, valid_from, valid_to, is_current
                FROM dim_ticker WHERE is_current = True ORDER BY ticker
            """)

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_date_range() -> tuple[str, str]:
        """Return (min_date, max_date) of all trading days in the database."""
        with GoldDataProvider() as gdp:
            df = gdp.execute("""
                SELECT MIN(date) AS min_date, MAX(date) AS max_date
                FROM dim_date
                WHERE is_trading_day = True
            """)
            return df.iloc[0]["min_date"], df.iloc[0]["max_date"]

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_trading_calendar(start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT date, year, quarter, month, day, day_of_week, is_trading_day, is_holiday, holiday_name
                FROM dim_date
                WHERE date BETWEEN ? AND ?
                ORDER BY date
            """, (start_date, end_date))
