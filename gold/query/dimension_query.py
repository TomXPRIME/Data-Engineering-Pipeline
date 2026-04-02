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
                return gdp.execute(f"""
                    SELECT ticker, company_name, sector, industry, valid_from, valid_to, is_current
                    FROM dim_ticker
                    WHERE sector = '{sector}' AND is_current = True
                    ORDER BY ticker
                """)
            return gdp.execute("""
                SELECT ticker, company_name, sector, industry, valid_from, valid_to, is_current
                FROM dim_ticker WHERE is_current = True ORDER BY ticker
            """)

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_trading_calendar(start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute(f"""
                SELECT date, year, quarter, month, day, day_of_week, is_trading_day, is_holiday, holiday_name
                FROM dim_date
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date
            """)