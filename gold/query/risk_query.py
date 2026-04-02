"""Risk and volatility queries."""
import streamlit as st
import pandas as pd
from gold.query.gold_data_provider import GoldDataProvider


class RiskQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_rolling_volatility(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute(f"""
                SELECT ticker, date, close, annualized_vol_20d, annualized_vol_60d, annualized_return_20d
                FROM fact_rolling_volatility
                WHERE ticker = '{ticker}'
                  AND date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date
            """)

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_ar1_results(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute(f"""
                SELECT ticker, date, close, daily_return, alpha_ar1, beta_ar1, r_squared_ar1, n_obs
                FROM fact_ar1_results
                WHERE ticker = '{ticker}'
                  AND date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date
            """)