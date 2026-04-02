"""Fundamental data queries with AS OF join support."""
import streamlit as st
import pandas as pd
from gold.query.gold_data_provider import GoldDataProvider


class FundamentalsQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_snapshot(ticker: str) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT ticker, fiscal_date, latest_report_date,
                       revenue, net_income, assets, liabilities
                FROM v_fundamental_snapshot
                WHERE ticker = ?
                ORDER BY fiscal_date DESC
                LIMIT 10
            """, (ticker,))

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_history(ticker: str, cutoff_date: str) -> pd.DataFrame:
        """Bloomberg-style: only financials published on or before cutoff_date."""
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT ticker, price_date, fiscal_date, report_type, freq,
                       revenue, net_income, total_assets, total_liabilities
                FROM v_fundamental_history
                WHERE ticker = ?
                  AND price_date <= ?
                  AND fiscal_date IS NOT NULL
                ORDER BY price_date DESC, fiscal_date DESC
            """, (ticker, cutoff_date))

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_quarterly(ticker: str, start_year: int = 2018) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("""
                SELECT ticker, fiscal_date, report_type, freq,
                       revenue, net_income, total_assets, total_liabilities, eps
                FROM fact_quarterly_fundamentals
                WHERE ticker = ?
                  AND EXTRACT(YEAR FROM fiscal_date) >= ?
                ORDER BY fiscal_date DESC
            """, (ticker, start_year))
