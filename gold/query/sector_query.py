"""Sector rotation queries."""
import streamlit as st
import pandas as pd
from gold.query.gold_data_provider import GoldDataProvider


class SectorQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_sector_rotation(year: int = None, quarter: int = None) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            if year and quarter:
                return gdp.execute("""
                    SELECT sector, year, quarter, avg_close, total_volume, avg_volatility,
                           avg_ticker_count, qoq_return, momentum_rank
                    FROM v_sector_rotation
                    WHERE year = ? AND quarter = ?
                    ORDER BY momentum_rank
                """, (year, quarter))
            return gdp.execute("SELECT * FROM v_sector_rotation ORDER BY year, quarter, momentum_rank")
