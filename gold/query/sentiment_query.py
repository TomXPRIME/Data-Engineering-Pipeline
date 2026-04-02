"""Sentiment data queries."""
import streamlit as st
import pandas as pd
from gold.query.gold_data_provider import GoldDataProvider


class SentimentQuery:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_sentiment_view(ticker: str = None) -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            if ticker:
                return gdp.execute(f"""
                    SELECT ticker, transcript_date, sentiment_score, close_on_event,
                           next_1d_return, next_5d_return
                    FROM v_sentiment_price_view
                    WHERE ticker = '{ticker}'
                    ORDER BY transcript_date DESC
                """)
            return gdp.execute("SELECT * FROM v_sentiment_price_view ORDER BY transcript_date DESC")

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_binned_returns() -> pd.DataFrame:
        with GoldDataProvider() as gdp:
            return gdp.execute("SELECT * FROM v_sentiment_binned_returns ORDER BY sentiment_bucket")