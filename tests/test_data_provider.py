"""
Unit tests for pipeline.data_provider.SPXDataProvider.

Run: python -m pytest tests/test_data_provider.py -v
"""
import sys
from pathlib import Path

import pytest

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.data_provider import SPXDataProvider


@pytest.fixture
def provider():
    return SPXDataProvider()


def test_get_price_returns_dataframe(provider):
    """get_price returns a DataFrame for valid ticker and date."""
    df = provider.get_price("AAPL", "2024-01-16")  # Tuesday - Jan 15 was MLK Day market holiday
    assert len(df) > 0, "Expected rows for AAPL on 2024-01-16"
    expected_cols = {"Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
    assert set(df.columns).issuperset(expected_cols), f"Missing columns: {expected_cols - set(df.columns)}"


def test_get_price_weekend_returns_empty(provider):
    """get_price returns empty DataFrame for non-trading day."""
    df = provider.get_price("AAPL", "2024-01-13")  # Saturday
    assert len(df) == 0, "Expected empty DataFrame for weekend"


def test_get_ticker_list_returns_list(provider):
    """get_ticker_list returns a non-empty list."""
    tickers = provider.get_ticker_list()
    assert isinstance(tickers, list), "Expected list"
    assert len(tickers) > 0, "Expected non-empty ticker list"
    assert "AAPL" in tickers or "MSFT" in tickers, "Expected common tickers in list"


def test_get_trading_dates_returns_list(provider):
    """get_trading_dates returns list of dates in range."""
    dates = provider.get_trading_dates("2024-01-02", "2024-01-10")
    assert isinstance(dates, list), "Expected list"
    assert len(dates) >= 5, "Expected at least 5 trading days in Jan 2024 first week"
    assert "2024-01-02" in dates, "Expected 2024-01-02 as first trading day"


def test_get_price_invalid_ticker_raises(provider):
    """get_price raises ValueError for invalid ticker."""
    with pytest.raises(ValueError):
        provider.get_price("INVALID_TICKER_XYZ", "2024-01-15")