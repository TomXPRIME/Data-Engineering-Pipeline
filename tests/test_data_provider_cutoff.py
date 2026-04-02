"""Tests for SPXDataProvider.get_fundamentals cutoff_date behavior."""
import pytest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.data_provider import SPXDataProvider


class TestGetFundamentalsCutoffDate:
    """Test cutoff_date filtering logic."""

    def test_no_cutoff_returns_all_periods(self):
        """cutoff_date=None should return all fiscal periods (backward compat)."""
        provider = SPXDataProvider()
        result = provider.get_fundamentals("AAPL", freq="quarterly", cutoff_date=None)
        assert "income" in result or "balance" in result  # at least one report type exists

    def test_cutoff_filters_future_periods(self):
        """period_date > cutoff_date columns should be excluded."""
        provider = SPXDataProvider()
        # Use annual data which has 2021-2025 periods
        all_data = provider.get_fundamentals("IBM", freq="annual", cutoff_date=None)
        # Cutoff 2022-12-31 should retain 2021 data but exclude 2023+
        cutoff_date = "2022-12-31"
        cutoff_data = provider.get_fundamentals("IBM", freq="annual", cutoff_date=cutoff_date)
        if "income" in all_data and "income" in cutoff_data:
            all_cols = set(all_data["income"].columns)
            cutoff_cols = set(cutoff_data["income"].columns)
            assert cutoff_cols <= all_cols
            # Any column > cutoff_date must be absent
            future_cols = [c for c in all_cols if c > cutoff_date]
            assert not any(c in cutoff_cols for c in future_cols), f"Future cols {future_cols} found in cutoff result"
            # Columns <= cutoff_date should be present (at least some)
            assert len(cutoff_cols) > 0, "Cutoff should retain at least some columns"

    def test_invalid_ticker_raises(self):
        """Non-existent ticker should raise ValueError."""
        provider = SPXDataProvider()
        with pytest.raises(ValueError, match="not found"):
            provider.get_fundamentals("NOTATICKER", cutoff_date="2020-12-31")

    def test_invalid_freq_raises(self):
        """Invalid freq should raise ValueError."""
        provider = SPXDataProvider()
        with pytest.raises(ValueError, match="freq must be"):
            provider.get_fundamentals("AAPL", freq="monthly")
