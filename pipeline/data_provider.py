"""
SPX Data Provider - Simulated Financial API

Encapsulates raw data (CSV/PDF), simulates Yahoo Finance API behavior.
All data access must go through this class.

Design: docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md
"""

import os
import glob
from pathlib import Path
from typing import Optional

import pandas as pd


# Data paths
DATA_DIR = Path(__file__).parent.parent / "data"
PRICE_CSV = DATA_DIR / "price" / "spx_20yr_ohlcv_data.csv"
FUNDAMENTAL_DIR = DATA_DIR / "fundamental" / "SPX_Fundamental_History"
TRANSCRIPT_DIR = DATA_DIR / "transcript" / "SPX_20yr_PDF_Library_10GB"
TICKERS_CSV = DATA_DIR / "reference" / "tickers.csv"


class DataIntegrityError(ValueError):
    """Raised when data exists but is corrupted/unreadable."""
    pass


class SPXDataProvider:
    """
    Simulates Yahoo Finance API behavior for SPX 500 data.

    All data access goes through this class. Data is read from local
    CSV/PDF files and returned in the same format as a financial API.

    Error Handling:
        - Data exists → DataFrame/dict/bytes
        - No data → empty DataFrame/empty dict/FileNotFoundError
        - Ticker not found → ValueError
        - Corrupted file → DataIntegrityError
    """

    def __init__(self):
        """Initialize the data provider, loading price data index."""
        self._price_df: Optional[pd.DataFrame] = None
        self._tickers_list: Optional[list] = None
        self._transcript_index: Optional[list] = None

    def _load_price_data(self) -> pd.DataFrame:
        """Lazy load of price data with caching."""
        if self._price_df is None:
            self._price_df = pd.read_csv(
                PRICE_CSV,
                parse_dates=["Date"],
                dtype={"Ticker": str, "Volume": float}
            )
        return self._price_df

    def _load_tickers(self) -> list:
        """Load and cache ticker list."""
        if self._tickers_list is None:
            df = pd.read_csv(TICKERS_CSV)
            # Ticker column is named 'tickers' in the CSV
            self._tickers_list = df["tickers"].dropna().unique().tolist()
        return self._tickers_list

    def _build_transcript_index(self) -> list:
        """Build transcript index from PDF filenames."""
        if self._transcript_index is None:
            index = []
            pattern = str(TRANSCRIPT_DIR / "*.pdf")
            for pdf_path in glob.glob(pattern):
                filename = Path(pdf_path).stem  # e.g., "AAPL_2007-07-18"
                parts = filename.rsplit("_", 1)
                if len(parts) == 2:
                    ticker, date_str = parts
                    index.append({
                        "ticker": ticker,
                        "date": date_str,
                        "path": pdf_path
                    })
            self._transcript_index = index
        return self._transcript_index

    def get_price(self, ticker: str, date: str) -> pd.DataFrame:
        """
        Get OHLCV price data for a ticker on a specific date.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            date: Date in 'YYYY-MM-DD' format

        Returns:
            DataFrame with columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
            Returns empty DataFrame if no data for that date (e.g., weekend/holiday)

        Raises:
            ValueError: If ticker is not found in dataset

        Example:
            >>> provider = SPXDataProvider()
            >>> df = provider.get_price('AAPL', '2024-01-15')
            >>> print(df.head())
        """
        df = self._load_price_data()

        # Check if ticker exists
        if ticker not in df["Ticker"].values:
            raise ValueError(f"Ticker '{ticker}' not found in dataset")

        # Filter by ticker and date
        result = df[(df["Ticker"] == ticker) & (df["Date"].dt.strftime("%Y-%m-%d") == date)]

        # Return empty DataFrame with correct columns if no data
        if len(result) == 0:
            return pd.DataFrame(columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

        return result.reset_index(drop=True)

    def get_fundamentals(self, ticker: str, freq: str = "quarterly", cutoff_date: Optional[str] = None) -> dict:
        """
        Get fundamental data for a ticker.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            freq: 'annual' or 'quarterly' (default: 'quarterly')
            cutoff_date: If provided, only return fiscal periods with period_date <= cutoff_date.
                         Simulates "knowledge cutoff" — as of a given date, you can only see
                         financial data that would have been publicly available.
                         Format: 'YYYY-MM-DD' or 'YYYYQn' or 'YYYY' for annual.

        Returns:
            dict with keys: 'income', 'balance', 'cashflow', 'profile'
            Each value is a DataFrame with dates as columns, metrics as rows.
            Returns empty dict if no data for that ticker.

        Raises:
            ValueError: If ticker not found and freq is invalid

        Example:
            >>> provider = SPXDataProvider()
            >>> data = provider.get_fundamentals('AAPL', 'quarterly')
            >>> print(data.keys())  # ['income', 'balance', 'cashflow', 'profile']
            >>> cutoff_data = provider.get_fundamentals('AAPL', 'quarterly', cutoff_date='2020-12-31')
        """
        if freq not in ("annual", "quarterly"):
            raise ValueError(f"freq must be 'annual' or 'quarterly', got '{freq}'")

        # Validate ticker exists
        if ticker not in self._load_tickers():
            raise ValueError(f"Ticker '{ticker}' not found in dataset")

        result = {}

        # Check which files exist for this ticker
        for report_type in ("income", "balance", "cashflow", "profile"):
            filename = f"{ticker}_{report_type}_{freq}.csv"
            filepath = FUNDAMENTAL_DIR / filename

            if filepath.exists():
                try:
                    df = pd.read_csv(filepath, index_col=0)
                    if cutoff_date is not None:
                        valid_cols = [c for c in df.columns if c <= cutoff_date]
                        df = df[valid_cols]
                    result[report_type] = df
                except Exception as e:
                    raise DataIntegrityError(f"Corrupted file {filepath}: {e}")
            else:
                # File doesn't exist for this ticker/freq combination
                pass

        return result

    def get_transcript(self, ticker: str, date: str) -> bytes:
        """
        Get earnings call transcript PDF as bytes.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            date: Date in 'YYYY-MM-DD' format

        Returns:
            Raw PDF bytes

        Raises:
            FileNotFoundError: If transcript doesn't exist for that date
            DataIntegrityError: If PDF exists but is corrupted

        Example:
            >>> provider = SPXDataProvider()
            >>> pdf_bytes = provider.get_transcript('AAPL', '2024-02-01')
        """
        filename = f"{ticker}_{date}.pdf"
        filepath = TRANSCRIPT_DIR / filename

        if not filepath.exists():
            raise FileNotFoundError(f"Transcript not found: {filename}")

        try:
            with open(filepath, "rb") as f:
                return f.read()
        except Exception as e:
            raise DataIntegrityError(f"Failed to read PDF {filepath}: {e}")

    def list_transcripts(self, ticker: Optional[str] = None, year: Optional[int] = None) -> list:
        """
        List available transcripts, optionally filtered by ticker and/or year.

        Args:
            ticker: Optional ticker to filter by (e.g., 'AAPL')
            year: Optional year to filter by (e.g., 2024)

        Returns:
            List of dicts: [{'ticker': 'AAPL', 'date': '2024-02-01'}, ...]

        Example:
            >>> provider = SPXDataProvider()
            >>> transcripts = provider.list_transcripts(ticker='AAPL', year=2024)
            >>> print(len(transcripts))  # Number of AAPL transcripts in 2024
        """
        index = self._build_transcript_index()

        if ticker:
            index = [x for x in index if x["ticker"] == ticker]

        if year:
            index = [x for x in index if x["date"].startswith(str(year))]

        return index

    def get_trading_dates(self, start_date: str, end_date: str) -> list:
        """
        Get all trading dates in the specified range.

        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format

        Returns:
            List of dates as strings 'YYYY-MM-DD' that have trading data

        Example:
            >>> provider = SPXDataProvider()
            >>> dates = provider.get_trading_dates('2024-01-01', '2024-01-31')
            >>> print(dates)  # ['2024-01-02', '2024-01-03', ...]
        """
        df = self._load_price_data()

        # Filter by date range
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        filtered = df.loc[mask, "Date"].dt.strftime("%Y-%m-%d").unique()

        return sorted(filtered.tolist())

    def get_ticker_list(self) -> list:
        """
        Get list of all available tickers.

        Returns:
            List of ticker symbols as strings

        Example:
            >>> provider = SPXDataProvider()
            >>> tickers = provider.get_ticker_list()
            >>> print(tickers[:5])  # ['AAPL', 'MSFT', ...]
        """
        return sorted(self._load_tickers())
