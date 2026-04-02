"""
dim_ticker SCD Type 2 Generator

Generates output/gold/dim_ticker.parquet with SCD Type 2 dimensions for tickers.

SCD Type 2 tracks historical changes to sector and industry. When these attributes
change for a ticker, a new row is created with updated valid_from/valid_to dates.

Output schema:
    ticker: str       - Ticker symbol (e.g., 'AAPL')
    company_name: str - Company short name
    sector: str      - GICS sector
    industry: str    - GICS industry
    valid_from: str  - Start date of this version (YYYY-MM-DD)
    valid_to: str    - End date of this version (YYYY-MM-DD)
    is_current: bool - True if this is the latest version
"""
import sys
from pathlib import Path

# Add project root to path for pipeline imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from datetime import timedelta
from typing import Optional

import pandas as pd

from pipeline.data_provider import SPXDataProvider

logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
SILVER_FUND_DIR = PROJECT_ROOT / "output" / "silver" / "fundamentals"
RAW_FUND_DIR = PROJECT_ROOT / "data" / "fundamental" / "SPX_Fundamental_History"
OUTPUT_PATH = PROJECT_ROOT / "output" / "gold" / "dim_ticker.parquet"
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Far-future date for open-ended intervals
FUTURE_DATE = "2099-12-31"


def _date_minus_days(date_str: str, days: int) -> str:
    """Subtract days from a date string and return YYYY-MM-DD format."""
    date = pd.to_datetime(date_str) - timedelta(days=days)
    return date.strftime("%Y-%m-%d")


def _detect_scd_changes(records: list[dict]) -> list[dict]:
    """
    Detect SCD Type 2 changes from a list of ticker profile records.

    Records should be sorted by report_date ascending.

    When sector or industry changes for a ticker, the previous row's valid_to
    is set to (new_valid_from - 1 day) and a new row is created.

    Args:
        records: List of dicts with keys: ticker, company_name, sector, industry, report_date

    Returns:
        List of SCD Type 2 rows with valid_from, valid_to, is_current
    """
    if not records:
        return []

    # Sort by ticker and report_date
    sorted_records = sorted(records, key=lambda r: (r["ticker"], r["report_date"]))

    scd_rows = []
    ticker_versions = {}  # ticker -> list of versions

    for record in sorted_records:
        ticker = record["ticker"]
        if ticker not in ticker_versions:
            ticker_versions[ticker] = []

        versions = ticker_versions[ticker]

        # Check if sector or industry changed from the last version
        if versions:
            last_version = versions[-1]
            sector_changed = record["sector"] != last_version["sector"]
            industry_changed = record["industry"] != last_version["industry"]

            if sector_changed or industry_changed:
                # Close out the previous version
                last_version["valid_to"] = _date_minus_days(record["report_date"], 1)
                last_version["is_current"] = False

                # Create new version
                new_version = {
                    "ticker": ticker,
                    "company_name": record["company_name"],
                    "sector": record["sector"],
                    "industry": record["industry"],
                    "valid_from": record["report_date"],
                    "valid_to": FUTURE_DATE,
                    "is_current": True,
                }
                versions.append(new_version)
            else:
                # No change - update the existing (latest) version's report_date
                # to be the latest, but don't create a new row
                last_version["valid_from"] = record["report_date"]
        else:
            # First version for this ticker
            versions.append({
                "ticker": ticker,
                "company_name": record["company_name"],
                "sector": record["sector"],
                "industry": record["industry"],
                "valid_from": record["report_date"],
                "valid_to": FUTURE_DATE,
                "is_current": True,
            })

    # Flatten ticker_versions to scd_rows
    for ticker, versions in ticker_versions.items():
        scd_rows.extend(versions)

    return scd_rows


def _load_profile_from_raw_csv(ticker: str) -> Optional[dict]:
    """
    Load ticker profile from raw profile_metadata.csv file.

    Returns dict with keys: company_name, sector, industry, or None if not found.
    """
    profile_path = RAW_FUND_DIR / f"{ticker}_profile_metadata.csv"
    if not profile_path.exists():
        return None

    df = pd.read_csv(profile_path, header=None, names=["key", "value"])
    df = df.dropna(subset=["key"])

    profile = {}
    for _, row in df.iterrows():
        key = str(row["key"]).strip()
        value = str(row["value"]).strip() if pd.notna(row["value"]) else None

        if key == "shortName":
            profile["company_name"] = value
        elif key == "sector":
            profile["sector"] = value
        elif key == "industry":
            profile["industry"] = value

    if "company_name" in profile and "sector" in profile and "industry" in profile:
        return profile
    return None


def _get_tickers_from_silver() -> list[str]:
    """Get ticker list from Silver fundamentals partition directories."""
    if not SILVER_FUND_DIR.exists():
        return []

    tickers = []
    for ticker_dir in SILVER_FUND_DIR.iterdir():
        if ticker_dir.is_dir() and ticker_dir.name.startswith("ticker="):
            ticker = ticker_dir.name.replace("ticker=", "")
            tickers.append(ticker)

    return sorted(tickers)


def _get_tickers_from_data_provider() -> list[str]:
    """Fallback: get ticker list from SPXDataProvider."""
    provider = SPXDataProvider()
    return provider.get_ticker_list()


def generate_dim_ticker() -> pd.DataFrame:
    """
    Generate dim_ticker SCD Type 2 DataFrame.

    Reads ticker profile data from Silver fundamentals partitions (or raw data),
    detects SCD Type 2 changes when sector or industry change over time,
    and returns a DataFrame with SCD columns.

    Returns:
        DataFrame with columns: ticker, company_name, sector, industry,
                                 valid_from, valid_to, is_current
    """
    # Get ticker list
    tickers = _get_tickers_from_silver()
    if not tickers:
        logger.warning("No tickers found in Silver fundamentals, using DataProvider fallback")
        tickers = _get_tickers_from_data_provider()

    logger.info(f"Processing {len(tickers)} tickers for dim_ticker SCD Type 2")

    # Collect all profile records
    all_records = []

    for ticker in tickers:
        profile = _load_profile_from_raw_csv(ticker)
        if profile is None:
            # If no profile found, create a minimal record
            logger.warning(f"No profile metadata for {ticker}, using ticker as company name")
            profile = {
                "company_name": ticker,
                "sector": "Unknown",
                "industry": "Unknown",
            }

        all_records.append({
            "ticker": ticker,
            "company_name": profile["company_name"],
            "sector": profile["sector"],
            "industry": profile["industry"],
            "report_date": "2004-01-01",  # Use data start date as valid_from for single records
        })

    # Detect SCD changes and generate SCD rows
    scd_rows = _detect_scd_changes(all_records)

    # Create DataFrame
    df = pd.DataFrame(scd_rows)

    # Ensure correct column order
    columns = ["ticker", "company_name", "sector", "industry", "valid_from", "valid_to", "is_current"]
    df = df[columns]

    logger.info(f"Generated dim_ticker with {len(df)} rows ({df['ticker'].nunique()} unique tickers)")

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logger.info("Generating dim_ticker.parquet...")
    df = generate_dim_ticker()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    logger.info(f"Saved dim_ticker.parquet to {OUTPUT_PATH}")
    logger.info(f"Total rows: {len(df)}, Unique tickers: {df['ticker'].nunique()}")
    print(df.head(10).to_string())
