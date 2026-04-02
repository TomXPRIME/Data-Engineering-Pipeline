"""
dim_date Generator - US Stock Market Calendar Dimension Table

Generates a date dimension table with trading day flags for the US stock market calendar.
Output: output/gold/dim_date.parquet
"""

import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path


# US Stock Market Holidays (10 observed holidays)
US_HOLIDAYS = {
    "New Year's Day": lambda d: d.month == 1 and d.day == 1,
    "MLK Day": lambda d: _is_nth_weekday(d, 1, 3, 0),  # 3rd Monday of January
    "Presidents Day": lambda d: _is_nth_weekday(d, 2, 3, 0),  # 3rd Monday of February
    "Memorial Day": lambda d: _is_last_weekday(d, 5, 0),  # Last Monday of May
    "Independence Day": lambda d: d.month == 7 and d.day == 4,
    "Labor Day": lambda d: _is_nth_weekday(d, 9, 1, 0),  # 1st Monday of September
    "Thanksgiving": lambda d: _is_last_weekday(d, 11, 3),  # Last Thursday of November
    "Christmas Day": lambda d: d.month == 12 and d.day == 25,
}


def _is_nth_weekday(d: date, month: int, n: int, weekday: int) -> bool:
    """
    Check if date d is the n-th occurrence of weekday in the given month.
    weekday: 0=Monday, 3=Thursday, etc.
    """
    if d.month != month:
        return False
    if d.weekday() != weekday:
        return False

    first_day = date(d.year, month, 1)
    first_weekday = first_day.weekday()

    # Calculate the n-th occurrence
    days_to_add = (weekday - first_weekday) % 7 + (n - 1) * 7
    nth_date = first_day + relativedelta(days=days_to_add)

    return d == nth_date


def _is_last_weekday(d: date, month: int, weekday: int) -> bool:
    """
    Check if date d is the last occurrence of weekday in the given month.
    weekday: 0=Monday, 3=Thursday, etc.
    """
    if d.month != month:
        return False
    if d.weekday() != weekday:
        return False

    # Find last day of month
    if month == 12:
        last_day = date(d.year + 1, 1, 1) - relativedelta(days=1)
    else:
        last_day = date(d.year, month + 1, 1) - relativedelta(days=1)

    # Go backwards from last day to find last occurrence of weekday
    days_back = (last_day.weekday() - weekday) % 7
    last_weekday_date = last_day - relativedelta(days=days_back)

    return d == last_weekday_date


def _is_holiday(d: date) -> tuple:
    """Check if date is a US market holiday. Returns (is_holiday, holiday_name)."""
    for name, check_fn in US_HOLIDAYS.items():
        if check_fn(d):
            return True, name
    return False, None


def _is_trading_day(d: date) -> bool:
    """
    Check if date is a trading day (weekday and not a holiday).
    US stock market is closed on weekends and 10 observed holidays.
    """
    # Weekend check (Saturday=5, Sunday=6)
    if d.weekday() >= 5:
        return False

    # Holiday check
    is_holiday, _ = _is_holiday(d)
    if is_holiday:
        return False

    return True


def generate_dim_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Generate a date dimension table for US stock market calendar.

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)

    Returns:
        DataFrame with columns: date, year, quarter, month, day, day_of_week,
        is_trading_day, is_holiday, holiday_name, trading_day_offset
    """
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    records = []
    trading_day_counter = None  # Will be set on first trading day

    for dt in date_range:
        d = dt.date()

        is_holiday_flag, holiday_name = _is_holiday(d)
        is_trading = _is_trading_day(d)

        if is_trading:
            if trading_day_counter is None:
                trading_day_counter = 0
            else:
                trading_day_counter += 1
            trading_day_offset = trading_day_counter
        else:
            trading_day_offset = None

        records.append({
            'date': d.isoformat(),
            'year': d.year,
            'quarter': (d.month - 1) // 3 + 1,
            'month': d.month,
            'day': d.day,
            'day_of_week': d.weekday(),  # 0=Monday, 6=Sunday
            'is_trading_day': is_trading,
            'is_holiday': is_holiday_flag,
            'holiday_name': holiday_name if is_holiday_flag else None,
            'trading_day_offset': trading_day_offset,
        })

    return pd.DataFrame(records)


def main():
    """Generate the full dim_date table (2000-01-01 to 2100-12-31)."""
    output_path = Path('output/gold/dim_date.parquet')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("Generating dim_date table for 2000-01-01 to 2100-12-31...")
    df = generate_dim_date('2000-01-01', '2100-12-31')

    print(f"Total rows: {len(df)}")
    print(f"Trading days: {df['is_trading_day'].sum()}")
    print(f"Writing to {output_path}...")

    df.to_parquet(output_path, index=False)
    print("Done.")


if __name__ == '__main__':
    main()
