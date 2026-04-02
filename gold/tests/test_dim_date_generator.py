import pandas as pd

def test_dim_date_range():
    from gold.dim_date_generator import generate_dim_date
    df = generate_dim_date('2000-01-01', '2100-12-31')
    assert df['date'].min() == '2000-01-01'
    assert df['date'].max() == '2100-12-31'
    assert len(df) > 0

def test_trading_day_flags():
    from gold.dim_date_generator import generate_dim_date
    df = generate_dim_date('2000-01-01', '2000-01-31')
    # Jan 1 2000 was a Saturday — should NOT be trading day
    jan1 = df[df['date'] == '2000-01-01'].iloc[0]
    assert jan1['is_trading_day'] == False
    # Jan 3 2000 was Monday — should be trading day
    jan3 = df[df['date'] == '2000-01-03'].iloc[0]
    assert jan3['is_trading_day'] == True

def test_holiday_flags():
    from gold.dim_date_generator import generate_dim_date
    df = generate_dim_date('2024-07-01', '2024-07-07')
    # July 4 2024 is Independence Day holiday
    july4 = df[df['date'] == '2024-07-04'].iloc[0]
    assert july4['is_holiday'] == True
    assert july4['is_trading_day'] == False  # holiday is not a trading day
