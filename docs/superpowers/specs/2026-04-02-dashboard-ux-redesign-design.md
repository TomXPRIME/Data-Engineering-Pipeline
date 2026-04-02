# Dashboard UX Redesign

> **STATUS: ✅ COMPLETED (2026-04-02)** — All design items implemented and verified.
> This document is kept for historical reference only.

**Date:** 2026-04-02
**Status:** Completed

## Problem

1. Ticker display bug — subheaders in Tabs 3, 4, 6 sometimes show wrong ticker or don't sync with sidebar selection
2. Ticker input is free-text (`st.text_input`) — should be searchable dropdown
3. Column names exposed raw database field names (e.g., `avg_close`, `trade_date`, `annualized_vol_20d`) — unprofessional and unreadable

## Architecture

### Global Ticker State
- Use `st.session_state.selected_ticker` as the single source of truth
- Dropdown placed at top of Tabs 2, 3, 4, 6 (individual ticker tabs)
- Tabs 1 and 5 (market-wide tabs) have no ticker selector
- Ticker list fetched from `DimensionQuery.get_all_tickers()` — all ~800+ SPX tickers
- `st.selectbox` provides built-in search-as-you-type

### Sidebar
- Contains only: project title, date range picker
- No ticker selector in sidebar

### Column Name Mapping
- Build a mapping dictionary at module level
- Rename columns in every `st.dataframe()` call before display
- No underscores, proper capitalization, professional financial English

### Layout Improvements
- Use `st.divider()` between sections
- Use `st.container()` with `border=True` for metric cards
- Consistent heading hierarchy: `st.title()` for page, `st.header()` for tab, `st.subheader()` for section
- Consistent empty-state and error-state messaging

## Field Name Mapping

### Market Overview (Tab 1)
| Raw | Display |
|-----|---------|
| `trade_date` | Trading Date |
| `number_of_tickers` | Number of Tickers |
| `avg_close` | Average Close Price |
| `avg_return` | Average Return |
| `total_volume` | Total Volume |

### Stock Analysis (Tab 2)
| Raw | Display |
|-----|---------|
| `date` | Date |
| `open` | Open |
| `high` | High |
| `low` | Low |
| `close` | Close |
| `adj_close` | Adjusted Close |
| `volume` | Volume |
| `daily_return` | Daily Return |
| `next_1d_return` | Next 1-Day Return |
| `next_5d_return` | Next 5-Day Return |

### Fundamental History (Tab 3)
| Raw | Display |
|-----|---------|
| `ticker` | Ticker |
| `price_date` | Report Date |
| `fiscal_date` | Fiscal Period End |
| `report_type` | Report Type |
| `freq` | Frequency |
| `revenue` | Revenue |
| `net_income` | Net Income |
| `total_assets` | Total Assets |
| `total_liabilities` | Total Liabilities |

### Sentiment Analytics (Tab 4)
| Raw | Display |
|-----|---------|
| `ticker` | Ticker |
| `transcript_date` | Transcript Date |
| `sentiment_score` | Sentiment Score |
| `close_on_event` | Close Price on Event Date |
| `next_1d_return` | Next 1-Day Return |
| `next_5d_return` | Next 5-Day Return |
| `sentiment_bucket` | Sentiment Category |
| `avg_1d_return` | Average 1-Day Forward Return |

### Sector Rotation (Tab 5)
| Raw | Display |
|-----|---------|
| `sector` | Sector |
| `year` | Year |
| `quarter` | Quarter |
| `avg_close` | Average Close Price |
| `total_volume` | Total Volume |
| `avg_volatility` | Average Volatility |
| `avg_ticker_count` | Number of Constituents |
| `qoq_return` | Quarter-over-Quarter Return |
| `momentum_rank` | Momentum Rank |

### Risk & Performance (Tab 6)
| Raw | Display |
|-----|---------|
| `date` | Date |
| `close` | Close Price |
| `annualized_vol_20d` | 20-Day Annualized Volatility |
| `annualized_vol_60d` | 60-Day Annualized Volatility |
| `annualized_return_20d` | 20-Day Annualized Return |
| `alpha_ar1` | AR(1) Alpha |
| `beta_ar1` | AR(1) Beta |
| `r_squared_ar1` | AR(1) R-Squared |
| `n_obs` | Observations |

### Metric Labels
| Raw | Display |
|-----|---------|
| "Latest Avg Close" | Average Close Price |
| "Trading Days" | Trading Days |
| "Period Change" | Period Return |
| "Latest Close" | Close Price |
| "Volume" | Trading Volume |
| "Daily Return" | Daily Return |

## Implementation Plan

1. Add `COLUMN_MAPPING` dict to `dashboard.py`
2. Add `rename_columns()` helper function
3. Replace `st.text_input` with `st.selectbox` for ticker in Tabs 2/3/4/6
4. Sync all tabs to `st.session_state.selected_ticker`
5. Move ticker selector from sidebar to individual tab content
6. Apply `rename_columns()` to all `st.dataframe()` calls
7. Update metric labels to professional names
8. Add `st.divider()` between sections
9. Update heading hierarchy and layout structure
