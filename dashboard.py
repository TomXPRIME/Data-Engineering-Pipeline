from pathlib import Path
import importlib
import re
import sys

import pandas as pd
import streamlit as st

# Avoid local `duckdb/` directory shadowing the duckdb package import.
PROJECT_ROOT = str(Path(__file__).resolve().parent)
sys.path = [p for p in sys.path if p not in ("", PROJECT_ROOT)]
duckdb = importlib.import_module("duckdb")


DB_PATH = Path(__file__).resolve().parent / "duckdb" / "spx_analytics.duckdb"
EXPECTED_VIEWS = [
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_fundamental_snapshot",
    "v_sentiment_price_view",
    "v_sector_rotation",
]


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def check_views(conn) -> tuple[list[str], list[str]]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'main'
        """
    ).fetchall()
    existing = {r[0] for r in rows}
    available = [v for v in EXPECTED_VIEWS if v in existing]
    missing = [v for v in EXPECTED_VIEWS if v not in existing]
    return available, missing


@st.cache_data(ttl=300)
def load_market_daily() -> pd.DataFrame:
    conn = get_connection()
    return conn.execute(
        """
        SELECT *
        FROM v_market_daily_summary
        ORDER BY trade_date
        """
    ).df()


@st.cache_data(ttl=300)
def load_ticker_profile() -> pd.DataFrame:
    conn = get_connection()
    return conn.execute(
        """
        SELECT *
        FROM v_ticker_profile
        ORDER BY ticker
        """
    ).df()


@st.cache_data(ttl=300)
def load_fundamental_snapshot() -> pd.DataFrame:
    conn = get_connection()
    return conn.execute(
        """
        SELECT *
        FROM v_fundamental_snapshot
        ORDER BY ticker
        """
    ).df()


@st.cache_data(ttl=300)
def load_sentiment_price(
    ticker: str | None, row_limit: int
) -> pd.DataFrame:
    conn = get_connection()
    if ticker:
        return conn.execute(
            """
            SELECT *
            FROM v_sentiment_price_view
            WHERE ticker = ?
            ORDER BY transcript_date DESC
            LIMIT ?
            """,
            [ticker, row_limit],
        ).df()
    return conn.execute(
        """
        SELECT *
        FROM v_sentiment_price_view
        ORDER BY transcript_date DESC
        LIMIT ?
        """,
        [row_limit],
    ).df()


def render_overview(
    market_daily: pd.DataFrame,
    ticker_profile: pd.DataFrame,
    fundamental_snapshot: pd.DataFrame,
    sentiment_price: pd.DataFrame,
) -> None:
    st.subheader("Pipeline Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Trading Days", f"{len(market_daily):,}")
    c2.metric("Tickers", f"{ticker_profile['ticker'].nunique():,}")
    c3.metric(
        "Fundamental Snapshots",
        f"{fundamental_snapshot['ticker'].nunique():,}",
    )
    c4.metric("Sentiment Records", f"{len(sentiment_price):,}")

    st.markdown("---")
    st.markdown("#### Market Trend")
    chart_df = market_daily.copy()
    chart_df["trade_date"] = pd.to_datetime(chart_df["trade_date"])
    st.line_chart(chart_df.set_index("trade_date")["avg_close"])

    st.markdown("#### Average Daily Return")
    st.line_chart(chart_df.set_index("trade_date")["avg_return"])


def render_market_daily(market_daily: pd.DataFrame) -> None:
    st.subheader("v_market_daily_summary")
    st.dataframe(market_daily, use_container_width=True, hide_index=True)


def render_ticker_profile(ticker_profile: pd.DataFrame, selected_ticker: str | None) -> None:
    st.subheader("v_ticker_profile")
    df = ticker_profile
    if selected_ticker:
        df = df[df["ticker"] == selected_ticker]
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("#### Sector Distribution (Top 15)")
    sector_counts = (
        ticker_profile["sector"]
        .fillna("Unknown")
        .value_counts()
        .head(15)
        .rename_axis("sector")
        .reset_index(name="count")
    )
    st.bar_chart(sector_counts.set_index("sector")["count"])


def render_fundamental_history(con, ticker_list: list) -> None:
    """Bloomberg-style fundamental history with cutoff_date (knowledge cutoff)."""
    st.subheader("Fundamental History")

    col1, col2, col3 = st.columns(3)
    with col1:
        ticker = st.selectbox("Ticker", ticker_list)
    with col2:
        cutoff = st.text_input("Cutoff Date (YYYY-MM-DD)", value="2024-12-31")
    with col3:
        freq_filter = st.selectbox("Frequency", ["both", "quarterly", "annual"])

    if cutoff:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', cutoff):
            st.warning("Cutoff date must be in YYYY-MM-DD format")
            return

    col4, col5 = st.columns([2, 1])
    with col4:
        report_types = st.multiselect(
            "Report Type", ["income", "balance", "cashflow"], default=["income"]
        )
    with col5:
        top_n = st.number_input("Top N periods", value=8, min_value=2, max_value=20)

    if not ticker:
        st.info("Select a ticker to view fundamentals.")
        return

    # Build parameterized query
    params = [ticker, cutoff]
    freq_sql = ""
    if freq_filter == "quarterly":
        freq_sql = "AND freq = ?"
        params.append("quarterly")
    elif freq_filter == "annual":
        freq_sql = "AND freq = ?"
        params.append("annual")

    report_sql = ""
    if report_types:
        placeholders = ", ".join(["?"] * len(report_types))
        report_sql = f"AND report_type IN ({placeholders})"
        params.extend(report_types)

    query = f"""
        SELECT ticker, fiscal_date, report_type, freq, metric, value
        FROM v_fundamental_history
        WHERE ticker = ? AND fiscal_date <= ? {freq_sql} {report_sql}
        ORDER BY fiscal_date DESC, metric
        LIMIT 1000
    """
    try:
        df = con.execute(query, params).fetchdf()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return

    if df.empty:
        st.warning(f"No fundamental data for {ticker} as of {cutoff}")
        return

    # Pivot: rows=metrics, columns=fiscal periods
    pivot = df.pivot_table(
        index="metric",
        columns="fiscal_date",
        values="value",
        aggfunc="first"
    )
    # Show only top N most recent periods
    pivot = pivot[sorted(pivot.columns, reverse=True)[:top_n]]

    st.markdown(f"**{ticker} — Fundamental History (as of {cutoff})**")
    st.dataframe(pivot, use_container_width=True)

    st.caption(f"Rows: {len(df)} | Freq: {freq_filter} | Report types: {report_types}")


def render_sentiment_price(sentiment_price: pd.DataFrame) -> None:
    st.subheader("v_sentiment_price_view")
    st.dataframe(sentiment_price, use_container_width=True, hide_index=True)

    if not sentiment_price.empty:
        st.markdown("#### Sentiment vs 1-Day Return")
        corr_df = sentiment_price[["sentiment_score", "next_1d_return"]].dropna()
        if not corr_df.empty:
            st.scatter_chart(corr_df, x="sentiment_score", y="next_1d_return")
        else:
            st.info("No non-null pairs for sentiment and 1-day return.")


def sector_rotation_page(con):
    """v_sector_rotation — quarterly sector performance ranking."""
    st.header("Sector Rotation")
    st.caption("Data source: v_sector_rotation")

    df = con.execute("SELECT * FROM v_sector_rotation ORDER BY year, quarter, momentum_rank").fetchdf()

    if df.empty:
        st.warning("No sector rotation data available.")
        return

    df["yearq"] = df["year"] * 10 + df["quarter"]
    latest_q = df["yearq"].max()
    latest = df[df["yearq"] == latest_q].sort_values("momentum_rank")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Sectors tracked", df["sector"].nunique())
    with col2:
        st.metric("Quarters covered", df[["year", "quarter"]].drop_duplicates().shape[0])

    st.subheader(f"Latest Quarter ({latest['year'].iloc[0]} Q{latest['quarter'].iloc[0]}) — Sector Ranking")
    st.dataframe(latest[["momentum_rank", "sector", "avg_close", "avg_volatility", "total_volume"]], use_container_width=True)

    st.subheader("Sector Momentum Rank Over Time")
    pivot = df.pivot_table(index="sector", columns="yearq", values="momentum_rank")
    st.dataframe(pivot, use_container_width=True)


def sentiment_page(con):
    """v_sentiment_binned_returns — sentiment bucket vs forward returns."""
    st.header("Sentiment Binned Returns")
    st.caption("Data source: v_sentiment_binned_returns")

    df = con.execute("SELECT * FROM v_sentiment_binned_returns").fetchdf()

    if df.empty:
        st.warning("No sentiment data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total transcripts", df["transcript_count"].sum())
    with col2:
        pos_ret = df[df["sentiment_bucket"] == "POSITIVE"]["avg_1d_return"].values
        st.metric("Positive avg 1d return", f"{pos_ret[0]:.4f}" if len(pos_ret) > 0 else "N/A")
    with col3:
        neg_ret = df[df["sentiment_bucket"] == "NEGATIVE"]["avg_1d_return"].values
        st.metric("Negative avg 1d return", f"{neg_ret[0]:.4f}" if len(neg_ret) > 0 else "N/A")

    st.subheader("Average 1-Day Return by Sentiment Bucket")
    chart_df = df[["sentiment_bucket", "avg_1d_return", "avg_5d_return"]].set_index("sentiment_bucket")
    st.bar_chart(chart_df)

    st.subheader("Full Sentiment Bucket Statistics")
    st.dataframe(df, use_container_width=True)


def volatility_page(con):
    """v_rolling_volatility — 20d vs 60d annualized volatility analysis."""
    st.header("Volatility Analysis")
    st.caption("Data source: v_rolling_volatility")

    ticker_filter = st.sidebar.text_input("Ticker (optional)", value="")
    max_date = st.sidebar.text_input("Max date (YYYY-MM-DD)", value="2024-12-31")

    query = "SELECT * FROM v_rolling_volatility WHERE date <= ?"
    params = [max_date]
    if ticker_filter:
        query += " AND ticker = ?"
        params.append(ticker_filter.upper())
    query += " ORDER BY date DESC LIMIT 5000"

    df = con.execute(query, params).fetchdf()

    if df.empty:
        st.warning("No volatility data available.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Tickers", df["ticker"].nunique())
        st.metric("Date range", f"{df['date'].min()} to {df['date'].max()}")
    with col2:
        avg_vol = df["annualized_vol_20d"].mean()
        st.metric("Avg 20d Vol (annualized)", f"{avg_vol:.4f}" if avg_vol else "N/A")

    st.subheader("20d vs 60d Volatility Scatter")
    scatter_df = df.dropna(subset=["annualized_vol_20d", "annualized_vol_60d"]).head(1000)
    st.scatter_chart(
        scatter_df[["annualized_vol_20d", "annualized_vol_60d"]].rename(
            columns={"annualized_vol_20d": "20d Vol", "annualized_vol_60d": "60d Vol"}
        )
    )

    st.subheader("Volatility Time Series (last 200 rows)")
    st.line_chart(df[["date", "annualized_vol_20d", "annualized_vol_60d"]].head(200).set_index("date"))


def _hist_bar_chart(series: pd.Series, bins: int = 30, title: str = "") -> None:
    """Render a histogram as a bar chart using pandas cut + value_counts."""
    counts, edges = pd.cut(series, bins=bins, include_lowest=True, retbins=True)
    vc = counts.value_counts().sort_index()
    vc_df = vc.rename("count").reset_index()
    vc_df.columns = ["bin", "count"]
    # Format bin labels as ranges
    vc_df["bin"] = vc_df["bin"].apply(lambda x: f"{x.left:.3f}-{x.right:.3f}")
    st.bar_chart(vc_df.set_index("bin")["count"])


def ar1_page(con):
    """v_ar1_time_series — AR(1) autoregressive model results."""
    st.header("AR(1) Time Series Model")
    st.caption("Data source: v_ar1_time_series | Interpretation: beta≈1 = random walk, beta≈0 = uncorrelated returns")

    ticker_filter = st.sidebar.text_input("Ticker (optional)", value="")

    query = "SELECT * FROM v_ar1_time_series"
    params = []
    if ticker_filter:
        query += " WHERE ticker = ?"
        params.append(ticker_filter.upper())
    query += " ORDER BY date DESC LIMIT 5000"

    df = con.execute(query, params).fetchdf()

    if df.empty:
        st.warning("No AR(1) data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Tickers", df["ticker"].nunique())
    with col2:
        avg_beta = df["beta_ar1"].mean()
        st.metric("Avg beta (mean reversion)", f"{avg_beta:.4f}" if avg_beta else "N/A")
    with col3:
        avg_r2 = df["r_squared_ar1"].mean()
        st.metric("Avg R-squared", f"{avg_r2:.6f}" if avg_r2 else "N/A")

    beta_series = df["beta_ar1"].dropna().tail(1000)
    st.subheader("Beta Distribution (last 1000 rows)")
    _hist_bar_chart(beta_series, bins=30)

    st.subheader("R-squared vs Beta (scatter)")
    scatter_df = df[["beta_ar1", "r_squared_ar1"]].dropna().tail(2000)
    st.scatter_chart(scatter_df.rename(columns={"beta_ar1": "Beta", "r_squared_ar1": "R-squared"}))

    st.subheader("Sample AR(1) Coefficients")
    display_df = df[["ticker", "date", "daily_return", "alpha_ar1", "beta_ar1", "r_squared_ar1", "n_obs"]].head(20)
    st.dataframe(display_df, use_container_width=True)

    st.info("**Interpretation:** beta≈1 means random walk (past returns don't predict future). beta≈0 means uncorrelated returns (white noise). |beta|<1 means deviations decay over time.")


def momentum_page(con):
    """v_momentum_signals — multi-period momentum + trend classification."""
    st.header("Momentum Signals")
    st.caption("Data source: v_momentum_signals")

    ticker_filter = st.sidebar.text_input("Ticker (optional)", value="")

    query = "SELECT * FROM v_momentum_signals"
    params = []
    if ticker_filter:
        query += " WHERE ticker = ?"
        params.append(ticker_filter.upper())
    query += " ORDER BY date DESC LIMIT 5000"

    df = con.execute(query, params).fetchdf()

    if df.empty:
        st.warning("No momentum data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total rows", len(df))
    with col2:
        up = (df["trend_signal"] == "STRONG_UPTREND").sum() + (df["trend_signal"] == "WEAK_UPTREND").sum()
        st.metric("Uptrend signals", up)
    with col3:
        down = (df["trend_signal"] == "STRONG_DOWNTREND").sum() + (df["trend_signal"] == "WEAK_DOWNTREND").sum()
        st.metric("Downtrend signals", down)

    st.subheader("Trend Signal Distribution")
    signal_counts = df["trend_signal"].value_counts()
    st.bar_chart(signal_counts)

    momentum_series = df["momentum_5d"].dropna().tail(1000)
    st.subheader("Momentum Distribution (5d)")
    _hist_bar_chart(momentum_series, bins=30)

    st.subheader("Sample Data")
    st.dataframe(df.head(20), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="SPX Gold Dashboard", layout="wide")
    st.title("SPX 500 Data Pipeline - Phase 6 Dashboard")
    st.caption("Source: DuckDB Gold Views")

    if not DB_PATH.exists():
        st.error(f"DuckDB not found: {DB_PATH}")
        st.stop()

    conn = get_connection()
    available_views, missing_views = check_views(conn)
    if missing_views:
        st.warning(
            "Missing Gold views: "
            + ", ".join(missing_views)
            + ". Run `python gold/build_gold_layer.py` first."
        )
    if not available_views:
        st.stop()

    ticker_profile = load_ticker_profile() if "v_ticker_profile" in available_views else pd.DataFrame()
    ticker_list = ticker_profile["ticker"].dropna().sort_values().unique().tolist() if not ticker_profile.empty else []

    st.sidebar.header("Controls")
    page = st.sidebar.selectbox(
        "Page",
        [
            "Overview",
            "Market Daily Summary",
            "Ticker Profile",
            "Fundamental History",
            "Sentiment Price View",
            "Sector Rotation",
            "Sentiment Analysis",
            "Volatility",
            "AR(1) Model",
            "Momentum",
        ],
    )
    ticker_option = st.sidebar.selectbox("Ticker (optional)", ["All"] + ticker_list)
    selected_ticker = None if ticker_option == "All" else ticker_option
    row_limit = st.sidebar.slider("Sentiment row limit", 100, 20000, 2000, step=100)

    market_daily = load_market_daily() if "v_market_daily_summary" in available_views else pd.DataFrame()
    fundamental_snapshot = (
        load_fundamental_snapshot() if "v_fundamental_snapshot" in available_views else pd.DataFrame()
    )
    sentiment_price = (
        load_sentiment_price(selected_ticker, row_limit)
        if "v_sentiment_price_view" in available_views
        else pd.DataFrame()
    )

    if page == "Overview":
        render_overview(market_daily, ticker_profile, fundamental_snapshot, sentiment_price)
    elif page == "Market Daily Summary":
        render_market_daily(market_daily)
    elif page == "Ticker Profile":
        render_ticker_profile(ticker_profile, selected_ticker)
    elif page == "Fundamental History":
        render_fundamental_history(conn, ticker_list)
    elif page == "Sentiment Price View":
        render_sentiment_price(sentiment_price)
    elif page == "Sector Rotation":
        sector_rotation_page(conn)
    elif page == "Sentiment Analysis":
        sentiment_page(conn)
    elif page == "Volatility":
        volatility_page(conn)
    elif page == "AR(1) Model":
        ar1_page(conn)
    elif page == "Momentum":
        momentum_page(conn)


if __name__ == "__main__":
    main()
