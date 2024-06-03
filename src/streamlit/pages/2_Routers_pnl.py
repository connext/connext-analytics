# Things to implement on this page:
"""
    using the data pulled in home for router_daily_metrics__raw
    1. create router single select filter in side bar, keep othe fiilters same
    2. create multi line plot in plotly for tvl, apy and others
    3. create a aggreagrte daily line plot of balance, tvl apr and others
"""

import pytz
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px

# Raw Data
from utility import display_data, ROUTER_DAILY_METRICS_RAW


def apply_router_metrics_sidebar_filters(df):
    st.sidebar.header("Filters")

    st.sidebar.subheader("Time Range Picker")
    # skip today

    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=8),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    from_date = st.sidebar.date_input(
        "Start Date",
        value=default_start,
        min_value=default_start,
        max_value=default_end,
    )
    to_date = st.sidebar.date_input(
        "End Date",
        value=default_end,
        min_value=default_start,
        max_value=default_end,
    )

    if from_date is not None and to_date is not None:
        start_date, end_date = from_date, to_date

        # convert date to isoformat
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

    # Other filters (Chain, Router, Asset)
    selected_chain = st.sidebar.multiselect(
        "Chains:", options=df["chain"].unique(), default=[]
    )
    selected_router = st.sidebar.multiselect(
        "Routers:", options=df["router_address"].unique(), default=[]
    )
    selected_asset = st.sidebar.multiselect(
        "Tokens/Assets:", options=df["asset"].unique(), default=[]
    )

    if selected_chain:
        df = df[df["chain"].isin(selected_chain)]
    if selected_router:
        df = df[df["router_address"].isin(selected_router)]
    if selected_asset:
        df = df[df["asset"].isin(selected_asset)]

    return df


def main():
    df = display_data(ROUTER_DAILY_METRICS_RAW)
    filtered_data = apply_router_metrics_sidebar_filters(df)


# Example DataFrame
# df = pd.DataFrame(...)

fig = px.line(df, x="date", y=["tvl", "apy"], title="TVL and APY Over Time")
st.plotly_chart(fig, use_container_width=True)


# Assuming df is time-indexed
daily_df = df.resample("D").sum()  # or any other aggregation
fig = px.line(
    daily_df, x=daily_df.index, y=["balance", "tvl", "apr"], title="Daily Metrics"
)
st.plotly_chart(fig, use_container_width=True)
