import pytz
import streamlit as st
import pandas as pd
import pandas_gbq as gbq
from datetime import datetime, timedelta

st.set_page_config(layout="wide")
st.title("Connext Routers")


@st.cache_data(ttl=3600)
def get_raw_data_from_bq_df(sql_file_name) -> pd.DataFrame:
    """
    Get raw data from BigQuery
    Cols included are
    - date
    - router_address
    - chain
    - asset
    - tvl
    - daily_fee_earned
    - total_fee_earned
    - daily_liquidity_added
    - router_locked_total
    - calculated_router_locked_total
    - total_balance
    - daily_apr
    """
    with open(f"src/streamlit/sql/{sql_file_name}.sql", "r") as file:
        sql = file.read()
    return gbq.read_gbq(sql)


def apply_sidebar_filters(df):
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
        "Routers:", options=df["router_name"].unique(), default=[]
    )
    selected_asset = st.sidebar.multiselect(
        "Tokens/Assets:", options=df["asset_group"].unique(), default=[]
    )

    if selected_chain:
        df = df[df["chain"].isin(selected_chain)]
    if selected_router:
        df = df[df["router_name"].isin(selected_router)]
    if selected_asset:
        df = df[df["asset_group"].isin(selected_asset)]

    return df


def display_data(filtered_data):
    st.markdown("## Raw data:")
    st.data_editor(
        filtered_data,
        hide_index=True,
        # column_config={
        #     "date": "Date",
        #     "router_address": "Router Address",
        #     "chain": "Chain",
        #     "asset": "Asset",
        #     "tvl": "TVL",
        #     "daily_fee_earned": "Daily Fee Earned",
        #     "total_fee_earned": "Total Fee Earned",
        #     "daily_liquidity_added": "Daily Liquidity movement(+/-)",
        #     "router_locked_total": "Router Locked Total",
        #     "calculated_router_locked_total": "Calculated-Router Locked Total",
        #     "balance": "Balance",
        # },
    )


ROUTER_DAILY_METRICS_RAW = get_raw_data_from_bq_df("router_daily_metrics")
