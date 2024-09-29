# Adding the streamlit pages to the sidebar
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq as gbq
import pytz
import streamlit as st

st.set_page_config(layout="wide")


# TODO: [ ] - Add a cache for the dataframes to 3600 seconds lateer
@st.cache_data(ttl=36000)
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
    with open(f"src/streamlit/sql/{sql_file_name}.sql") as file:
        sql = file.read()
    return gbq.read_gbq(sql)


def apply_sidebar_filters(df):
    st.sidebar.header("Filters")

    st.sidebar.subheader("Time Range Picker")
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=15),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    from_date = st.sidebar.date_input(
        "Start Date", value=default_start, max_value=default_end, key="start_date"
    )
    to_date = st.sidebar.date_input(
        "End Date",
        value=default_end,
        min_value=default_start,
        max_value=default_end,
        key="end_date",
    )

    if from_date and to_date:
        start_date, end_date = from_date, to_date
        df["datetime"] = pd.to_datetime(df["date"])
        df["day"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df = df[(df["day"] >= start_date) & (df["day"] <= end_date)]

    selected_chain = st.sidebar.multiselect(
        "Chains:", options=df["chain"].unique(), default=[], key="chain"
    )
    selected_router = st.sidebar.multiselect(
        "Routers:", options=df["router_name"].unique(), default=[], key="router"
    )
    selected_asset = st.sidebar.multiselect(
        "Tokens/Assets:", options=df["asset_group"].unique(), default=[], key="asset"
    )

    if selected_chain:
        df = df[df["chain"].isin(selected_chain)]
    if selected_router:
        df = df[df["router_name"].isin(selected_router)]
    if selected_asset:
        df = df[df["asset_group"].isin(selected_asset)]

    return df


def clean_df(df):
    """THis is agg data on chain asset for all router combined"""
    df_clean = (
        df.groupby(["date", "asset_group", "chain"])
        .agg(
            {
                "total_balance_usd": "sum",
                "router_fee_usd": "sum",
                "router_volume_usd": "sum",
            }
        )
        .reset_index()
    )
    df_clean["date"] = pd.to_datetime(df_clean["date"])
    # Calculate APR -> remove data points where there is no locked amount
    df_clean = df_clean[df_clean["total_balance_usd"] > 0]
    df_clean["apr"] = (
        df_clean["router_fee_usd"].fillna(0) / df_clean["total_balance_usd"]
    ) * 365
    df_clean["apr"] = round(100 * df_clean["apr"], 2)

    # 7-d running avg of APR and 14-d running avg of APR
    df_clean["apr_7d"] = df_clean["apr"].rolling(7).mean()
    df_clean["apr_14d"] = df_clean["apr"].rolling(14).mean()

    # calculate utilization: SUM(volume)/SUM(locked_usd) AS utilization_last_1_day,
    df_clean["utilization"] = (
        df_clean["router_volume_usd"] / df_clean["total_balance_usd"]
    )

    return df_clean.reset_index(drop=True)


def clean_slow_volume_data(data):
    data["router_name"] = "slow_path"
    return data


# def combine_raw_daily_with_slow(raw_df, slow_df):

#     # new df: ROUTER_DAILY_METRICS_RAW concat with slow_volume_raw
#     slow = slow_df.copy()
#     slow["date"] = pd.to_datetime(slow["date"])
#     slow["date"] = slow["date"].dt.date
#     slow_raw_daily = slow.groupby(
#         ["date", "chain", "asset_group", "asset", "router_name"]
#     ).agg({"destination_slow_volume_usd": "sum"})
#     slow_raw_daily = slow_raw_daily.reset_index()
#     raw_df["date"] = pd.to_datetime(raw_df["date"])
#     raw_df["date"] = raw_df["date"].dt.date
#     daily_slow_fast = pd.concat([raw_df, slow_raw_daily])
#     return daily_slow_fast


def convert_hourly_utlilization_data_to_daily(hourly_df):
    df = hourly_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.date
    df = df.groupby(["date", "chain", "asset_group", "asset", "router_name"]).agg(
        {
            "total_locked_usd": "mean",
            "router_fee_usd": "sum",
            "router_volume_usd": "sum",
            "total_balance_usd": "mean",
            "destination_slow_volume_usd": "sum",
        }
    )
    return df.reset_index()


# Data
ROUTER_DAILY_METRICS_RAW = get_raw_data_from_bq_df("router_daily_metrics")

ROUTER_UTILIZATION_RAW = get_raw_data_from_bq_df("router_utilization_hourly")
SLOW_VOLUME_RAW = get_raw_data_from_bq_df("raw_slow_path_volume__hourly")
SLOW_VOLUME_RAW = clean_slow_volume_data(SLOW_VOLUME_RAW)


# concat hourly slow data with utilization hourly
ROUTER_UTILIZATION_RAW_SLOW = pd.concat([ROUTER_UTILIZATION_RAW, SLOW_VOLUME_RAW])

# ROUTER_UTILIZATION_RAW from hourly to daily from utliation data
ROUTER_DAILY_METRICS_RAW_SLOW = convert_hourly_utlilization_data_to_daily(
    ROUTER_UTILIZATION_RAW_SLOW
)
