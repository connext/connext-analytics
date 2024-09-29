# Adding the streamlit pages to the sidebar
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_gbq as gbq
import pytz
import streamlit as st


def page_settings():
    st.set_page_config(layout="wide")


@st.cache_data(ttl=86400)
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
    with open(f"src/streamlit_netflow/sql/{sql_file_name}.sql") as file:
        sql = file.read()
    return gbq.read_gbq(sql)


def apply_universal_sidebar_filters(df, date_col="date"):
    """
    Apply universal sidebar filters to the dataframe
    Filters applied and columns needed in dataframe:
    - asset_group
    - bridge
    - chain
    - date
    """
    st.sidebar.header("Filters")

    selected_asset = st.sidebar.multiselect(
        "Tokens/Assets:",
        options=df["asset_group"].unique(),
        default=["WETH", "USDC", "USDT", "DAI"],
        key="asset",
    )

    selected_bridges = st.sidebar.multiselect(
        "Bridges:", options=df["bridge"].unique(), default=[], key="bridge"
    )

    st.sidebar.subheader("Time Range Picker")

    # last 30 days
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=31),
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
        if df[date_col].dtype == "O":
            df["datetime"] = pd.to_datetime(df[date_col])
        else:
            df["datetime"] = df[date_col]
        df["day"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df = df[(df["day"] >= start_date) & (df["day"] <= end_date)]

    selected_chain = st.sidebar.multiselect(
        "Chains:", options=df["chain"].unique(), default=[], key="chain"
    )

    if selected_chain:
        df = df[df["chain"].isin(selected_chain)]
    if selected_bridges:
        df = df[df["bridge"].isin(selected_bridges)]
    if selected_asset:
        df = df[df["asset_group"].isin(selected_asset)]

    return df


def netting_calculations(df):
    """
    Calcuations:
        # amount netted = inflow - outflow
        # % netted = (amount netted / volume) * 100
        # add a column for % netted
        # return the df
    """
    # df["pct_netted"] = 100 - abs(
    #     (df["inflow"].fillna(0) - df["outflow"].fillna(0)) / df["volume"]
    # )
    df["pct_netted"] = (
        100
        - abs(
            (df["inflow"].fillna(0) - df["outflow"].fillna(0))
            / (df["inflow"].fillna(0) + df["outflow"].fillna(0)).replace(0, np.nan)
        )
        * 100
    )
    df["netted"] = abs(df["inflow"].fillna(0) - df["outflow"].fillna(0))
    return df


def get_df_by_netting_window(df_data, netting_window, group_by):
    """
    INPUT: Raw dataframe
    netting_window: 1H, 3H, 6H, 12H, 1D
    group_by: asset_group, chain, bridge
    - based on group by values provided add the group by column to the group by list
    OUTPUT: new GROUPED data based on the netting window provided

    """
    df = df_data.copy()

    # Convert 'date' column to datetime if not already
    df["date"] = pd.to_datetime(df["date"])

    # Define the frequency mapping
    freq_map = {
        "1-Hour": "H",
        "3-Hour": "3H",
        "6-Hour": "6H",
        "12-Hour": "12H",
        "1-Day": "D",
    }

    # Get the frequency from the mapping, Default to hourly if not found
    freq = freq_map.get(netting_window, "H")
    if isinstance(group_by, str):
        group_by = [group_by]

    var_group_by = ["chain", "asset_group", *group_by]
    # remove duplicate from var_group_by
    var_group_by = list(set(var_group_by))

    df_clean = (
        df.set_index("date")
        .groupby([pd.Grouper(freq=freq), *var_group_by])
        .agg({"inflow": "sum", "outflow": "sum", "volume": "sum"})
        .reset_index()
    )

    df_clean = netting_calculations(df_clean)

    # get Daily avg based on the new aggreagtions

    df_final = (
        df_clean.set_index("date")
        .groupby([pd.Grouper(freq="D"), *group_by])
        .agg({"pct_netted": "mean", "netted": "sum", "volume": "sum"})
    ).rename(
        columns={
            "pct_netted": "avg_pct_netted",
            "netted": "total_netted",
            "volume": "total_volume",
        }
    )
    df_final = df_final[df_final["avg_pct_netted"] > 0]
    return df_final.reset_index()


page_settings()
# Data
ALL_BRIDGES_HOURLY_DATA = get_raw_data_from_bq_df("all_bridges_netflow__hourly")
ALL_CONNEXT_TXS = get_raw_data_from_bq_df("batching_netflow_raw_txs")
