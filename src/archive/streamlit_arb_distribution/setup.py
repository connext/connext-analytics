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
    with open(
        f"src/streamlit_arb_distribution/sql/{sql_file_name}.sql",
    ) as file:
        sql = file.read()
    return gbq.read_gbq(sql)


def apply_universal_sidebar_filters(df, date_col="date", xcaller_col="xcall_caller"):
    """
    Apply universal sidebar filters to the dataframe
    Filters applied and columns needed in dataframe:
    - asset_group
    - bridge
    - chain
    - date
    """
    st.sidebar.header("Filters")
    st.sidebar.subheader("Time Range Picker")

    # last 14 days
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=7),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    from_date = st.sidebar.date_input(
        "Start Date(inclusive)",
        value=default_start,
        max_value=default_end,
        key="start_date",
    )
    to_date = st.sidebar.date_input(
        "End Date(inclusive)",
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
    return df


page_settings()
# Data
ARB_DISTRIBUTION_DATA = get_raw_data_from_bq_df("arb_distribution")
ARB_HOURLY_PRICE_DATA = get_raw_data_from_bq_df("arb_distribution_hourly_price")
