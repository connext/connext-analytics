# Adding the streamlit pages to the sidebar
import numpy as np
import pytz
import pandas as pd
import streamlit as st
import pandas_gbq as gbq
from datetime import datetime, timedelta


def page_settings():
    st.set_page_config(layout="wide")


@st.cache_data(ttl=86400)
def get_raw_data_from_bq_df(sql_file_name) -> pd.DataFrame:
    """
    Get raw data from BigQuery
    """
    with open(
        f"src/streamlit_bridge_comparisons/sql/{sql_file_name}.sql",
        "r",
    ) as file:
        sql = file.read()
    return gbq.read_gbq(sql)


page_settings()
# Data
bridge_comparisons_raw = get_raw_data_from_bq_df("bridge_comparisons_raw")
