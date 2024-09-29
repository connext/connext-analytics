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
    """
    with open(
        f"src/streamlit_bridge_comparisons/sql/{sql_file_name}.sql",
    ) as file:
        sql = file.read()
    return gbq.read_gbq(sql)


page_settings()
# Data
bridge_comparisons_raw = get_raw_data_from_bq_df("bridge_comparisons_raw")
