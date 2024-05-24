import streamlit as st
import pandas as pd
import pandas_gbq as gbq


@st.cache_data(ttl=3600)
def get_raw_data_from_bq_df(sql_file_name) -> pd.DataFrame:
    with open(f"src/streamlit/sql/{sql_file_name}.sql", "r") as file:
        sql = file.read()
    return gbq.read_gbq(sql)
