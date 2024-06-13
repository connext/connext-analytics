import re
import streamlit as st
import pandas as pd
import plotly.express as px
from setup import ROUTER_UTILIZATION_RAW, apply_sidebar_filters


def clean_df(data):

    cleaned_data = data.dropna()
    return cleaned_data


def main():
    st.title("Router Liquidty Experiments")
    filter_data = apply_sidebar_filters(ROUTER_UTILIZATION_RAW)
    st.write(filter_data)

    return filter_data


if __name__ == "__main__":
    main()
