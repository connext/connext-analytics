# Things to implement on this page:
# Router Metrics and Utilizations
import numpy as np
from annotated_types import UpperCase
from numpy import mean
import pytz
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px

# Raw Data
from utility import display_data, ROUTER_DAILY_METRICS_RAW, apply_sidebar_filters


def weighted_mean(data, val_col, wt_col):
    return (data[val_col] * data[wt_col]).sum() / data[wt_col].sum()


def plot_line_metrics(df, metric_name):

    metric_selector = {
        "TVL": "total_balance_usd",
        "APR": "apr",
        "APR-7D": "apr_7d",
        "APR-14D": "apr_14d",
        "FEE": "router_fee_usd",
    }
    aggregation_selector = {
        "TVL": "sum",
        "APR": "mean",
        "APR-7D": "mean",
        "APR-14D": "mean",
        "FEE": "sum",
    }

    df_agg = df[["date", "asset_group", "chain", metric_selector[metric_name]]]

    # tvl -> sum and apr -> avg
    df_agg = df_agg.groupby(["date", "asset_group"]).agg(
        {metric_selector[metric_name]: aggregation_selector[metric_name]}
    )
    df_agg.reset_index(inplace=True)

    # rename cols
    df_agg.rename(columns={metric_selector[metric_name]: metric_name}, inplace=True)

    # plot
    fig = px.line(
        df_agg,
        x="date",
        y=metric_name,
        color="asset_group",
        title=f"{metric_name} Over Time",
        labels={"date": "Date", metric_name: metric_name},
        markers=True,
    )
    # Set y-axis as percentage from 0 to 100 if metric is APR related
    if "APR" in metric_name:
        fig.update_layout(
            yaxis=dict(range=[1, 100])  # 0 to 1 corresponds to 0% to 100%
        )

    st.plotly_chart(fig, use_container_width=True)


def clean_df(df):
    """THis is agg data on chain asset for all router combined"""
    df_clean = (
        df.groupby(["date", "asset_group", "chain"])
        .agg({"total_balance_usd": "sum", "router_fee_usd": "sum"})
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

    return df_clean.reset_index(drop=True)


def main():

    st.title("Router Metrics and Utilizations")

    st.write(ROUTER_DAILY_METRICS_RAW)
    filter_data = apply_sidebar_filters(ROUTER_DAILY_METRICS_RAW)

    new_agg_filtered_data = clean_df(filter_data)

    st.text(f"Cleaned Data Columns: {new_agg_filtered_data.columns}")

    st.subheader("Daily Avg. APR Across Routers")
    plot_line_metrics(new_agg_filtered_data, "APR")

    st.subheader("Daily Avg. APR-7D Across Routers")
    plot_line_metrics(new_agg_filtered_data, "APR-7D")

    st.subheader("Daily Avg. APR-14D Across Routers")
    plot_line_metrics(new_agg_filtered_data, "APR-14D")

    st.subheader("Daily Agg. Fee Across Routers")
    plot_line_metrics(new_agg_filtered_data, "FEE")

    st.subheader("Daily Avg. TVL Across Routers")
    plot_line_metrics(new_agg_filtered_data, "TVL")

if __name__ == "__main__":
    main()
