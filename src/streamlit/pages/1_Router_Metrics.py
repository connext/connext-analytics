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
        "TVL": "weekly_balance",
        "APR": "weekly_apr",
        "APR-7D": "avg_7d_apr",
        "APR-14D": "avg_14d_apr",
        "FEE": "weekly_fee_earned",
    }
    aggregation_selector = {
        "TVL": "sum",
        "APR": "mean",
        "APR-7D": "mean",
        "APR-14D": "mean",
        "FEE": "sum",
    }

    df_agg = df[["date", "asset", metric_selector[metric_name]]]

    # tvl -> sum and apr -> avg
    df_agg = df_agg.groupby(["date", "asset"]).agg(
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
        color="asset",
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


def clean_asset_names(df):
    # Remove 'next' from the start of the strings in the 'asset' column
    df["asset"] = df["asset"].str.replace(r"^next", "", regex=True)
    # aggreagte meric for new changes

    df_clean = (
        df.groupby(["date", "asset", "chain", "router_address"])
        .agg({"tvl": "sum", "daily_fee_earned": "sum", "balance": "sum"})
        .reset_index()
    )

    df_clean["date"] = pd.to_datetime(df_clean["date"])

    # Set the 'date' column as the index
    df_clean.set_index("date", inplace=True)

    # Resample the data to weekly frequency
    weekly_data = (
        df_clean.groupby("asset")
        .resample("W")
        .agg({"daily_fee_earned": "sum", "balance": "sum"})
        .rename(
            columns={
                "daily_fee_earned": "weekly_fee_earned",
                "balance": "weekly_balance",
            }
        )
    )

    # Calculate the weekly APR
    weekly_data["weekly_apr"] = np.where(
        weekly_data["weekly_balance"] > 0,
        (weekly_data["weekly_fee_earned"] / weekly_data["weekly_balance"]) * 52,
        None,
    )

    # Convert to percentage and round
    weekly_data["weekly_apr"] = round(100 * weekly_data["weekly_apr"], 2)

    # Reset the index if needed
    weekly_data.reset_index(inplace=True)

    # df_clean["weekly_apr"] = np.where(
    #     df_clean["balance"] > 0,
    #     (df_clean["daily_fee_earned"] / df_clean["balance"]) * 365,
    #     None,
    # )
    # df_clean["daily_apr"] = round(100 * df_clean["daily_apr"], 2)
    return weekly_data


def main():

    st.title("Router Metrics and Utilizations")

    filtered_data = apply_sidebar_filters(ROUTER_DAILY_METRICS_RAW)

    new_agg_filtered_data = clean_asset_names(filtered_data)
    st.text(new_agg_filtered_data.columns)

    st.data_editor(new_agg_filtered_data)

    st.subheader("Daily Avg. APR Across Routers")
    plot_line_metrics(new_agg_filtered_data, "APR")

    st.subheader("Daily Agg. Fee Across Routers")
    plot_line_metrics(new_agg_filtered_data, "FEE")

    st.subheader("Daily Avg. TVL Across Routers")
    plot_line_metrics(new_agg_filtered_data, "TVL")

    # new_agg_filtered_data

    # st.subheader("Aggregated APR")
    # plot_line_metrics(filtered_data, "APR")

    # st.subheader("Aggregated APR-7D")
    # plot_line_metrics(filtered_data, "APR-7D")

    # st.subheader("Aggregated APR-14D")
    # plot_line_metrics(filtered_data, "APR-14D")


if __name__ == "__main__":
    main()
