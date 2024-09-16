import streamlit as st
import plotly.express as px
from setup import (
    ROUTER_DAILY_METRICS_RAW,
    apply_sidebar_filters,
    clean_df,
)


def weighted_mean(data, val_col, wt_col):
    return (data[val_col] * data[wt_col]).sum() / data[wt_col].sum()


def plot_line_metrics(df, metric_name):

    metric_selector = {
        "TVL": "total_balance_usd",
        "APR": "apr",
        "APR-7D": "apr_7d",
        "APR-14D": "apr_14d",
        "FEE": "router_fee_usd",
        "Utilization": "utilization",
        "Volume": "router_volume_usd",
    }
    aggregation_selector = {
        "TVL": "sum",
        "APR": "mean",
        "APR-7D": "mean",
        "APR-14D": "mean",
        "FEE": "sum",
        "Utilization": "mean",
        "Volume": "sum",
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
        fig.update_layout(yaxis=dict(autorange=True))

    st.plotly_chart(fig, use_container_width=True)


def main():

    st.title("Router Metrics and Utilizations")

    filter_data = apply_sidebar_filters(ROUTER_DAILY_METRICS_RAW)
    new_agg_filtered_data_router_metrics = clean_df(filter_data)

    st.subheader("Daily Avg. APR Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "APR")

    st.subheader("Running 7-day Avg. APR Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "APR-7D")

    st.subheader("Running 14-day Avg. APR Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "APR-14D")

    st.subheader("Agg. Fee Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "FEE")

    st.subheader("Agg. TVL Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "TVL")

    # utilization
    st.subheader("Agg. Utilization Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "Utilization")

    # Router Volume
    st.subheader("Agg. Volume Across Routers")
    plot_line_metrics(new_agg_filtered_data_router_metrics, "Volume")

    st.markdown("#### Raw Data")
    st.text(f"Cleaned Data Columns: {new_agg_filtered_data_router_metrics.columns}")
    st.write(ROUTER_DAILY_METRICS_RAW)


if __name__ == "__main__":
    main()
