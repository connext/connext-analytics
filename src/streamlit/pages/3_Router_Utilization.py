import re
import streamlit as st
import pandas as pd
import plotly.express as px
from setup import ROUTER_UTILIZATION_RAW, apply_sidebar_filters


def clean_utilization_data(data, agg_freq):

    new_data = data[(data["asset_group"] != "") & (data["chain"] != "")]

    # replace router_volume_usd where there is 0 with null
    new_data["router_volume_usd"] = new_data["router_volume_usd"].replace(0, None)
    # st.write(new_data)

    # get last value of total_balance_usd for each group
    new_data["total_balance_usd"] = new_data.groupby(
        ["datetime", "chain", "asset", "router"]
    )["total_balance_usd"].transform("last")
    if agg_freq == "H":
        daily_avg = new_data.groupby(["day", "chain", "asset_group", "hour"]).agg(
            {
                "slow_volume_usd": "sum",
                "router_fee_usd": "sum",
                "router_volume_usd": "sum",
                "total_balance_usd": "last",
            }
        )
    else:
        daily_avg = data.groupby(["day", "chain", "asset_group"]).agg(
            {
                "slow_volume_usd": "sum",
                "router_fee_usd": "sum",
                "router_volume_usd": "sum",
                "total_balance_usd": "last",
            }
        )

    # calculate utilization: SUM(volume)/SUM(locked_usd) AS utilization_last_1_day,
    daily_avg["utilization"] = (
        daily_avg["router_volume_usd"] / daily_avg["total_balance_usd"]
    )

    # get total row count
    daily_avg["total_count"] = 1

    # utlization filter: if > 1 then 1 if < 0 then 0
    daily_avg["utilization_percentage"] = daily_avg["utilization"].apply(
        lambda x: 1 if x > 1 else 0 if x < 0 else x
    )
    # convert utilization to percentage
    daily_avg["utilization_percentage"] = daily_avg["utilization_percentage"] * 100

    # get % of rows for each day when utilization is above 1 and below 0
    daily_avg["anomoly_count"] = daily_avg["utilization"].apply(
        lambda x: 1 if x > 1 or x < 0 else 0
    )

    daily_avg["utilization_anomoly"] = (
        daily_avg["anomoly_count"] / daily_avg["total_count"]
    )
    daily_avg["utilization_anomoly_percentage"] = daily_avg["utilization_anomoly"] * 100

    # create utilization capacity levels: >0.6,0.6-0.8, >0.8
    # create 4 bins for utilization capacity levels
    bins = [-0.1, 25, 50, 75, 100]
    labels = ["<25%", "25%-50%", "50%-75%", "75%-100%"]
    daily_avg["utilization_capacity_levels"] = pd.cut(
        daily_avg["utilization_percentage"],
        bins=bins,
        labels=labels,
    )

    return daily_avg.reset_index()


def plot_daily_utilization(data, metric):
    fig = px.line(
        data,
        x="day",
        y=metric,
        color="chain",
        facet_col="asset_group",
        # title=f"Daily Utilization - {metric}",
    )
    # Update legend to show at the top
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=1.5, xanchor="center", x=0.5)
    )

    # metric_title = metric.replace("_", " ")
    # fig.update_layout(
    #     title=" ".join(word.capitalize() for word in metric.split("_"))
    # )
    # Update y-axis to show percentage
    fig.update_layout(yaxis=dict(ticksuffix="%"))

    # Update legend to show percentage
    fig.for_each_trace(lambda trace: trace.update(name=f"{trace.name} (%)"))
    st.plotly_chart(fig)


def utilization_heatmap(data, metric):
    """data: Raw data that will be pivoted and then plotted as a heatmap"""

    # Pivot data for heatmap
    data_agg = data.groupby(["hour", "day", "chain", "asset_group"]).agg(
        {metric: "mean"}
    )
    pivot_data = data_agg.pivot_table(
        index="hour", columns=["day", "chain", "asset_group"], values=metric
    )

    pivot_data.columns = ["_".join([str(c) for c in col]) for col in pivot_data.columns]

    # Reset the index to prepare for melting
    pivot_data_reset = pivot_data.reset_index()

    # Melt the pivot table to long format
    pivot_long = pivot_data_reset.melt(
        id_vars=["hour"], var_name="variable", value_name=metric
    ).dropna()

    # Split the 'variable' column into separate columns for date, chain, and asset_group
    pivot_long[["day", "chain", "asset_group"]] = pivot_long["variable"].str.split(
        "_", expand=True
    )

    # Create heatmap

    fig = px.density_heatmap(
        pivot_long,
        x="day",
        y="hour",
        z=metric,
        histfunc="avg",
        facet_col="chain",
        facet_row="asset_group",
        color_continuous_scale="Blues",
        labels={metric: "Utilization (%)"},
        height=1000,
        width=2000,
    )
    fig.update_layout(coloraxis_colorbar=dict(title="Utilization (%)"))
    fig.update_yaxes(title="")
    fig.update_xaxes(title="", tickangle=45, mirror="ticks")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    st.plotly_chart(fig)


def plot_capacity_levels(data):

    total_count_per_group = (
        data.groupby(["chain", "asset_group"]).size().reset_index(name="total_count")
    )

    # Aggregate the data by chain, asset_group, and utilization_capacity_levels
    data_agg = (
        data[["chain", "asset_group", "utilization_capacity_levels", "day"]]
        .groupby(["chain", "asset_group", "utilization_capacity_levels"])
        .count()
        .reset_index()
    )
    data_agg.rename(columns={"day": "utilization_capacity_levels_count"}, inplace=True)

    # Merge the total count per group with the aggregated data
    data_agg = data_agg.merge(total_count_per_group, on=["chain", "asset_group"])

    # Calculate the percentage of count from the total instances for each group
    data_agg["utilization_capacity_levels_percentage"] = (
        data_agg["utilization_capacity_levels_count"] / data_agg["total_count"] * 100
    )

    fig = px.histogram(
        data_agg,
        x="chain",
        y="utilization_capacity_levels_percentage",
        facet_row="utilization_capacity_levels",
        facet_col="asset_group",
        histfunc="avg",
        height=500,
        width=2000,
        facet_row_spacing=0.1,
        facet_col_spacing=0.1,
    )

    fig.update_yaxes(title="", mirror="ticks", side="left")
    fig.update_xaxes(title="", tickangle=45, mirror="ticks")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    st.plotly_chart(fig)


def plot_anomoly_count(data):
    data_agg = data.groupby(["chain", "asset_group"], as_index=False).agg(
        {"anomoly_count": "sum"}
    )
    fig = px.bar(data_agg, x="asset_group", y="anomoly_count", facet_col="chain")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    st.plotly_chart(fig)


def main():
    st.title("Router Utilization Dashboard")
    filter_data = apply_sidebar_filters(ROUTER_UTILIZATION_RAW)

    filter_data_daily = clean_utilization_data(filter_data, agg_freq="D")
    filter_data_hourly = clean_utilization_data(filter_data, agg_freq="H")

    # ----------------- DAILY UTILIZATION ----------------- #
    st.subheader("Daily Avg. Utilization")
    st.text(
        "Utilization is the ratio of the total volume of a router over the total locked value"
    )
    plot_daily_utilization(filter_data_daily, "utilization_percentage")

    # ----------------- DAILY UTILIZATION CAPACITY LEVELS ----------------- #
    st.subheader("Daily Avg. Utilization Capacity Levels")
    st.markdown(
        """
    Utilization capacity levels are the percentage of the total locked value that is utilized.
    Its the percentage of utilization levels in a given time period. It shows, What % of times
    the given token<> asset pair is below to particular Utilization group (25%, 50%, 75%, 100%)

    #### Capacity Levels:
    - **<25%: Under Utilized**
    - **25%-50%: Moderately Under Utilized**
    - **50%-75%: Moderately Utilized**
    - **>75%: Over Utilized**
    """
    )
    plot_capacity_levels(filter_data_daily)

    # ----------------- HOURLY UTILIZATION HEATMAP ----------------- #
    st.subheader("Hourly Utilization Heatmap")
    st.markdown(
        """
    Heatmap shows the utilization for each hour of the day for each asset<> Chain pair.
    The color scale is from 0-100% and the darker the color, the higher the utilization percentage.
    Here the Metric is Utilization (Absolute %).

    **Definition:** Ratio of the total volume over the total locked value, for selected filters.
    
    **Formula:** for every Hour, asset<>chain pair
    ```
    (SUM(Router Volume) / SUM(Total Available Locked Value)) * 100%
    ```
    """
    )
    utilization_heatmap(filter_data_hourly, "utilization_percentage")

    # ----------------- DAILY UTILIZATION ANOMOLY COUNT ----------------- #
    st.subheader("Daily Avg. Utilization Anomoly")
    st.text(
        "Anomoly count is the number of days where the utilization is above 100% or below 0%"
    )
    plot_anomoly_count(filter_data_daily)

    # ---Raw Data---#
    st.subheader("Daily Raw Data")
    st.write(filter_data_daily)
    return None


if __name__ == "__main__":
    main()
