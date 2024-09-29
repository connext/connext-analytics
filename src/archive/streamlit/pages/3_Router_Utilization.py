import re

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from setup import (ROUTER_UTILIZATION_RAW, ROUTER_UTILIZATION_RAW_SLOW,
                   apply_sidebar_filters)


def router_utlization_metrics(data):
    new_data = data[(data["asset_group"] != "") & (data["chain"] != "")].copy()
    new_data = new_data[~new_data["router"].isna()]

    # replace router_volume_usd where there is 0 with null
    new_data["router_volume_usd"] = new_data["router_volume_usd"].replace(0, None)
    # st.write(new_data)

    # get last value of total_balance_usd for each group
    new_data["total_locked_usd"] = new_data.groupby(
        ["datetime", "chain", "asset", "router"]
    )["total_locked_usd"].transform("last")

    hourly_avg = (
        new_data.groupby(["day", "router"])
        .agg(
            {
                "router_fee_usd": "sum",
                "router_volume_usd": "sum",
                "total_locked_usd": "sum",
            }
        )
        .reset_index()
    )
    hourly_avg["router_utilization"] = hourly_avg["router_volume_usd"] / hourly_avg[
        "total_locked_usd"
    ].replace(0, None)
    hourly_avg.loc[hourly_avg["total_locked_usd"] < 0, "router_utilization"] = 1

    by_router_utilization = hourly_avg[["day", "router", "router_utilization"]]
    # st.write(hourly_avg)

    router_utilization_avg = hourly_avg.groupby("day").agg(
        {
            "router_utilization": "mean",
            "router_volume_usd": "sum",
            "total_locked_usd": "sum",
        }
    )

    # keep only utilizations
    router_utilization_avg = router_utilization_avg[
        ["router_utilization"]
    ].reset_index()
    router_utilization_avg["router"] = "avg"

    # total utilization
    total_utilization = (
        new_data.groupby("day")
        .agg(
            router_volume_usd_mean=("router_volume_usd", "mean"),
            router_volume_usd_sum=("router_volume_usd", "sum"),
            total_locked_usd_mean=("total_locked_usd", "mean"),
            total_locked_usd_sum=("total_locked_usd", "sum"),
        )
        .reset_index()
    )

    total_utilization["avg_router_utilization"] = (
        total_utilization["router_volume_usd_mean"]
        / total_utilization["total_locked_usd_mean"]
    )

    total_utilization["total_router_utilization"] = (
        total_utilization["router_volume_usd_sum"]
        / total_utilization["total_locked_usd_sum"]
    )

    avg_total_utilization = total_utilization.copy()
    avg_total_utilization["router"] = "total_avg"
    # rename column
    avg_total_utilization.rename(
        columns={"avg_router_utilization": "router_utilization"}, inplace=True
    )

    avg_total_utilization = avg_total_utilization[
        ["day", "router", "router_utilization"]
    ]
    # st.write(avg_total_utilization)
    total_utilization["router"] = "total"
    total_utilization = total_utilization[["day", "router", "total_router_utilization"]]
    # rename column
    total_utilization.rename(
        columns={"total_router_utilization": "router_utilization"}, inplace=True
    )
    # st.write(total_utilization)

    # add all rows to the dataframe

    router_utilization_avg = pd.concat(
        [
            router_utilization_avg,
            by_router_utilization,
            total_utilization,
            avg_total_utilization,
        ]
    )
    # multiple by 100

    router_utilization_avg["router_utilization"] = (
        router_utilization_avg["router_utilization"] * 100
    )
    # if router_utilization > 1 then set to 1
    router_utilization_avg.loc[
        router_utilization_avg["router_utilization"] > 100, "router_utilization"
    ] = 100
    # if router_utilization < 0 then set to 0
    router_utilization_avg.loc[
        router_utilization_avg["router_utilization"] < 0, "router_utilization"
    ] = 0
    return router_utilization_avg


def clean_utilization_data(data, agg_freq):
    new_data = data[(data["asset_group"] != "") & (data["chain"] != "")]
    new_data = new_data[~new_data["router"].isna()]

    # replace router_volume_usd where there is 0 with null
    new_data["router_volume_usd"] = new_data["router_volume_usd"].replace(0, None)
    # st.write(new_data)

    # get last value of total_balance_usd for each group
    new_data["total_balance_usd"] = new_data.groupby(
        ["datetime", "chain", "asset", "router"]
    )["total_balance_usd"].transform("last")

    hourly_avg = new_data.groupby(["day", "chain", "asset_group", "hour"]).agg(
        {
            "router_fee_usd": "sum",
            "router_volume_usd": "sum",
            "total_balance_usd": "sum",
            "total_locked_usd": "sum",
        }
    )

    # TODO testing: locked instead of balance -> take absolute value
    hourly_avg["utilization"] = hourly_avg["router_volume_usd"] / hourly_avg[
        "total_locked_usd"
    ].replace(0, None)

    # where locked usd is negative set utilization to 1
    hourly_avg.loc[hourly_avg["total_locked_usd"] < 0, "utilization"] = 1

    # get total row count
    hourly_avg["total_count"] = 1

    # utlization filter: if > 1 then 1 if < 0 then 0
    hourly_avg["utilization_percentage"] = hourly_avg["utilization"].apply(
        lambda x: 1 if x > 1 else 0 if x < 0 else x
    )
    # convert utilization to percentage
    hourly_avg["utilization_percentage"] = hourly_avg["utilization_percentage"] * 100

    # get % of rows for each day when utilization is above 1 and below 0
    hourly_avg["anomoly_count"] = hourly_avg["utilization"].apply(
        lambda x: 1 if x > 1 or x < 0 else 0
    )

    hourly_avg["utilization_anomoly"] = (
        hourly_avg["anomoly_count"] / hourly_avg["total_count"]
    )
    hourly_avg["utilization_anomoly_percentage"] = (
        hourly_avg["utilization_anomoly"] * 100
    )

    if agg_freq == "H":
        # create utilization capacity levels: >0.6,0.6-0.8, >0.8
        # create 4 bins for utilization capacity levels
        hourly_data = hourly_avg.copy()
        bins = [-0.1, 25, 50, 75, 100]
        labels = ["<25%", "25%-50%", "50%-75%", "75%-1000%"]
        hourly_data["utilization_capacity_levels"] = pd.cut(
            hourly_data["utilization_percentage"],
            bins=bins,
            labels=labels,
        )
        return hourly_data.reset_index()
    elif agg_freq == "D":
        # aggregate hourly to daily
        daily_avg = hourly_avg.groupby(["day", "chain", "asset_group"]).agg(
            {
                "router_fee_usd": "sum",
                "router_volume_usd": "sum",
                "total_balance_usd": "mean",
                "total_locked_usd": "mean",
                "utilization_percentage": "mean",
                "anomoly_count": "sum",
                "total_count": "sum",
            }
        )
        daily_avg["utilization_anomoly"] = (
            daily_avg["anomoly_count"] / daily_avg["total_count"]
        )
        daily_avg["utilization_anomoly_percentage"] = (
            daily_avg["utilization_anomoly"] * 100
        )
        # bins
        bins = [-0.1, 25, 50, 75, 100]
        labels = ["<25%", "25%-50%", "50%-75%", "75%-100%"]
        daily_avg["utilization_capacity_levels"] = pd.cut(
            daily_avg["utilization_percentage"],
            bins=bins,
            labels=labels,
        )
        return daily_avg.reset_index()
    else:
        raise ValueError(f"Invalid aggregation frequency: {agg_freq}")


def plot_daily_utilization(data, metric):
    fig = px.line(
        data,
        x="day",
        y=metric,
        facet_col="chain",
        facet_row="asset_group",
        width=2000,
        height=1000,
        # title=f"Daily Utilization - {metric.replace('_', ' ').title()}",
        labels={"date": "Date", metric.replace("_", " "): metric},
        markers=True,
        color_discrete_sequence=["green"],
    )

    # Update legend to show at the top
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=1.5, xanchor="center", x=0.5)
    )

    # Update title and axis labels
    fig.update_layout(
        # title=" ".join(word.capitalize() for word in metric.split("_")),
        xaxis_title="Day",
        yaxis_title="Utilization (%)",
    )

    # Clean up annotations
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=1.5, xanchor="center", x=0.5)
    )

    fig.update_yaxes(title="", mirror="ticks", side="left")
    fig.update_xaxes(title="", tickangle=45, mirror="ticks")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    # Update y-axis to show percentage
    fig.update_layout(yaxis=dict(ticksuffix="%"))

    # Update legend to show percentage
    fig.for_each_trace(lambda trace: trace.update(name=f"{trace.name} (%)"))

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

    fig = px.density_heatmap(
        data_agg,
        x="asset_group",
        y="chain",
        z="utilization_capacity_levels_percentage",
        facet_col="utilization_capacity_levels",
        color_continuous_scale="Greens",
        labels={"utilization_capacity_levels_percentage": "Capacity Levels (%)"},
        height=500,
        width=2000,
    )

    fig.update_layout(
        title="Utilization Capacity Levels Heatmap",
        xaxis_title="Asset Group",
        yaxis_title="Chain",
        coloraxis_colorbar=dict(title="Capacity Levels (%)"),
    )

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
        color_continuous_scale="Greens",
        labels={metric: "Utilization (%)"},
        height=1000,
        width=2000,
    )
    fig.update_layout(coloraxis_colorbar=dict(title="Utilization (%)"))
    fig.update_yaxes(title="")
    fig.update_xaxes(title="", tickangle=45, mirror="ticks")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    st.plotly_chart(fig)


def plot_router_utilization(data):
    # add a drop down selector to show which plot to show
    router_to_show = st.multiselect(
        "Select plot view",
        options=["avg", "total", "total_avg", "individual"],
        default=["avg", "total", "total_avg"],
    )
    st.markdown(
        """
    **Avg. Router Utilization(Teal)**: This line indicates the average utilization of each router, determined by summing all volumes and the total locked at the hourly level.
    
    **Total Utilization(Green)**: This line represents the total utilization, calculated by summing all volumes and the total locked at the daily level.

    **Avg. Utilization(Red)**: This line displays the average utilization of each router, calculated by averaging the volumes and the total locked at the daily level.    
        """
    )

    # st.write(data)

    if "individual" in router_to_show:
        # plot line chart for router_utilization color by router
        fig = px.line(
            data[
                (data["router"] != "total_avg")
                & (data["router"] != "total")
                & (data["router"] != "avg")
            ],
            x="day",
            y="router_utilization",
            color="router",
        )
        st.plotly_chart(fig)

    else:
        # for router all add as dash line
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=data["day"],
                y=data[data["router"] == "avg"]["router_utilization"],
                name="Avg. Router Utilization",
                mode="lines",
                line=dict(color="teal"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=data["day"],
                y=data[data["router"] == "total"]["router_utilization"],
                name="Total Utilization",
                mode="lines",
                line=dict(color="green"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=data["day"],
                y=data[data["router"] == "total_avg"]["router_utilization"],
                name="Avg. Utilization",
                mode="lines",
                line=dict(color="red"),
            )
        )
        st.plotly_chart(fig)

    st.text(
        """
        Note: The utilization is the ratio of the Fast volume of a router over the total locked value.
        Why is Total Utilization lower than avg Router Utilization. Because the total utilization is calculated by summing all volumes and total locked at the day level.
        """
    )


def combined_fast_slow_vol_daily(df, slow_df):
    df_agg = df[
        [
            "day",
            "asset_group",
            "chain",
            "router_volume_usd",
            "total_locked_usd",
            "router_fee_usd",
            "total_balance_usd",
            "utilization_percentage",
        ]
    ].copy()

    # df_agg_router = df_agg.groupby(["day", "chain", "asset_group", "router"]).agg(
    #     {
    #         "router_volume_usd": "sum",
    #         "router_fee_usd": "sum",
    #         "total_locked_usd": "sum",
    #     }
    # )

    # get last value of total_balance_usd for each group
    df_agg["total_balance_usd_last"] = df_agg.groupby(["day", "chain", "asset_group"])[
        "total_balance_usd"
    ].transform("last")
    df_agg["total_locked_usd_last"] = df_agg.groupby(["day", "chain", "asset_group"])[
        "total_locked_usd"
    ].transform("last")

    df_agg = df_agg.groupby(["day"]).agg(
        {
            "router_volume_usd": "sum",
            "router_fee_usd": "sum",
            "total_balance_usd": "sum",
            "total_locked_usd": "sum",
        }
    )
    df_agg.reset_index(inplace=True)

    # slow df agg

    slow_df["day"] = slow_df["date"].dt.date
    slow_df["hour"] = slow_df["date"].dt.hour
    slow_df_agg = slow_df.groupby(
        [
            "day",
        ]
    ).agg({"destination_slow_volume_usd": "sum"})
    df_agg = df_agg.merge(slow_df_agg, on=["day"], how="left")
    return df_agg


def plot_mixed_metrics(df):
    """Plot mixed metrics with dual y-axes.

    - 1st y-axis: Daily aggregate of Volume, Locked, Balance, and Fee for all asset_group across all chains
    - 2nd y-axis: Utilization for each asset_group across all chains
    """
    # Create figure with secondary y-axis
    fig = go.Figure()

    # Add traces
    fig.add_trace(
        go.Scatter(
            x=df["day"],
            y=df["router_volume_usd"],
            name="Fast Volume",
            mode="lines+markers",
            marker=dict(color="blue"),
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["day"],
            y=df["destination_slow_volume_usd"],
            name="Slow Volume",
            mode="lines+markers",
            marker=dict(color="lightblue"),
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["day"],
            y=df["total_balance_usd"],
            name="Balance (USD)",
            mode="lines+markers",
            marker=dict(color="green"),
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["day"],
            y=df["total_locked_usd"],
            name="Liquidity (USD)",
            mode="lines+markers",
            marker=dict(color="yellow"),
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["day"],
            y=df["router_fee_usd"],
            name="Fee (USD)",
            mode="lines+markers",
            marker=dict(color="red"),
            yaxis="y2",
        )
    )

    # Update layout for dual y-axes
    fig.update_layout(
        yaxis=dict(
            title="Volume | Balance | Locked (USD)",
        ),
        yaxis2=dict(
            title="Fee (USD)",
            overlaying="y",
            side="right",
        ),
        legend=dict(orientation="h", yanchor="top", y=1.1, xanchor="center", x=0.5),
    )

    st.plotly_chart(fig, use_container_width=True)


def plot_anomoly_count(data):
    data_agg = data.groupby(["chain", "asset_group"], as_index=False).agg(
        {"anomoly_count": "sum"}
    )
    fig = px.bar(data_agg, x="asset_group", y="anomoly_count", facet_col="chain")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    st.plotly_chart(fig)


def main():
    st.title("Router Utilization Dashboard")

    # Apply filters to both dataframes with unique key prefixes
    filter_data = apply_sidebar_filters(ROUTER_UTILIZATION_RAW_SLOW)
    fast_data = filter_data[filter_data["router_name"] != "slow_path"]
    slow_data = filter_data[filter_data["router_name"] == "slow_path"]

    filter_data_hourly = clean_utilization_data(fast_data, agg_freq="H")
    filter_data_daily = clean_utilization_data(fast_data, agg_freq="D")
    df_slow_fast_agg = combined_fast_slow_vol_daily(filter_data_daily, slow_data)

    # filter_slow_daily = clean_slow_volume_data(SLOW_VOLUME_RAW, agg_freq="D")

    # ----------------- DAILY Avg METRICS ----------------- #
    st.markdown("---")
    st.subheader("Daily Avg. Liquidity | Volume | Balance | Fee")
    # TODO: Multi-Line plot: Daily Avg. Liqudity | Volume for all

    plot_mixed_metrics(df_slow_fast_agg)

    # ------- router utlization: Total and avg-----------------#

    st.markdown("---")
    st.subheader("Router Utilization: Total and Avg")
    router_utilization_metrics = router_utlization_metrics(fast_data)
    # st.write(router_utilization_metrics)
    plot_router_utilization(router_utilization_metrics)

    st.markdown("---")

    # ----------------- DAILY UTILIZATION ----------------- #
    st.subheader("Current- Daily Avg. Utilization")
    st.markdown(
        """
        Utilization is the ratio of the Fast volume of a router over the total locked value,
        
        **Note: Select Metric to see the line plot, by default it is Utilization (Absolute %)**
        """
    )
    # add a metric selector for plot_daily_utilization -> add to sidebar at top
    st.sidebar.markdown("---")
    st.sidebar.subheader("Daily Avg. Metrics - Line Plot")
    metric = st.sidebar.selectbox(
        "Select Metric",
        [
            "utilization_percentage",
            "router_volume_usd",
            "router_fee_usd",
            "total_balance_usd",
        ],
        index=0,
    )
    plot_daily_utilization(filter_data_daily, metric)
    st.markdown("---")

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
    # plot_capacity_levels(filter_data_daily)
    st.markdown("---")

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

    st.markdown("---")
    # ----------------- DAILY UTILIZATION ANOMOLY COUNT ----------------- #
    st.subheader("Daily Avg. Utilization Anomoly")
    st.text(
        "Anomoly count is the number of days where the utilization is above 100% or below 0%"
    )
    plot_anomoly_count(filter_data_daily)
    st.markdown("---")

    # ---Raw Data---#
    st.subheader("Raw Data: Hourly Aggregated")
    st.write(filter_data_hourly)
    st.markdown("---")
    return None


if __name__ == "__main__":
    main()
