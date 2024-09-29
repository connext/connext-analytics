import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
# Raw Data
from setup import (ALL_BRIDGES_HOURLY_DATA, apply_universal_sidebar_filters,
                   get_df_by_netting_window)


def plot_netted_volume_by_asset_group(df, metric_col):
    fig = px.line(
        df,
        x="date",
        y=metric_col,
        color="asset_group",
        # title="Daily Average % Volume Netted by Assets",
    )
    fig.update_layout(yaxis_title=metric_col)
    st.plotly_chart(fig)


def plot_netted_volume_by_chain(df):
    fig = px.line(
        df,
        x="date",
        y="avg_pct_netted",
        color="chain",
        # title="Daily Average % Volume Netted by Chain",
    )
    fig.update_layout(yaxis_title="% Volume Netted")
    st.plotly_chart(fig)


def plot_netted_volume_by_bridge(df, metric_col):
    # Calculate the difference between connext and router_protocol
    df_connext = (
        df[df["bridge"] == "connext"].groupby("date")[metric_col].mean().reset_index()
    )
    df_router = (
        df[df["bridge"] == "router_protocol"]
        .groupby("date")[metric_col]
        .mean()
        .reset_index()
    )
    df_diff = pd.merge(
        df_connext, df_router, on="date", suffixes=("_connext", "_router")
    )
    df_diff["diff"] = df_diff[f"{metric_col}_connext"] - df_diff[f"{metric_col}_router"]

    # single metrics

    if metric_col == "avg_pct_netted":
        st.metric(
            label="Average % Netting Difference (Connext - Router)",
            value=f"{round(df_diff['diff'].mean(), 2)}%",
        )
    elif metric_col == "total_volume":
        st.metric(
            label="Average Volume Difference (Connext - Router)($)",
            value=f"${round(df_diff['diff'].mean() / 1_000_000, 2)}M",
        )

    # Create subplots with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # Add bar plot for the difference metric
    fig.add_trace(
        go.Bar(
            x=df_diff["date"],
            y=df_diff["diff"],
            name="Difference (Connext - Router)",
            marker_color="teal",
        ),
        secondary_y=False,
    )
    # title
    if metric_col == "avg_pct_netted":
        title = "Avg. % Volume Netted"
    elif metric_col == "total_volume":
        title = "Total Volume ($)"

    # Add line plot for the original metric
    for bridge in df["bridge"].unique():
        bridge_data = df[df["bridge"] == bridge]
        fig.add_trace(
            go.Scatter(
                x=bridge_data["date"],
                y=bridge_data[metric_col],
                mode="lines",
                name=f"{bridge} bridge {title}",
            ),
            secondary_y=False,
        )

    # Update layout
    fig.update_layout(
        # title=f"{metric_col} by Bridge with Difference",
        yaxis_title=title,
        legend=dict(
            orientation="h",  # Horizontal legend
            yanchor="bottom",  # Anchor the legend at the bottom
            y=1,  # Position the legend slightly above the plot
            xanchor="left",  # Center the legend horizontally
            x=0,  # Position the legend in the middle of the plot
        ),
    )

    st.plotly_chart(fig)


def main():
    st.title("Bridges % Volume Netted")
    st.text("Daily Average % Volume Netted by selected Netting Window")
    st.sidebar.subheader("Filters")
    netting_window_options = st.sidebar.select_slider(
        label="**Volume Netting Window:**",
        options=["1-Hour", "3-Hour", "6-Hour", "12-Hour", "1-Day"],
        value="3-Hour",
    )
    raw_netting_data = apply_universal_sidebar_filters(ALL_BRIDGES_HOURLY_DATA)

    # -----------------------------------------------------------------------------
    # 1. Add three columns for filtering on bridges: only Connext, only Router, and both
    # -----------------------------------------------------------------------------
    combined_bridge_data = raw_netting_data.groupby(
        ["date", "chain", "asset_group", "asset"]
    ).agg({"inflow": "sum", "outflow": "sum", "volume": "sum"})
    combined_bridge_data["bridge"] = "all"
    combined_bridge_data = combined_bridge_data.reset_index()

    # append combined_bridge_data with raw_netting_data
    combined_bridge_data = pd.concat([combined_bridge_data, raw_netting_data])
    # st.write(combined_bridge_data)
    filtered_data_bridge = get_df_by_netting_window(
        df_data=combined_bridge_data,
        netting_window=netting_window_options,
        group_by=["bridge"],
    )

    # -----------------------------------------------------------------------------
    # 1.3. calculate variance
    # -----------------------------------------------------------------------------
    filtered_data_bridge_var = (
        filtered_data_bridge.groupby(["bridge"])["avg_pct_netted"].var().reset_index()
    )
    # st.write(filtered_data_bridge_var)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Avg. % Volume Netted:**")
        plot_netted_volume_by_bridge(filtered_data_bridge, "avg_pct_netted")
        subcol1, subcol2, subcol3 = st.columns(3)
        with subcol1:
            st.metric(
                label="Connext + Router Protocol Variance",
                value=f"""{round(
                    filtered_data_bridge_var[filtered_data_bridge_var['bridge'] == 'all']['avg_pct_netted'].mean(), 2)}%
                    """,
            )
        with subcol2:
            st.metric(
                label="Connext Variance",
                value=f"""{round(
                    filtered_data_bridge_var[filtered_data_bridge_var['bridge'] == 'connext']['avg_pct_netted'].mean(), 2)}%
                    """,
            )
        with subcol3:
            st.metric(
                label="Router Protocol Variance",
                value=f"""{round(
                    filtered_data_bridge_var[filtered_data_bridge_var['bridge'] == 'router_protocol']
                    ['avg_pct_netted'].mean(), 2)}%
                    """,
            )

    with col2:
        st.markdown("**Total Volume:**")
        plot_netted_volume_by_bridge(filtered_data_bridge, "total_volume")

    # -----------------------------------------------------------------------------
    # 2. Add three columns for filtering on bridges: only Connext, only Router, and both
    # -----------------------------------------------------------------------------

    st.markdown("---")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Connext", divider=True)
        connext_data = raw_netting_data[raw_netting_data["bridge"] == "connext"]
        asset_group_filtered_data_connext = get_df_by_netting_window(
            df_data=connext_data,
            netting_window=netting_window_options,
            group_by=["asset_group"],
        )

        st.markdown("**Avg. % Volume Netted**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_connext, "avg_pct_netted"
        )

        st.markdown("**Total Volume**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_connext, "total_volume"
        )

    with col2:
        st.subheader(
            "Router Protocol",
            divider=True,
        )
        router_data = raw_netting_data[raw_netting_data["bridge"] == "router_protocol"]
        asset_group_filtered_data_router = get_df_by_netting_window(
            df_data=router_data,
            netting_window=netting_window_options,
            group_by=["asset_group"],
        )

        st.markdown("**Avg. % Volume Netted**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_router, "avg_pct_netted"
        )
        st.markdown("**Total Volume**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_router, "total_volume"
        )

    with col3:
        st.subheader(
            "Connext + Router Protocol",
            divider=True,
        )
        both_data = raw_netting_data[
            raw_netting_data["bridge"].isin(["connext", "router_protocol"])
        ]
        asset_group_filtered_data_both = get_df_by_netting_window(
            df_data=both_data,
            netting_window=netting_window_options,
            group_by=["asset_group"],
        )
        st.markdown("**Avg. % Volume Netted**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_both, "avg_pct_netted"
        )
        st.markdown("**Total Volume**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_both, "total_volume"
        )

    # Raw Data

    # showcase asset and asset group
    st.markdown(
        """
    ---
    #### Assets and Asset Groups

    Below assets are clubed together into asset groups for simplicity.
    """,
        unsafe_allow_html=True,
    )
    raw_netting_data_unique = (
        raw_netting_data[
            [
                "asset_group",
                "asset",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    styled_df = raw_netting_data_unique.sort_values(by="asset_group", ascending=False)
    st.write(styled_df)


if __name__ == "__main__":
    main()
