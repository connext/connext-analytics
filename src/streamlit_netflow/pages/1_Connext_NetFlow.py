# Things to implement on this page:
# Router Metrics and Utilizations
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Raw Data
from setup import (
    ALL_BRIDGES_HOURLY_DATA,
    apply_universal_sidebar_filters,
    get_df_by_netting_window,
)


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
    fig = px.line(
        df,
        x="date",
        y=metric_col,
        color="bridge",
    )
    fig.update_layout(yaxis_title=metric_col)
    st.plotly_chart(fig)


def main():
    st.title("Bridges % Volume Netted")
    st.subheader("Daily Average % Volume Netted for selected Netting Window")
    st.sidebar.subheader("Filters")
    netting_window_options = st.sidebar.select_slider(
        label="**Volume Netting Window:**",
        options=["1-Hour", "3-Hour", "6-Hour", "12-Hour", "1-Day"],
        value="3-Hour",
    )
    raw_netting_data = apply_universal_sidebar_filters(ALL_BRIDGES_HOURLY_DATA)

    # ----Daily Avg. Netting by Bridge--------------------------------
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
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Total Volume:**")
        plot_netted_volume_by_bridge(filtered_data_bridge, "total_volume")
    with col2:
        st.markdown("**Avg. % Volume Netted:**")
        plot_netted_volume_by_bridge(filtered_data_bridge, "avg_pct_netted")

    # Add three columns for filtering on bridges: only Connext, only Router, and both
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Connext:")
        connext_data = raw_netting_data[raw_netting_data["bridge"] == "connext"]
        asset_group_filtered_data_connext = get_df_by_netting_window(
            df_data=connext_data,
            netting_window=netting_window_options,
            group_by=["asset_group"],
        )
        st.markdown("**Total Volume:**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_connext, "total_volume"
        )
        st.markdown("**Avg. % Volume Netted:**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_connext, "avg_pct_netted"
        )

    with col2:
        st.markdown("**Router Protocol:**")
        router_data = raw_netting_data[raw_netting_data["bridge"] == "router_protocol"]
        asset_group_filtered_data_router = get_df_by_netting_window(
            df_data=router_data,
            netting_window=netting_window_options,
            group_by=["asset_group"],
        )
        st.markdown("**Total Volume:**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_router, "total_volume"
        )
        st.markdown("**Avg. % Volume Netted:**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_router, "avg_pct_netted"
        )

    with col3:
        st.markdown("**Connext + Router Protocol:**")
        both_data = raw_netting_data[
            raw_netting_data["bridge"].isin(["connext", "router_protocol"])
        ]
        asset_group_filtered_data_both = get_df_by_netting_window(
            df_data=both_data,
            netting_window=netting_window_options,
            group_by=["asset_group"],
        )
        st.markdown("**Total Volume:**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_both, "total_volume"
        )
        st.markdown("**Avg. % Volume Netted:**")
        plot_netted_volume_by_asset_group(
            asset_group_filtered_data_both, "avg_pct_netted"
        )

    # Raw Data
    # st.subheader("Data: Avg. % Volume Netted by Asset Group")
    # st.write(asset_group_filtered_data_connext)
    # st.write(asset_group_filtered_data_router)
    # st.write(asset_group_filtered_data_both)

    # st.subheader("Data: Avg. % Volume Netted by Bridge")
    # st.write(filtered_data_bridge)


if __name__ == "__main__":
    main()
