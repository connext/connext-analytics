import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from setup import (ROUTER_DAILY_METRICS_RAW_SLOW, apply_sidebar_filters,
                   clean_df)


def simulate_apr(df, apr_target):
    """
    simulate_apr
    Args:
        df (pd.DataFrame): dataframe of the router metrics- that is pre filtered based on the sidebar
        apr_target (int): the target APR to reach
        last_x_days (int): the timeframe to simulate

    Returns:
        pd.DataFrame: dataframe of the simulated APR
    """

    # Select the timeframe -> APR target is in %
    df["required_liquidity"] = df.apply(
        lambda row: (row["router_fee_usd"] * 365) / (apr_target / 100), axis=1
    )
    # rolling 3d required liquidity: none zero and null rows
    df["rolling_3d_required_liquidity"] = (
        df["required_liquidity"].rolling(window=3).sum()
    )
    df["rolling_7d_required_liquidity"] = (
        df["required_liquidity"].rolling(window=7).sum()
    )

    # Corrected aggregation: specify each APR and liquidity column separately
    aggregated_df = (
        df.groupby("date")
        .agg(
            {
                "apr": "mean",
                "apr_7d": "mean",
                "apr_14d": "mean",
                "required_liquidity": "sum",
                "total_balance_usd": "sum",
                "router_volume_usd": "sum",
                "rolling_3d_required_liquidity": "mean",
                "rolling_7d_required_liquidity": "mean",
            }
        )
        .reset_index()
    )

    return aggregated_df


def simulate_apr_with_volume(df, apr_target, slow_df):
    """
    simulate_apr
    Args:
        df (pd.DataFrame): dataframe of the router metrics- that is pre filtered based on the sidebar
        apr_target (int): the target APR to reach
        last_x_days (int): the timeframe to simulate

    Returns:
        pd.DataFrame: dataframe of the simulated APR
    """
    agg_slow_df = (
        slow_df.groupby(["date"])
        .agg({"destination_slow_volume_usd": "sum"})
        .reset_index()
    )
    agg_slow_df["date"] = pd.to_datetime(agg_slow_df["date"]).dt.date

    agg_fast_df = (
        df.groupby("date")
        .agg(
            {
                "apr": "mean",
                "router_volume_usd": "sum",
                "router_fee_usd": "sum",
                "total_balance_usd": "sum",
            }
        )
        .reset_index()
    )
    agg_fast_df["date"] = agg_fast_df["date"].dt.date

    # combine agg_fast_df and agg_slow_df
    combined_df = pd.merge(agg_fast_df, agg_slow_df, on="date", how="left")
    combined_df["volume"] = (
        combined_df["router_volume_usd"] + combined_df["destination_slow_volume_usd"]
    )

    combined_df["slow_fast_fee"] = combined_df["volume"] * 0.0005

    # Select the timeframe -> APR target is in % -> change fee to calculate using volume from df + slow_df
    combined_df["required_liquidity"] = combined_df.apply(
        lambda row: (row["slow_fast_fee"] * 365) / (apr_target / 100), axis=1
    )
    # rolling 3d required liquidity: none zero and null rows
    combined_df["rolling_3d_required_liquidity"] = (
        combined_df["required_liquidity"].rolling(window=3).sum()
    )
    combined_df["rolling_7d_required_liquidity"] = (
        combined_df["required_liquidity"].rolling(window=7).sum()
    )

    return combined_df


def plot_aggregated_data(
    df, date_col, liquidity_cols, current_liquidity, volume_col, apr_cols, target_apr
):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Plot each liquidity column as a bar chart
    for col in liquidity_cols:
        fig.add_trace(
            go.Bar(
                x=df[date_col],
                y=df[col],
                name=col,
                orientation="v",
            ),
            secondary_y=False,
        )
    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df[current_liquidity],
            name="Current Liquidity",
            orientation="v",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Line(
            x=df[date_col],
            y=df[volume_col],
            name="Volume",
            mode="lines",
            line=dict(width=2, dash="dot", color="green"),
        ),
        secondary_y=False,
    )

    # Plot each APR column as a line chart
    for col in apr_cols:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df[col],
                name=col,
                mode="lines",
                line=dict(width=2),
            ),
            secondary_y=True,
        )

    # Add a dotted line for the target APR
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=[target_apr] * len(df),
            name="Target APR",
            mode="lines",
            line=dict(color="red", width=2, dash="dot"),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Liquidity",
        xaxis=dict(type="date"),
    )
    fig.update_yaxes(title_text="APR (%)", secondary_y=True)
    fig.update_xaxes(
        dtick="D1",
        tickformat="%Y-%m-%d",
        ticklabelmode="period",
    )

    return fig


def simulate_apr_parameters(df, slow_df):
    """
    INPUTS:
        DF: dataframe of the router metrics- that is pre filtered based on the sidebar
    """
    st.subheader("Simulate APR and Volume to understand Liquidity Requirements")
    st.markdown(
        """
        **Explanation**

        The APR formula using daily aggregation: `router_fee_usd / total_balance_usd (Liquidity) * 365`
        Since liquidity is added back after being supplied, we've calculated the average daily liquidity from hourly data.

        APR is influenced by fee and liquidity. We can simulate the APR for a given token, chain, and router using historical Connext data. This allows us to simulate liquidity for the desired APR and volumes (fast only, total (fast+slow) volume).

        **How to use?**
        - SELECT a Token and Chain of interest.
        - SELECT an APR Target (default: 15%).
        - SELECT a timeframe to simulate (default: last 30 days).

        Note: All selections are defaulted to "all."
    """
    )

    APR_TARGET = st.sidebar.slider(
        "Select APR Target (%)", min_value=1, max_value=100, value=15
    )

    simulated_df = simulate_apr(df, apr_target=APR_TARGET)
    simulate_df_with_volume = simulate_apr_with_volume(
        df=df, apr_target=APR_TARGET, slow_df=slow_df
    )

    st.markdown(
        f"""
        ---
        #### Simulated Liquidity requirements for achieving {APR_TARGET}% APR using Fast Volume
        """
    )

    col1, col2 = st.columns([1, 2])
    with col1:
        st.markdown(
            f"""
            
            Inputs(from our historical data):
            - last {simulated_df.shape[0]} days
            - Fast volume
            
            User Input(sidebar):
            - APR: {APR_TARGET}%

            Simulated(output):
            - Required Liquidity
            """
        )

    with col2:
        fig = plot_aggregated_data(
            simulated_df,
            date_col="date",
            liquidity_cols=["required_liquidity"],
            current_liquidity="total_balance_usd",
            volume_col="router_volume_usd",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        f"""
        ---
        #### Simulated Liquidity requirements for achieving {APR_TARGET} % APR using Total Volume (Fast+Slow)
        """
    )

    col1, col2 = st.columns([1, 2])
    with col1:
        st.markdown(
            f"""
            
            Inputs(from our historical data):
            - last {simulated_df.shape[0]} days
            - Total volume(Fast+Slow)
            
            User Input(sidebar):
            - APR: {APR_TARGET}%

            Simulated(output):
            - Required Liquidity
            """
        )

    with col2:
        fig = plot_aggregated_data(
            simulate_df_with_volume,
            date_col="date",
            liquidity_cols=["required_liquidity"],
            current_liquidity="total_balance_usd",
            volume_col="volume",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        """
    ---
        
    ### Rolling Liquidity
    We apply 3-day and 7-day rolling windows to the liquidity data to smooth out short-term fluctuations and observe overall trends.
    
    <span style='color: red;'>**Note: To use rolling data, ensure the data is filtered for a single token.**</span>
    """,
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Rolling 3-Day Liquidity for Fast Volume**")

        fig = plot_aggregated_data(
            simulated_df,
            date_col="date",
            liquidity_cols=["rolling_3d_required_liquidity"],
            current_liquidity="total_balance_usd",
            volume_col="router_volume_usd",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Rolling 3-Day Liquidity for Total Volume**")

        fig = plot_aggregated_data(
            simulate_df_with_volume,
            date_col="date",
            liquidity_cols=["rolling_3d_required_liquidity"],
            current_liquidity="total_balance_usd",
            volume_col="volume",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Rolling 7-Day Liquidity for Fast Volume**")

        fig = plot_aggregated_data(
            simulated_df,
            date_col="date",
            liquidity_cols=["rolling_7d_required_liquidity"],
            current_liquidity="total_balance_usd",
            volume_col="router_volume_usd",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Rolling 7-Day Liquidity for Total Volume**")
        fig = plot_aggregated_data(
            simulate_df_with_volume,
            date_col="date",
            liquidity_cols=["rolling_7d_required_liquidity"],
            current_liquidity="total_balance_usd",
            volume_col="volume",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)


def relationship_between_utilization_and_apr(df):
    st.subheader("Relationship Between Utilization and APR")
    st.markdown(
        """
        The relationship between utilization and APR is a key indicator of the efficiency of the router's liquidity management.
        """
    )

    # Create the scatter plot with tokens and chain as categorical values

    # for the dataframe average the values for each chain, asset
    df_agg = (
        df.groupby(["chain", "asset_group"])[
            ["utilization", "apr", "apr_7d", "apr_14d"]
        ]
        .mean()
        .reset_index()
    )

    # Assuming df_agg is already defined as shown previously
    unique_chains = df_agg["chain"].unique()
    num_plots = len(unique_chains)
    num_rows = (num_plots + 1) // 2
    fig = make_subplots(
        rows=num_rows,
        cols=2,
        subplot_titles=[f"Plot {i+1}" for i in range(num_plots)],
    )

    for idx, chain in enumerate(unique_chains):
        df_chain = df_agg[df_agg["chain"] == chain]
        # Calculate the appropriate row and column
        row = (idx // 2) + 1  # Integer division to determine row
        col = (idx % 2) + 1  # Modulo operation to alternate columns
        fig.add_trace(
            go.Scatter(
                x=df_chain["utilization"],
                y=df_chain["apr"],
                mode="markers",
                name=chain,
                hovertext=df_chain["asset_group"],
            ),
            row=row,
            col=col,
        )

    fig.update_layout(
        height=100 * num_rows,
        width=400,
        title_text="Scatter Plot of APR vs Utilization by Chain",
    )

    st.plotly_chart(fig, use_container_width=True)


def main():
    filter_data = apply_sidebar_filters(ROUTER_DAILY_METRICS_RAW_SLOW)
    fast_data = filter_data[filter_data["router_name"] != "slow_path"]
    slow_data = filter_data[filter_data["router_name"] == "slow_path"]
    new_agg_filtered_data_router_metrics = clean_df(fast_data)
    simulate_apr_parameters(df=new_agg_filtered_data_router_metrics, slow_df=slow_data)

    return None


if __name__ == "__main__":
    main()
