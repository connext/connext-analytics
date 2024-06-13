from ast import main
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from setup import apply_sidebar_filters, clean_df, ROUTER_DAILY_METRICS_RAW


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
                "rolling_3d_required_liquidity": "mean",
                "rolling_7d_required_liquidity": "mean",
            }
        )
        .reset_index()
    )

    return aggregated_df


def plot_aggregated_data(
    df, date_col, liquidity_cols, current_liquidity, apr_cols, target_apr
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
        title="Aggregated Liquidity and APR over Time",
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


def simulate_apr_parameters(df):
    """
    INPUTS:
        DF: dataframe of the router metrics- that is pre filtered based on the sidebar
    """
    st.subheader("Simulate APR Parameters")
    st.markdown(
        """
        **Explanation**

        APR formula using daily aggregation: `router_fee_usd / total_balance_usd(this is Liquidity) * 365`
        Given APR is influced by fee and liquidity, we can simulate the APR for a given token and router.
        using historical data from the last 7,30, 60, 90 days, we can simulate the APR for a given token and router.
        
        **Steps**
        - SELECT a Token and Chain
        - SELECT a APR Target
        - SELECT a timeframe to simulate
        - by keeping the volume and fee constant, check to see how much of liquidity is needed to reach the target APR.
        """
    )

    APR_TARGET = st.sidebar.slider(
        "Select APR Target (%)", min_value=1, max_value=100, value=15
    )

    # [ ] TODO: add the option to select the timeframe to simulate to sidebar
    # LAST_X_DAYS = st.sidebar.slider(
    #     "Select Timeframe(X days of data to simulate)",
    #     min_value=7,
    #     max_value=180,
    #     value=30,
    # )

    # do a plotly bar plot for required_liquidity and current liquidity ie balance with a line chart of current apr in 1 plot
    # date | required_liquidity | apr | total_balance_usd
    simulated_df = simulate_apr(df, apr_target=APR_TARGET)

    st.write(simulated_df)
    st.subheader(f"Simulated Target: {APR_TARGET} % APR with Current Liquidity and APR")

    fig = plot_aggregated_data(
        simulated_df,
        date_col="date",
        liquidity_cols=["required_liquidity"],
        current_liquidity="total_balance_usd",
        apr_cols=["apr"],
        target_apr=APR_TARGET,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        """
    <span style='color: red;'>**Note: TO USE ROLLING DATA MAKE SURE TO FILTER THE DATA FOR JUST 1 TOKEN**</span>
    """,
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"### Rolling 3D Liquidity and APR for {APR_TARGET} % APR")

        fig = plot_aggregated_data(
            simulated_df,
            date_col="date",
            liquidity_cols=["rolling_3d_required_liquidity"],
            current_liquidity="total_balance_usd",
            apr_cols=["apr"],
            target_apr=APR_TARGET,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown(f"### Rolling 7D Liquidity and APR for {APR_TARGET} % APR")

        fig = plot_aggregated_data(
            simulated_df,
            date_col="date",
            liquidity_cols=["rolling_7d_required_liquidity"],
            current_liquidity="total_balance_usd",
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


# fig = px.scatter(
#     df_agg,
#     x="utilization",
#     y="apr",
#     color="asset_group",
#     symbol="chain",
#     hover_data=["chain", "asset_group"],
#     labels={
#         "utilization": "Utilization (Router Volume USD / Total Balance USD)",
#         "apr": "APR ((Router Fee USD / Total Balance USD) * 365)",
#     },
#     title="Scatter Plot of APR vs Utilization",
# )
# st.plotly_chart(fig, use_container_width=True)


def liquidity_gameplan():
    st.title("Effective Liquidity Modelling")
    st.subheader("Liquidity Gameplan")
    st.markdown(
        """
        This page will be used to help you plan the liquidity gameplan for your token.

        ## Caculations Steps

        - raw data: we will use has these cols:
            - data | date | chain | asset_group | total_balance_usd | router_fee_usd | router_volume_usd | apr | apr_7d | apr_14d | utilization
        - Column definitions are for each router:
            - `date`: date of the data point
            - `chain`: chain
            - `asset_group`: asset group of the token(USDC/nextUSDC is a group: USDC)
            - `total_balance_usd`: balance of the token in usd for a router on a given date.
            - `router_fee_usd`: fee collected of the token in usd on a given date
            - `router_volume_usd`: router volume of the token in usd
            - `apr`: annualized return of the token. 
                - Formula: `(total_balance_usd - router_fee_usd) / router_fee_usd * 365`
            - `apr_7d`: 7-day running average of the APR. 
                - Formula: `apr.rolling(7).mean()`
            - `apr_14d`: 14-day running average of the APR. 
                - Formula: `apr.rolling(14).mean()`
        

        ### Effective Utilization metric calculations
        
        - `utilization`: utilization of the token per chain. 
        - Formula: `router_volume_usd / total_balance_usd`
        - **significance**
            - Transaction Demand vs. Available Liquidity:
                - Utilization indicates the demand for transactions relative to the available liquidity. A higher utilization ratio suggests that a large volume of transactions is being processed relative to the available liquidity.

            - Efficiency of Liquidity Usage:
                - High utilization means that the router's liquidity is being used efficiently to facilitate transactions. Conversely, low utilization indicates that a significant portion of the liquidity is not being used for transactions.

            - Liquidity Stress Indicator:
                - Utilization can serve as an indicator of liquidity stress. Extremely high utilization may suggest that the router is close to its capacity, potentially leading to liquidity shortages if transaction volumes increase further.
        - **Interpretation**
            - High Utilization:
                - Indicates high transaction volume relative to the available liquidity.
                - Suggests efficient use of liquidity, but if too high, it may indicate potential liquidity stress.
                - A router with high utilization might need to increase its liquidity to maintain smooth operations.
            - Low Utilization:
                - Indicates low transaction volume relative to the available liquidity.
                - Suggests that there is excess liquidity that is not being utilized.
                - A router with low utilization has ample liquidity to handle an increase in transaction volume without issues.

        Importance of Calculating Utilization at a X-Hour Window Period with Finite Router Liquidity

        1. **Granularity and Precision**:
        - **Granularity**: Captures detailed fluctuations during the day
        - **Precision**: Reflects minor changes in demand and supply accurately.

        2. **Managing Utilization Values**:
        - **0 to 1 Range**: Ensures values are meaningful and within control.
        - **Prevent Overutilization**: High values (close to 1) indicate potential liquidity shortages.
        - **Prevent Underutilization**: Low values (close to 0) indicate inefficient use of liquidity.

        3. **Finite Router Liquidity**:
        - **Liquidity Locking**: Once utilized, liquidity is locked and cannot be reused within the same window.
        - **Efficient Use**: Ensures finite liquidity is used effectively.
        - **Avoiding Shortages**: Prevents situations where all liquidity is locked up, leading to shortages.
        - **Dynamic Reallocation**: Allows better management and reallocation of liquidity.
        
        """
    )


def main():

    filter_data = apply_sidebar_filters(ROUTER_DAILY_METRICS_RAW)
    new_agg_filtered_data_router_metrics = clean_df(filter_data)
    simulate_apr_parameters(df=new_agg_filtered_data_router_metrics)
    liquidity_gameplan()


if __name__ == "__main__":
    main()
