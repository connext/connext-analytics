import streamlit as st


def display_data(filtered_data):
    st.markdown("## Raw data:")
    st.data_editor(
        filtered_data,
        hide_index=True,
    )


def liquidity_gameplan():
    st.markdown(
        """
        #### Effective Utilization Metric Calculations
        
        - **Utilization**: Token utilization per chain
        - Formula: `router_volume_usd / total_balance_usd`
        - **Significance**:
            - **Transaction Demand vs. Available Liquidity**:
                - Indicates transaction demand relative to available liquidity. Higher values suggest high transaction volumes.
            - **Efficiency of Liquidity Usage**:
                - High utilization means efficient liquidity use. Low utilization indicates excess liquidity.
            - **Liquidity Stress Indicator**:
                - High utilization may indicate potential liquidity stress and the need for more liquidity to handle transactions.

        #### Interpretation
        - **High Utilization**:
            - High transaction volume relative to liquidity
            - Efficient liquidity use, but potential stress if too high
            - May need increased liquidity to maintain smooth operations
        - **Low Utilization**:
            - Low transaction volume relative to liquidity
            - Excess liquidity not being used
            - Can handle an increase in transaction volume without issues

        #### Importance of Calculating Utilization at an X-Hour Window Period with Finite Router Liquidity

        1. **Granularity and Precision**:
            - **Granularity**: Captures daily fluctuations
            - **Precision**: Reflects minor changes in demand and supply

        2. **Managing Utilization Values**:
            - **0 to 1 Range**: Ensures meaningful and controlled values
            - **Prevent Overutilization**: Indicates potential liquidity shortages
            - **Prevent Underutilization**: Indicates inefficient liquidity use

        3. **Finite Router Liquidity**:
            - **Liquidity Locking**: Used liquidity is locked and cannot be reused within the same window
            - **Efficient Use**: Ensures effective use of finite liquidity
            - **Avoiding Shortages**: Prevents liquidity lockup leading to shortages
            - **Dynamic Reallocation**: Allows better liquidity management and reallocation
        """
    )
    return None


def main() -> None:
    st.title("Connext Routers")

    # Plot Summaries
    st.markdown(
        """
        ## Page Summaries

        ### Router Metrics
        - [Daily Avg. APR Across Routers](#daily-avg-apr-across-routers)
        - [Running 7-day Avg. APR Across Routers](#running-7-day-avg-apr-across-routers)
        - [Running 14-day Avg. APR Across Routers](#running-14-day-avg-apr-across-routers)
        - [Agg. Fee Across Routers](#agg-fee-across-routers)
        - [Agg. TVL Across Routers](#agg-tvl-across-routers)
        - [Agg. Utilization Across Routers](#agg-utilization-across-routers)
        - [Agg. Volume Across Routers](#agg-volume-across-routers)

        ### Liquidity Modelling
        - [Simulated Liquidity requirements for achieving target APR using Fast Volume](#simulated-liquidity-requirements-for-achieving-target-apr-using-fast-volume)
        - [Simulated Liquidity requirements for achieving target APR using Total Volume (Fast+Slow)](#simulated-liquidity-requirements-for-achieving-target-apr-using-total-volume-fastslow)
        - [Rolling Liquidity (3-day and 7-day)](#rolling-liquidity-3-day-and-7-day)

        ### Router Utilization
        - [Daily Avg. Utilization](#daily-avg-utilization)
        - [Utilization Capacity Levels](#utilization-capacity-levels)
        - [Hourly Utilization Heatmap](#hourly-utilization-heatmap)
        - [Daily Avg. Utilization Anomaly Count](#daily-avg-utilization-anomaly-count)
        """
    )

    st.markdown(
        f"""
        
        
        #### [ ] TODO: Assumptions
        #### [ ] TODO: Takeaways
        #### [ ] TODO: How to use
        #### [ ] TODO: Calculations
        {liquidity_gameplan()}
        #### [ ] TODO: Raw Data
            
            - Source Table
        """
    )

    return None


if __name__ == "__main__":
    main()

    # # ----------------- LIQUIDITY GAMEPLAN ----------------- #
    # st.markdown("---")
    # liquidity_gameplan()
