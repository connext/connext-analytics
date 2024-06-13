import pytz
import pandas as pd
import streamlit as st
import pandas_gbq as gbq
from datetime import datetime, timedelta
from setup import apply_sidebar_filters, ROUTER_DAILY_METRICS_RAW


def display_data(filtered_data):
    st.markdown("## Raw data:")
    st.data_editor(
        filtered_data,
        hide_index=True,
        # column_config={
        #     "date": "Date",
        #     "router_address": "Router Address",
        #     "chain": "Chain",
        #     "asset": "Asset",
        #     "tvl": "TVL",
        #     "daily_fee_earned": "Daily Fee Earned",
        #     "total_fee_earned": "Total Fee Earned",
        #     "daily_liquidity_added": "Daily Liquidity movement(+/-)",
        #     "router_locked_total": "Router Locked Total",
        #     "calculated_router_locked_total": "Calculated-Router Locked Total",
        #     "balance": "Balance",
        # },
    )


# Main function to display the app
def main():

    st.title("Connext Routers")

    # Applying sidebar filters
    filtered_data = apply_sidebar_filters(ROUTER_DAILY_METRICS_RAW)

    # Displaying the data
    display_data(filtered_data)

    # Example of adding Markdown
    st.markdown(
        """
        ## Metric Definations

        #### Source Table
        The SQL query retrieves data from the BigQuery table:
        ```
        `mainnet-bigq.y42_connext_y42_dev_metrics.routers_tvl_agg__daily`
        ```

        #### Calculations and Definitions

        1. **Router Attributes**

            - `Date`: The Enf of day date for the Router Liquidity addition or fee collected.
            - `Router Address`: Identifier for the router.
            - `Chain`: Blockchain identifier.
            - `Asset`: Type of crypto asset.
            - `Router Fee`: Fees collected by the router on the daily bais.
            - `Total Fee Earned`: Cumulative fees earned by the router for this chain-asset pair in its lifetime.
            - `Daily Liquidity Added`: Liquidity added on the given day.
            Summation of Liquidity added or removed on given day.
            - `Router Locked Total`: Total liquidity locked by the router in its lifetime, currently held.
            Its a running total  of above amount.
            - `total_balance`: Total balance available.
            - `APR`: Annual Percentage Rate calculated daily based on the router fees and total locked amount. The fees are not added back
            to the tvl. Hence there is no compounding of APR ie APY.
                

        2. **Daily APR Calculation**:
        - `APR`: Annual Percentage Rate calculated daily based on the router fees and total locked amount.
        - Formula:
            ```sql
            (r.router_fee / NULLIF(r.total_locked, 0)) * 365
            ```
        - This calculation annualizes the daily router fee as a percentage of the total locked amount,
        assuming the daily rate holds constant across the year.

        3. **Moving Averages for APR**:
        - `avg_7d_apr`: 7-day moving average of the daily APR.
        - `avg_14d_apr`: 14-day moving average of the daily APR.
        - These are calculated using window functions that average the `daily_apr` over the specified period
        (7 days and 14 days) for each router, chain, and asset combination.
        
        ## TODO

        - [ ] Addition of Router Names
        - [ ] Addition of utilization metric and volume
        - [ ] USD pricing data
        - [ ] Last data update indicator
        - [ ] Feedback input taker
        """
    )


if __name__ == "__main__":
    main()
