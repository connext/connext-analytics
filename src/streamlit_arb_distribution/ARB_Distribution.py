import pandas as pd
import streamlit as st
from setup import (
    apply_universal_sidebar_filters,
    ARB_DISTRIBUTION_DATA,
    ARB_HOURLY_PRICE_DATA,
)


def theory():
    st.markdown(
        """
        
        #### [Requirements](https://www.notion.so/Arb-Distribution-0297a6e965674e11867c98dc0420b610)

        **Calculation Considerations**
        - Hourly Pricing data
        - Amount converted to USD -> ARB
        - ARB distribution by user and aggregated to start of Week (based on xcall_timestamp)

        **Steps:**

        1) Calculate gas fees used on origin chain

        a) Gas fees
        - `xcall_gas_price * xcall_gas_limit`

        b) Relayer fees
        - Sum of:
            - `relayer_fees["0x0000000000000000000000000000000000000000"] * native gas price`
            - `relayer_fees["<origin_transacting_asset>"] * transacting asset price`
            - Note: usually relayer fee is paid in native or transacting but sometimes it can be both

        2) Calculate liquidity fees

        a) Router fees
        - `origin_transacting_amount * 5bps`

        b) AMM fees
        - For L2s as origin: `origin_transacting_amount * 13bps`
        - For L1 as origin: `origin_transacting_amount * 8bps`

        3) Convert fees to ARB
        - Add 1) and 2)
        - Convert total to equivalent ARB amount at the time of transfer

    """
    )


def user_aggregator(df):

    df_agg = df.groupby(
        ["xcall_caller", "date", "origin_chain", "destination_chain"]
    ).agg(
        {
            "usd_gas_fee_amount": "sum",
            "usd_relay_fee_1_amount": "sum",
            "usd_relay_fee_2_amount": "sum",
            "usd_amm_fee_amount": "sum",
            "usd_router_fee_amount": "sum",
            "usd_destination_amount": "sum",
        }
    )

    return df_agg


def user_weekly_distribution(df, arb_price):
    df_weekly = df.copy()

    arb_price["hour"] = pd.to_datetime(arb_price["date"]).dt.hour
    arb_price["date"] = pd.to_datetime(arb_price["date"]).dt.date

    # join on date
    df_weekly = df_weekly.merge(
        arb_price, left_on=["day", "hour"], right_on=["date", "hour"], how="left"
    )

    # drop df_weekly date_y
    df_weekly.drop(columns=["date_x", "date_y"], inplace=True)

    # week of the month
    df_weekly["week_of_year"] = pd.to_datetime(df["date"]).dt.isocalendar().week
    df_weekly["month"] = pd.to_datetime(df["date"]).dt.month
    df_weekly["year"] = pd.to_datetime(df["date"]).dt.year
    df_weekly["week_start_date"] = (
        pd.to_datetime(df["date"]).dt.to_period("W").dt.start_time
    )
    df_weekly["week_end_date"] = (
        pd.to_datetime(df["date"]).dt.to_period("W").dt.end_time
    )
    df_weekly["total_fee_usd"] = (
        df_weekly["usd_gas_fee_amount"].fillna(0)
        + df_weekly["usd_relay_fee_1_amount"].fillna(0)
        + df_weekly["usd_relay_fee_2_amount"].fillna(0)
        + df_weekly["usd_amm_fee_amount"].fillna(0)
        + df_weekly["usd_router_fee_amount"].fillna(0)
    )

    df_weekly["total_fee_arb"] = df_weekly["total_fee_usd"] / df_weekly["price"]

    df_weekly = df_weekly.groupby(
        ["xcall_caller", "week_start_date", "week_end_date", "asset"]
    ).agg(
        {
            "total_fee_usd": "sum",
            "total_fee_arb": "sum",
            "price": "mean",
        }
    )

    # filter greate than 0
    df_weekly = df_weekly[df_weekly["total_fee_usd"] > 0]

    # sort by week of month
    df_weekly = df_weekly.reset_index().sort_values(
        by="week_start_date", ascending=False
    )

    cols_order = [
        "week_start_date",
        "week_end_date",
        "xcall_caller",
        "asset",
        "price",
        "total_fee_arb",
        "total_fee_usd",
    ]

    return df_weekly[cols_order].reset_index(drop=True)


def main() -> None:
    st.title("ARB Distribution")

    st.text("Date Filter is inclusive of the date selected. Dates are in UTC timezone.")

    # filter_data
    filter_raw_data = apply_universal_sidebar_filters(ARB_DISTRIBUTION_DATA)

    st.subheader("ARB Distribution Weekly Data")
    weekly_df = user_weekly_distribution(filter_raw_data, ARB_HOURLY_PRICE_DATA)
    st.data_editor(weekly_df)
    theory()
    st.markdown("---")
    st.subheader("Raw Data for Destination: ARB chain")
    st.data_editor(filter_raw_data)
    return None


if __name__ == "__main__":
    main()
