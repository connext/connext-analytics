import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from setup import (ARB_DISTRIBUTION_DATA, ARB_HOURLY_PRICE_DATA,
                   apply_universal_sidebar_filters)


def theory():
    st.markdown(
        """
        [Requirements](https://www.notion.so/Arb-Distribution-0297a6e965674e11867c98dc0420b610) | [Calculation and SQL](https://github.com/connext/connext-analytics/blob/main/src/streamlit_arb_distribution/sql/arb_distribution.sql) | Houlry Pricing Data Used
    """
    )


def clean_cal_data(df, arb_data):
    df_weekly = df.copy()
    arb_price = arb_data.copy()
    arb_price["hour"] = pd.to_datetime(arb_price["date"]).dt.hour
    arb_price["day"] = pd.to_datetime(arb_price["date"]).dt.date

    # join on date
    df_weekly = df_weekly.merge(
        arb_price, left_on=["day", "hour"], right_on=["day", "hour"], how="left"
    )

    # drop df_weekly date_y
    df_weekly.drop(columns=["date_x", "date_y"], inplace=True)

    # week of the month
    df_weekly["week_of_year"] = (
        pd.to_datetime(df_weekly["datetime"]).dt.isocalendar().week
    )
    df_weekly["month"] = pd.to_datetime(df_weekly["datetime"]).dt.month
    df_weekly["year"] = pd.to_datetime(df_weekly["datetime"]).dt.year
    df_weekly["week_start_date"] = (
        pd.to_datetime(df_weekly["datetime"]).dt.to_period("W").dt.start_time
    )
    df_weekly["week_end_date"] = (
        pd.to_datetime(df_weekly["datetime"]).dt.to_period("W").dt.end_time
    )
    df_weekly["total_fee_usd"] = (
        df_weekly["usd_gas_fee_amount"].fillna(0)
        + df_weekly["usd_relay_fee_1_amount"].fillna(0)
        + df_weekly["usd_relay_fee_2_amount"].fillna(0)
        + df_weekly["usd_amm_fee_amount"].fillna(0)
        + df_weekly["usd_router_fee_amount"].fillna(0)
    )

    df_weekly["total_fee_arb"] = df_weekly["total_fee_usd"] / df_weekly["price"]

    # st.write(df_weekly)

    return df_weekly


def aggregate_flow(df):
    df_agg = df.groupby(["origin_chain", "destination_chain"]).agg(
        {
            "total_fee_usd": "sum",
            "total_fee_arb": "sum",
            "usd_destination_amount": "sum",
            "price": "mean",
            "transfer_id": "nunique",
        }
    )
    df_agg.rename(columns={"transfer_id": "transfer_count"}, inplace=True)
    return df_agg.reset_index()


def user_weekly_distribution(df):
    df_weekly = df.groupby(
        [
            "user_address",
            "destination_asset",
        ]
    ).agg(
        total_fee_usd=("total_fee_usd", "sum"),
        total_fee_arb=("total_fee_arb", "sum"),
        arb_usd_price=("price", "mean"),
        first_tx_date=("datetime", "min"),
        last_tx_date=("datetime", "max"),
    )
    # filter greate than 0
    df_weekly = df_weekly[df_weekly["total_fee_usd"] > 0]

    # sort by week of month
    df_weekly = df_weekly.reset_index()

    cols_order = [
        "user_address",
        "destination_asset",
        "arb_usd_price",
        "total_fee_arb",
        "total_fee_usd",
    ]

    return df_weekly[cols_order].reset_index(drop=True)


def plot_sankey(df, col="usd_destination_amount"):
    # Prepare data for Sankey diagram
    all_nodes = list(set(df["origin_chain"]).union(set(df["destination_chain"])))
    node_indices = {node: idx for idx, node in enumerate(all_nodes)}

    source_indices = [node_indices[origin] for origin in df["origin_chain"]]
    target_indices = [
        node_indices[destination] for destination in df["destination_chain"]
    ]
    values = df[col]

    # Add metric values to the labels
    all_nodes_with_values = [
        f"{node} ({df[df['origin_chain'] == node][col].sum() + df[df['destination_chain'] == node][col].sum():,.2f})"
        for node in all_nodes
    ]

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=all_nodes_with_values,  # Updated labels with values
                ),
                link=dict(
                    source=source_indices,
                    target=target_indices,
                    value=values,
                ),
            )
        ]
    )

    fig.update_layout(
        title_text=f"Flow of {col} from origin to destination",
        font_size=10,
    )
    st.plotly_chart(fig)


def plot_daily_arb_distribution(df, col):
    daily = (
        df.groupby(["day", "origin_chain"])
        .agg(
            {
                "usd_destination_amount": "sum",
                "total_fee_usd": "sum",
                "total_fee_arb": "sum",
            }
        )
        .reset_index()
    )

    fig = px.bar(
        daily,
        x="day",
        y=col,
        color="origin_chain",
        title=f"Daily {col} by Source Chain",
        labels={"origin_chain": "Source Chain", col: col.replace("_", " ").title()},
    )
    fig.update_layout(yaxis_title=col.replace("_", " ").title())
    fig.update_layout(xaxis_title="Date")
    st.plotly_chart(fig)


def get_only_eth_june_30_data(df):
    df_filter = df[
        (df["datetime"] >= "2024-06-30")
        & (df["datetime"] < "2024-07-01")
        & (df["origin_chain"] == "Ethereum Mainnet")
    ]
    cols_2_keep = [
        "datetime",
        "origin_chain",
        "destination_chain",
        "destination_amount",
        "usd_destination_amount",
        "transfer_id",
    ]
    return df_filter[cols_2_keep]


def main() -> None:
    st.title("ARB Distribution")

    st.text(
        f"Date Filter is inclusive of the date selected. Dates are in UTC timezone. {theory()}"
    )

    # filter_data
    filter_raw_data = apply_universal_sidebar_filters(ARB_DISTRIBUTION_DATA)
    df_clean = clean_cal_data(filter_raw_data, ARB_HOURLY_PRICE_DATA)
    st.markdown("---")

    agg_flow = aggregate_flow(df_clean)

    destination_volume_usd = agg_flow["usd_destination_amount"].sum()
    fee_arb = agg_flow["total_fee_arb"].sum()
    fee_usd = agg_flow["total_fee_usd"].sum()

    weekly_df = user_weekly_distribution(df_clean)

    st.markdown(
        """
    ### Weekly Aggregated Data by Xcaller.
    """
    )
    st.download_button(
        label="Download Weekly Data",
        data=weekly_df.to_csv(index=False),
        file_name="weekly_arb_distribution.csv",
        mime="text/csv",
    )
    st.dataframe(
        weekly_df,
        width=1500,
    )
    st.markdown("---")

    col = st.selectbox(
        "Select a metric",
        ["usd_destination_amount", "total_fee_usd", "total_fee_arb"],
    )

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        st.subheader(
            """
            Summary
            """
        )
        st.metric(
            label="ARB Destination Volume",
            value="$ " + str(round(destination_volume_usd, 0)),
        )
        st.metric(label="Fee (USD)", value="$ " + str(round(fee_usd, 0)))
        st.metric(label="Fee (ARB)", value="ARB " + str(round(fee_arb, 0)))
        st.markdown("---")

    with c2:
        st.subheader("Flow of USD Destination Amount")

        plot_sankey(agg_flow, col=col)
    with c3:
        plot_daily_arb_distribution(df_clean, col=col)

    st.markdown("---")
    st.subheader("Raw Data")
    st.text("Filter applied raw data from the Transfers table. See SQL for details.")
    st.data_editor(df_clean)

    # From <> to chain aggregate raw data
    st.dataframe(agg_flow)
    return None


if __name__ == "__main__":
    main()
