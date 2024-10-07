import asyncio
import pandas as pd
from datetime import datetime, timedelta
import pytz
import streamlit as st
from setup import (
    get_raw_data_from_postgres_by_sql_for_invoices,
    get_chain_id_to_chain_name_data_from_bq,
    get_raw_data_from_bq_df,
)

from home import clean_all_metrics
from active_invoices_pipeline import get_all_active_invoices

# Steps
# geenerate invoice cross tabl of invoices
# generate invoice dsahboard


def invoices():
    invoices_df = asyncio.run(get_all_active_invoices())

    # clean the invoices_df
    # convert int to datetime

    # invoices_df["timestamp"] = pd.to_datetime(invoices_df["timestamp"])
    invoices_df["hub_invoice_enqueued_timestamp"] = pd.to_datetime(
        invoices_df["hub_invoice_enqueued_timestamp"], unit="s"
    )

    #  lower case invoice_id
    invoices_df["intent_id"] = invoices_df["intent_id"].str.lower()

    return invoices_df


def clean_active_invocie_df(c_data, p_data):
    active_invoices_df = invoices()
    invoice_intent_df = get_raw_data_from_postgres_by_sql_for_invoices("raw_invoices")

    # create one data from merge of invoice_intent_df and active_invoices_df
    merged_df = pd.merge(
        invoice_intent_df, active_invoices_df, left_on="id", right_on="intent_id"
    )

    merged_df["date"] = pd.to_datetime(
        merged_df["origin_timestamp"],
        unit="s",
    )
    cols_2_keep = [
        "id",
        "status",
        "hub_status_x",
        "date",
        "from_chain_id",
        "from_asset_symbol",
        "origin_timestamp",
        "hub_added_timestamp",
        "hub_invoice_enqueued_timestamp",
        "hub_invoice_entry_epoch",
        "origin_amount",
        "hub_invoice_amount",
        "owner",
        "discountBps",
        "destinations",
    ]
    merged_df = merged_df[cols_2_keep]
    # rename cols:
    merged_df = merged_df.rename(
        columns={
            "destinations": "to_chain_id",
            "hub_status_x": "hub_status",
        }
    )
    df_clean = merged_df.copy()
    # clean cols type
    df_clean["from_chain_id"] = df_clean["from_chain_id"].astype(int)
    df_clean["to_chain_id"] = df_clean["to_chain_id"].str[0].astype(int)
    df_clean["to_asset_symbol"] = df_clean["from_asset_symbol"]

    # 1. Chain IDs to Names using merge
    df_clean = df_clean.merge(
        c_data[["chainid", "name"]].rename(
            columns={"chainid": "from_chain_id", "name": "from_chain_name"}
        ),
        on="from_chain_id",
        how="left",
    )

    df_clean = df_clean.merge(
        c_data[["chainid", "name"]].rename(
            columns={"chainid": "to_chain_id", "name": "to_chain_name"}
        ),
        on="to_chain_id",
        how="left",
    )

    # 2. Pull price from BigQuery using merge
    df_clean = df_clean.merge(
        p_data[["symbol", "price", "date"]].rename(
            columns={"symbol": "from_asset_symbol", "price": "from_asset_price"}
        ),
        on=["from_asset_symbol"],
        how="left",
    )

    df_clean = df_clean.merge(
        p_data[["symbol", "price", "date"]].rename(
            columns={"symbol": "to_asset_symbol", "price": "to_asset_price"}
        ),
        on=["to_asset_symbol"],
        how="left",
    )

    # calculations
    cols_active_invoices = [
        "id",
        "status",
        "hub_status",
        "date",
        "from_chain_name",
        "to_chain_name",
        "from_asset_symbol",
        "to_asset_symbol",
        "origin_amount",
        "hub_invoice_amount",
        "owner",
        "discountBps",
        "origin_timestamp",
        "hub_added_timestamp",
        "hub_invoice_enqueued_timestamp",
        "hub_invoice_entry_epoch",
    ]
    return df_clean[cols_active_invoices]


def apply_universal_sidebar_invoice_filter(df):
    """
    Apply universal sidebar invoice filter.
    """

    # last 7 days
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=7),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    from_date = st.sidebar.date_input(
        "Start Date(inclusive)",
        value=default_start,
        max_value=default_end,
        key="start_date",
    )
    to_date = st.sidebar.date_input(
        "End Date(inclusive)",
        value=default_end,
        min_value=default_start,
        max_value=default_end,
        key="end_date",
    )

    # # sidebar filters inputs
    # # token filter
    # asset_options = list(set(pricing_data["symbol"].to_list()))
    # selected_asset = st.sidebar.multiselect(
    #     "Tokens/Assets:",
    #     options=asset_options,
    #     default=asset_options,
    #     key="asset",
    # )
    # chain_options = list(set(chain_metdata["name"].to_list()))
    # selected_chain = st.sidebar.multiselect(
    #     "Chains:",
    #     options=chain_options,
    #     default=chain_options,
    #     key="chain",
    # )

    # get the current time- apply date filters
    df = df[
        (df["date"] >= pd.to_datetime(from_date))
        & (df["date"] <= pd.to_datetime(to_date))
    ]

    return df


def show_active_invoices_by_condition(active_df, condition: int = 3):
    """condition: # of Hours more than.
    If an active invoice is in system for more than x hours, then intent is selected
    """
    # clean the active_df

    # get the current time
    current_time = datetime.now(pytz.utc)

    # calculate the time x hours ago
    x_hours_ago = current_time - timedelta(hours=condition)
    st.write(x_hours_ago)

    # if current timestamp is more than 3 hours between hub_added_timestamp and current time, then intent is selected
    active_df["is_intent_selected"] = (
        pd.to_datetime(
            active_df["hub_added_timestamp"],
            utc=True,
            unit="s",
        )
        >= x_hours_ago
    )

    # filter the dataframe to include only rows where the 'timestamp' column is greater than or equal to x_hours_ago
    filtered_df = active_df[active_df["is_intent_selected"]]

    filtered_df["url"] = f"https://explorer.everclear.org/intents/{filtered_df['id']}"

    return filtered_df


def main():
    # get metadata
    st.sidebar.header("Filters")
    st.sidebar.subheader("Time Range")

    chain_metdata = get_chain_id_to_chain_name_data_from_bq()
    latest_pricing_data = get_raw_data_from_bq_df("latest_pricing")
    clean_merged_df = clean_active_invocie_df(chain_metdata, latest_pricing_data)
    st.write(clean_merged_df)
    # Invoices with > 3hrs in system
    invoices_with_3hrs = show_active_invoices_by_condition(clean_merged_df, condition=3)

    st.header("Invoices with > 3hrs in system")
    if invoices_with_3hrs.empty:
        st.write("**No invoices with > 3hrs in system**")
    else:
        st.dataframe(
            invoices_with_3hrs,
            hide_index=True,
        )
    st.header("All Active Invoices")
    st.dataframe(clean_merged_df, hide_index=True)


if __name__ == "__main__":
    main()
