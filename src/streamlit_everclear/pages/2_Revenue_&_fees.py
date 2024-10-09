import logging
import pandas as pd
from datetime import datetime, timedelta
import pytz
import streamlit as st
from setup import (
    get_raw_data_from_postgres_by_sql,
    get_chain_id_to_chain_name_data_from_bq,
    get_raw_data_from_bq_df,
    get_db_url,
)

# TODO:

# [ ] Adding Revenue
# [ ] Adding Fees - gas fee paid - by Everclear for message (Relay)
# [ ] Adding Message Cost - by Everclear for message Cost
#  [ ] Add Date: Lasty X days

# calculations steps for each
# 1. Revenue -> 1 bps fo orgin amount
# 2. Discount applied to invoice if any - per intent
# 3. pricing rate per intent by different timestamps


def apply_sidebar_filters(
    old_df: pd.DataFrame,
    selected_chain,
    selected_asset,
    from_date,
    to_date,
    status_flag,
    hub_status_flag,
    intent_message_status_flag,
    settlement_message_status_flag,
):
    """
    Apply sidebar filters to the DataFrame based on selected chains, assets, and date range.
    """
    df = old_df.copy()

    try:
        if from_date and to_date:
            df["datepart"] = pd.to_datetime(df["origin_timestamp"]).dt.date
            df = df[(df["datepart"] >= from_date) & (df["datepart"] <= to_date)]

        if selected_chain:
            df = df[
                df["from_chain_name"].isin(selected_chain)
                | df["to_chain_name"].isin(selected_chain)
            ]

        if selected_asset:
            df = df[
                df["from_asset_symbol"].isin(selected_asset)
                | df["to_asset_symbol"].isin(selected_asset)
            ]

        if status_flag:
            df = df[df["status"].isin(status_flag)]

        if hub_status_flag:
            df = df[df["hub_status"].isin(hub_status_flag)]

        if intent_message_status_flag:
            df = df[df["intent_message_status"].isin(intent_message_status_flag)]
        if settlement_message_status_flag:
            df = df[
                df["settlement_message_status"].isin(settlement_message_status_flag)
            ]

        return df
    except Exception as e:
        logging.error(f"Error in apply_sidebar_filters: {e}")


def clean_revenue_gas_data(df, price_df, chains_json):
    """
    clean_revenue_gas_data
        - for simplcity adding price based on origin timestamp( closest hour)

    price for
        - origin amount
        - quote amount
        - gas amount(intent/settle messages)
        -
    """
    cdf = df.copy()
    cdf["price_hour"] = cdf["origin_timestamp"].dt.floor("H")
    price_df["price_hour"] = price_df["date"].dt.floor("H")
    price_df["gas_token_symbol"] = price_df["symbol"].apply(
        lambda x: "ETH" if x == "WETH" else "BNB"
    )

    # merge price data on from_asset_symbol, price_hour <> symbol, price_hour
    cdf = cdf.merge(
        price_df,
        left_on=["price_hour", "from_asset_symbol"],
        right_on=["price_hour", "symbol"],
        how="inner",
    )
    cdf.rename(
        columns={"price": "price_for_amounts"},
        inplace=True,
    )
    cdf.drop(columns=["symbol", "date", "gas_token_symbol"], inplace=True)
    # merge price for intent message gas token symbol
    cdf = cdf.merge(
        price_df[price_df["symbol"].isin(["WETH", "BNB"])],
        left_on=["price_hour", "intent_gas_token_symbol"],
        right_on=["price_hour", "gas_token_symbol"],
        how="inner",
    )

    cdf.rename(
        columns={"price": "price_for_intent_message_gas_token"},
        inplace=True,
    )

    cdf.drop(columns=["symbol", "date", "gas_token_symbol"], inplace=True)

    # merge price for settlement gas token symbol
    # st.write(cdf)
    cdf = cdf.merge(
        price_df[price_df["symbol"].isin(["WETH", "BNB"])],
        left_on=["price_hour", "settlement_gas_token_symbol"],
        right_on=["price_hour", "gas_token_symbol"],
        how="inner",
    )
    cdf.rename(
        columns={"price": "price_for_settlement_gas_token"},
        inplace=True,
    )
    cdf.drop(columns=["symbol", "date", "gas_token_symbol"], inplace=True)

    # calculate price based on origin amount
    cdf["origin_amount_usd"] = cdf["price_for_amounts"] * cdf["origin_amount"]
    cdf["hub_invoice_amount_usd"] = cdf["price_for_amounts"] * cdf["hub_invoice_amount"]
    cdf["settlement_amount_usd"] = cdf["price_for_amounts"] * cdf["settlement_amount"]
    cdf["quote_for_intent_message_amount_usd"] = (
        cdf["price_for_amounts"] * cdf["intent_message_quote_per_intent"]
    )
    cdf["intent_message_gas_amount_usd"] = (
        cdf["price_for_intent_message_gas_token"]
        * cdf["intent_message_gas_amount_per_intent"]
    )
    cdf["quote_for_settlement_message_amount_usd"] = (
        cdf["price_for_amounts"] * cdf["settlement_message_quote_per_intent"]
    )
    cdf["settlement_message_gas_amount_usd"] = (
        cdf["price_for_settlement_gas_token"]
        * cdf["settlement_message_gas_amount_per_intent"]
    )

    # TODO later create status flags:
    # convert chains to names

    cdf["from_chain_name"] = cdf["from_chain_id"].map(chains_json)
    cdf["to_chain_name"] = cdf["to_chain_id"].map(chains_json)

    # gas symbols cols rename
    cdf.rename(
        columns={
            "intent_gas_token_symbol": "symbol_for_intent_message_gas_token",
            "settlement_gas_token_symbol": "symbol_for_settlement_gas_token",
        },
        inplace=True,
    )
    cols_to_keep = [
        "id",
        "status",
        "hub_status",
        "intent_message_status",
        "settlement_message_status",
        "origin_timestamp",
        "settlement_timestamp",
        "from_chain_name",
        "from_chain_id",
        "to_chain_name",
        "to_chain_id",
        "from_asset_symbol",
        "to_asset_symbol",
        "symbol_for_intent_message_gas_token",
        "symbol_for_settlement_gas_token",
        "price_for_amounts",
        "price_for_intent_message_gas_token",
        "price_for_settlement_gas_token",
        "origin_amount",
        "hub_invoice_amount",
        "settlement_amount",
        "intent_message_gas_amount_per_intent",
        "settlement_message_gas_amount_per_intent",
        "intent_message_gas_amount_usd",
        "origin_amount_usd",
        "settlement_amount_usd",
        "hub_invoice_amount_usd",
        "settlement_message_gas_amount_usd",
        "quote_for_intent_message_amount_usd",
        "quote_for_settlement_message_amount_usd",
    ]

    # convert message status to flags
    return cdf[cols_to_keep]


def calculate_revenue_fees_metrics(cdf):
    """
    calculate_revenue_fees_metrics
    """
    df = cdf.copy()
    # rename and document
    df.rename(
        columns={
            "symbol_for_intent_message_gas_token": "intent_message_gas_token",
            "symbol_for_settlement_gas_token": "settlement_message_gas_token",
            "price_for_amounts": "from_asset_symbol_token_price",
            "price_for_intent_message_gas_token": "intent_message_gas_token_price",
            "price_for_settlement_gas_token": "settlement_message_gas_token_price",
            "quote_for_intent_message_amount_usd": "intent_message_cost_usd",
            "quote_for_settlement_message_amount_usd": "settlement_message_cost_usd",
        },
        inplace=True,
    )
    df["revenue_usd"] = df["origin_amount_usd"] * 0.0001
    # total cost
    df["total_cost_usd"] = (
        df["intent_message_cost_usd"]
        + df["settlement_message_cost_usd"]
        + df["intent_message_gas_amount_usd"]
        + df["settlement_message_gas_amount_usd"]
    )
    df["market_maker_discount_usd"] = (
        df["hub_invoice_amount_usd"] - df["settlement_amount_usd"]
    )

    # total profit
    df["total_profit_usd"] = df["revenue_usd"] - df["total_cost_usd"]

    cols_to_keep = [
        "id",
        "status",
        "origin_timestamp",
        "settlement_timestamp",
        # chain and token
        "from_chain_name",
        "to_chain_name",
        "from_asset_symbol",
        "to_asset_symbol",
        # gas tokens
        "intent_message_gas_token",
        "settlement_message_gas_token",
        # all amounts
        "revenue_usd",
        "total_cost_usd",
        "total_profit_usd",
        # invloved amounts
        "intent_message_cost_usd",
        "settlement_message_cost_usd",
        "intent_message_gas_amount_usd",
        "settlement_message_gas_amount_usd",
        # prices
        "from_asset_symbol_token_price",
        "intent_message_gas_token_price",
        "settlement_message_gas_token_price",
    ]
    return df[cols_to_keep]


def main():
    st.title("Revenue & Fees")
    # hourly price data
    # TODO: add revenue data
    revenue_gas_raw_data = get_raw_data_from_postgres_by_sql(
        sql_file_name="revenue_fee_gas_per_intent",
        db_url=get_db_url(mode="prod"),
    )
    chain_metdata = get_chain_id_to_chain_name_data_from_bq()
    chains_maps = {
        int(row["chainid"]): row["name"]
        for row in chain_metdata.to_dict(orient="records")
    }

    hourly_pricing = get_raw_data_from_bq_df("hourly_pricing")

    # data view
    # st.write(revenue_gas_raw_data)

    # clean data
    cleaned_data = clean_revenue_gas_data(
        revenue_gas_raw_data, hourly_pricing, chains_maps
    )

    st.sidebar.header("Filters")
    st.sidebar.subheader("Time Range")
    # last 7 days
    default_start, default_end = (
        cleaned_data["origin_timestamp"].dt.date.min(),
        cleaned_data["origin_timestamp"].dt.date.max(),
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

    # sidebar filters inputs
    # token filter
    asset_options = list(set(hourly_pricing["symbol"].to_list()))
    selected_asset = st.sidebar.multiselect(
        "Tokens/Assets:",
        options=asset_options,
        default=asset_options,
        key="asset",
    )
    chain_options = list(set(chain_metdata["name"].to_list()))
    selected_chain = st.sidebar.multiselect(
        "Chains:",
        options=chain_options,
        default=chain_options,
        key="chain",
    )
    status_options = list(set(cleaned_data["status"].to_list()))
    status_flag = st.sidebar.multiselect(
        "Status:",
        options=status_options,
        default=status_options,
        key="status",
    )
    hub_status_options = list(set(cleaned_data["hub_status"].to_list()))
    hub_status_flag = st.sidebar.multiselect(
        "Hub Status:",
        options=hub_status_options,
        default=hub_status_options,
        key="hub_status",
    )
    intent_message_status_options = list(
        set(cleaned_data["intent_message_status"].to_list())
    )
    intent_message_status_flag = st.sidebar.multiselect(
        "Intent Message Status:",
        options=intent_message_status_options,
        default=intent_message_status_options,
        key="intent_message_status",
    )
    settlement_message_status_options = list(
        set(cleaned_data["settlement_message_status"].to_list())
    )
    settlement_message_status_flag = st.sidebar.multiselect(
        "Settlement Message Status:",
        options=settlement_message_status_options,
        default=settlement_message_status_options,
        key="settlement_message_status",
    )

    final_df = apply_sidebar_filters(
        cleaned_data,
        selected_chain,
        selected_asset,
        from_date,
        to_date,
        status_flag,
        hub_status_flag,
        intent_message_status_flag,
        settlement_message_status_flag,
    )
    cal_final_df = calculate_revenue_fees_metrics(final_df)
    st.metric("Total numer of Intents", len(final_df))
    # st.dataframe(final_df, use_container_width=True, hide_index=True)
    st.markdown(
        f"""
        ---
        ### Metrics Definitions
        - **Revenue**: Revenue is calculated as 0.01% of the origin amount.
        - **Cost**: Cost is the sum of intent message cost, settlement message cost, intent message gas cost, and settlement message gas cost. 
        This represenrs Hyperlane cost. The quote per intents and gas for it.
        - **Profit**: Profit is the difference between revenue and cost.
        - **Intent Message Cost**: Hyperlane message cost for sending intent message.
        - **Settlement Message Cost**: Hyperlane message cost for sending settlement message.
        - **Intent Message Gas Cost**: Hyperlane gas cost for sending intent message.
        - **Settlement Message Gas Cost**: Hyperlane gas cost for sending settlement message.
        
        Some Key points:
        - Gas is calculated as Gas Price * Gas Limit
        - Price data used is Hourly and anchored at Origin timestamp, ie when intent was created(for simplicity)
        - Use combinaion of Status, Hub Status, Intent Message Status, and Settlement Message Status to get the desired data.
        - By Default all options are selected which gives in data
        
        **Filters**
        - **Time Range**: Select a date range
        - **Chains**: Select chains(will show if in either origin or destination chains) 
        - **Tokens**: Select tokens(will show if in either origin and destination tokens)
        - **Status**: Select status(optional)
            - `settled_and_completed`: Intent is completed
            - `dispatched_hub`: Intent is on Hub
        - **Hub Status**: Select hub status(optional)
            - `dispatched_and_unsupported`: Intent is on Hub but destination chain is not supported by Everclear
            - `dispatched`: Itent is supported
        - **Intent Message Status**
            - `pending`
            - `delivered`
        - **Settlement Message Status**
            - `pending`
            - `delivered`
        ---
        ### Revenue & Fees Metrics
        **for dates between {from_date} to {to_date}**
        """
    )

    st.dataframe(
        cal_final_df.sort_values("origin_timestamp", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

    # flow metrics -> aggreagted by chains and tokens
    st.markdown(
        """
        ---
        ### Metrics Aggreagted by Chains and Tokens
        
        """
    )

    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    with col1:
        st.metric("Total Revenue", f"${cal_final_df['revenue_usd'].sum():.0f}")
    with col2:
        st.metric("Total Cost", f"${cal_final_df['total_cost_usd'].sum():.0f}")
    with col3:
        st.metric("Total Profit", f"${cal_final_df['total_profit_usd'].sum():.0f}")
    with col4:
        st.metric(
            "Intent message Cost",
            f"${cal_final_df['intent_message_cost_usd'].sum():.2f}",
        )
    with col5:
        st.metric(
            "Settlement Message Cost",
            f"${cal_final_df['settlement_message_cost_usd'].sum():.2f}",
        )
    with col6:
        st.metric(
            "Settlement Message Gas Cost",
            f"${cal_final_df['settlement_message_gas_amount_usd'].sum():.0f}",
        )
    with col7:
        st.metric(
            "Intent Message Gas Cost",
            f"${cal_final_df['intent_message_gas_amount_usd'].sum():.0f}",
        )

    final_agg = (
        cal_final_df.groupby(
            ["from_chain_name", "to_chain_name", "from_asset_symbol", "to_asset_symbol"]
        )
        .agg(
            total_revenue_usd=("revenue_usd", "sum"),
            total_cost_usd=("total_cost_usd", "sum"),
            total_profit_usd=("total_profit_usd", "sum"),
        )
        .reset_index()
    )
    st.dataframe(final_agg, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
