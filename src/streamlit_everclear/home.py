import logging
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import pytz
import streamlit as st
from setup import (
    apply_sidebar_filters,
    get_agg_data_from_sql_template,
    get_chain_id_to_chain_name_data_from_bq,
    get_pricing_data_from_bq,
    get_raw_data_from_postgres_by_sql,
    get_db_url,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

st.set_page_config(
    page_title="Everclear",
    page_icon="💸",
    layout="wide",
)
PROD_URL = get_db_url(mode="prod")


def get_metrics(
    from_date: datetime,
    to_date: datetime,
    mode: str = "prod",
) -> pd.DataFrame:
    all_agg_metrics = pd.DataFrame()
    all_daily_metrics = pd.DataFrame()
    for metrics in [
        "settled_metrics",
        "netting_metrics",
        "overall_metrics",
        "settlement_rate_metrics",
    ]:
        logger.info(f"Getting {metrics} agg data")
        agg = get_agg_data_from_sql_template(
            f"agg_{metrics}",
            date_filter={"from_date": from_date, "to_date": to_date},
            mode=mode,
        )

        agg["from_chain_id"] = agg["from_chain_id"].astype(int)
        agg["to_chain_id"] = agg["to_chain_id"].astype(int)

        daily = get_raw_data_from_postgres_by_sql(f"daily_{metrics}", db_url=PROD_URL)
        daily["day"] = pd.to_datetime(daily["day"])
        daily["from_chain_id"] = daily["from_chain_id"].astype(int)
        daily["to_chain_id"] = daily["to_chain_id"].astype(int)

        if all_agg_metrics.empty:
            all_agg_metrics = agg
            logger.info(f"Initialized all_agg_metrics with {metrics}")
        else:
            all_agg_metrics = pd.merge(
                all_agg_metrics,
                agg,
                on=[
                    "from_chain_id",
                    "from_asset_symbol",
                    "to_chain_id",
                    "to_asset_symbol",
                ],
                how="outer",
            )
            logger.info(f"Merged {metrics} into all_agg_metrics")

        if all_daily_metrics.empty:
            all_daily_metrics = daily
            logger.info(f"Initialized all_daily_metrics with {metrics}")
        else:
            all_daily_metrics = pd.merge(
                all_daily_metrics,
                daily,
                on=[
                    "day",
                    "from_chain_id",
                    "from_asset_symbol",
                    "to_chain_id",
                    "to_asset_symbol",
                ],
                how="outer",
            )
            logger.info(f"Merged {metrics} into all_daily_metrics")

    return all_agg_metrics, all_daily_metrics


def clean_all_metrics(df: pd.DataFrame, c_data: pd.DataFrame, p_data: pd.DataFrame):
    """In this function, convert raw to clean
    1. chain ids to names
    2. pull price from big query
    3. convert the amounts to USD amounts
    4. return the clean df
    """

    df_clean = df.copy()

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
            columns={
                "symbol": "from_asset_symbol",
                "price": "from_asset_price",
                "date": "day",
            }
        ),
        on=["from_asset_symbol", "day"],
        how="left",
    )

    df_clean = df_clean.merge(
        p_data[["symbol", "price", "date"]].rename(
            columns={
                "symbol": "to_asset_symbol",
                "price": "to_asset_price",
                "date": "day",
            }
        ),
        on=["to_asset_symbol", "day"],
        how="left",
    )

    # 3. Adding USD columns for all amounts
    df_clean["volume_settled_by_mm_usd"] = (
        df_clean["volume_settled_by_mm"] * df_clean["from_asset_price"]
    )
    df_clean["discounts_by_mm_usd"] = (
        df_clean["discounts_by_mm"] * df_clean["from_asset_price"]
    )
    df_clean["avg_discounts_by_mm_usd"] = (
        df_clean["avg_discounts_by_mm"] * df_clean["from_asset_price"]
    )
    df_clean["rewards_for_invoices_usd"] = (
        df_clean["rewards_for_invoices"] * df_clean["from_asset_price"]
    )
    df_clean["avg_rewards_by_invoices_usd"] = (
        df_clean["avg_rewards_by_invoices"] * df_clean["from_asset_price"]
    )
    df_clean["netting_volume_usd"] = (
        df_clean["netting_volume"] * df_clean["from_asset_price"]
    )
    df_clean["clearing_volume_usd"] = (
        df_clean["clearing_volume"] * df_clean["from_asset_price"]
    )
    df_clean["protocol_revenue_usd"] = (
        df_clean["protocol_revenue"] * df_clean["from_asset_price"]
    )
    df_clean["rebalancing_fee_usd"] = (
        df_clean["rebalancing_fee"] * df_clean["from_asset_price"]
    )
    df_clean["avg_intent_size_usd"] = (
        df_clean["avg_intent_size"] * df_clean["from_asset_price"]
    )

    # keep only the columns we need
    cols_to_keep = [
        "day",
        "from_chain_name",
        "to_chain_name",
        "from_asset_symbol",
        "to_asset_symbol",
        "volume_settled_by_mm_usd",
        "discounts_by_mm_usd",
        "avg_discounts_by_mm_usd",
        "rewards_for_invoices_usd",
        "avg_rewards_by_invoices_usd",
        "netting_volume_usd",
        "clearing_volume_usd",
        "protocol_revenue_usd",
        "rebalancing_fee_usd",
        "avg_intent_size_usd",
        "total_intents",
        "total_invoices_by_mm",
        "avg_settlement_time_in_hrs_by_mm",
        "apy",
        "avg_discount_epoch_by_mm",
        "avg_netting_time_in_hrs",
        "avg_intents_settled_in_6_hrs",
        "avg_intents_settled_in_24_hrs",
        "mm_avg_intents_settled_in_1_hr",
        "mm_avg_intents_settled_in_3_hr",
        "avg_intents_in_6_hrs",
        "avg_intents_in_24_hrs",
        "mm_avg_intents_in_1_hr",
        "mm_avg_intents_in_3_hr",
        "daily_avg_settlement_rate_6h",
        "daily_avg_settlement_rate_24h",
        "mm_daily_avg_settlement_rate_1h_percentage",
        "mm_daily_avg_settlement_rate_3h_percentage",
    ]

    # cleaing volume
    df_clean["clearing_volume_usd"] = df_clean["netting_volume_usd"].fillna(
        0
    ) + df_clean["volume_settled_by_mm_usd"].fillna(0)
    return df_clean[cols_to_keep]


def create_weekly_cohort_plot(df: pd.DataFrame) -> None:
    # Ensure the date columns are in datetime format
    df["cohort_week"] = pd.to_datetime(df["cohort_week"])
    df["week"] = pd.to_datetime(df["week"])

    # Pivot the data to create a matrix
    cohort_pivot = df.pivot_table(
        index="cohort_week", columns="weeks_since_cohort", values="retention_rate"
    )

    # Plot the heatmap using Plotly
    fig = px.imshow(
        cohort_pivot,
        labels=dict(x="Weeks Since Cohort", y="Cohort Week", color="Retention Rate"),
        x=cohort_pivot.columns,
        y=cohort_pivot.index,
        color_continuous_scale="Blues",
    )

    fig.update_layout(
        xaxis_title="Weeks Since Cohort",
        yaxis_title="Cohort Week",
    )

    st.plotly_chart(fig)


def plot_trend_chart_by_col(df, metric_col, agg_type):
    """
    Plot a trend chart by column
    TYPES:
        sum: sum of the metric
        avg: average of the metric
    """
    if agg_type == "sum":
        plot_df = df.groupby("day")[metric_col].sum(numeric_only=True).reset_index()
    elif agg_type == "avg":
        plot_df = df.groupby("day")[metric_col].mean(numeric_only=True).reset_index()
    fig = px.bar(plot_df, x="day", y=metric_col)
    fig.update_traces(marker_color="#6F83E4")

    st.plotly_chart(fig)


def metric_dashboard(mode: str = "prod") -> None:
    """Steps:
    1. get all raw data for each metric on daily basis- 13 metrics
    2.  create a formated data based on the metrics
    3. apply filters on the data
    4. plot the data into line chart and big number
    """

    st.sidebar.header("Filters")
    st.sidebar.subheader("Time Range")

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

    chain_metdata = get_chain_id_to_chain_name_data_from_bq()

    pricing_data = get_pricing_data_from_bq()
    pricing_data["date"] = pd.to_datetime(pricing_data["date"])
    agg_avg_price = pricing_data.groupby("symbol").agg({"price": "mean"}).reset_index()
    # add todays date
    todays_date = datetime.now(pytz.utc).strftime("%Y-%m-%d")
    agg_avg_price["date"] = todays_date

    # sidebar filters inputs
    # token filter
    asset_options = list(set(pricing_data["symbol"].to_list()))
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

    # get metrics
    raw_agg_metrics, raw_daily_metrics = get_metrics(
        from_date=from_date,
        to_date=to_date,
        mode=mode,
    )
    user_retention = get_raw_data_from_postgres_by_sql(
        "daily_user_retention", db_url=PROD_URL
    )

    # add todays date to raw_agg_metrics
    raw_agg_metrics["day"] = todays_date

    # clean data
    clean_agg_metrics = clean_all_metrics(raw_agg_metrics, chain_metdata, agg_avg_price)
    clean_agg_metrics["day"] = f"{str(from_date)} to {str(to_date)}"
    clean_daily_metrics = clean_all_metrics(
        raw_daily_metrics, chain_metdata, pricing_data
    )

    # apply sidebar filters
    filtered_clean_agg_metrics = apply_sidebar_filters(
        clean_agg_metrics,
        selected_chain,
        selected_asset,
        is_agg=True,
        from_date=from_date,
        to_date=to_date,
    )
    filtered_clean_daily_metrics = apply_sidebar_filters(
        clean_daily_metrics,
        selected_chain,
        selected_asset,
        is_agg=False,
        from_date=from_date,
        to_date=to_date,
    )

    # ----------------------------------------------------------------------------
    # Metric 1: Clearing Volume
    st.markdown("---")
    st.subheader("Clearing Volume")
    # display metrics
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['clearing_volume_usd'].sum():,.0f}",
        )

    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "clearing_volume_usd", agg_type="sum"
        )

    # ----------------------------------------------------------------------------

    # ----------------------------------------------------------------------------
    # Metric 2: Netting Volume
    st.markdown("---")
    st.subheader("Netting Volume")
    # display metrics
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['netting_volume_usd'].sum():,.0f}",
        )

    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "netting_volume_usd", agg_type="sum"
        )

    # ----------------------------------------------------------------------------

    # Metric 3: Settlement_Volume
    st.markdown("---")
    st.subheader("Settlement Volume- by Market Maker")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['volume_settled_by_mm_usd'].sum():,.0f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "volume_settled_by_mm_usd", agg_type="sum"
        )
    # Metric 4: Total_rebalaicing fee
    st.markdown("---")
    st.subheader("Total Rebalancing Fee")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['rebalancing_fee_usd'].sum():,.0f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "rebalancing_fee_usd", agg_type="sum"
        )

    # Metric 5: Amount of intents
    st.markdown("---")
    st.subheader("Total #. of intents")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"{filtered_clean_agg_metrics['total_intents'].sum():,.0f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "total_intents", agg_type="sum"
        )

    # Metric 6: Average intent size
    st.markdown("---")
    st.subheader("Average intent size")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['avg_intent_size_usd'].mean():,.0f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "avg_intent_size_usd", agg_type="avg"
        )
    # Metric 7: Netting_Time
    st.markdown("---")
    st.subheader("Avg. Netting Time")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"{filtered_clean_agg_metrics['avg_netting_time_in_hrs'].mean():,.3f} hrs",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "avg_netting_time_in_hrs", agg_type="avg"
        )
    # Metric 8: Settlement_Time
    st.markdown("---")
    st.subheader("Avg. Settlement Time- by Market Maker")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"{filtered_clean_agg_metrics['avg_settlement_time_in_hrs_by_mm'].mean():,.0f} hrs",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics,
            "avg_settlement_time_in_hrs_by_mm",
            agg_type="avg",
        )
    # Metric 9: Total_Protocol_Revenue
    st.markdown("---")
    st.subheader("Total Protocol Revenue")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['protocol_revenue_usd'].sum():,.0f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "protocol_revenue_usd", agg_type="sum"
        )

    # Metric 10: APY Market Maker
    st.markdown("---")
    st.subheader("APY Market Maker")
    col1, col2 = st.columns([1, 3])

    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"""
            {
                filtered_clean_agg_metrics[filtered_clean_agg_metrics['apy'] < 500]
                ['apy'].mean():,.2f} %
            """,
        )

    with col2:
        st.subheader("APY by Market Maker- by Paths(chain and asset pairs)")
        st.dataframe(
            # remove none
            filtered_clean_agg_metrics[filtered_clean_agg_metrics["apy"].notna()][
                [
                    "from_chain_name",
                    "to_chain_name",
                    "from_asset_symbol",
                    "to_asset_symbol",
                    "apy",
                    "discounts_by_mm_usd",
                    "volume_settled_by_mm_usd",
                ]
            ].sort_values(by="volume_settled_by_mm_usd", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    # Metric 11: Average amount of epochs
    st.markdown("---")
    st.subheader("Average amount of epochs")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"for dates: {from_date} to {to_date}",
            value=f"{filtered_clean_agg_metrics['avg_discount_epoch_by_mm'].mean():,.0f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "avg_discount_epoch_by_mm", agg_type="avg"
        )

    # Metric 12: Discount_value
    st.markdown("---")
    st.subheader("Discount amount- by Market Maker")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"Total discount amount for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['discounts_by_mm_usd'].sum():,.0f}",
        )
        st.metric(
            label=f"Avg. discount amount for dates: {from_date} to {to_date}",
            value=f"$ {filtered_clean_agg_metrics['avg_discounts_by_mm_usd'].mean():,.2f}",
        )
    with col2:
        plot_trend_chart_by_col(
            filtered_clean_daily_metrics, "discounts_by_mm_usd", agg_type="sum"
        )

    # settlement rate
    st.markdown("---")
    st.subheader("Settlement Rate by chain and asset pairs")
    st.markdown(
        """Note: Null values indicate that there were no intents for the day.

    1. 6h Settlement Rate (%): For all intents(netted + market maker settled) settled in 6 hours
    2. 24h Settlement Rate (%): For all intents(netted + market maker settled) settled in 24 hours
    3. 1h Market Maker Rate (%): For intents settled by only by market maker in 1 hour
    4. 3h Market Maker Rate (%): For intents settled by only by market maker in 3 hours
    """
    )
    # Mapping of new column names
    settlement_cols = [
        "from_chain_name",
        "to_chain_name",
        "from_asset_symbol",
        "to_asset_symbol",
        "total_intents",
        "daily_avg_settlement_rate_6h",
        "daily_avg_settlement_rate_24h",
        "mm_daily_avg_settlement_rate_1h_percentage",
        "mm_daily_avg_settlement_rate_3h_percentage",
    ]
    new_column_names = {
        "from_chain_name": "From Chain",
        "to_chain_name": "To Chain",
        "from_asset_symbol": "From Asset",
        "to_asset_symbol": "To Asset",
        "total_intents": "Total Intents",
        "daily_avg_settlement_rate_6h": "6h Settlement Rate (%)",
        "daily_avg_settlement_rate_24h": "24h Settlement Rate (%)",
        "mm_daily_avg_settlement_rate_1h_percentage": "1h Market Maker Rate (%)",
        "mm_daily_avg_settlement_rate_3h_percentage": "3h Market Maker Rate (%)",
    }
    only_settlement = filtered_clean_agg_metrics[settlement_cols].reset_index(drop=True)
    # Rename columns and display the dataframe
    st.dataframe(
        only_settlement.rename(columns=new_column_names).sort_values(
            by="Total Intents", ascending=False
        ),
        use_container_width=True,
        hide_index=True,
    )
    # Metric: Wallet_retention rate
    st.markdown(
        """
    ---
    ### Wallet Retention Rate
    Note: Wallet Retention Rate is aggregated on weekly basis. The plot will be useful few weeks from now.
    """
    )
    create_weekly_cohort_plot(user_retention)

    st.markdown("---")
    st.header("Metrics Data")
    st.subheader("Aggreagted data ")
    st.dataframe(filtered_clean_agg_metrics)
    st.subheader("Daily data ")
    st.dataframe(filtered_clean_daily_metrics)


def main() -> None:
    st.title("Everclear Metrics")
    st.markdown(
        """
        """
    )

    metric_dashboard()
    return None


if __name__ == "__main__":
    main()
