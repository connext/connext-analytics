import streamlit as st
import pandas as pd
import plotly.express as px
from setup import (
    get_raw_data_from_postgres_by_sql,
    get_agg_data_from_sql_template,
    apply_date_filter_to_df,
    convert_to_token_address,
    get_chains_assets_metadata,
)
from datetime import datetime, timedelta
import pytz
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

st.set_page_config(
    page_title="Everclear",
    page_icon="💸",
    layout="wide",
)


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
    ]:
        logger.info(f"Getting {metrics} agg data")
        agg = get_agg_data_from_sql_template(
            f"agg_{metrics}",
            date_filter={"from_date": from_date, "to_date": to_date},
            mode=mode,
        )

        agg["from_chain_id"] = agg["from_chain_id"].astype(int)
        agg["from_asset_address"] = agg["from_asset_address"].astype(str)
        agg["to_chain_id"] = agg["to_chain_id"].astype(int)
        agg["to_asset_address"] = agg["to_asset_address"].astype(str)

        logger.info(f"Getting {metrics} daily data")
        daily = get_raw_data_from_postgres_by_sql(f"daily_{metrics}", mode=mode)
        daily["day"] = pd.to_datetime(daily["day"])
        daily["from_chain_id"] = daily["from_chain_id"].astype(int)
        daily["from_asset_address"] = daily["from_asset_address"].astype(str)
        daily["to_chain_id"] = daily["to_chain_id"].astype(int)
        daily["to_asset_address"] = daily["to_asset_address"].astype(str)

        if all_agg_metrics.empty:
            all_agg_metrics = agg
            logger.info(f"Initialized all_agg_metrics with {metrics}")
        else:
            all_agg_metrics = pd.merge(
                all_agg_metrics,
                agg,
                on=[
                    "from_chain_id",
                    "from_asset_address",
                    "to_chain_id",
                    "to_asset_address",
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
                    "from_asset_address",
                    "to_chain_id",
                    "to_asset_address",
                ],
                how="outer",
            )
            logger.info(f"Merged {metrics} into all_daily_metrics")

    return all_agg_metrics, all_daily_metrics


def clean_all_metrics(df, tokens_metadata):
    """In this function, convert raw to clean
    1. adding token metadata
    2. token decimal
    3. token symbols
    4. pull price from big query
    5. convert the amounts to USD amounts
    6. return the clean df
    """

    df_clean = df.copy()
    # convert padded address to token address: from address
    df_clean["from_asset_address"] = df_clean["from_asset_address"].apply(
        convert_to_token_address
    )
    st.dataframe(tokens_metadata)

    return df_clean


def metric_dashboard(mode: str = "prod") -> None:
    """Steps:
    1. get all raw data for each metric on daily basis- 13 metrics
    2.  create a formated data based on the metrics
    3. apply filters on the data
    4. plot the data into line chart and big number
    """
    st.title("Everclear Testnet")

    st.sidebar.header("Filters")
    st.sidebar.subheader("Time Range")

    # last 7 days
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=7),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    tokens_metadata = get_chains_assets_metadata()

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

    # get metrics
    raw_agg_metrics, raw_daily_metrics = get_metrics(
        from_date=from_date,
        to_date=to_date,
        mode=mode,
    )

    # display settled metrics
    st.dataframe(raw_agg_metrics)
    st.dataframe(raw_daily_metrics)

    # clean metrics
    # clean_agg_metrics = clean_all_metrics(raw_agg_metrics, tokens_metadata)
    # st.dataframe(clean_agg_metrics)
    st.dataframe(tokens_metadata)


def main() -> None:
    st.title("Everclear")
    st.markdown(
        """
        """
    )

    metric_dashboard()
    return None


if __name__ == "__main__":
    main()
