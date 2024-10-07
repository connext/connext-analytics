import pytz
from datetime import datetime, timedelta
from typing import List, Tuple
import pandas_gbq as gbq
import logging
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter
from src.gcp_utilitty import get_secret_gcp_secrete_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dune = DuneClient(api_key=get_secret_gcp_secrete_manager("source_DUNE_API_KEY_1"))
# query_result = dune.get_latest_result(4025884)

ACROSS_V3_QUERY_ID_v0 = 4025884
ACROSS_V2_QUERY_ID_v0 = 4073734
STARGATE_V2_QUERY_ID_v0 = 4025197
STARGATE_V1_QUERY_ID_v0 = 4073890
EVM_CHAINS_TOKEN_METADATA_QUERY_ID = 4123583
TOKENS_METADATA_QUERY_ID = 4136542
ALL_ETH_BASED_TOKENS_PRICES = 4136881


def get_dune_txs(
    query_id: int, from_date: int, to_date: int, batch_size: int = 30000
) -> pd.DataFrame:
    """Dates in seconds"""
    query = QueryBase(
        name="DUNE PIPELINE QUERY",
        query_id=query_id,
        params=[
            QueryParameter.number_type(name="from_date", value=from_date),
            QueryParameter.number_type(name="to_date", value=to_date),
        ],
    )

    results = dune.run_query_dataframe(query, batch_size=batch_size, ping_frequency=10)
    return results


def get_dune_metadata_pipeline(query_id: int, dest_table_name: str):
    """
    ALwyas data gets replaced
    dest_table_name: str -> eg: dune.evm_chains_token_metadata
    """
    query = QueryBase(name="DUNE PIPELINE QUERY", query_id=query_id)

    results = dune.run_query_dataframe(query, ping_frequency=10)
    if not results.empty:
        logger.info(results.head())
        # add api call date
        results["data_fetched_at"] = datetime.now(pytz.UTC).isoformat()
        gbq.to_gbq(
            dataframe=results,
            project_id="mainnet-bigq",
            destination_table=dest_table_name,
            if_exists="replace",
            api_method="load_csv",
        )

    return None


def get_n_days_intervals(from_date: datetime, end_date: datetime, n: int = 30):
    """Generate start and end timestamps in seconds for each n-day period from start_date to end_date."""
    current_date = from_date
    thirty_days = timedelta(days=n)

    while current_date < end_date:
        # Calculate the end of the 30-day period
        potential_end_date = current_date + thirty_days
        actual_end_date = min(potential_end_date, end_date)

        # Ensure end date does not exceed the provided end_date
        actual_end_date = min(actual_end_date, end_date)

        # Convert to timestamps in seconds
        start_ts = int(current_date.timestamp())
        end_ts = int(actual_end_date.timestamp())

        yield start_ts, end_ts

        # Move to the next period
        current_date = actual_end_date + timedelta(seconds=1)

        # Stop if we've reached end_date
        if current_date > end_date:
            break


def fetch_and_push_all_n_days_data(
    query_id: int,
    table_name: str,
    from_date: datetime,
    end_date=datetime.now(pytz.UTC),
    n: int = 30,
):
    """
    Fetch data for each month and push to BigQuery.
    Timestamp logic:
        from_date is included in the data pull
        to_date is excluded from the data pull
    """
    try:

        for from_ts, to_ts in get_n_days_intervals(from_date, end_date=end_date, n=n):
            logger.info(f"Fetching data from {from_ts} to {to_ts}")
            df = get_dune_txs(query_id, from_ts, to_ts)
            if not df.empty:
                logger.info(df.head())
                gbq.to_gbq(
                    dataframe=df,
                    project_id="mainnet-bigq",
                    destination_table=table_name,
                    if_exists="append",
                    api_method="load_csv",
                )
            else:
                logger.info(f"No data fetched for the period {from_ts} to {to_ts}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


def across_v3_txs_pipeline(n: int = 30):
    # TODO pull from the big quey the last date and add 1 day to it before passing to the function
    fetch_and_push_all_n_days_data(
        ACROSS_V3_QUERY_ID_v0,
        "mainnet-bigq.dune.across_v3_txs_v0",
        from_date=pytz.UTC.localize(datetime(2024, 2, 1)),
        n=30,
    )


def across_v2_txs_pipeline(n: int = 180):
    fetch_and_push_all_n_days_data(
        ACROSS_V2_QUERY_ID_v0,
        "mainnet-bigq.dune.across_v2_txs_v0",
        from_date=pytz.UTC.localize(datetime(2023, 1, 1)),
        n=n,
    )


def stargate_v2_txs_pipeline(n: int = 30):
    fetch_and_push_all_n_days_data(
        STARGATE_V2_QUERY_ID_v0,
        "mainnet-bigq.dune.stargate_v2_txs_v0",
        from_date=pytz.UTC.localize(datetime(2024, 5, 1)),
        n=30,
    )


def stargate_v1_txs_pipeline(n: int = 30):
    fetch_and_push_all_n_days_data(
        STARGATE_V1_QUERY_ID_v0,
        "mainnet-bigq.dune.stargate_v1_txs_v0",
        from_date=pytz.UTC.localize(datetime(2024, 2, 1)),
        n=30,
    )


def evm_chains_token_metadata_pipeline():
    get_dune_metadata_pipeline(
        EVM_CHAINS_TOKEN_METADATA_QUERY_ID,
        "mainnet-bigq.dune.evm_chains_token_metadata",
    )


def tokens_metadata_pipeline():
    # patch
    df = pd.read_csv("data/token_metadata.csv")
    df["data_fetched_at"] = datetime.now(pytz.UTC).isoformat()
    gbq.to_gbq(
        dataframe=df,
        project_id="mainnet-bigq",
        destination_table="mainnet-bigq.dune.tokens_metadata",
        if_exists="replace",
        api_method="load_csv",
    )

    # get_dune_metadata_pipeline(
    #     TOKENS_METADATA_QUERY_ID,
    #     "mainnet-bigq.dune.tokens_metadata",
    # )


def all_eth_based_tokens_prices_pipeline(n: int = 30):
    fetch_and_push_all_n_days_data(
        ALL_ETH_BASED_TOKENS_PRICES,
        "mainnet-bigq.dune.all_eth_based_tokens_prices",
        from_date=pytz.UTC.localize(datetime(2024, 1, 1)),
        n=30,
    )


def pipeline_all_dune_txs():
    across_v3_txs_pipeline()
    across_v2_txs_pipeline()
    stargate_v2_txs_pipeline()
    stargate_v1_txs_pipeline()


if __name__ == "__main__":
    # pull across v2:
    # evm_chains_token_metadata_pipeline()
    tokens_metadata_pipeline()
