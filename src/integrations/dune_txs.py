import numpy as np
import pytz
from datetime import datetime, timedelta
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

ACROSS_V3_QUERY_ID_v0 = 4025884
ACROSS_V2_QUERY_ID_v0 = 4073734
STARGATE_V2_QUERY_ID_v0 = 4025197
STARGATE_V1_QUERY_ID_v0 = 4073890
EVM_CHAINS_TOKEN_METADATA_QUERY_ID = 4123583
TOKENS_METADATA_QUERY_ID = 4136542
ALL_ETH_BASED_TOKENS_PRICES = 4136881
ALL_EVERCLEAR_TOKENS_PRICES = 4157302


def get_max_token_price_timestamp_from_bq() -> int:
    sql = "SELECT max(CAST(minute AS TIMESTAMP)) AS max_timestamp FROM `mainnet-bigq.dune.all_everclear_tokens_prices`"
    df = gbq.read_gbq(sql)
    final_start_date = np.array(df["max_timestamp"])[0]
    # convert to int
    return int(final_start_date.timestamp())


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


# Function to filter out non-letter characters
def filter_letters(s):
    # drop rows where symbol is not a letter or a dot
    if not isinstance(s, str):
        return False
    return s.isalpha()


def tokens_metadata_pipeline():
    # patch
    df = pd.read_csv("data/token_metadata.csv")
    print(df.count())
    df["data_fetched_at"] = pd.to_datetime("now")
    df_filtered = df[df["symbol"].apply(filter_letters)].copy()
    df_filtered.reset_index(drop=True, inplace=True)
    df_filtered.dropna(inplace=True)
    print(df_filtered.count())
    print(df_filtered.head())
    print(df_filtered.dtypes)
    print(df_filtered.isna().sum())

    gbq.to_gbq(
        dataframe=df_filtered,
        project_id="mainnet-bigq",
        destination_table="mainnet-bigq.dune.tokens_metadata",
        if_exists="replace",
        api_method="load_csv",
    )


def all_eth_based_tokens_prices_pipeline(n: int = 30):
    fetch_and_push_all_n_days_data(
        ALL_ETH_BASED_TOKENS_PRICES,
        "mainnet-bigq.dune.all_eth_based_tokens_prices",
        from_date=pytz.UTC.localize(datetime(2024, 1, 1)),
        n=30,
    )


def get_all_everclear_tokens_prices_pipeline():
    """date as UTC, convert the number to timestamp from BQ"""

    max_date_bq = get_max_token_price_timestamp_from_bq()
    from_date, to_date = max_date_bq, int(datetime.now(pytz.UTC).timestamp())
    logger.info(
        f"Pulling data for Everclear tokens prices from {from_date} to {to_date}"
    )
    df = get_dune_txs(
        ALL_EVERCLEAR_TOKENS_PRICES,
        from_date,
        to_date,
    )
    if not df.empty:
        logger.info(df.head())
        # keep only min timestamp data for all symbols
        max_timestamps_by_symbol = df.groupby("symbol")["minute"].max().reset_index()
        min_of_max_timestamps = np.array(max_timestamps_by_symbol["minute"].min())
        logger.info(
            f"Min of max timestamps: {min_of_max_timestamps}, filter out all data above this"
        )

        df_final = df[df["minute"] <= min_of_max_timestamps].reset_index(drop=True)
        if not df_final.empty:
            logger.info(f"Adding data to BigQuery: length {len(df_final)}")
            gbq.to_gbq(
                dataframe=df_final,
                project_id="mainnet-bigq",
                destination_table="dune.all_everclear_tokens_prices",
                if_exists="append",
                api_method="load_csv",
            )
        else:
            logger.info(
                f"""data not up to date for all symbols,
                  from {from_date} to {to_date} with min timestamp {min_of_max_timestamps}
                """
            )
    else:
        logger.info(f"No data fetched for the period {from_date} to {to_date}")


def pipeline_all_dune_txs():
    across_v3_txs_pipeline()
    across_v2_txs_pipeline()
    stargate_v2_txs_pipeline()
    stargate_v1_txs_pipeline()
