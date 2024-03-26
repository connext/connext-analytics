from ast import Dict
from datetime import datetime, timedelta
import dlt
import pytz
import pandas as pd
import logging
from typing import Iterator, Sequence
from dlt.common.typing import TDataItems
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dlt.extract.source import DltResource
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from src.integrations.utilities import get_secret_gcp_secrete_manager
from src.integrations.models.dune import (
    BridgesNativeEvmEth,
    BridgesTokensEvmEth,
    StargateBridgesDailyAgg,
)
from src.integrations.utilities import get_latest_value_from_bq_table_by_col

# Logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DUNE_START_DATE = 1703980800  # dec 31st 2023- only this year

# Dune Client
dune = DuneClient(api_key=get_secret_gcp_secrete_manager("source_DUNE_API_KEY_1"))


def epoch_date_before_today_utc() -> int:
    end_date = datetime.now(pytz.UTC) - timedelta(days=1)
    return (
        end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)
    ).timestamp()


def get_result_by_query_id(
    id: int, start_date: int, end_date: int = None, new: bool = False
):
    """
    get_result_by_query_id _summary_

    _extended_summary_

    Args:
        id (int): _description_
        start_date (int): _description_
        end_date (int, optional): _description_. Defaults to None.
        new (bool, optional): Get Data after running query to get new data or get old executed data. Defaults to False.
        new= False: to use for local deployment, run the query in UI and query the result via API

    Yields:
        _type_: _description_
    """
    #
    if end_date is None:
        query = QueryBase(
            name="DUNE PIPELINE QUERY",
            query_id=id,
            params=[
                QueryParameter.number_type(name="start_date", value=start_date),
            ],
        )
    else:
        query = QueryBase(
            name="DUNE PIPELINE QUERY",
            query_id=id,
            params=[
                QueryParameter.number_type(name="start_date", value=start_date),
                QueryParameter.number_type(name="end_date", value=end_date),
            ],
        )

    if new:
        results = dune.run_query_dataframe(query, batch_size=1000)

    else:
        results = dune.get_latest_result_dataframe(query, batch_size=1000)
    results.to_csv(f"data/dune_query_result_{id}.csv", index=False)
    yield results.to_dict("records")


def get_start_end_date(
    table_id: str, start_date: int, end_date: int = epoch_date_before_today_utc()
) -> Dict:
    """
    Input:
        start_date (epoch timestamp): Data to pull. Defaults to DUNE_START_DATE. or pulls from database
        end_date (epoch timestamp): Date till data to pull. Defaults to epoch_date_before_today_utc().

    Output:
        _type_: _description_
     _summary_

    _extended_summary_

    Returns:
        _type_: _description_
    """

    final_start_date = get_latest_value_from_bq_table_by_col(
        table_id=table_id,
        col="date",
        base_val=start_date,
    )
    return {
        "start_date": int(final_start_date),
        "end_date": int(end_date),
    }


# Bridges
@dlt.resource(
    table_name="source_native_evm_eth_bridges",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(BridgesNativeEvmEth),
)
def get_native_evm_eth__bridges(
    native_evm_eth__bridges_query_id=dlt.config.value,
) -> Iterator[TDataItems]:

    # df = pd.read_csv("data/native_evM_bridge_txs_daily_1_jan_march_24.csv")
    # df["date"] = pd.to_datetime(df["date"])
    # yield df.to_dict("records")

    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_native_evm_eth_bridges",
        start_date=DUNE_START_DATE,
    )

    if date_param["start_date"] < date_param["end_date"]:
        print("adding data to bq from csv")
        yield get_result_by_query_id(
            native_evm_eth__bridges_query_id,
            new=True,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        )
    else:
        logging.info("Data uptodate in the DB")


@dlt.resource(
    table_name="source_tokens_evm_eth_bridges",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(BridgesTokensEvmEth),
)
def get_tokens_evm_eth__bridges(
    tokens_evm_eth__bridges_query_id=dlt.config.value,
) -> Iterator[TDataItems]:

    # df = pd.read_csv("data/native_evM_bridge_txs_daily_1_jan_march_24.csv")
    # df["date"] = pd.to_datetime(df["date"])
    # yield df.to_dict("records")

    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_tokens_evm_eth_bridges",
        start_date=DUNE_START_DATE,
    )

    if date_param["start_date"] < date_param["end_date"]:
        yield get_result_by_query_id(
            tokens_evm_eth__bridges_query_id,
            new=True,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        )
    else:
        logging.info("Data uptodate in the DB")


@dlt.resource(
    table_name="source_stargate_bridges",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(StargateBridgesDailyAgg),
)
def get_stargate_bridges_daily_agg(
    stargate_daily_agg_query_id=dlt.config.value,
) -> Iterator[TDataItems]:

    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_stargate_bridges",
        start_date=DUNE_START_DATE,
    )
    if date_param["start_date"] < date_param["end_date"]:
        yield get_result_by_query_id(
            stargate_daily_agg_query_id,
            new=True,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        )
    else:
        logging.info("Data uptodate in the DB")


# Sources
@dlt.source(
    max_table_nesting=0,
)
def dune_bridges() -> Sequence[DltResource]:
    return [
        # get_native_evm_eth__bridges,
        get_tokens_evm_eth__bridges,
        # get_stargate_bridges_daily_agg,
    ]


if __name__ == "__main__":

    logging.info("Running DLT Dune Bridges")
    p = dlt.pipeline(
        pipeline_name="dune",
        destination="bigquery",
        dataset_name="dune",
    )
    p.run(dune_bridges(), loader_file_format="jsonl")
    logging.info("Finished DLT Dune Bridges!")

    # print(
    #     get_start_end_date(
    #         table_id="mainnet-bigq.dune.source_native_evm_eth_bridges",
    #         start_date=DUNE_START_DATE,
    #     )
    # )
