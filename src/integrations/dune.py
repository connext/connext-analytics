from ast import Dict
from datetime import datetime, timedelta, tzinfo
import dlt
import pytz
import json
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
from src.integrations.models.dune import BridgesNativeEvmEth
from src.integrations.utilities import get_latest_value_from_bq_table_by_col

# Logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
DUNE_START_DATE = 1709251200

# Dune Client
dune = DuneClient(api_key=get_secret_gcp_secrete_manager("source_DUNE_API_KEY_2"))
dune_query_id_native_evm_bridges_daily = 3541092


def epoch_date_before_today_utc() -> int:
    end_date = datetime.now(pytz.UTC) - timedelta(days=1)
    return (
        end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)
        - timedelta(days=1)
    ).timestamp()


def get_result_by_query_id(id: int, start_date: int, end_date: int, new: bool = False):

    query = QueryBase(
        name="native EVM bridges ETH",
        query_id=id,
        params=[
            QueryParameter.number_type(name="start_date", value=start_date),
            QueryParameter.number_type(name="end_date", value=end_date),
        ],
    )
    if new:
        results = dune.run_query(query).get_rows()
    else:
        results = dune.get_latest_result(query).get_rows()

    with open(f"data/dune_query_result_{id}.json", "w") as f:
        json.dump(results, f, indent=4)

    yield results


def get_start_end_date(
    start_date: int, end_date: int = epoch_date_before_today_utc()
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
        table_id="mainnet-bigq.dune.source_native_evm_eth__bridges",
        col="date",
        base_val=start_date,
    )
    return {
        "start_date": int(final_start_date.timestamp()),
        "end_date": int(end_date),
    }


# Bridges
@dlt.resource(
    table_name="source_native_evm_eth__bridges",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(BridgesNativeEvmEth),
)
def get_native_evm_eth__bridges(
    native_evm_eth__bridges_query_id=dlt.config.value,
) -> Iterator[TDataItems]:

    # --
    # PUSH LOCAL FILE DATA: START
    # --

    # with open("data/dune_query_result_3537139.json", "r") as f:
    #     results = json.load(f)

    # df = pd.DataFrame(results)
    # df = df.rename(columns={"from": "from_address", "to": "to_address"})
    # df["date"] = pd.to_datetime(df["date"], utc=True)
    # yield df.to_dict("records")

    # --
    # PUSH LOCAL FILE DATA: END
    # --

    date_param = get_start_end_date(start_date=DUNE_START_DATE)
    start_date = date_param["start_date"]
    end_date = date_param["end_date"]

    yield get_result_by_query_id(
        native_evm_eth__bridges_query_id,
        new=True,  # update data by query rerun
        start_date=start_date,
        end_date=end_date,
    )


# Sources
@dlt.source(
    max_table_nesting=0,
)
def dune_bridges() -> Sequence[DltResource]:
    return [get_native_evm_eth__bridges]


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
    #     get_result_by_query_id(
    #         dune_query_id_native_evm_bridges_daily, from_date="2024-03-01", new=True
    #     )
    # )

    # with open("data/dune_query_result_3537139.json", "r") as f:
    #     results = json.load(f)

    # df = pd.DataFrame(results)
    # df = df.rename(columns={"from": "from_address", "to": "to_address"})
    # df["date"] = pd.to_datetime(df["date"], utc=True)
    # print(df.to_dict("records")[0])
    # print(df.dtypes)
    # print(df.shape)
