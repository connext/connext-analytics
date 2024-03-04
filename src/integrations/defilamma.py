import asyncio
import json
from datetime import datetime, timedelta
from operator import le
from os import times
import time
import pytz
import dlt
import logging
import pandas as pd
from defillama2 import DefiLlama
from typing import Callable
from pprint import pprint
from typing import Iterator, Sequence
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from pendulum import today
from src.integrations.models.defilamma import (
    DefilammaChains,
    DefilammaProtocols,
    DefilammaStables,
    DefilammaBridges,
    DefilammaBridgesHistoryWallets,
    DefilammaBridgesHistoryTokens,
)
from src.integrations.utilities import get_raw_from_bq
from src.integrations.helpers_http import AsyncHTTPClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
# (EL)T: DLT- via Prefect
dl = DefiLlama()


@dlt.resource(
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(DefilammaStables),
)
def defilamma_stables(stablecoins_list_url=dlt.config.value) -> Iterator[TDataItems]:

    req_datetime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    resp = requests.get(stablecoins_list_url)
    resp.raise_for_status()
    lst = resp.json()["peggedAssets"]
    res = []
    res_chains = []
    for d0 in lst:
        _ = d0.pop("chainCirculating")
        chains = d0.pop("chains")
        res.append(pd.DataFrame(d0).reset_index(drop=True))
        res_chains.append(chains)
    df = pd.concat(res)
    df["chains"] = res_chains
    del df["priceSource"]
    df["id"] = df.id.astype(int)
    df["price"] = df["price"].astype(float)

    df["upload_timestamp"] = req_datetime
    skip_null_price = ~df["price"].isna()
    yield df[skip_null_price].to_dict(orient="records")


# Protocols
@dlt.resource(
    table_name="source_defilamma__protocols",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(DefilammaProtocols),
)
def defilamma_protocols() -> Iterator[TDataItems]:
    req_datetime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    df = dl.get_protocols()
    df["upload_timestamp"] = req_datetime
    yield df.to_dict(orient="records")


# Chains
@dlt.resource(
    table_name="source_defilamma__chains",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(DefilammaChains),
)
def defilamma_chains(chains_url=dlt.config.value) -> Iterator[TDataItems]:
    req_datetime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    resp = requests.get(chains_url)
    resp.raise_for_status()
    lst_chains = resp.json()
    df = pd.DataFrame(lst_chains)
    df["upload_timestamp"] = req_datetime
    yield df.to_dict(orient="records")


# Bridges
@dlt.resource(
    table_name="source_defilamma__bridges",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(DefilammaBridges),
)
def defilamma_bridges(bridge_url=dlt.config.value) -> Iterator[TDataItems]:
    req_datetime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    resp = requests.get(bridge_url)
    resp.raise_for_status()
    lst_bridge = resp.json()["bridges"]
    df = pd.DataFrame(lst_bridge)
    df["upload_timestamp"] = req_datetime
    yield df.to_dict(orient="records")


# def alt_defilamma_bridge_day_stats(timestamp, chain, id):
#     """
#     INPUTS:
#         timestamp: Unix timestamp. start of the day in UTC
#         chain: chain name -> defilamma_chains -> name
#         id: bridge id from defilamma_bridges -> id

#     Working API call:
#         1.CURL:
#         curl -X 'GET' \
#         'https://bridges.llama.fi/bridgedaystats/1709251200/Arbitrum?id=12' \
#         -H 'accept: */*'
#         2.Python:
#         defilamma_bridge_day_stats(timestamp=1709251200, chain="Arbitrum", id=12)
#     """
#     base_url = "https://bridges.llama.fi/bridgedaystats/"
#     additional_url = f"{timestamp}/{chain}?id={id}"
#     url = base_url + additional_url
#     headers = {"accept": "*/*"}
#     response = requests.get(url, headers=headers)
#     return response.json()


@staticmethod
def convert_raw_bridgestats_to_df(res_data, upload_datetime):

    # Flattening the data into two DataFrames
    df1_data = []
    df2_data = []
    for res in res_data:
        print(res)
        if "data" in res:
            data = res["data"]
            for key, value in data.items():
                if key in ["totalTokensDeposited", "totalTokensWithdrawn"]:
                    for sub_key, sub_value in value.items():
                        df1_data.append(
                            {
                                "date": data["date"],
                                "status_code": res["status_code"],
                                "url": res["url"],
                                "key": sub_key,
                                **sub_value,
                            }
                        )
                elif key in ["totalAddressDeposited", "totalAddressWithdrawn"]:
                    for sub_key, sub_value in value.items():
                        df2_data.append(
                            {
                                "date": data["date"],
                                "status_code": res["status_code"],
                                "url": res["url"],
                                "key": sub_key,
                                **sub_value,
                            }
                        )

    df1 = pd.DataFrame(df1_data)
    df1["upload_timestamp"] = upload_datetime
    print(df1)
    df2 = pd.DataFrame(df2_data)
    df2["upload_timestamp"] = upload_datetime
    print(df2)
    final = [
        {
            "type": "source_defilamma__bridges_history_wallets",
            "schema": pydantic_to_table_schema_columns(DefilammaBridgesHistoryWallets),
            "data": df2.to_dict(orient="records"),
        },
        {
            "type": "source_defilamma__bridges_history_tokens",
            "schema": pydantic_to_table_schema_columns(DefilammaBridgesHistoryTokens),
            "data": df1.to_dict(orient="records"),
        },
    ]

    for item in final:
        yield dlt.mark.with_hints(
            item=item["data"],
            hints=dlt.mark.make_hints(
                table_name=item["type"],
                columns=item["schema"],
            ),
        )


@staticmethod
def generate_daily_unix_timestamps(
    start_date="2024-01-01", end_date=datetime.now(pytz.UTC)
):
    # [ ] TODO: get  the lastest date from big query table and pull the data from that date.
    # That will be the start date

    "Generate epoch timestamps in GMT, starting from the start of the day"
    # Convert the start_date to a datetime object with GMT timezone
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    # Set the timezone to GMT and the time part to the start of the day (midnight)
    start_date = start_date.replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
    )

    # Ensure the end_date is at the start of the day in GMT
    end_date = end_date.replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
    )

    timestamps = []
    current_date = start_date
    while current_date < end_date:
        timestamp = int(current_date.timestamp())
        timestamps.append(timestamp)

        # Move to the next day
        current_date += timedelta(days=1)

    return timestamps


@dlt.resource(
    write_disposition="append",
)
def defilamma_bridge_day_stats(
    bridgedaystats_url=dlt.config.value,
) -> Iterator[TDataItems]:
    """
    INPUTS:
        timestamp: Unix timestamp. start of the day in UTC
        chain: chain name -> defilamma_chains -> name
        id: bridge id from defilamma_bridges -> id

    Working API call:
        1.CURL:
        curl -X 'GET' \
        'https://bridges.llama.fi/bridgedaystats/1709251200/Arbitrum?id=12' \
        -H 'accept: */*'
        2.Python:
        defilamma_bridge_day_stats(timestamp=1659251200, chain="Arbitrum", id=12)
    """

    df_defilamma_supported_bridge_chain_pair = get_raw_from_bq(
        sql_file_name="defilamma_supported_bridge_chain_pair"
    )
    defilamma_supported_bridge_chain_pair = (
        df_defilamma_supported_bridge_chain_pair.to_dict(orient="records")
    )
    req_datetime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    timestamps = generate_daily_unix_timestamps()
    len(f"timestamps length: {len(timestamps)}")

    # Initialize and call API
    bridgedaystats_client = AsyncHTTPClient(max_concurrency=5)
    for timestamp in timestamps:
        urls = []
        for pair in defilamma_supported_bridge_chain_pair:
            additional_url = f"{timestamp}/{pair['name']}?id={pair['id']}"
            url = bridgedaystats_url + additional_url
            urls.append(url)
        logging.info(f"Data to pull, urls length: {len(urls)}")

        data = asyncio.run(
            bridgedaystats_client.get_all_responses(
                method="GET", urls=urls, headers={"accept": "*/*"}
            )
        )

        yield convert_raw_bridgestats_to_df(res_data=data, upload_datetime=req_datetime)


# Sources
@dlt.source(
    max_table_nesting=0,
)
def defilamma_raw() -> Sequence[DltResource]:
    return [
        defilamma_protocols,
        defilamma_stables,
        defilamma_bridges,
        defilamma_chains,
        defilamma_bridge_day_stats,
    ]


# Main

if __name__ == "__main__":

    "Running DLT defilamma"
    p = dlt.pipeline(
        pipeline_name="defilamma",
        destination="bigquery",
        dataset_name="raw",
    )
    p.run(defilamma_raw(), loader_file_format="parquet")
