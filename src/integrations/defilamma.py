import asyncio
from datetime import datetime, timedelta
import pytz
import dlt
import logging
import pandas as pd
from defillama2 import DefiLlama
from typing import Iterator, Sequence
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from src.integrations.models.defilamma import (
    DefilammaChains,
    DefilammaProtocols,
    DefilammaStables,
    DefilammaBridges,
    DefilammaBridgesHistoryWallets,
    DefilammaBridgesHistoryTokens,
)
from src.integrations.utilities import (
    get_raw_from_bq,
    get_latest_value_from_bq_table_by_col,
)
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
                                "key_type": key,
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
                                "key_type": key,
                                "key": sub_key,
                                **sub_value,
                            }
                        )

    df1 = pd.DataFrame(df1_data)
    df1["upload_timestamp"] = upload_datetime
    df2 = pd.DataFrame(df2_data)
    df2["upload_timestamp"] = upload_datetime
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
def generate_daily_unix_timestamps(end_date=datetime.now(pytz.UTC)):

    # get latest upload date from Defilamma token history table

    # TODO: changes this on weekend

    # 1st jan 23
    # unix_start_date = 1672531200

    unix_start_date = get_latest_value_from_bq_table_by_col(
        table_id="mainnet-bigq.raw.source_defilamma_bridges_history_tokens", col="date"
    )

    end_date = end_date.replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
    )
    end_date = int((end_date - timedelta(days=1)).timestamp())

    timestamps = []
    current_date = unix_start_date + 86400

    logging.info(
        f"""
            ALL dates in UTC
                 latest date in bq: {unix_start_date}
                 current_date( 1 day added to latest date): {current_date}
                 pull data till date: {end_date}
        """
    )
    if current_date > end_date:
        logging.info(
            f"Data upto-date! current_date: {current_date} > end_date: {end_date}"
        )

    else:
        while current_date <= end_date:
            timestamp = int(current_date)
            timestamps.append(timestamp)

            # Move to the next day
            # add a day in secs
            current_date += 86400

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
    if timestamps:
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

            yield convert_raw_bridgestats_to_df(
                res_data=data, upload_datetime=req_datetime
            )
    else:
        logging.info(
            f"No data to pull, data up to date till {req_datetime}, we pull delta: 1 day data"
        )


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


# if __name__ == "__main__":

#     logging.info("Running DLT defilamma")
#     p = dlt.pipeline(
#         pipeline_name="defilamma",
#         destination="bigquery",
#         dataset_name="raw",
#     )
#     p.run(defilamma_raw(), loader_file_format="jsonl")
#     logging.info("Finished DLT defilamma")
