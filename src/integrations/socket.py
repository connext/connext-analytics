from email import header
import os
import nest_asyncio
import asyncio
from pprint import pprint
import httpx
import pandas as pd
import logging
import json
import numpy as np
import pandas_gbq
from dotenv import load_dotenv
from itertools import product
from datetime import datetime
from asyncio import Semaphore
from google.cloud import storage
from src.integrations.utilities import get_raw_from_bq
from src.integrations.utilities import get_secret_gcp_secrete_manager

nest_asyncio.apply()

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "mainnet-bigq"
URL_SOCKET__BASE = "https://api.socket.tech/v2"

source_socket__api_key = get_secret_gcp_secrete_manager(
    secret_name="source_socket__api_key"
)


HEADERS = {"API-KEY": f"{source_socket__api_key}"}


async def get_data(ext_url: str, headers: dict = HEADERS):
    url = URL_SOCKET__BASE + ext_url
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        logging.info(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        return None


def get_chains(ext_url="/supported/chains"):

    chains_data = asyncio.run(get_data(ext_url=ext_url))
    if chains_data:
        chains_df = pd.json_normalize(chains_data["result"])
        pandas_gbq.to_gbq(
            dataframe=chains_df,
            project_id=PROJECT_ID,
            destination_table="raw.source_socket__chains",
            if_exists="replace",
        )
        logging.info("Socket chain data loaded to BigQuery:raw.source_socket__chains")
    return None


def get_bridges(ext_url="/supported/bridges"):

    bridges_data = asyncio.run(get_data(ext_url=ext_url))
    if bridges_data:
        bridges_df = pd.json_normalize(bridges_data["result"])
        pandas_gbq.to_gbq(
            dataframe=bridges_df,
            project_id=PROJECT_ID,
            destination_table="raw.source_socket__bridges",
            if_exists="replace",
        )
        logging.info("Socket bridge data loaded to BigQuery:raw.source_socket__bridges")
    return None


def get_tokens(ext_url="/token-lists/all"):
    headers = HEADERS.copy()
    headers.update({"isShortList": "true"})

    tokens_data = asyncio.run(get_data(ext_url=ext_url, headers=headers))
    print(headers)
    if tokens_data:

        for item in tokens_data["result"].items():
            result = {
                key: pd.json_normalize(value)
                for key, value in tokens_data["result"].items()
            }
            tokens_df = pd.concat(result.values(), ignore_index=True)

        # all_tokens = []
        # for key in tokens_data["tokens"]:
        #     all_tokens.extend(tokens_data["tokens"][key])
        # tokens_df = pd.DataFrame(all_tokens)

        pprint(tokens_df)
        pandas_gbq.to_gbq(
            dataframe=tokens_df,
            project_id=PROJECT_ID,
            destination_table="raw.source_socket__tokens",
            if_exists="replace",
        )
        logging.info("Socket Tokens data loaded to BigQuery:raw.source_socket__tokens")

    return None


def get_routes_pathways_from_bq():
    """
    INPUT PULLED:
        'allowDestinationCall': 'True',
        'fromAddress': '0x32d222E1f6386B3dF7065d639870bE0ef76D3599',
        'fromAmount': '1000150022503375424192512',
        'fromChainId': '42161.0',
        'fromTokenAddress': '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1',
        'toChainId': '59144.0',
        'toTokenAddress': '0x4AF15ec2A0BD43Db75dd04E62FAA3B8EF36b00d5'
    payload req:
        - fromChainId=137
        - fromTokenAddress=0x2791bca1f2de4661ed88a30c99a7a9449aa84174
        - toChainId=56
        - toTokenAddress=0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3
        - fromAmount=100000000
        - userAddress=0x3e8cB4bd04d81498aB4b94a392c334F5328b237b
        - uniqueRoutesPerBridge=true
        - sort=output
    """
    try:
        df = get_raw_from_bq(sql_file_name="generate_routes_pathways")
        pprint(df.columns)
        df["fromChainId"] = df["fromChainId"].astype(float).astype(int)
        df["toChainId"] = df["toChainId"].astype(float).astype(int)
        df["uniqueRoutesPerBridge"] = "false"
        df["sort"] = "output"
        del df["allowDestinationCall"]
        df.rename(
            columns={"fromAddress": "userAddress"},
            inplace=True,
        )

        return df.to_dict(orient="records")
        # return [
        #     {
        #         "fromChainId": "137",
        #         "fromTokenAddress": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        #         "toChainId": "56",
        #         "toTokenAddress": "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
        #         "fromAmount": "100000000",
        #         "userAddress": "0x3e8cB4bd04d81498aB4b94a392c334F5328b237b",
        #         "uniqueRoutesPerBridge": "false",
        #         "sort": "output",
        #     }
        # ]
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        raise


async def get_routes(sem, url, payload):

    try:
        async with sem:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    params=payload,
                    headers=HEADERS,
                    timeout=httpx.Timeout(10.0, connect=10.0, read=10.0),
                )
                response.raise_for_status()
                return response.json()

    except httpx.HTTPStatusError as exc:
        logging.info(f"Request failed with status code {exc.response.status_code}")
        logging.info(f"Error message: {exc}")
        return None
    except Exception as exc:
        logging.info(f"An unexpected error occurred: {str(exc)}")
        return None


async def get_all_routes(max_concurrency=10, ext_url="/quote"):
    payloads = get_routes_pathways_from_bq()
    url = URL_SOCKET__BASE + ext_url
    sem = Semaphore(max_concurrency)
    tasks = []
    for payload in payloads:
        task = get_routes(sem, url, payload)
        tasks.append(task)
    responses = await asyncio.gather(*tasks)

    filtered_responses = [r for r in responses if r is not None]
    return filtered_responses


if __name__ == "__main__":

    logging.info("Starting the script")
    # pprint(get_chains(ext_url="/supported/chains"))

    # pprint(get_bridges())

    # pprint(get_tokens())
    # pprint(get_routes_pathways_from_bq())

    routes = asyncio.run(get_all_routes())
    pprint(routes)
    pprint(len(routes))
