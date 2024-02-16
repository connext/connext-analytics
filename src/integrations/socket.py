import json
import os
import nest_asyncio
import asyncio
import httpx
import pandas as pd
import logging
import numpy as np
import pandas_gbq
from google.cloud import storage
from datetime import datetime
from asyncio import Semaphore
from src.integrations.utilities import (
    get_raw_from_bq,
    convert_lists_and_booleans_to_strings,
    get_secret_gcp_secrete_manager,
)

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
            dataframe=convert_lists_and_booleans_to_strings(chains_df),
            project_id=PROJECT_ID,
            destination_table="raw.source_socket__chains",
            if_exists="replace",
        )
        logging.info("Socket chain data loaded to BigQuery:raw.source_socket__chains")
    return {"message": "Socket chain data loaded to BigQuery:raw.source_socket__chains"}


def get_bridges(ext_url="/supported/bridges"):

    bridges_data = asyncio.run(get_data(ext_url=ext_url))
    if bridges_data:
        bridges_df = pd.json_normalize(bridges_data["result"])
        pandas_gbq.to_gbq(
            dataframe=convert_lists_and_booleans_to_strings(bridges_df),
            project_id=PROJECT_ID,
            destination_table="raw.source_socket__bridges",
            if_exists="replace",
        )
        logging.info("Socket bridge data loaded to BigQuery:raw.source_socket__bridges")
    return {
        "message": "Socket bridge data loaded to BigQuery:raw.source_socket__bridges"
    }


def get_tokens(ext_url="/token-lists/all"):
    headers = HEADERS.copy()
    headers.update({"isShortList": "true"})

    tokens_data = asyncio.run(get_data(ext_url=ext_url, headers=headers))
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

        pandas_gbq.to_gbq(
            dataframe=convert_lists_and_booleans_to_strings(tokens_df),
            project_id=PROJECT_ID,
            destination_table="raw.source_socket__tokens",
            if_exists="replace",
        )
        logging.info("Socket Tokens data loaded to BigQuery:raw.source_socket__tokens")

    return {
        "message": "Socket Tokens data loaded to BigQuery:raw.source_socket__tokens"
    }


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

        df["fromChainId"] = df["fromChainId"].astype(float).astype(int)
        df["toChainId"] = df["toChainId"].astype(float).astype(int)
        df["fromAmount"] = df["fromAmount"].apply(lambda x: int(float(x)))
        df["uniqueRoutesPerBridge"] = "false"
        df["sort"] = "output"
        # del df["allowDestinationCall"]
        df.rename(
            columns={"fromAddress": "userAddress"},
            inplace=True,
        )

        return df.to_dict(orient="records")
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


async def get_all_routes(max_concurrency=3, ext_url="/quote"):
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


def get_upload_data_from_socket_cs_bucket(
    greater_than_date_routes, greater_than_date_steps, bucket_name="socket_routes"
):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        logging.info(f"Pulling data for: {blob.name}")

        # seperate common parameters
        name = os.path.splitext(blob.name)[0]
        dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")
        data = json.loads(blob.download_as_text())
        logging.info(f"data: {len(data)}")

        # Routes
        if dt > greater_than_date_routes:
            # 1. convert the routes data to df
            df = convert_socket_routes_json_to_df(json_blob=data)
            df["upload_datetime"] = dt

            # 2. upload to bq
            pandas_gbq.to_gbq(
                dataframe=df,
                project_id=PROJECT_ID,
                destination_table="raw.source_socket__routes",
                if_exists="append",
                chunksize=10000,
                api_method="load_csv",
            )

            logging.info(f"Socket Routers, {df.shape} rows Added!")

        else:
            logging.info(
                f"Socket Routers, {dt} is not greater than {greater_than_date_routes}, Data Already Added!"
            )

        # Routes Steps
        if dt > greater_than_date_steps:
            # 1. convert convert_socket_routes_steps_json_to_df
            df_socket_steps = convert_socket_routes_steps_json_to_df(json_blob=data)
            df_socket_steps["upload_datetime"] = dt

            # 2. upload to bq
            pandas_gbq.to_gbq(
                dataframe=df_socket_steps,
                project_id=PROJECT_ID,
                destination_table="raw.source_socket__routes_steps",
                if_exists="append",
                chunksize=10000,
                api_method="load_csv",
            )

            logging.info(
                f"Steps for Socket Routers, {df_socket_steps.shape} rows Added!"
            )

        else:
            logging.info(
                f"Steps for  Socket Routers, {dt} is not greater than {greater_than_date_routes}, Data Already Added!"
            )


def convert_socket_routes_json_to_df(json_blob):

    normalized_data_df = pd.DataFrame()
    for r in json_blob:
        route = r["result"]
        metadata = {
            k: v for k, v in route.items() if k not in ["routes", "bridgeRouteErrors"]
        }
        metadata_df = pd.json_normalize(metadata, sep="_")
        routes_df = pd.DataFrame()
        if route["routes"]:
            for r in route["routes"]:
                route_data = {
                    k: v
                    for k, v in r.items()
                    if k not in ["chainGasBalances", "minimumGasBalances"]
                }
                route_df = pd.json_normalize(route_data, sep="_")
                routes_df = pd.concat([routes_df, route_df], ignore_index=True)

        flattened_df = pd.merge(routes_df, metadata_df, how="cross")
        normalized_data_df = pd.concat(
            [normalized_data_df, flattened_df], ignore_index=True
        )
    return convert_lists_and_booleans_to_strings(normalized_data_df)


def convert_socket_routes_steps_json_to_df(json_blob):
    all_steps = []
    for r in json_blob:
        if "routes" in r["result"]:
            routes = r["result"]["routes"]
            for r in routes:
                if "userTxs" in r:
                    for u in r["userTxs"]:
                        if "steps" in u:
                            steps = u["steps"]
                            step_counter = 0
                            for step in steps:
                                step["step_id"] = step_counter
                                step["route_id"] = r["routeId"]
                                step["routePath"] = u["routePath"]
                                step["userTxIndex"] = u["userTxIndex"]
                                all_steps.append(step)
                                step_counter += 1

    steps_df = pd.json_normalize(all_steps)
    return convert_lists_and_booleans_to_strings(steps_df)
