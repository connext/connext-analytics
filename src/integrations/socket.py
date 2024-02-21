import json
import os
from pprint import pprint
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
from src.integrations.helpers_routes_aggreagators import (
    get_greater_than_date_from_bq_table,
)

nest_asyncio.apply()

# Define the maximum number of retries and the initial delay
MAX_RETRIES = 500
INITIAL_DELAY = 10  # in seconds

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


async def get_routes(sem, url, payload, retries=MAX_RETRIES, delay=INITIAL_DELAY):

    async with sem:
        async with httpx.AsyncClient() as client:
            for attempt in range(retries):
                try:
                    response = await client.get(
                        url,
                        params=payload,
                        headers=HEADERS,
                        timeout=httpx.Timeout(15.0, connect=15.0, read=15.0),
                    )
                    response.raise_for_status()
                    output = response.json()
                    output["payload"] = payload
                    output["payload"]["status_code"] = response.status_code
                    return output

                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 429:
                        print(f"Too Many Requests. Retrying in {delay} seconds...")
                        await asyncio.sleep(delay)
                        delay *= 2  # Exponential backoff

                    else:
                        logging.info(
                            f"Request failed with status code {exc.response.status_code}"
                        )
                        logging.info(f"Error message: {exc}")
                        output = {}
                        output["payload"] = payload
                        output["payload"]["status_code"] = exc.response.status_code

                        return output

                except Exception as exc:
                    logging.info(f"An unexpected error occurred: {str(exc)}")
                    output = {}
                    output["payload"] = payload
                    output["payload"]["status_code"] = 0
                    return output


async def get_all_routes(payloads, max_concurrency=3, ext_url="/quote"):

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

        # Routes
        if dt > greater_than_date_routes:
            data = json.loads(blob.download_as_text())
            logging.info(f"data: {len(data)}")
            # 1. convert the routes data to df
            df = convert_socket_routes_json_to_df(json_blob=data)
            df["upload_datetime"] = dt

            # 1.2. upload to bq
            pandas_gbq.to_gbq(
                dataframe=df,
                project_id=PROJECT_ID,
                destination_table="raw.source_socket__routes",
                if_exists="append",
                chunksize=10000,
                api_method="load_csv",
            )
            logging.info(f"Socket Routers, {df.shape} rows Added!")

            # 2. Get payload data to df
            logging.info(f"Uploading payload data for:{name} ")
            payload_df = convert_routes_payload_to_df(json_blob=data)
            payload_df["aggregator"] = "socket"
            payload_df["upload_datetime"] = dt

            # upload to bq
            pandas_gbq.to_gbq(
                dataframe=convert_lists_and_booleans_to_strings(payload_df),
                project_id=PROJECT_ID,
                destination_table="raw.source_socket_routes__payloads_logs",
                if_exists="append",
                chunksize=100000,
                api_method="load_csv",
            )
            logging.info(f"Socket Payloads, {payload_df.shape} rows Added!")

            # 3. Also upload unsuported data to bq
            # df_unsupported_paths = convert_no_routes_success_calls_to_df(json_blob=data)
            # df_unsupported_paths["upload_datetime"] = dt
            # pandas_gbq.to_gbq(
            #     dataframe=df_unsupported_paths,
            #     project_id=PROJECT_ID,
            #     destination_table="raw.source_socket__routes_unsupported_paths",
            #     if_exists="replace",
            #     chunksize=10000,
            #     api_method="load_csv",
            # )

        else:
            logging.info(
                f"Socket Routers, {dt} is not greater than {greater_than_date_routes}, Data Already Added!"
            )

        # [ ] Skipping these: Move these to Cloud run job
        # Routes Steps
        # if dt > greater_than_date_steps:
        #     logging.info(f"pulling steps for:{name} ")
        #     data = json.loads(blob.download_as_text())
        #     # 1. convert convert_socket_routes_steps_json_to_df
        #     df_socket_steps = convert_socket_routes_steps_json_to_df(json_blob=data)
        #     df_socket_steps["upload_datetime"] = dt

        #     # 2. upload to bq
        #     pandas_gbq.to_gbq(
        #         dataframe=df_socket_steps,
        #         project_id=PROJECT_ID,
        #         destination_table="raw.source_socket__routes_steps",
        #         if_exists="append",
        #         chunksize=10000,
        #         api_method="load_csv",
        #     )

        #     logging.info(
        #         f"Steps for Socket Routers, {df_socket_steps.shape} rows Added!"
        #     )

        # else:
        #     logging.info(
        #         f"Steps for  Socket Routers, {dt} is not greater than {greater_than_date_routes}, Data Already Added!"
        #     )


def convert_socket_routes_json_to_df(json_blob):

    normalized_data_df = pd.DataFrame()
    for r in json_blob:
        if "result" in r:
            route = r["result"]
            metadata = {
                k: v
                for k, v in route.items()
                if k not in ["routes", "bridgeRouteErrors"]
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
        if "result" in r:
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


def convert_no_routes_success_calls_to_df(json_blob):

    all_calls = []

    # file_name = "data/ad_hoc_socket_routes.json"
    # with open(file_name, "r") as f:
    #     json_blob = json.load(f)

    for r in json_blob:
        if "routes" in r["result"]:
            routes = r["result"]["routes"]
            if len(routes) == 0:
                del r["result"]["routes"]
                r["route_length"] = len(routes)
                all_calls.append(r)

    df = pd.json_normalize(all_calls)
    df = convert_lists_and_booleans_to_strings(df)
    return df


def convert_routes_payload_to_df(json_blob):
    """
    To this add:
        1. date of pull: update_datetime
        2. type fo aggreagator
    """
    all_payloads = []
    for r in json_blob:
        if "payload" in r:
            all_payloads.append(r["payload"])
    df = pd.json_normalize(all_payloads)
    return df


# if __name__ == "__main__":

# output = get_upload_data_from_socket_cs_bucket(
#     greater_than_date_routes=get_greater_than_date_from_bq_table(
#         table_id="mainnet-bigq.raw.source_socket__routes",
#         date_col="upload_datetime",
#     ),
#     greater_than_date_steps=get_greater_than_date_from_bq_table(
#         table_id="mainnet-bigq.raw.source_socket__routes_steps",
#         date_col="upload_datetime",
#     ),
# )

# [X] download latest json data:
# storage_client = storage.Client()
# bucket = storage_client.get_bucket("socket_routes")
# blobs = bucket.list_blobs()
# for blob in blobs:
#     pprint(blob.name)
#     if blob.name == "2024-02-21_13-27-49.json":
#         data = json.loads(blob.download_as_text())
#         with open(f"data/ad_hoc_socket_routes_{blob.name}", "w") as f:
#             json.dump(data, f)
#         with open(f"data/sample_ad_hoc_socket_routes_{blob.name}", "w") as f:
#             json.dump(data[0], f)
#         break

# with open("data/sample_ad_hoc_socket_routes_2024-02-21_13-27-49.json", "r") as f:
#     data = [json.load(f)]

# df = convert_routes_payload_to_df(data)
# print(df)
# print(df.columns)

# 2. get file to dataframe
# with open("data/new_socket_routes.json", "r") as f:
#     data = json.load(f)
# df = convert_no_routes_success_calls_to_df(json_blob=data)
# df.to_csv("data/new_socket_routes.csv")

# 3. get df from csv
# df = pd.read_csv("data/ad_hoc_socket_routes_2024-02-20_14-09-59.csv")

# # pprint(df.columns)
# # pprint(df.head())
# pprint(df.nunique())
