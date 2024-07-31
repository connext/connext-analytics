import os
import asyncio
from pprint import pprint
import httpx
import pandas as pd
import logging
import json
from jinja2 import Template
import numpy as np
import pandas_gbq
from dotenv import load_dotenv
from itertools import product
from datetime import datetime
from asyncio import Semaphore
from google.cloud import storage
from src.integrations.utilities import (
    get_raw_from_bq,
    nearest_power_of_ten,
    get_secret_gcp_secrete_manager,
    upload_json_to_gcs,
    convert_lists_and_booleans_to_strings,
)

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


PROJECT_ID = "mainnet-bigq"
source_lifi__api_key = get_secret_gcp_secrete_manager(
    secret_name="source_lifi__api_key"
)

# Load the .env file"
BASE_URL = "https://li.quest/v1"
HEADERS = {
    "accept": "application/json",
    "x-lifi-api-key": f"{source_lifi__api_key}",
}


async def get_data(ext_url: str):
    url = BASE_URL + ext_url
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.text
    except httpx.HTTPError as e:
        logging.info(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        return None


# Handle JSON to Dataframe aswell as Great Expectations beforeHand
async def all_chains(ext_url="/chains"):

    result = await get_data(ext_url=ext_url)
    if result is not None:
        result_j = json.loads(result)
        df = pd.json_normalize(result_j["chains"])
        logging.info(f"{df.shape}")

        df = df.rename(columns=lambda x: x.replace(".", "_"))
        df["faucetUrls"] = df["faucetUrls"].apply(
            lambda x: x[0] if type(x) == list else x
        )
        df["metamask_blockExplorerUrls"] = df["metamask_blockExplorerUrls"].apply(
            lambda x: x[0] if type(x) == list else x
        )
        df["metamask_rpcUrls"] = df["metamask_rpcUrls"].apply(
            lambda x: x[0] if type(x) == list else x
        )

        return df


async def connections_pd_explode(df):
    """combine tokens from in and out into 1 column"""

    df = df.explode("fromTokens").reset_index(drop=True)
    df = df.explode("toTokens").reset_index(drop=True)
    from_tokens_df = pd.json_normalize(df["fromTokens"]).add_prefix("from_")
    to_tokens_df = pd.json_normalize(df["toTokens"]).add_prefix("to_")
    df = pd.concat([df, from_tokens_df, to_tokens_df], axis=1)
    return df.drop(columns=["fromTokens", "toTokens"])


async def get_connections(ext_url: str = "/connections"):
    url = BASE_URL + ext_url
    bridges = "amarok"
    params = {"allowBridges": bridges}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, headers=HEADERS)
            response.raise_for_status()
            connections_df = pd.json_normalize(response.json()["connections"])
            return await connections_pd_explode(df=connections_df)

    except httpx.HTTPStatusError as e:
        logging.info(f"Error: {e}")
        return None


async def get_tokens(ext_url: str = "/tokens"):
    url = BASE_URL + ext_url
    try:
        async with httpx.AsyncClient() as client:
            print(HEADERS)
            response = await client.get(url, headers=HEADERS)
            response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404, 500)
            tokens_data = response.json()
            if tokens_data:
                all_tokens = []
                for key in tokens_data["tokens"]:
                    all_tokens.extend(tokens_data["tokens"][key])
                    tokens_df = pd.DataFrame(all_tokens)
            return tokens_df
    except httpx.HTTPStatusError as e:
        logging.info(f"Error: {e}")
        return None


async def get_tools(ext_url: str = "/tools"):
    "Add a bridge/chain filter based on ids to consider to generate paths"
    url = BASE_URL + ext_url

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS)
            response.raise_for_status()
            tools_data = response.json()
            if tools_data:
                bridges_df = pd.json_normalize(tools_data["bridges"])
                exchanges_df = pd.json_normalize(tools_data["exchanges"])
                tools_df = pd.concat(
                    [
                        bridges_df.assign(source="bridges"),
                        exchanges_df.assign(source="exchanges"),
                    ]
                )

                return pd.json_normalize(
                    tools_df[tools_df["key"] == "amarok"]["supportedChains"].values[0]
                ), tools_df.explode("supportedChains").reset_index(drop=True)

    except httpx.HTTPStatusError as e:
        logging.info(f"Error: {e}")
        return None


def generate_alt_pathways_by_chain_key_inputs(chain_keys: list, tokens: list):
    """
    chain_keys is a list of chain keys
    from get_chains() enpoints -> stored in BQ lifi chain

        eg: ["era", "bas", "ava","pze"]
    """
    try:
        sql_file_name = "generate_alternative_chains_ids_for_pathways"
        sql_dir = os.path.join("src", "sql", f"{sql_file_name}.sql")

        with open(sql_dir, "r") as sql_file:
            file_content = sql_file.read()
            query = Template(file_content).render(
                keys=", ".join(f'"{key}"' for key in chain_keys)
            )

            df_chain_ids = pandas_gbq.read_gbq(query)
            logging.info(f"df_chain_ids: {df_chain_ids.shape}")

            alt_pathways_df = generate_pathways(
                connext_chains_ids=df_chain_ids,
                chains=asyncio.run(all_chains()),
                tokens=tokens,
                tokens_df=asyncio.run(get_tokens()),
            )

            logging.info(f"alt_pathways_df: {alt_pathways_df}")
            return alt_pathways_df

    except FileNotFoundError:
        print(f"The file {sql_dir} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def generate_pathways(
    connext_chains_ids: pd.DataFrame,
    chains: pd.DataFrame,
    tokens: list,
    tokens_df: pd.DataFrame,
):
    """generate json inputs from the chains data to query routes

    connext_chains_ids: These come from support chains col in tools
    endpoint that gives bridges and exchanges
    chains: This is stracted from chains endpoint
    tokens: This is a list of tokens symbols provided as a list
    tokens_df: This is stracted from Tokens endpoint
    """

    pathways_df = pd.merge(
        connext_chains_ids,
        chains[["id", "key", "name"]],
        how="outer",
        left_on="fromChainId",
        right_on="id",
    ).rename(columns={"name": "fromChainName", "key": "fromChainKey"})

    pathways_df = pd.merge(
        pathways_df,
        chains[["id", "key", "name"]],
        how="outer",
        left_on="toChainId",
        right_on="id",
    ).rename(columns={"name": "toChainName", "key": "toChainKey"})

    # Drop unnecessary columns
    pathways_df.drop(["id_y", "id_x"], axis=1, inplace=True)

    # Create a Cartesian product of the DataFrame and the list of tokens
    cartesian_product = pd.DataFrame(
        list(product(pathways_df["toChainName"], pathways_df["toChainKey"], tokens)),
        columns=["toChainName", "toChainKey", "tokenName"],
    )

    # Concatenate the Cartesian product DataFrame with the existing pathways_df DataFrame
    pathways_df = pd.concat([pathways_df] * len(tokens), ignore_index=True)

    # Add the 'tokenName' column to the existing DataFrame with values from the Cartesian product
    pathways_df["tokenName"] = cartesian_product["tokenName"]

    # Merge with tokens_df
    pathways_df = pd.merge(
        pathways_df,
        tokens_df[
            ["address", "chainId", "symbol", "decimals", "name", "coinKey", "priceUSD"]
        ],
        left_on=["fromChainId", "tokenName"],
        right_on=["chainId", "symbol"],
    )

    pathways_df.drop(["chainId", "symbol"], axis=1, inplace=True)
    pathways_df.rename(
        columns={
            "address": "fromTokenAddress",
            "decimals": "fromDecimals",
            "name": "fromName",
            "coinKey": "fromCoinKey",
        },
        inplace=True,
    )

    pathways_df = pd.merge(
        pathways_df,
        tokens_df[["address", "chainId", "symbol", "decimals", "name", "coinKey"]],
        left_on=["toChainId", "tokenName"],
        right_on=["chainId", "symbol"],
    )
    pathways_df.drop(["chainId", "symbol"], axis=1, inplace=True)
    pathways_df.rename(
        columns={
            "address": "toTokenAddress",
            "decimals": "toDecimals",
            "name": "toName",
            "coinKey": "toCoinKey",
        },
        inplace=True,
    )

    pathways = pathways_df.drop_duplicates().to_dict(orient="records")

    multiple_pathways = []
    for p in pathways:
        for i in [1, 2, 3, 4, 5, 6]:
            pathway = {
                "allowDestinationCall": True,
                "fromChainId": p["fromChainId"],
                "fromTokenAddress": p["fromTokenAddress"],
                # "fromAddress": p["fromTokenAddress"],
                # "fromAddress": "0x32d222E1f6386B3dF7065d639870bE0ef76D3599",
                "toChainId": p["toChainId"],
                "toTokenAddress": p["toTokenAddress"],
            }

            pathway["fromAmount"] = int(
                float((10 ** (p["fromDecimals"])) * (10**i)) / float(p["priceUSD"])
            )

            multiple_pathways.append(pathway)
    df_multiple_pathways = pd.DataFrame(multiple_pathways)
    df_multiple_pathways["fromAmount"] = df_multiple_pathways["fromAmount"].apply(
        nearest_power_of_ten
    )
    df_multiple_pathways = df_multiple_pathways.drop_duplicates()
    logging.info(f"Number of pathways: {df_multiple_pathways.shape}")
    return df_multiple_pathways.to_dict("records")


async def get_routes(sem, url, payload):
    try:
        async with sem:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers=HEADERS,
                    timeout=httpx.Timeout(10.0, connect=10.0, read=10.0),
                )
                response.raise_for_status()
                output = response.json()
                output["payload"] = payload
                output["payload"]["status_code"] = response.status_code
                return output

    except httpx.HTTPStatusError as exc:
        logging.info(f"Request failed with status code {exc.response.status_code}")
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


async def main_routes(
    payloads: list, max_concurrency: int = 5, ext_url: str = "/advanced/routes"
) -> list:
    url = BASE_URL + ext_url
    sem = Semaphore(max_concurrency)
    tasks = []
    for payload in payloads:
        task = get_routes(sem, url, payload)
        tasks.append(task)
    responses = await asyncio.gather(*tasks)
    filtered_responses = [r for r in responses if r is not None]
    return filtered_responses


def convert_json_to_df(json_file):
    # Initialize an empty DataFrame to store the normalized data
    normalized_data_df = pd.DataFrame()

    # Loop through each route in the JSON file
    for r in json_file:
        if "routes" in r:
            routes = r["routes"]
            for route in routes:
                # Normalize the steps for the current route
                steps_df = pd.json_normalize(route, record_path="steps")
                # pprint(f"steps_df: {steps_df.columns}")

                # Initialize empty lists to store fees and gas costs data
                fees_data = []
                gas_costs_data = []

                # Loop through each step to extract and normalize fees and gas costs
                for step in route["steps"]:
                    # Normalize fees and append to the fees_data list
                    if step["estimate"]["feeCosts"]:
                        fees = pd.json_normalize(step["estimate"]["feeCosts"])
                        fees_data.append(fees)

                    # Normalize gas costs and append to the gas_costs_data list
                    if step["estimate"]["gasCosts"]:
                        gas_costs = pd.json_normalize(step["estimate"]["gasCosts"])
                        gas_costs_data.append(gas_costs)

                if fees_data:
                    fees_df = pd.concat(fees_data, ignore_index=True)
                else:
                    fees_df = pd.DataFrame()

                # Add a prefix to each fee column name to prevent conflicts
                fees_df.columns = ["fee_" + col for col in fees_df.columns]
                # pprint(f"fees_df: {fees_df.columns}")

                if gas_costs_data:
                    gas_costs_df = pd.concat(gas_costs_data, ignore_index=True)
                else:
                    gas_costs_df = pd.DataFrame()
                gas_costs_df.columns = ["gas_" + col for col in gas_costs_df.columns]
                # pprint(f"gas_costs_df: {gas_costs_df.columns}")

                # Extract the route metadata (excluding the 'steps')
                metadata = {k: v for k, v in route.items() if k != "steps"}

                # Normalize the metadata
                metadata_df = pd.json_normalize(metadata)
                # pprint(f"metadata_df: {metadata_df.columns}")

                # Add a prefix to each metadata column name to prevent conflicts
                metadata_df.columns = ["route_" + col for col in metadata_df.columns]

                # Repeat the metadata for each step in the current route
                repeated_metadata_df = pd.concat(
                    [metadata_df] * len(steps_df), ignore_index=True
                )
                # pprint(f"repeated_metadata_df: {repeated_metadata_df.columns}")

                # Concatenate the steps DataFrame with the repeated metadata DataFrame
                enriched_steps_df = pd.concat([steps_df, repeated_metadata_df], axis=1)
                # pprint(f"enriched_steps_df: {enriched_steps_df.columns}")

                # Concatenate the fees and gas costs DataFrames with the enriched steps DataFrame
                enriched_df = pd.concat(
                    [enriched_steps_df, fees_df, gas_costs_df], axis=1
                )
                # pprint(f"will all enriched_df: {enriched_df.columns}")

                # Concatenate the enriched DataFrame with the normalized_data_df

                try:
                    normalized_data_df = pd.concat(
                        [normalized_data_df, enriched_df], ignore_index=True
                    )
                    # print(f"{normalized_data_df.shape} df size")
                except pd.errors.InvalidIndexError:
                    # print("Error occurred during concatenation.")

                    if len(enriched_df.columns) == len(enriched_df.columns.unique()):
                        print("All column names are unique.")
                    else:
                        print("There are duplicate column names.")
                        # Assume df is your DataFrame
                        duplicate_columns = enriched_df.columns[
                            enriched_df.columns.duplicated()
                        ]

                        print("Duplicate column names: ", duplicate_columns.tolist())
                        print(f"original cols: {enriched_df.columns}")

    return normalized_data_df.dropna()


def get_greater_than_date_from_bq_lifi_routes():
    try:
        df = get_raw_from_bq(sql_file_name="latest_date_from_lifi_routes")
        return np.array(df["latest_upload_datetime"].dt.to_pydatetime())[0].replace(
            tzinfo=None
        )
    except pandas_gbq.exceptions.GenericGBQException as e:
        if "404 Not found" in str(e):
            return datetime(2024, 1, 1, 1, 1, 1)
        else:
            raise


def get_upload_data_from_lifi_cs_bucket(greater_than_date, bucket_name="lifi_routes"):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        logging.info(f"Pulling data for: {blob.name}")

        name = os.path.splitext(blob.name)[0]
        dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")

        if dt > greater_than_date:
            data = json.loads(blob.download_as_text())
            print(f"data: {len(data)}")

            # 1. convert the data to df
            df = convert_json_to_df(json_file=data)
            name = os.path.splitext(blob.name)[0]
            df["upload_datetime"] = dt
            df.columns = df.columns.str.lower()
            df.columns = df.columns.str.replace(".", "_")
            for col in df.columns:
                if df[col].apply(isinstance, args=(list,)).any():
                    df[col] = df[col].apply(
                        lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
                    )

                    df = df.astype(
                        {col: "int" for col in df.select_dtypes(include=[bool]).columns}
                    )

            # upload to bq
            pandas_gbq.to_gbq(
                dataframe=df,
                project_id=PROJECT_ID,
                destination_table="stage.source_lifi__routes",
                if_exists="append",
                chunksize=100000,
                api_method="load_csv",
            )
            logging.info(f"Lifi Routers, {df.shape} rows Added!")

            # 2. Upload payload data along with this
            logging.info(f"Uploading payload data for:{name} ")
            payload_df = convert_routes_payload_to_df(json_blob=data)
            payload_df["aggregator"] = "lifi"
            payload_df["upload_datetime"] = dt

            # upload to bq
            pandas_gbq.to_gbq(
                dataframe=convert_lists_and_booleans_to_strings(payload_df),
                project_id=PROJECT_ID,
                destination_table="raw.source__lifi_routes__payloads_logs",
                if_exists="append",
                chunksize=100000,
                api_method="load_csv",
            )
            logging.info(f"Lifi Payloads, {payload_df.shape} rows Added!")

        else:
            logging.info(
                f"{dt} is not greater than {greater_than_date}, Data Already Added!"
            )


def convert_no_routes_success_calls_to_df(json_blob):

    all_calls = []

    for r in json_blob:
        if len(r["routes"]) == 0:
            if "unavailableRoutes" in r:
                unav = r["unavailableRoutes"]
                if "failed" in unav:
                    if len(unav["failed"]) != 0:
                        all_calls.extend(unav["failed"])
                        break
    pprint(all_calls)

    df = pd.json_normalize(all_calls)
    df = convert_lists_and_booleans_to_strings(df)
    # df.to_csv("data/eg_1_lifi_no_routes_success_calls.csv")
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
    for item in all_payloads:
        item.pop("options", None)
    df = pd.json_normalize(all_payloads)
    return df


# if __name__ == "__main__":
# get_upload_data_from_lifi_cs_bucket(get_greater_than_date_from_bq_lifi_routes())

# convert_no_routes_success_calls_to_df

#    # [X] download latest json data:
# storage_client = storage.Client()
# bucket = storage_client.get_bucket("lifi_routes")
# blobs = bucket.list_blobs()
# for blob in blobs:
#     pprint(blob.name)
#     if blob.name == "2024-02-21_09-17-38.json":
#         data = json.loads(blob.download_as_text())
#         with open(f"data/ad_hoc_lifi_routes_{blob.name}", "w") as f:
#             json.dump(data, f)
#         with open(f"data/sample_ad_hoc_lifi_routes_{blob.name}", "w") as f:
#             json.dump(data[0], f)
#         break

# 2. get file to dataframe
# with open("data/sample_ad_hoc_lifi_routes_2024-02-21_09-17-38.json", "r") as f:
#     data = [json.load(f)]
# # with open("data/ad_hoc_lifi_routes_2024-02-21_09-17-38.json", "r") as f:
# #     data = json.load(f)
# df = convert_json_to_df(data)
# df_payload = convert_routes_payload_to_df(data)
# pprint(df)
# pprint(df_payload)

# df = convert_json_to_df(data)
# df.to_csv("data/ad_hoc_lifi_routes_2024-02-21_09-17-38.csv")
# pprint(df)
