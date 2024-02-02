import asyncio
import httpx
import pandas as pd
import logging
import json
from itertools import product
from pprint import pprint
from datetime import datetime
import os
import dotenv
from asyncio import Semaphore
from src.integrations.utilities import get_secret_gcp_secrete_manager

dotenv.load_dotenv()
BASE_URL = "https://li.quest/v1"
PROJECT_ID = "mainnet-bigq"
source_lifi__api_key = os.getenv("source_lifi__api_key")
HEADERS = {
    "accept": "application/json",
    "x-lifi-api-key": f"{source_lifi__api_key}",
}

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def get_data(ext_url: str):
    url = BASE_URL + ext_url
    try:
        async with httpx.AsyncClient() as client:
            print(f"This is a header: {HEADERS}")
            response = await client.get(url, headers=HEADERS)
            print(f"This is a header: {response.headers}")
            response.raise_for_status()
            logging.info("call successfull!")
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
    print(len(result))
    if result is not None:
        result_j = json.loads(result)
        df = pd.json_normalize(result_j["chains"])
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
                )

    except httpx.HTTPStatusError as e:
        logging.info(f"Error: {e}")
        return None


def generate_pathways(
    connext_chains_ids: pd.DataFrame,
    chains: pd.DataFrame,
    tokens: list,
    tokens_df: pd.DataFrame,
):
    """generate json inputs from the chains data to query routes"""

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
                "fromAddress": "0x32d222E1f6386B3dF7065d639870bE0ef76D3599",
                "toChainId": p["toChainId"],
                "toTokenAddress": p["toTokenAddress"],
            }

            pathway["fromAmount"] = int(
                float((10 ** (p["fromDecimals"])) * (10**i)) / float(p["priceUSD"])
            )

            multiple_pathways.append(pathway)
    df_multiple_pathways = pd.DataFrame(multiple_pathways)
    df_multiple_pathways = df_multiple_pathways.drop_duplicates()
    logging.info(f"Number of pathways: {df_multiple_pathways.shape}")
    return df_multiple_pathways.to_dict("records")


async def get_routes(sem, url, payload):
    try:
        async with sem:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, headers=HEADERS)
                response.raise_for_status()
                return response.json()

    except httpx.HTTPStatusError as exc:
        logging.info(f"Request failed with status code {exc.response.status_code}")
        logging.info(f"Error message: {exc}")
        return None
    except Exception as exc:
        logging.info(f"An unexpected error occurred: {str(exc)}")
        return None


async def main_routes(payloads, max_concurrency=10, ext_url="/advanced/routes"):
    url = BASE_URL + ext_url
    sem = Semaphore(max_concurrency)
    tasks = []
    for payload in payloads:
        task = get_routes(sem, url, payload)
        tasks.append(task)
    responses = await asyncio.gather(*tasks)
    filtered_responses = [r for r in responses if r is not None]
    return filtered_responses

    # with open("data.json", "w") as f:
    #     json.dump(filtered_responses, f, indent=4)

    # normalized_data_df = pd.DataFrame()
    # for r in filtered_responses:
    #     routes = r["routes"]
    #     for route in routes:
    #         # Normalize the steps for the current route
    #         steps_df = pd.json_normalize(route, record_path="steps")

    #         # Initialize empty lists to store fees and gas costs data
    #         fees_data = []
    #         gas_costs_data = []

    #         # Loop through each step to extract and normalize fees and gas costs
    #         for step in route["steps"]:
    #             # Normalize fees and append to the fees_data list
    #             if step["estimate"]["feeCosts"]:
    #                 fees = pd.json_normalize(step["estimate"]["feeCosts"])
    #                 fees_data.append(fees)

    #             # Normalize gas costs and append to the gas_costs_data list
    #             if step["estimate"]["gasCosts"]:
    #                 gas_costs = pd.json_normalize(step["estimate"]["gasCosts"])
    #                 gas_costs_data.append(gas_costs)

    #         # Concatenate all fees and gas costs data
    #         fees_df = pd.concat(fees_data, ignore_index=True)
    #         gas_costs_df = pd.concat(gas_costs_data, ignore_index=True)

    #         # Extract the route metadata (excluding the 'steps')
    #         metadata = {k: v for k, v in route.items() if k != "steps"}

    #         # Normalize the metadata
    #         metadata_df = pd.json_normalize(metadata)

    #         # Add a prefix to each metadata column name to prevent conflicts
    #         metadata_df.columns = ["route_" + col for col in metadata_df.columns]

    #         # Repeat the metadata for each step in the current route
    #         repeated_metadata_df = pd.concat(
    #             [metadata_df] * len(steps_df), ignore_index=True
    #         )

    #         # Concatenate the steps DataFrame with the repeated metadata DataFrame
    #         enriched_steps_df = pd.concat([steps_df, repeated_metadata_df], axis=1)

    #         # Concatenate the fees and gas costs DataFrames with the enriched steps DataFrame
    #         enriched_df = pd.concat([enriched_steps_df, fees_df, gas_costs_df], axis=1)

    #         # Append the enriched DataFrame to the normalized data DataFrame
    #         normalized_data_df = pd.concat(
    #             [normalized_data_df, enriched_df], ignore_index=True
    #         )

    # return normalized_data_df


async def lifi_routes_cs_2_bq():
    "Pull json files from cs"










# if __name__ == "__main__":
#     # 1. get all chains:
#     chains_df = asyncio.run(all_chains())
#     pandas_gbq.to_gbq(
#         dataframe=chains_df,
#         project_id=project_id,
#         destination_table="stage.source_lifi__chains",
#         if_exists="replace",
#     )


#     # 2. connections
#     # connections = asyncio.run(get_connections())
#     # print(connections.drop_duplicates().shape)

#     # Get tokens
#     tokens_df = asyncio.run(get_tokens())
#     # pprint(tokens_df)

#     # Get Bridges

#     tools_df = asyncio.run(get_tools())

#     # 3. Routes
#     # 3.1 get all pathways
#     pathways = generate_pathways(
#         connext_chains_ids=tools_df,
#         chains=chains_df,
#         token_df=tokens_df,
#         tokens=["ETH", "USDT", "DAI", "USDC", "WETH"],
#     )

#     routes = asyncio.run(main_routes(payloads=pathways))
