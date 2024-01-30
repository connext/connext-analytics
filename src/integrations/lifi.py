import asyncio
import httpx
import pandas as pd
import logging
import json
from itertools import product
from pprint import pprint
from datetime import datetime
import os
from asyncio import Semaphore


# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

base_url = "https://li.quest/v1"


async def get_data(ext_url: str):
    url = base_url + ext_url
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
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
        df["tokenlistUrl"] = df["tokenlistUrl"].fillna("")
        df["faucetUrls"] = df["faucetUrls"].fillna("")
        logging.info(f"The dataframe we pulled, shape: {df.shape}")
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
    url = base_url + ext_url
    bridges = "amarok"
    params = {"allowBridges": bridges}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            connections_df = pd.json_normalize(response.json()["connections"])
            return await connections_pd_explode(df=connections_df)

    except httpx.HTTPStatusError as e:
        logging.info(f"Error: {e}")
        return None


async def get_tokens(ext_url: str = "/tokens"):
    url = base_url + ext_url
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404, 500)
            tokens_data = response.json()
            if tokens_data:
                all_tokens = []
                for key in tokens_data["tokens"]:
                    all_tokens.extend(tokens_data["tokens"][key])
                    tokens_df = pd.DataFrame(all_tokens)
            return tokens_df
    except httpx.HTTPStatusError as e:
        print(f"Error: {e}")
        return None


async def get_tools(ext_url: str = "/tools"):
    url = base_url + ext_url

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
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
        print(f"Error: {e}")
        return None


def generate_pathways(
    connext_chains_ids: pd.DataFrame,
    chains: pd.DataFrame,
    tokens: list,
    token_df: pd.DataFrame,
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
        chains_df[["id", "key", "name"]],
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

    pathways = pathways_df.to_dict(orient="records")

    multiple_pathways = []
    for p in pathways:
        for i in [1, 2, 3, 4, 5, 6]:
            pathway = {
                "allowDestinationCall": True,
                "fromChainId": p["fromChainId"],
                "fromTokenAddress": p["fromTokenAddress"],
                "fromAddress": p["fromTokenAddress"],
                "toChainId": p["toChainId"],
                "toTokenAddress": p["toTokenAddress"],
            }

            pathway["fromAmount"] = int(
                float((10 ** (p["fromDecimals"])) * (10**i)) / float(p["priceUSD"])
            )

            multiple_pathways.append(pathway)

    return multiple_pathways


async def get_routes(sem, url, payload):
    try:
        async with sem:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                pprint(response.status_code)
                return response.json()
    except httpx.HTTPError as e:
        print(f"HTTP error occurred for {url}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred for {url}: {e}")
        return None


async def main_routes(payloads, max_concurrency=20, ext_url="/advanced/routes"):
    url = base_url + ext_url
    sem = Semaphore(max_concurrency)
    tasks = []
    for payload in payloads:
        task = get_routes(sem, url, payload)
        tasks.append(task)
    responses = await asyncio.gather(*tasks)
    return responses


if __name__ == "__main__":
    # 1. get all chains:
    chains_df = asyncio.run(all_chains())
    # pprint(chains_df.dtypes)

    # 2. connections
    # connections = asyncio.run(get_connections())
    # print(connections.drop_duplicates().shape)

    # Get tokens
    tokens_df = asyncio.run(get_tokens())
    # pprint(tokens_df)

    # Get Bridges

    tools_df = asyncio.run(get_tools())
    # pprint(tools_df.dtypes)

    # 3. Routes
    # 3.1 get all pathways
    pathways = generate_pathways(
        connext_chains_ids=tools_df,
        chains=chains_df,
        token_df=tokens_df,
        tokens=["ETH", "USDT", "DAI", "USDC", "WETH"],
    )

    routes = asyncio.run(main_routes(payloads=pathways))
    pprint(len(routes))
