import requests
import pandas as pd
import logging
import json
import ast


# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

base_url = "https://li.quest/v1"


def get_data(ext_url: str):
    url = base_url + ext_url
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as e:
        logging.info(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        return None


# Handle JSON to Dataframe aswell as Great Expectations beforeHand
def all_chains(ext_url="/chains"):
    result = get_data(ext_url=ext_url)
    if result is not None:
        result_j = json.loads(result)
        df = pd.json_normalize(result_j["chains"])

        df["faucetUrls"] = df["faucetUrls"].apply(
            lambda x: x[0] if isinstance(x, list) else ""
        )
        df["tokenlistUrl"] = df["tokenlistUrl"].fillna("")
        df["faucetUrls"] = df["faucetUrls"].fillna("")
        logging.info(
            f"The dataframe we pulled, shape: {df.dtypes} and size: {df.isna().sum()}"
        )
        return df


def connections_pd_explode(df):
    """combine tokens from in and out into 1 column"""

    df = df.explode("fromTokens").reset_index(drop=True)
    df = df.explode("toTokens").reset_index(drop=True)
    from_tokens_df = pd.json_normalize(df["fromTokens"])
    to_tokens_df = pd.json_normalize(df["toTokens"])
    df = pd.concat([df, from_tokens_df, to_tokens_df], axis=1)

    return df.drop(columns=["fromTokens", "toTokens"])


def get_connections(ext_url: str = "/connections"):
    url = base_url + ext_url
    bridges = "amarok"
    params = {"allowBridges": bridges}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        connections_df = pd.json_normalize(response.json()["connections"])
        # print(connections_df.shape)
        return connections_pd_explode(df=connections_df)

    except requests.exceptions.RequestException as e:
        logging.info(f"Error: {e}")
        return None


import asyncio
import httpx


async def fetch_data(url, semaphore):
    try:
        async with semaphore, httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text
    except httpx.HTTPError as e:
        print(f"HTTP error occurred for {url}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred for {url}: {e}")
        return None


async def main():
    urls = ["https://li.quest/v1/chains"] * 10  # Example: Make 10 parallel API calls
    concurrency_limit = 3  # Limit concurrency to 3 requests at a time

    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency_limit)

    # Create a list of tasks
    tasks = [fetch_data(url, semaphore) for url in urls]

    # Use asyncio.gather to run all tasks concurrently
    results = await asyncio.gather(*tasks)

    # Process the results as needed
    for i, result in enumerate(results, start=1):
        if result is not None:
            print(f"Response {i}: {result}")


if __name__ == "__main__":
    # 1. get all chains:
    # chains_df = all_chains()
    # print(chains_df)

    # 2. connections
    connections_df = get_connections()
    print(connections_df.head())
