import json
import logging
import os

import pandas as pd
import pandas_gbq as gbq
import requests

from src.integrations.utilities import (convert_lists_and_booleans_to_strings,
                                        get_secret_gcp_secrete_manager)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
PROJECT_ID = "mainnet-bigq"
API_KEY = get_secret_gcp_secrete_manager(secret_name="CMC_NAIK_AI_free_tier_API_KEY")

# these tokens are used in the volume and price analysis against all bridges
TOKEN_SYMBOL_LIST = ["ETH", "USDT", "DAI", "USDC", "WETH", "WBTC", "EZETH"]


def get_cryptocurrency_map() -> dict:
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
    else:
        logger.error(f"Error: {response.status_code}")

    return data


def convert_token_symbols_to_ids(
    token_map_raw_data: dict,
    token_symbol_list: list = TOKEN_SYMBOL_LIST,
):
    """
    convert_token_symbols_to_ids

    sample data:
        {
        "data": [
            {
            "id": 1,
            "rank": 1,
            "name": "Bitcoin",
            "symbol": "BTC",
            "slug": "bitcoin",
            "is_active": 1,
            "first_historical_data": "2010-07-13T00:05:00.000Z",
            "last_historical_data": "2024-07-29T05:30:00.000Z",
            "platform": null
            }
        }

        STEPS:
        1. iterate through the token_symbol_list
        2. append the token_id to the token_id_list where the symbol matches
        3. make sure the list size is the same as the token_symbol_list
    Args:
        token_map_raw_data (dict): _description_
        token_symbol_list (list, optional): _description_. Defaults to TOKEN_SYMBOL_LIST.

    Returns:
        _type_: _description_
    """
    token_maps_list = token_map_raw_data["data"]

    token_id_list = []
    for token_data in token_maps_list:
        token_symbol = token_data["symbol"]
        if token_symbol in token_symbol_list:
            token_id_list.append(token_data["id"])
    return token_id_list


def extract_contract_data_for_all_token_metadata(token_metadata: dict) -> dict:
    """
    extract_contract_data_for_all_token_metadata
    Sample data:
    token_metadata= {
      "status": {
      },
      "data": {
      "825": {
          "id": 825,
          "name": "Tether USDt",
          "symbol": "USDT",
          "slug": "tether",
          "contract_address": [
          {
              "contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
              "platform": {
              "name": "Ethereum",
              "coin": {
                  "id": "1027",
                  "name": "Ethereum",
                  "symbol": "ETH",
                  "slug": "ethereum"
              }
              }
          },
          ]
    """
    token_contract_platform_data = []
    for token_id, token_meta in token_metadata["data"].items():
        for contract in token_meta.get("contract_address", []):
            contract_data = {
                "token_id": token_id,
                "token_name": token_meta["name"],
                "token_symbol": token_meta["symbol"],
                "contract_address": contract["contract_address"],
                "platform_name": contract["platform"]["name"],
                "platform_coin_id": contract["platform"]["coin"]["id"],
                "platform_coin_name": contract["platform"]["coin"]["name"],
                "platform_coin_symbol": contract["platform"]["coin"]["symbol"],
            }
            token_contract_platform_data.append(contract_data)

    df = pd.DataFrame(token_contract_platform_data)
    return df


def get_cryptocurrency_metadata_by_token_ids(token_id_list: list) -> pd.DataFrame:
    # Set up the headers, including the API key
    comma_seperated_token_ids = ",".join(map(str, token_id_list))
    url = f"https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?id={comma_seperated_token_ids}"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }

    # Make the request
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        df_token_contracts = extract_contract_data_for_all_token_metadata(data)
        logger.info(
            "Cryptocurrency metadata saved to data/cryptocurrency_metadata.json"
        )
    else:
        logger.error(f"Error: {response.status_code}")
    return df_token_contracts


def cmc_pipeline():
    token_map_raw_data = get_cryptocurrency_map()
    token_id_list = convert_token_symbols_to_ids(token_map_raw_data)
    logger.info(f"Token ID list generated: {len(token_id_list)}")
    token_contracts_df = convert_lists_and_booleans_to_strings(
        get_cryptocurrency_metadata_by_token_ids(token_id_list)
    )
    crypto_map_df = convert_lists_and_booleans_to_strings(
        pd.json_normalize(token_map_raw_data["data"])
    )

    # push to Big Query
    gbq.to_gbq(
        dataframe=crypto_map_df,
        project_id=PROJECT_ID,
        destination_table="raw.source_coinmarketcap__crypto_map",
        if_exists="append",
        chunksize=10000,
        api_method="load_csv",
    )
    gbq.to_gbq(
        dataframe=token_contracts_df,
        project_id=PROJECT_ID,
        destination_table="raw.source_coinmarketcap__token_contracts_by_chains",
        if_exists="append",
        chunksize=10000,
        api_method="load_csv",
    )


if __name__ == "__main__":
    cmc_pipeline()
    # push to Big Query after converting to dataframe(json_normalize)
