import json
import logging
import random
import time
from datetime import datetime
from typing import Iterator, Sequence

import httpx
import pandas as pd
import pandas_gbq as gbq
import pytz
import requests
from requests.exceptions import HTTPError, RequestException, Timeout

from src.integrations.models.all_bridge_explorer import (
    AllBridgeExplorerTokenInfo, AllBridgeExplorerTransfer)
from src.integrations.utilities import get_raw_from_bq, pydantic_schema_to_list

# Base URL for the API
# base_url = "https://explorer-variant-filter.api.allbridgecoreapi.net/transfers"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
TABLE_NAME_EXPLORER_TRANSFERS = "raw.source_all_bridge_explorer_transfers_v2"
TABLE_NAME_EXPLORER_TOKENS_INFO = "raw.source_all_bridge_explorer_tokens_v2"


def get_all_bridge_explorer_transfers(
    all_bridge_explorer_transfers_url,
    max_retries=100,
    base_delay=5,
) -> Iterator[AllBridgeExplorerTransfer]:
    """
    Logic: Paginate through the API and append to the table, pull till timestamp is found to be less than
    the last tx timestamp also check for tx hash to be found.
    If found keep data before that tx hash, discard rest and break.


    URL start date hy page: https://core.allbridge.io/explorer?status=Complete&page=85000
    from where to page 1.

    """
    page = 1
    page_size = 20
    status = "Complete"

    page_txs = []

    logging.info(
        f"Starting transfer fetch. Last processed timestamp: {last_tx_timestamp}, hash: {last_tx_hash}"
    )

    found_last_tx = False
    while True:
        logging.info(f"Fetching page: {page}, page size: {page_size}")
        url = f"{all_bridge_explorer_transfers_url}?status={status}&page={page}&limit={page_size}"

        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                logging.info(
                    f"Successfully fetched data from URL: {url}, status code: {response.status_code}"
                )
                break  # Success, exit the retry loop
            except (Timeout, HTTPError) as e:
                if isinstance(e, HTTPError) and e.response.status_code == 400:
                    logging.error(f"Bad request error for URL: {url}. Error: {str(e)}")
                    raise  # Don't retry on 400 errors
                if attempt < max_retries - 1:
                    delay = (base_delay * 2**attempt) + (
                        random.randint(0, 1000) / 1000
                    )
                    logging.warning(
                        f"Request failed. Retrying in {delay:.2f} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logging.error(f"Max retries reached for URL: {url}")
                    raise
            except RequestException as e:
                logging.error(f"Error fetching data from URL: {url}. Error: {str(e)}")
                raise

        data = response.json()
        transactions = data.get("items", [])
        logging.info(f"Retrieved {len(transactions)} transactions from page {page}")

        for transaction in transactions:
            record = {
                "id": transaction.get("id"),
                "status": transaction.get("status"),
                "timestamp": transaction.get("timestamp"),
                "from_chain_symbol": transaction.get("fromChainSymbol"),
                "to_chain_symbol": transaction.get("toChainSymbol"),
                "from_amount": transaction.get("fromAmount"),
                "to_amount": transaction.get("toAmount"),
                "stable_fee": transaction.get("stableFee"),
                "from_token_address": transaction.get("fromTokenAddress"),
                "to_token_address": transaction.get("toTokenAddress"),
                "from_address": transaction.get("fromAddress"),
                "to_address": transaction.get("toAddress"),
                "messaging_type": transaction.get("messagingType"),
                "partner_id": transaction.get("partnerId"),
                "from_gas": transaction.get("fromGas"),
                "to_gas": transaction.get("toGas"),
                "relayer_fee_in_native": transaction.get("relayerFeeInNative"),
                "relayer_fee_in_tokens": transaction.get("relayerFeeInTokens"),
                "send_transaction_hash": transaction.get("sendTransactionHash"),
                "receive_transaction_hash": transaction.get("receiveTransactionHash"),
                "api_url": url,
            }
            if (
                record.get("timestamp") == last_tx_timestamp
                and record.get("id") == last_tx_hash
            ):
                logging.info(
                    f"Reached the last processed transaction. ID: {record['id']}, Timestamp: {record['timestamp']}"
                )
                found_last_tx = True
                break
            elif record.get("timestamp") > last_tx_timestamp or (
                record.get("timestamp") == last_tx_timestamp
                and record.get("id") != last_tx_hash
            ):
                tx = AllBridgeExplorerTransfer(**record)
                page_txs.append(tx.model_dump())
                logging.debug(
                    f"Added transaction to page_txs. ID: {record['id']}, Timestamp: {record['timestamp']}"
                )

        if page % 500 == 0 or found_last_tx:
            logging.info(f"Loading data after {page} pages")
            if page_txs:
                df = pd.DataFrame(page_txs)
                logging.info(
                    f"Pushing {len(df)} transactions to BQ and schema: {df.dtypes}"
                )

                gbq.to_gbq(
                    dataframe=df,
                    project_id="mainnet-bigq",
                    destination_table=TABLE_NAME_EXPLORER_TRANSFERS,
                    if_exists="append",
                    api_method="load_csv",
                    table_schema=pydantic_schema_to_list(
                        AllBridgeExplorerTransfer.model_json_schema()
                    ),
                )
                page_txs = []

        page += 1
        if found_last_tx:
            break
    logging.info(
        f"Finished fetching transfers. Total transactions processed: {len(page_txs)}"
    )


def to_snake_case(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def convert_pool_info(token):
    pool_info = token.pop("poolInfo", {})
    for key, value in pool_info.items():
        snake_case_key = "pool_info_" + to_snake_case(key)
        token[snake_case_key] = value
    return token


def get_all_bridge_explorer_token_info(
    all_bridge_explorer_token_info_url,
):
    """
    get_all_bridge_explorer_token_info _summary_
    _extended_summary_
    Args:
        all_bridge_explorer_token_info_url (_type_, optional): _description_.
    Yields:
        List[AllBridgeExplorerTokenInfo]:
        {
            "blockchain": "ETH",
            "name": "USD Coin",
            "poolAddress": "0xa7062bbA94c91d565Ae33B893Ab5dFAF1Fc57C4d",
            "tokenAddress": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
            "symbol": "USDC",
            "feeShare": "0.0015",
            "apr": "0.21306338825453272",
            "apr7d": "0.21306338825453272",
            "apr30d": "0.37379985935935633",
            "lpRate": "0.50001586866652729236",
            "cctpAddress": "0xC51397b75B783E31469bFaADE79913F3f82210d6",
            "cctpFeeShare": "0.001",
            "poolInfoaValue": "20",
            "poolInfodValue": "8067966542",
            "poolInfotokenBalance": "4348721982",
            "poolInfovUsdBalance": "3719845832",
            "poolInfototalLpAmount": "8067882456",
            "poolInfoaccRewardPerShareP": "897816473927811762",
            "poolInfop": 52
        }

    """
    response = requests.get(all_bridge_explorer_token_info_url)

    if response.status_code == 200:
        logging.info(
            f"url: {all_bridge_explorer_token_info_url}, status code: {response.status_code}"
        )

        data = response.json()
        simplified_data = []
        for blockchain, info in data.items():
            for token in info["tokens"]:
                token["blockchain"] = blockchain
                token = {to_snake_case(k): v for k, v in token.items()}
                token = convert_pool_info(token)
                # add url
                token["api_url"] = all_bridge_explorer_token_info_url
                # updated date
                token["updated_at"] = datetime.now(tz=pytz.UTC).isoformat()
                tk = AllBridgeExplorerTokenInfo(**token)
                simplified_data.append(tk.model_dump())

        # push to table
        if simplified_data:
            df = pd.DataFrame(simplified_data)
            gbq.to_gbq(
                dataframe=df,
                project_id="mainnet-bigq",
                destination_table=TABLE_NAME_EXPLORER_TOKENS_INFO,
                if_exists="replace",
                table_schema=pydantic_schema_to_list(
                    AllBridgeExplorerTokenInfo.model_json_schema()
                ),
            )

    else:
        print(f"Request failed with status code: {response.status_code}")


def main():
    all_bridge_explorer_transfers_url = (
        "https://explorer-variant-filter.api.allbridgecoreapi.net/transfers"
    )
    all_bridge_explorer_token_info_url = (
        "https://core.api.allbridgecoreapi.net/token-info"
    )

    get_all_bridge_explorer_transfers(
        all_bridge_explorer_transfers_url,
    )
    get_all_bridge_explorer_token_info(
        all_bridge_explorer_token_info_url,
    )


if __name__ == "__main__":
    main()
