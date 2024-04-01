import pytz
from datetime import datetime
import logging
import dlt
from dlt.sources.helpers import requests
from dlt.extract.source import DltResource
from typing import Iterator, Sequence
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from src.integrations.models.all_bridge_explorer import (
    AllBridgeExplorerTransfer,
    AllBridgeExplorerTokenInfo,
)


# Base URL for the API
# base_url = "https://explorer-variant-filter.api.allbridgecoreapi.net/transfers"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@dlt.resource(
    table_name="source_all_bridge_explorer_transfers",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(AllBridgeExplorerTransfer),
)
def get_all_bridge_explorer_transfers(
    all_bridge_explorer_transfers_url=dlt.config.value,
) -> Iterator[AllBridgeExplorerTransfer]:

    page = 1
    page_size = 20
    page_remains = 1
    status = "Complete"

    page_txs = []

    while page_remains > 0:

        logging.info(f"Fetching page: {page}, page remain: {page_remains}")
        url = f"{all_bridge_explorer_transfers_url}?status={status}&page={page}&limit={page_size}"

        response = requests.get(url)
        response.raise_for_status()  # This will raise an HTTPError for bad responses

        logging.info(f"url: {url}, status code: {response.status_code}")
        data = response.json()
        transactions = data["items"] if "items" in data else []
        meta = data["meta"]

        for transaction in transactions:
            record = {
                "id": transaction.get("id"),
                "status": transaction.get("status"),
                "timestamp": transaction.get("timestamp"),
                "from_chain_symbol": transaction.get("fromChainSymbol"),
                "to_chain_symbol": transaction.get("toChainSymbol"),
                "from_amount": transaction.get("fromAmount"),
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

            page_txs.append(AllBridgeExplorerTransfer(**record))

        page_remains = meta["totalPages"] - page
        page += 1
        if page > 10000:
            break

    yield page_txs


def to_snake_case(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def convert_pool_info(token):
    pool_info = token.pop("poolInfo", {})
    for key, value in pool_info.items():
        snake_case_key = "pool_info_" + to_snake_case(key)
        token[snake_case_key] = value
    return token


@dlt.resource(
    table_name="source_all_bridge_explorer_tokens",
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(AllBridgeExplorerTokenInfo),
)
def get_all_bridge_explorer_token_info(
    all_bridge_explorer_token_info_url=dlt.config.value,
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
                simplified_data.append(AllBridgeExplorerTokenInfo(**token))

        yield simplified_data
    else:
        print(f"Request failed with status code: {response.status_code}")


# Sources
@dlt.source(
    max_table_nesting=0,
)
def all_bridge_explorer_transfers() -> Sequence[DltResource]:
    return [get_all_bridge_explorer_transfers, get_all_bridge_explorer_token_info]


if __name__ == "__main__":

    logging.info("Running DLT All Bridge Explorer Transfers")
    p = dlt.pipeline(
        pipeline_name="all_bridge_explorer_transfers",
        destination="bigquery",
        dataset_name="raw",
    )
    p.run(all_bridge_explorer_transfers(), loader_file_format="jsonl")
    logging.info("Finished DLT All Bridge Explorer Transfers!")
