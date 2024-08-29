from datetime import datetime
import json
import logging
import time
import requests
import pandas as pd
import pandas_gbq as gbq
from typing import List
from requests.exceptions import Timeout
from .models.symbiosis_bridge_explorer import SymbiosisBridgeExplorerTransaction
from src.integrations.utilities import get_raw_from_bq, pydantic_schema_to_list

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

TABLE_NAME_EXPLORER_TRANSFERS = "raw.source_symbiosis_bridge_explorer_transactions"


def get_latest_id_from_bq_table() -> int:
    """
    Get the latest id from a BigQuery table
    """
    id = get_raw_from_bq("get_max_id_source_symbiosis_bridge_explorer_transactions")[
        "id"
    ][0]
    return int(id)


def normalize_transaction_data(tx: dict) -> dict:
    # Helper function to parse and format dates
    def parse_date(date_str):
        if date_str:
            try:
                # First, try parsing with fractional seconds
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                # If there's a ValueError, try parsing without fractional seconds
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
        return None

    def normalize_token_data(tx: dict) -> dict:
        """
        Extracts and normalizes token data from a transaction.
        """
        if tx.get("tokens", []):
            try:
                token = tx["tokens"][0]
                return {
                    "token_symbol": token.get("symbol"),
                    "token_name": token.get("name"),
                    "token_address": token.get("address"),
                    "token_decimals": token.get("decimals"),
                }
            except (KeyError, IndexError):
                pass
        # Return defaults if tokens are not present or in case of an error
        return {
            "token_symbol": None,
            "token_name": None,
            "token_address": None,
            "token_decimals": None,
        }

    token_data = normalize_token_data(tx)

    normalized_data = {
        "id": tx.get("id"),
        "from_client_id": tx.get("from_client_id"),
        "from_chain_id": tx.get("from_chain_id"),
        "from_tx_hash": tx.get("from_tx_hash"),
        "join_chain_id": tx.get("join_chain_id"),
        "join_tx_hash": tx.get("join_tx_hash"),
        "to_chain_id": tx.get("to_chain_id"),
        "to_tx_hash": tx.get("to_tx_hash"),
        "event_type": tx.get("event_type"),
        "type": tx.get("type"),
        "hash": tx.get("hash"),
        "state": tx.get("state"),
        "created_at": parse_date(tx.get("created_at")),
        "mined_at": parse_date(tx.get("mined_at")),
        "success_at": parse_date(tx.get("success_at")),
        "from_address": tx.get("from_address"),
        "from_sender": tx.get("from_sender"),
        "duration": tx.get("duration"),
        "to_address": tx.get("to_address"),
        "to_sender": tx.get("to_sender"),
        "amounts": json.dumps(tx.get("amounts", [])),
        "tokens": json.dumps(tx.get("tokens", [])),
        **token_data,
        "from_route": json.dumps(tx.get("from_route", [])),
        "to_route": json.dumps(tx.get("to_route", [])),
        "transit_token": json.dumps(tx.get("transit_token", {})),
        "from_amount_usd": tx.get("from_amount_usd"),
        "to_amount_usd": tx.get("to_amount_usd"),
        "to_tx_id": tx.get("to_tx_id"),
        "retry_active": tx.get("retry_active"),
    }
    return normalized_data


def pull_data_from_api(
    url: str,
    params: dict,
    max_retries: int = 50,
    base_timeout: int = 30,
    max_timeout: int = 120,
):
    for attempt in range(max_retries):
        try:
            timeout = min(base_timeout * (2**attempt), max_timeout)
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()

            json_data = response.json()
            if "records" not in json_data:
                raise ValueError("Unexpected response structure")

            return json_data["records"]
        except (requests.RequestException, Timeout, ValueError) as e:
            if attempt == max_retries - 1:
                logging.error(f"Max retries reached. Last error: {str(e)}")
                raise
            delay = base_timeout * (2**attempt)
            logging.warning(
                f"Request failed. Retrying in {delay:.2f} seconds. Error: {str(e)}"
            )
            time.sleep(delay)


def clean_api_response(
    transactions: List[dict],
) -> List[SymbiosisBridgeExplorerTransaction]:
    cleaned_txs = []
    for tx in transactions:
        normalized_tx = normalize_transaction_data(tx)
        cleaned_txs.append(SymbiosisBridgeExplorerTransaction(**normalized_tx))
    return cleaned_txs


def get_symbiosis_bridge_transactions(
    symbiosis_bridge_explorer_transactions_url: str = "https://api-v2.symbiosis.finance/explorer/v1/transactions",
    max_id: int = get_latest_id_from_bq_table(),
    batch_size: int = 100000,
    before=None,  # id before which to fetch transactions, change this to get data before a certain id
):
    all_txs = []
    logging.info(f"Getting transactions till id {max_id}")

    while True:
        transactions = pull_data_from_api(
            symbiosis_bridge_explorer_transactions_url, params={"before": before}
        )

        if transactions:
            cleaned_txs = clean_api_response(transactions)
            all_txs.extend(cleaned_txs)

            before = min(transactions, key=lambda x: x["id"])["id"]
            logging.info(f"Getting transactions before id {before}")

            # Push data to BigQuery every 1000 API calls
            if len(all_txs) >= batch_size:
                push_to_bigquery(all_txs, TABLE_NAME_EXPLORER_TRANSFERS)
                all_txs = []  # Clear the list after pushing to BigQuery
        if before <= max_id:
            logging.info(
                f"ALL records before {max_id} discarded and remaining records storing in bq"
            )
            all_txs = [tx for tx in all_txs if tx.id >= max_id]

            # Push any remaining transactions to BigQuery
            if all_txs:
                push_to_bigquery(all_txs, TABLE_NAME_EXPLORER_TRANSFERS)

            break

    logging.info("Pipeline completed.")
    return all_txs


def push_to_bigquery(
    transactions: List[SymbiosisBridgeExplorerTransaction], table_name: str
):
    df = pd.DataFrame([tx.model_dump() for tx in transactions])

    gbq.to_gbq(
        dataframe=df,
        project_id="mainnet-bigq",
        destination_table=table_name,
        if_exists="append",
        api_method="load_csv",
        table_schema=pydantic_schema_to_list(
            SymbiosisBridgeExplorerTransaction.model_json_schema()
        ),
    )

    logging.info(f"Pushed {len(df)} transactions to BigQuery table: {table_name}")


def main():
    get_symbiosis_bridge_transactions()


if __name__ == "__main__":
    main()
