from datetime import datetime
import dlt
import json
import logging
import time
from requests.exceptions import RequestException
from typing import Sequence
import requests
from dlt.extract.source import DltResource
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from .models.symbiosis_bridge_explorer import SymbiosisBridgeExplorerTransaction
from src.integrations.utilities import get_raw_from_bq

# LOGIC: response gives out last: false loop untill true
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Utility
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


@dlt.resource(
    table_name="source_symbiosis_bridge_explorer_transactions",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(SymbiosisBridgeExplorerTransaction),
)
def get_symbiosis_bridge_transactions(
    symbiosis_bridge_explorer_transactions_url: str = dlt.config.value,
    max_id: int = get_latest_id_from_bq_table(),
    max_retries: int = 3,
    base_delay: float = 10,
):
    """
    max_id: int = get_latest_id_from_bq_table()
        This is the max id from the bq table, the latest transaction id record in our database
    LOGIC:
        - response gives out last: false loop untill true
        - Loop using before parameter and yield each transactions list of 100
        - Loop until last is false
    """
    all_txs = []
    before = None  # for the first Pull
    logging.info(f"Getting transactions till id {max_id}")

    while True:
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    symbiosis_bridge_explorer_transactions_url,
                    params={"before": before},
                )
                response.raise_for_status()  # Raise an exception for non-200 status codes

                break  # Success, exit the retry loop
            except RequestException as e:
                if attempt == max_retries - 1:
                    logging.error(f"Max retries reached. Last error: {str(e)}")
                    raise  # Re-raise the last exception if all retries failed
                delay = base_delay * (2**attempt)  # Exponential backoff
                logging.warning(
                    f"Request failed. Retrying in {delay:.2f} seconds. Error: {str(e)}"
                )
                time.sleep(delay)

        # get min id from transactions response
        transactions = response.json()["records"]
        if transactions:
            for tx in transactions:
                normalized_tx = normalize_transaction_data(tx)
                all_txs.append(SymbiosisBridgeExplorerTransaction(**normalized_tx))

            # get min id from transactions response
            before = min(transactions, key=lambda x: x["id"])["id"]
            logging.info(f"Getting transactions before id {before}")

        # break the code when the max id is reached, break and store data in bq till max_id,
        # anything < max_id is already stored, discard anything < max_id
        if before == max_id:
            # discard anything < max_id
            logging.info(
                f"ALL records before {max_id} discarded and remaining records storing in bq"
            )
            all_txs = [tx for tx in all_txs if tx.get("id") >= max_id]
            break
    yield all_txs


# Sources
@dlt.source(max_table_nesting=0)
def symbiosis_bridge_explorer_pipeline() -> Sequence[DltResource]:
    return [get_symbiosis_bridge_transactions]


if __name__ == "__main__":

    logging.info("Running DLT symbiosis_bridge_explorer")

    p = dlt.pipeline(
        pipeline_name="symbiosis_bridge_explorer",
        destination="bigquery",
        dataset_name="raw",
    )
    p.run(symbiosis_bridge_explorer_pipeline(), loader_file_format="jsonl")
    logging.info("Finished DLT symbiosis_bridge_explorer")
