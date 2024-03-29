from datetime import datetime, timedelta
import dlt
import json
import pytz
import logging
from datetime import datetime
from typing import Iterator, Sequence
from dlt.sources.helpers import requests
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from .models.symbiosis_bridge_explorer import SymbiosisBridgeExplorerTransaction

# LOGIC: response gives out last: false loop untill true
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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

    if tx.get("tokens", []):

        token = tx["tokens"][0]
        token_symbol = token.get("symbol", "")
        token_name = token.get("name", "")
        token_address = token.get("address", "")
        token_decimals = token.get("decimals", 0)

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
        "token_symbol": token_symbol,
        "token_name": token_name,
        "token_address": token_address,
        "token_decimals": token_decimals,
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
    write_disposition="replace",
    columns=pydantic_to_table_schema_columns(SymbiosisBridgeExplorerTransaction),
)
def get_symbiosis_bridge_transactions(
    symbiosis_bridge_explorer_transactions_url: str = dlt.config.value,
):
    """
    LOGIC:
        - response gives out last: false loop untill true
        - Loop using before parameter and yield each transactions list of 100
        - Loop until last is false
    """
    # Initialzie the API parameters
    before = None
    all_txs = []

    while True:
        logging.info(f"Getting transactions before {before}")
        response = requests.get(
            symbiosis_bridge_explorer_transactions_url, params={"before": before}
        ).json()

        transactions = response["records"]
        last = response["last"]

        if transactions:
            for tx in transactions:
                normalized_tx = normalize_transaction_data(tx)
                all_txs.append(SymbiosisBridgeExplorerTransaction(**normalized_tx))

            # get min id from transactions response
            before = min(transactions, key=lambda x: x["id"])["id"]

        if last:
            break
        # if len(all_txs) > 1000:
        #     logging.info("Yielding 1000 transactions")
        #     break

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
