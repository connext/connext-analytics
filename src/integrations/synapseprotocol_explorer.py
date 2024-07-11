import json
import logging
import pandas as pd
import pandas_gbq as gbq
import dlt
import re
from dlt.extract.source import DltResource
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from dlt.common.typing import TDataItems
from dlt.sources.helpers import requests
from typing import Sequence
from urllib.parse import quote
from datetime import datetime, timedelta, timezone, date, time
from src.integrations.models.synapseprotocol_explorer import (
    TransactionInfo,
    FlattenedTransactionInfo,
)
from src.integrations.utilities import get_latest_value_from_bq_table_by_col

PROJECT_ID = "mainnet-bigq"
logging.basicConfig(level=logging.INFO)


def to_snake_case(s):
    # First, insert underscores between lowercase letters and uppercase letters
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    # Then, change any uppercase letters (with optional following numbers) that are preceded by letters or numbers to lowercase, prefixed with an underscore
    s = re.sub(r"([a-z0-9])([A-Z0-9])", r"\1_\2", s).lower()
    return s


def send_graphql_get_request(
    query, variables, endpoint="https://explorer.omnirpc.io/graphql"
):
    """
    Sends a GET request to a GraphQL endpoint with the provided query and variables.

    :param query: The GraphQL query string.
    :param variables: A dictionary of variables to be included in the query.
    :param endpoint: The URL of the GraphQL endpoint. Defaults to 'https://explorer.omnirpc.io/graphql'.
    :return: The response from the GraphQL server.
    """
    # Convert the query and variables to URL-encoded strings
    query_encoded = quote(query)
    variables_encoded = quote(json.dumps(variables))

    # Construct the URL with the query and variables
    url = f"{endpoint}?query={query_encoded}&variables={variables_encoded}"

    # Send the GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Request failed with status code {response.status_code}")


@dlt.resource(
    table_name="source_synapseprotocol_explorer_transactions",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(FlattenedTransactionInfo),
)
def get_synapse_data(
    start_date,
    end_date=datetime.combine(
        (datetime.now(timezone.utc) - timedelta(days=1)).date(), time.min, timezone.utc
    ),
):

    # Example usage
    query = """
    query GetBridgeTransactionsQuery($chainIDFrom: [Int], $chainIDTo: [Int], $addressFrom: String, $addressTo: String, $maxAmount: Int, $minAmount: Int, $maxAmountUsd: Int, $minAmountUsd: Int, $startTime: Int, $endTime: Int, $txnHash: String, $kappa: String, $pending: Boolean, $page: Int, $tokenAddressFrom: [String], $tokenAddressTo: [String], $useMv: Boolean) {
    bridgeTransactions(
        chainIDFrom: $chainIDFrom
        chainIDTo: $chainIDTo
        addressFrom: $addressFrom
        addressTo: $addressTo
        maxAmount: $maxAmount
        minAmount: $minAmount
        maxAmountUsd: $maxAmountUsd
        minAmountUsd: $minAmountUsd
        startTime: $startTime
        endTime: $endTime
        txnHash: $txnHash
        kappa: $kappa
        pending: $pending
        page: $page
        useMv: $useMv
        tokenAddressFrom: $tokenAddressFrom
        tokenAddressTo: $tokenAddressTo
    ) {
        ...TransactionInfo
        __typename
    }
    }

    fragment TransactionInfo on BridgeTransaction {
    fromInfo {
        ...SingleSideInfo
        __typename
    }
    toInfo {
        ...SingleSideInfo
        __typename
    }
    kappa
    pending
    swapSuccess
    __typename
    }

    fragment SingleSideInfo on PartialInfo {
    chainID
    destinationChainID
    address
    hash: txnHash
    value
    formattedValue
    tokenAddress
    tokenSymbol
    time
    eventType
    __typename
    }
    """

    # Calculate the number of days to loop through
    num_days = (end_date - start_date).days
    all_txs = []

    for day in range(num_days):

        current_day_start = start_date + timedelta(days=day)
        current_day_end = current_day_start + timedelta(days=1)
        logging.info(
            f"Getting data for date range {current_day_start} to {current_day_end}"
        )

        # Convert to timestamps
        start_time = int(current_day_start.timestamp())
        end_time = int(current_day_end.timestamp())

        page = 1

        while True:
            logging.info(f"Fetching data for page {page}")

            variables = {
                "pending": False,
                "page": page,
                "useMv": True,
                "startTime": start_time,
                "endTime": end_time,
            }
            # Assuming you have fetched the response as before
            try:
                response = send_graphql_get_request(query, variables)
                transactions = response["data"]["bridgeTransactions"]
                if transactions is not None:
                    for transaction_data in transactions:
                        # Validate the data using the Pydantic model
                        transaction = TransactionInfo(**transaction_data)
                        flat_transaction = transaction.to_flat_dict()

                        # apply to_snake_case to keys
                        flat_transaction = {
                            to_snake_case(key): value
                            for key, value in flat_transaction.items()
                        }

                        all_txs.append(FlattenedTransactionInfo(**flat_transaction))
                else:
                    logging.warning(
                        "No transactions found for the given query and variables."
                    )
                    break
            except requests.RequestException as e:
                if hasattr(e, "response") and e.response:
                    logging.error(
                        f"API request failed with status code {e.response.status_code} and message {e.response.text}"
                    )
                else:
                    logging.error(f"API request failed with error: {e}")
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")

            page += 1
            logging.info(f"Finished fetching page {page}, data so far: {len(all_txs)}")
    print(all_txs)
    yield all_txs


# Sources
@dlt.source(
    max_table_nesting=0,
)
def synapse_explorer_source() -> Sequence[DltResource]:

    start_date = get_latest_value_from_bq_table_by_col(
        "mainnet-bigq.raw.source_synapseprotocol_explorer_transactions",
        "from_time",
        base_val=datetime.combine(
            (datetime.now(timezone.utc) - timedelta(days=1)).date(),
            time.min,
            timezone.utc,
        ),
    )
    # convert to datetime
    start_date = datetime.fromtimestamp(start_date, tz=timezone.utc)
    # Convert to the start of the next day
    start_date = (start_date + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    return [get_synapse_data(start_date=start_date)]


if __name__ == "__main__":

    logging.info("Running DLT synapse_explorer")
    p = dlt.pipeline(
        pipeline_name="synapse_explorer",
        destination="bigquery",
        dataset_name="raw",
    )
    p.run(synapse_explorer_source(), loader_file_format="jsonl")
    logging.info("Finished DLT synapse_explorer")
