import json
import asyncio
import httpx
import logging
from src.integrations.utilities import (
    get_raw_from_bq,
    upload_to_gcs_via_folder,
)
import pandas_gbq
import pandas as pd

PROJECT_ID = "mainnet-bigq"
ARB_DEPOSITS_GCS_BUCKET_NAME = "arb-weth-deposits"
GCS_FOLDER_NAME = "metis"
TRANSACTION_URL_TEMPLATE = "https://andromeda-explorer.metis.io/api/v2/transactions/{}"


logging.basicConfig(level=logging.INFO)


def convert_json_to_df_and_upload_to_bq(json_list: list) -> None:

    final = []
    for d in json_list:
        final.append(
            {
                "timestamp": d["timestamp"],
                "hash": d["hash"],
                "from_address": d["from"]["hash"],
            }
        )
    df = pd.DataFrame(final)
    # upload to bq
    pandas_gbq.to_gbq(
        dataframe=df,
        project_id=PROJECT_ID,
        destination_table="stage.source_metis_weth_arb_chain_deposits__transactions",
        if_exists="append",
        chunksize=100000,
        api_method="load_csv",
    )
    logging.info(f"Metis WETH Arb chain Deposits, {df.shape} rows Added!")


async def fetch_transaction_data(
    transaction_id: list, retries: int = 5, backoff_factor: int = 2, timeout: int = 30
):
    """
    Fetches transaction data from the Metis network explorer API for a given transaction ID.
    Retries the request with exponential backoff in case of failures.

    curl eg:
        curl -X 'GET' \
        'https://andromeda-explorer.metis.io/api/v2/transactions/0xaa148281d42070e53674336582d77a05a8fef1c33d70f33b97d5cb438c6f8ef4' \
        -H 'accept: application/json'

    Args:
        transaction_id (list): _description_
        retries (int, optional): _description_. Defaults to 5.
        backoff_factor (int, optional): _description_. Defaults to 2.
        timeout (int, optional): _description_. Defaults to 30.

    Returns:
        None: The function uploads the data to GCS
    """

    url = TRANSACTION_URL_TEMPLATE.format(transaction_id)
    async with httpx.AsyncClient() as client:
        for attempt in range(retries):
            try:
                response = await client.get(url, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logging.error(f"An error occurred: {e}")
                if attempt < retries - 1:
                    sleep_time = backoff_factor * (2**attempt)
                    logging.info(f"Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    raise


async def main_fetch(parallel_fetch=10):
    all_data = []

    # get txs as list
    df = get_raw_from_bq(sql_file_name="transfers_list_origin_metis_arb_deposits")
    if df.empty:
        logging.info(
            """No new transactions found to pull data! Data is upto date in
            stage.source_metis_weth_arb_chain_deposits__transactions"""
        )
        return None
    transaction_ids = df["source_chain_hash"].tolist()
    logging.info(f"Fetched {len(transaction_ids)} transactions")

    for i in range(0, len(transaction_ids), parallel_fetch):
        batch = transaction_ids[i : i + parallel_fetch]
        tasks = [fetch_transaction_data(tx_id) for tx_id in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch data: {result}")
            else:
                all_data.append(result)

        logging.info(f"Fetched {len(all_data)} transactions so far")
        await asyncio.sleep(10)

    if all_data:

        # backup data
        upload_to_gcs_via_folder(
            data=all_data,
            bucket_name=ARB_DEPOSITS_GCS_BUCKET_NAME,
            folder_name=GCS_FOLDER_NAME,
        )

        # create a dataframe and upload to bq
        convert_json_to_df_and_upload_to_bq(json_list=all_data)
