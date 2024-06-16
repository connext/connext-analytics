import asyncio
import httpx
import logging
import pandas as pd
import pandas_gbq as gbq
from src.integrations.models.router_protocol import GraphQLResponseRouterProtocol

PROJECT_ID = "mainnet-bigq"
url = "https://api.pro-nitro-explorer.routernitro.com/graphql"

logging.basicConfig(level=logging.INFO)

query = """
query TransactionsList($where: NitroTransactionFilter, $sort: NitroTransactionSort, $limit: Int, $page: Int) {
  findNitroTransactionsByFilter(where: $where, sort: $sort, limit: $limit, page: $page) {
    limit
    page
    total
    data {
      src_timestamp
      dest_timestamp
      src_chain_id
      dest_chain_id
      src_tx_hash
      dest_tx_hash
      status
      src_address
      dest_address
      src_amount
      dest_amount
      dest_stable_amount
      src_symbol
      dest_symbol
      dest_stable_symbol
      has_message
      native_token_amount
    }
  }
}
"""

variables = {
    "where": {
        "src_chain_id": None,
        "dest_chain_id": None,
        "transaction_type": None,
    },
    "limit": 30,
    "page": 1,
}


async def fetch_data(page, retries=500, backoff_factor=2, timeout=30):
    variables["page"] = page
    async with httpx.AsyncClient() as client:
        for attempt in range(retries):
            try:
                response = await client.post(
                    url,
                    json={"query": query, "variables": variables},
                    timeout=timeout,
                )
                response.raise_for_status()
                response_data = response.json()
                return GraphQLResponseRouterProtocol(**response_data)

            except (httpx.RequestError, httpx.TimeoutException) as e:
                logging.error(f"An error occurred: {e}")
                if attempt < retries - 1:
                    sleep_time = backoff_factor * (2**attempt)
                    logging.info(f"Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    raise


def push_data_to_gbq(data):
    df = pd.DataFrame(data)
    logging.info(f"Pushing {len(df)} transactions to GBQ")
    gbq.to_gbq(
        dataframe=df,
        project_id=PROJECT_ID,
        destination_table="raw.source_router_protocol__transactions",
        if_exists="append",
        chunksize=10000,
        api_method="load_csv",
    )


async def main(parallel_fetch=10):
    all_data = []
    page = 1
    total_pages = None

    while True:
        try:
            tasks = [fetch_data(page + i) for i in range(parallel_fetch)]
            results = await asyncio.gather(*tasks)
            for data in results:
                transactions = data.data.findNitroTransactionsByFilter
                all_data_dicts = [tx.model_dump() for tx in transactions.data]
                all_data.extend(all_data_dicts)
                total_txs = transactions.total
                total_pages = (
                    total_txs + transactions.limit - 1
                ) // transactions.limit  # Calculate total pages

            if page > total_pages:
                logging.info(f"Pushing {len(all_data)} transactions to GBQ, All done!")
                push_data_to_gbq(all_data)
                break

            # Log the current page number
            logging.info(f"Current page: {page}")

            # push after every 10 pages
            if (page - 1) % 10 == 0:
                logging.info(
                    f"Pushing {len(all_data)} transactions to GBQ, page {page}"
                )
                push_data_to_gbq(all_data)
                all_data = []
                logging.info(f"Resetting all data to len: {len(all_data)}")

            page += parallel_fetch
            await asyncio.sleep(10)
            logging.info(f"Fetched page {page} of {total_pages}")

        except httpx.RequestError as e:
            logging.error(f"An error occurred: {e}")
            await asyncio.sleep(10)

    return None


if __name__ == "__main__":
    asyncio.run(main(parallel_fetch=10))
