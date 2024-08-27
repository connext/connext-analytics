import asyncio
import aiohttp
import logging
import pandas_gbq as gbq
import pandas as pd
from requests.structures import CaseInsensitiveDict

url = "https://stats-api.dln.trade/api/Orders/filteredList"
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"
PROJECT_ID = "mainnet-bigq"
TABLE_ID = "mainnet-bigq.raw.source_de_bridge_explorer__transactions"
logging.basicConfig(level=logging.INFO)

necessary = [
    "creationTimestamp",
    "state",
    "externalCallState",
    "orderId.bytesValue",
    "orderId.stringValue",
    "giveOfferWithMetadata.chainId.bigIntegerValue",
    "giveOfferWithMetadata.tokenAddress.stringValue",
    "giveOfferWithMetadata.amount.bigIntegerValue",
    "giveOfferWithMetadata.finalAmount.bigIntegerValue",
    "giveOfferWithMetadata.metadata.decimals",
    "giveOfferWithMetadata.metadata.name",
    "giveOfferWithMetadata.metadata.symbol",
    "giveOfferWithMetadata.decimals",
    "giveOfferWithMetadata.name",
    "giveOfferWithMetadata.symbol",
    "takeOfferWithMetadata.chainId.bigIntegerValue",
    "takeOfferWithMetadata.amount.bigIntegerValue",
    "takeOfferWithMetadata.finalAmount.bigIntegerValue",
    "takeOfferWithMetadata.metadata.decimals",
    "takeOfferWithMetadata.metadata.name",
    "takeOfferWithMetadata.metadata.symbol",
    "takeOfferWithMetadata.decimals",
    "takeOfferWithMetadata.name",
    "takeOfferWithMetadata.symbol",
    "finalPercentFee.bigIntegerValue",
    "fixFee.bigIntegerValue",
    "fixFee.stringValue",
    "unlockAuthorityDst.stringValue",
    "preswapData.chainId.bigIntegerValue",
    "preswapData.inAmount.bigIntegerValue",
    "preswapData.tokenInMetadata.name",
    "preswapData.tokenInMetadata.symbol",
    "preswapData.outAmount.bigIntegerValue",
    "preswapData.tokenOutMetadata.name",
    "preswapData.tokenOutMetadata.symbol",
    "orderMetadata.creationProcessType",
    "orderMetadata.origin",
    "orderMetadata.operatingExpensesAmount",
    "orderMetadata.recommendedTakeAmount",
]


async def fetch_data(data):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as response:
            response_json = await response.json()
        return response_json.get("orders", [])


async def post_deexplorer_data_call(skip=0):
    try:
        data = {
            "giveChainIds": [],
            "takeChainIds": [],
            "orderStates": ["Fulfilled", "SentUnlock", "ClaimedUnlock"],
            "skip": skip,
            "take": 100,
        }
        orders = await fetch_data(data)
        deexplorer_data = pd.json_normalize(orders)
        deexplorer_data = deexplorer_data.filter(items=necessary)
        deexplorer_data["creationTimestamp"] = deexplorer_data[
            "creationTimestamp"
        ].astype(int)
        return deexplorer_data.drop_duplicates()
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return pd.DataFrame()


def get_skip_count_from_bq():

    query = "SELECT COUNT(1) AS max FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions` "
    skip = gbq.read_gbq(query, project_id=PROJECT_ID).iloc[0]["max"]
    return skip


async def de_bridge_explorer_pipeline(max_date_in_db):
    """max_date_in_db: the max date from database, once we get to that date we kill the loop"""

    final_df = pd.DataFrame()
    skip = 0

    while True:
        df_deexplorer = await post_deexplorer_data_call(skip=skip)
        if df_deexplorer.empty:
            await asyncio.sleep(60)
            skip += 100
            logging.info(
                "No more orders to fetch. Sleeping for 60 seconds before next call"
            )
            continue

        min_timestamp = df_deexplorer["creationTimestamp"].min()
        logging.info(
            f"Fetched orders: skip={skip}, shape={df_deexplorer.shape}, Min timestamp from pulled data: {min_timestamp}"
        )

        df_deexplorer.columns = [
            col.lower().replace(".", "_") for col in df_deexplorer.columns
        ]
        df_deexplorer = df_deexplorer.astype(str)
        final_df = pd.concat([final_df, df_deexplorer])

        if min_timestamp < int(max_date_in_db):
            logging.info(
                "New data pulled from debridge explorer reached max date in database. Exiting..."
            )

            gbq.to_gbq(
                final_df,
                TABLE_ID,
                project_id=PROJECT_ID,
                if_exists="append",
                chunksize=10000,
            )
            logging.info(
                f"Uploaded {len(final_df)} rows for time: {min_timestamp} to bigquery"
            )
            final_df = pd.DataFrame()

            break

        skip += 100


def get_max_date_from_db() -> int:
    df = gbq.read_gbq(
        """SELECT max(creationtimestamp) AS max FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions`"""
    )
    return df.iloc[0]["max"]


if __name__ == "__main__":
    MAX_DATE = get_max_date_from_db()
    asyncio.run(de_bridge_explorer_pipeline(max_date_in_db=MAX_DATE))
