import logging
import pandas_gbq as gbq
import requests
import json
import pandas as pd
from requests.structures import CaseInsensitiveDict

url = "https://stats-api.dln.trade/api/Orders/filteredList"
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"
PROJECT_ID = "mainnet-bigq"

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


def post_deexplorer_data_call(skip=0):
    """
    Pyload:
        {
            "giveChainIds": [],
            "takeChainIds": [],
            "orderStates": [],
            "externalCallStates": [
                "Completed"
            ],
            "skip": 19100,
            "take": 100
        }

    _extended_summary_

    Args:
        skip (int, optional): _description_. Defaults to 0.

    Returns:
        _type_: _description_
    """
    data = {
        "giveChainIds": [],
        "takeChainIds": [],
        "orderStates": [],
        "externalCallStates": ["Completed"],
        "skip": skip,
        "take": 100,
    }
    deexplorer = requests.post(url, headers=headers, data=json.dumps(data))
    deexplorer = json.loads(deexplorer.text)
    if "orders" in deexplorer:
        actual = deexplorer["orders"]
    else:
        print(f"No 'orders' key found in the response for skip={skip}.")
        actual = []

    deexplorer_data = pd.json_normalize(actual)
    deexplorer_data = deexplorer_data.filter(items=necessary)
    return deexplorer_data


# convert the below into a function
def get_deexplorer_transactions(skip=0):

    all_orders_df = pd.DataFrame()

    # Initialize skip value
    skip = 0

    # Loop to fetch all orders
    while True:
        print(f"Fetching orders with skip={skip}...")
        df_deexplorer = post_deexplorer_data_call(skip=skip)
        if df_deexplorer.empty:
            print("No more orders to fetch.")
            break
        all_orders_df = pd.concat([all_orders_df, df_deexplorer], ignore_index=True)
        skip += 100

    # CLean before upload

    # convert the column names from camelCase to snake_case
    all_orders_df.columns = [
        col.lower().replace(".", "_") for col in all_orders_df.columns
    ]

    # Force string type for all columns
    all_orders_df = all_orders_df.astype(str)
    # all_orders_df.to_csv("data/de_bridge_explorer_all_orders.csv", index=False)
    return all_orders_df


def de_bridge_explorer_pipeline():

    df = get_deexplorer_transactions()
    gbq.to_gbq(
        dataframe=df,
        project_id=PROJECT_ID,
        destination_table="raw.source_de_bridge_explorer__transactions",
        if_exists="append",
        chunksize=10000,
        api_method="load_csv",
    )


if __name__ == "__main__":

    # TODO: Adding a schedule to run this pipeline
    # TODO: Uploading data that is new to the raw table
    # de_bridge_explorer_pipeline()
    print("Check TODO")
