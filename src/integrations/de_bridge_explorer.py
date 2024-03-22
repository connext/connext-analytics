import requests
import json
import pandas as pd
from requests.structures import CaseInsensitiveDict

url = "https://stats-api.dln.trade/api/Orders/filteredList"
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"

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


def get_deexplorer_data(skip=0):
    data = {
        "giveChainIds": [],
        "takeChainIds": [],
        "orderStates": ["Fulfilled", "SentUnlock", "ClaimedUnlock"],
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


# Initialize an empty DataFrame to store all orders
all_orders_df = pd.DataFrame()

# Initialize skip value
skip = 0

# Loop to fetch all orders
while True:
    print(f"Fetching orders with skip={skip}...")
    df_deexplorer = get_deexplorer_data(skip=skip)
    if df_deexplorer.empty:
        print("No more orders to fetch.")
        break
    all_orders_df = pd.concat([all_orders_df, df_deexplorer], ignore_index=True)
    skip += 100

print("All orders fetched.")
# print(all_orders_df)


# Save the filtered DataFrame to a CSV file
all_orders_df.to_csv("de_exp.csv", index=False)
print("deexplorer data saved to de_exp.csv")
