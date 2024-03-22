import requests
import pandas as pd

# Base URL for the API
base_url = "https://explorer-variant-filter.api.allbridgecoreapi.net/transfers"

# Number of pages to fetch - Adjust this as needed
total_pages = 99

# Size of each page
page_size = 20

# Initialize an empty DataFrame to store all records
all_records_df = pd.DataFrame()

# Loop through each page
for page in range(1, total_pages + 1):  # Adjusted to start from page 1
    # Construct the URL for the current page
    url = f"{base_url}?page={page}&limit={page_size}"

    # Make the request to the API
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Extract the list of transactions
        transactions = data["items"] if "items" in data else []

        # Process each transaction
        for transaction in transactions:
            # Extract relevant fields from the transaction
            record = {
                "id": transaction.get("id"),
                "status": transaction.get("status"),
                "timestamp": transaction.get("timestamp"),
                "fromChainSymbol": transaction.get("fromChainSymbol"),
                "toChainSymbol": transaction.get("toChainSymbol"),
                "fromAmount": transaction.get("fromAmount"),
                "stableFee": transaction.get("stableFee"),
                "fromTokenAddress": transaction.get("fromTokenAddress"),
                "toTokenAddress": transaction.get("toTokenAddress"),
                "fromAddress": transaction.get("fromAddress"),
                "toAddress": transaction.get("toAddress"),
                "messagingType": transaction.get("messagingType"),
                "partnerId": transaction.get("partnerId"),
                "fromGas": transaction.get("fromGas"),
                "toGas": transaction.get("toGas"),
                "relayerFeeInNative": transaction.get("relayerFeeInNative"),
                "relayerFeeInTokens": transaction.get("relayerFeeInTokens"),
                "sendTransactionHash": transaction.get("sendTransactionHash"),
                "receiveTransactionHash": transaction.get("receiveTransactionHash"),
            }

            # Create a DataFrame from the record and concatenate it with all_records_df
            record_df = pd.DataFrame([record])
            all_records_df = pd.concat([all_records_df, record_df], ignore_index=True)
    else:
        print(f"Failed to fetch page {page}. Status code: {response.status_code}")

# Optionally, convert the 'timestamp' column to a readable date format
all_records_df["timestamp"] = pd.to_datetime(all_records_df["timestamp"], unit="ms")

print(all_records_df)
# Save the DataFrame to a CSV file
all_records_df.to_csv("allbridge_transfers.csv", index=False)

print("Data fetched and saved to allbridge_transfers.csv")
