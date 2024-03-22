import requests
import pandas as pd

# Base URL for the API
base_url = "https://explorer.meson.fi/api/v1/swap"

# Number of pages to fetch
total_pages = 99

# Size of each page
page_size = 20

# Initialize an empty DataFrame to store all records
all_records_df = pd.DataFrame()

# Loop through each page
for page in range(total_pages):
    # Construct the URL for the current page
    url = f"{base_url}?page={page}&size={page_size}"

    # Make the request to the API
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Extract the list of transactions
        transactions = (
            data["result"]["list"]
            if "result" in data and "list" in data["result"]
            else []
        )

        # Process each transaction
        for transaction in transactions:
            # Extract relevant fields from the transaction using a more direct approach
            id_ = transaction["_id"] if "_id" in transaction else None
            encoded = transaction["encoded"] if "encoded" in transaction else None
            event_name = (
                transaction["events"][0]["name"]
                if "events" in transaction and len(transaction["events"]) > 0
                else None
            )
            initiator = transaction["initiator"] if "initiator" in transaction else None
            from_address = (
                transaction["fromTo"][0]
                if "fromTo" in transaction and len(transaction["fromTo"]) > 0
                else None
            )
            to_address = (
                transaction["fromTo"][1]
                if "fromTo" in transaction and len(transaction["fromTo"]) > 1
                else None
            )
            srFee = transaction["srFee"] if "srFee" in transaction else None
            lpFee = transaction["lpFee"] if "lpFee" in transaction else None
            created = transaction["created"] if "created" in transaction else None

            # Create a DataFrame from the record and concatenate it with all_records_df
            record_df = pd.DataFrame(
                [
                    {
                        "id": id_,
                        "encoded": encoded,
                        "event_name": event_name,
                        "initiator": initiator,
                        "from_address": from_address,
                        "to_address": to_address,
                        "srFee": srFee,
                        "lpFee": lpFee,
                        "created": created,
                    }
                ]
            )
            all_records_df = pd.concat([all_records_df, record_df], ignore_index=True)
    else:
        print(f"Failed to fetch page {page}. Status code: {response.status_code}")

# Convert the 'created' column to a string format 'YYYY-MM-DD HH:MM:SS'
all_records_df["created"] = pd.to_datetime(all_records_df["created"]).dt.strftime(
    "%Y-%m-%d %H:%M:%S"
)
print(all_records_df)
# Save the DataFrame to a CSV file
all_records_df.to_csv("meson.csv", index=False)

print("Data fetched and saved to meson.csv")
