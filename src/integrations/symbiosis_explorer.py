import requests
import json
import pandas as pd
from requests.structures import CaseInsensitiveDict

url = "https://api-v2.symbiosis.finance/explorer/v1/transactions"
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"


def get_symbiosis_data(skip=0):
    data = {"skip": skip, "take": 100}
    symbiosis = requests.get(url, headers=headers, data=json.dumps(data))
    symbiosis = json.loads(symbiosis.text)
    if "records" in symbiosis:
        actual = symbiosis["records"]
    else:
        print(f"No 'records' key found in the response for skip={skip}.")
        actual = []

    # Initialize an empty DataFrame to store the flattened data
    symbiosis_data = pd.DataFrame()

    # Loop through each record
    for record in actual:
        # Initialize a dictionary to hold the top-level fields
        top_level_fields = {}

        # List of keys to extract from the record
        keys_to_extract = [
            "id",
            "from_client_id",
            "from_chain_id",
            "join_chain_id",
            "to_chain_id",
            "event_type",
            "type",
            "state",
            "created_at",
            "mined_at",
            "success_at",
            "duration",
            "amounts",
            "from_amount_usd",
            "to_amount_usd",
            "to_tx_id",
            "retry_active",
        ]

        # Try to extract each key, catch KeyError and provide a default value
        for key in keys_to_extract:
            try:
                # For datetime fields, convert the string to a datetime object
                if key in ["created_at", "mined_at", "success_at"]:
                    top_level_fields[key] = pd.to_datetime(record[key])
                else:
                    top_level_fields[key] = record[key]
            except KeyError:
                # Provide a default value for missing keys
                top_level_fields[key] = None

        # Handle nested structures for tokens, from_route, and to_route
        try:
            for token in record["tokens"]:
                top_level_fields.update(
                    {
                        "token_symbol": token["symbol"],
                        "token_name": token["name"],
                        "token_decimals": token["decimals"],
                    }
                )
        except KeyError:
            top_level_fields.update(
                {"token_symbol": None, "token_name": None, "token_decimals": None}
            )

        try:
            for route in record["from_route"]:
                top_level_fields.update(
                    {
                        "from_route_chain_id": route["chain_id"],
                        "from_route_amount": route["amount"],
                        "from_route_token_symbol": route["token"]["symbol"],
                        "from_route_token_name": route["token"]["name"],
                        "from_route_token_decimals": route["token"]["decimals"],
                    }
                )
        except KeyError:
            top_level_fields.update(
                {
                    "from_route_chain_id": None,
                    "from_route_amount": None,
                    "from_route_token_symbol": None,
                    "from_route_token_name": None,
                    "from_route_token_decimals": None,
                }
            )

        try:
            for route in record["to_route"]:
                top_level_fields.update(
                    {
                        "to_route_chain_id": route["chain_id"],
                        "to_route_amount": route["amount"],
                        "to_route_token_symbol": route["token"]["symbol"],
                        "to_route_token_name": route["token"]["name"],
                        "to_route_token_decimals": route["token"]["decimals"],
                    }
                )
        except KeyError:
            top_level_fields.update(
                {
                    "to_route_chain_id": None,
                    "to_route_amount": None,
                    "to_route_token_symbol": None,
                    "to_route_token_name": None,
                    "to_route_token_decimals": None,
                }
            )

        # Append the record to the DataFrame
        symbiosis_data = pd.concat(
            [symbiosis_data, pd.DataFrame([top_level_fields])], ignore_index=True
        )

    # Convert the 'created_at', 'mined_at', and 'success_at' columns to a string format that Excel can recognize as a date and time
    symbiosis_data["created_at"] = symbiosis_data["created_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    symbiosis_data["mined_at"] = symbiosis_data["mined_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    symbiosis_data["success_at"] = symbiosis_data["success_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return symbiosis_data


# Initialize an empty DataFrame to store all records
all_records_df = pd.DataFrame()

# Initialize skip value
skip = 0

# Total number of transactions for March 2024
total_transactions_march_2024 = 83940

# Loop to fetch all records
while True:
    print(f"Fetching records with skip={skip}...")
    df_symbiosis = get_symbiosis_data(skip=skip)
    if df_symbiosis.empty:
        print("No more records to fetch.")
        break
    all_records_df = pd.concat([all_records_df, df_symbiosis], ignore_index=True)
    skip += 100

    # Check if the total number of fetched transactions has reached the expected total for March 2024
    if len(all_records_df) >= total_transactions_march_2024:
        print(
            f"Reached the total number of transactions for March 2024: {total_transactions_march_2024}"
        )
        break

print("All records fetched for March 2024.")
print(all_records_df)

# Save the filtered DataFrame to a CSV file

# all_records_df.to_csv("march_2024.csv", index=False)
# print("symbiosis data for March 2024 saved to march_2024.csv")
