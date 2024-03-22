import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import json
from datetime import datetime, timedelta


# Function to send the request and normalize the nested data
def get_api(page_number):
    url = "https://api.orbiter.finance/explore/v3/yj6toqvwh1177e1sexfy0u1pxx5j8o47"
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    # Calculate the date range
    start_date = datetime(2024, 3, 1)
    end_date = datetime.now() - timedelta(days=1)  # Yesterday
    # Format the dates
    start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    # Prepare the data with the current page number
    data = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "orbiter_txList",
        "params": [
            0,
            50,
            page_number,
            "",
            [start_date_str, end_date_str],
            "",
            "",
            "",
            "",
            "2",
        ],
    }
    # Send the request
    orbit = requests.post(url, headers=headers, json=data)
    orbit = json.loads(orbit.text)
    # Normalize the nested data
    orbit_data = pd.json_normalize(orbit["result"]["list"])
    return orbit_data


# Initialize the page number and the accumulated DataFrame
page_number = 1
accumulated_data = pd.DataFrame()

# Loop through each day
current_date = datetime(2024, 3, 1)
while current_date <= datetime.now() - timedelta(days=1):
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")
    df = get_api(page_number)
    # Check if the data is empty
    if df.empty:
        break
    # Use pd.concat to append the data to the accumulated DataFrame
    accumulated_data = pd.concat([accumulated_data, df], ignore_index=True)
    # Increment the page number for the next day
    page_number += 1
    # Move to the next day
    current_date += timedelta(days=1)

# Print the accumulated DataFrame
print("Accumulated Data:")
print(accumulated_data)

# Save the accumulated DataFrame to a CSV file
# accumulated_data.to_csv("orbiter.csv", index=False)
# print("Data saved to 'orbiter.csv'.")
