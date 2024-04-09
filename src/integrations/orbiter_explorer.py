import pytz
import logging
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import pandas_gbq as gbq
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "mainnet-bigq"


# utils
def to_snake_case(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


# Function to send the request and normalize the nested data
def get_api(page_number, start_date, end_date):

    url = "https://api.orbiter.finance/explore/v3/yj6toqvwh1177e1sexfy0u1pxx5j8o47"
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"

    start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

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
    orbit.raise_for_status()
    if orbit.status_code == 201:
        return pd.json_normalize(orbit.json()["result"]["list"])
    else:
        raise ValueError(
            f"Error fetching data from Orbiter Explorer API: {orbit.status_code}"
        )


def get_orbiter_transactions():

    # Initialize the page number and the accumulated DataFrame
    all_orders_df = pd.DataFrame()

    start_date = datetime(2024, 1, 1, tzinfo=pytz.utc)
    end_date = (start_date + timedelta(days=1)).astimezone(pytz.utc)
    final_date = datetime.utcnow().replace(
        tzinfo=pytz.utc, hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    logging.info(f"Data pull from {start_date} - {final_date}")
    # Loop through each day
    while start_date <= final_date:

        # for every day, we need to fetch all pages, starting from page 0
        page_number = 1
        logging.info(f"Fetching data for {start_date} - {end_date}")

        while True:
            df = get_api(page_number, start_date, end_date)
            # Check if the data is empty
            if df.empty:
                logging.info(
                    f"No more data found for {start_date.strftime('%Y-%m-%d')} at page: {page_number}"
                )
                break

            # Use pd.concat to append the data to the accumulated DataFrame
            all_orders_df = pd.concat([all_orders_df, df], ignore_index=True)
            # Increment the page number for the next day
            page_number += 1

        start_date = (start_date + timedelta(days=1)).astimezone(pytz.utc)
        end_date = (start_date + timedelta(days=1)).astimezone(pytz.utc)

    # Save the accumulated DataFrame to a CSV file

    all_orders_df.columns = [to_snake_case(col) for col in all_orders_df.columns]
    # all_orders_df.to_csv("data/orbiter_all_transactions.csv", index=False)

    # Force string type for all columns
    all_orders_df = all_orders_df.astype(str)
    return all_orders_df


def orbiter_explorer_pipeline():

    df = get_orbiter_transactions()
    gbq.to_gbq(
        dataframe=df,
        project_id=PROJECT_ID,
        destination_table="raw.source_orbiter_explorer__transactions",
        if_exists="append",
        chunksize=10000,
        api_method="load_csv",
    )


if __name__ == "__main__":
    orbiter_explorer_pipeline()
    # TODO: adding state to the pipeline based on destination
