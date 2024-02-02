import os
import pandas as pd
import json
from pprint import pprint
import pandas_gbq
from datetime import datetime
from src.integrations.lifi import PROJECT_ID
from google.cloud import storage


def get_data_from_cs_bucket(
    bucket_name="lifi_routes", greater_than_date=datetime(2024, 1, 1, 1, 1, 1)
):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    file_date = []
    for path in blobs:
        name = os.path.splitext(path.name)[0]
        dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")
        file_date.append(dt)
    print(f"before: {len(file_date)}")
    filtered_dates_file = [
        date.strftime("%Y-%m-%d_%H-%M-%S") + ".json"
        for date in file_date
        if date > greater_than_date
    ]
    print(f"after: {len(filtered_dates_file)}")

    # Iterate over each filtered_dates_file
    all_data = []
    blobs = bucket.list_blobs()
    for blob in blobs:
        data = json.loads(blob.download_as_text())
        print(f"data: {len(data)}")
        all_data.extend(data)

    with open("data/sample_route_data.json", "w") as f:
        json.dump(all_data, f, indent=4)
    print(f"all_data: {len(all_data)}")
    return all_data


def convert_json_to_df(json_file):
    # Initialize an empty DataFrame to store the normalized data
    normalized_data_df = pd.DataFrame()

    # Loop through each route in the JSON file
    for r in json_file:
        routes = r["routes"]
        for route in routes:
            # Normalize the steps for the current route
            steps_df = pd.json_normalize(route, record_path="steps")
            # pprint(f"steps_df: {steps_df.columns}")

            # Initialize empty lists to store fees and gas costs data
            fees_data = []
            gas_costs_data = []

            # Loop through each step to extract and normalize fees and gas costs
            for step in route["steps"]:
                # Normalize fees and append to the fees_data list
                if step["estimate"]["feeCosts"]:
                    fees = pd.json_normalize(step["estimate"]["feeCosts"])
                    fees_data.append(fees)

                # Normalize gas costs and append to the gas_costs_data list
                if step["estimate"]["gasCosts"]:
                    gas_costs = pd.json_normalize(step["estimate"]["gasCosts"])
                    gas_costs_data.append(gas_costs)

            if fees_data:
                fees_df = pd.concat(fees_data, ignore_index=True)
            else:
                fees_df = pd.DataFrame()

            # Add a prefix to each fee column name to prevent conflicts
            fees_df.columns = ["fee_" + col for col in fees_df.columns]
            # pprint(f"fees_df: {fees_df.columns}")

            if gas_costs_data:
                gas_costs_df = pd.concat(gas_costs_data, ignore_index=True)
            else:
                gas_costs_df = pd.DataFrame()
            gas_costs_df.columns = ["gas_" + col for col in gas_costs_df.columns]
            # pprint(f"gas_costs_df: {gas_costs_df.columns}")

            # Extract the route metadata (excluding the 'steps')
            metadata = {k: v for k, v in route.items() if k != "steps"}

            # Normalize the metadata
            metadata_df = pd.json_normalize(metadata)
            # pprint(f"metadata_df: {metadata_df.columns}")

            # Add a prefix to each metadata column name to prevent conflicts
            metadata_df.columns = ["route_" + col for col in metadata_df.columns]

            # Repeat the metadata for each step in the current route
            repeated_metadata_df = pd.concat(
                [metadata_df] * len(steps_df), ignore_index=True
            )
            # pprint(f"repeated_metadata_df: {repeated_metadata_df.columns}")

            # Concatenate the steps DataFrame with the repeated metadata DataFrame
            enriched_steps_df = pd.concat([steps_df, repeated_metadata_df], axis=1)
            # pprint(f"enriched_steps_df: {enriched_steps_df.columns}")

            # Concatenate the fees and gas costs DataFrames with the enriched steps DataFrame
            enriched_df = pd.concat([enriched_steps_df, fees_df, gas_costs_df], axis=1)
            # pprint(f"will all enriched_df: {enriched_df.columns}")

            # Concatenate the enriched DataFrame with the normalized_data_df

            try:
                normalized_data_df = pd.concat(
                    [normalized_data_df, enriched_df], ignore_index=True
                )
                # print(f"{normalized_data_df.shape} df size")
            except pd.errors.InvalidIndexError:
                # print("Error occurred during concatenation.")

                if len(enriched_df.columns) == len(enriched_df.columns.unique()):
                    print("All column names are unique.")
                else:
                    print("There are duplicate column names.")
                    # Assume df is your DataFrame
                    duplicate_columns = enriched_df.columns[
                        enriched_df.columns.duplicated()
                    ]

                    print("Duplicate column names: ", duplicate_columns.tolist())
                    print(f"original cols: {enriched_df.columns}")

    return normalized_data_df.dropna()


if __name__ == "__main__":
    pprint(get_data_from_cs_bucket(bucket_name="lifi_routes"))
    # get_data_from_cs_bucket()

    # with open("data/sample_route_data.json") as json_file:
    #     sample_route_data = json.load(json_file)
    # print(len(sample_route_data))
    # df = convert_json_to_df(sample_route_data)
    # df.columns = df.columns.str.lower()

    # df.to_csv("data/sample_route_data.csv", index=False)

    # pandas_gbq.to_gbq(
    #     dataframe=df,
    #     project_id=PROJECT_ID,
    #     destination_table="stage.source_lifi__routes",
    #     if_exists="replace",
    #     chunksize=100000,
    # )
