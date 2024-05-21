import os
import pandas as pd
import pandas_gbq


PROJECT_ID = "mainnet-bigq"


# upload 5 csvs to a table in a loop and append the data
# convert date col to string before upload

# List of CSV file paths
# link to folders with csv
# TOKENS
folder_link = "/Users/jaynaik/Desktop/cannonical_bridges/tokens"
csv_files = [f"{folder_link}/{file}" for file in os.listdir(folder_link)]

# Read and concatenate CSV files
df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
df["date"] = df["date"].astype(str)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table="dune.source_cannonical_bridges_flows_tokens_hourly",
    project_id="mainnet-bigq",
    if_exists="append",
    chunksize=10000,
)

# Native
folder_link = "/Users/jaynaik/Desktop/cannonical_bridges/native"
csv_files = [f"{folder_link}/{file}" for file in os.listdir(folder_link)]

# Read and concatenate CSV files
df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
df["date"] = df["date"].astype(str)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table="dune.source_cannonical_bridges_flows_native_hourly",
    project_id="mainnet-bigq",
    if_exists="append",
    chunksize=10000,
)
