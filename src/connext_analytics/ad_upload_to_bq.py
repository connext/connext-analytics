import pandas as pd
import pandas_gbq


PROJECT_ID = "mainnet-bigq"


folder_link = "data/l1_price_date_hourly.csv"
df = pd.read_csv(folder_link)
df["symbol"] = df["symbol"].astype(str)
df["date"] = df["date"].astype(str)
df["average_price"] = df["average_price"].astype(float)
df["max_price"] = df["max_price"].astype(float)
df["_dlt_load_id"] = "1719824418.550661"
df["_dlt_id"] = "manual"

print(df.head())
print(df.dtypes)
print(df.symbol.unique())

# pandas_gbq.to_gbq(
#     dataframe=df,
#     destination_table="dune.source_hourly_token_pricing_blockchain_eth",
#     project_id="mainnet-bigq",
#     if_exists="append",
#     chunksize=10000,
# )
