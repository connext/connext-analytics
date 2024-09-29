import asyncio
import logging

import httpx
import pandas as pd

BASE_URL = "https://chaindata.connext.ninja/"
HEADERS = {"Content-Type": "application/json"}
# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def get_chaindata_connext():
    url = BASE_URL
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS)
            response.raise_for_status()
            logging.info(f"Response status code: {response.status_code}")
            return response.json()
    except httpx.HTTPError as e:
        logging.info(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        return None


async def get_chaindata_connext_df():
    chaindata_connext = await get_chaindata_connext()
    if chaindata_connext:
        for item in chaindata_connext:
            if "assetId" in item and isinstance(item["assetId"], dict):
                item["assetId"] = [
                    {"key": key, **value}
                    for key, value in item["assetId"].items()
                    if value
                ]

        df = pd.DataFrame(chaindata_connext, columns=["assetId"])

        # All cols
        # remove assetId and normalize to get main
        chaindata_connext = [
            {key: value for key, value in item.items() if key != "assetId"}
            for item in chaindata_connext
        ]
        df_main = pd.json_normalize(chaindata_connext)

        exploded = df["assetId"].explode().to_frame()
        exploded.reset_index(inplace=True)
        exploded.rename(columns={"index": "org_index"}, inplace=True)
        df_expanded = pd.json_normalize(exploded["assetId"])
        df_combined = exploded.join(df_expanded).add_prefix("asset_")
        df_combined.drop("asset_assetId", axis=1, inplace=True)
        df_normalized = df_main.merge(
            df_combined, left_index=True, right_on="asset_org_index"
        )
        # drop col: asset_org_index
        df_normalized.drop("asset_org_index", axis=1, inplace=True)
        # replace "." in cols to "_" and lower cols
        df_normalized.columns = [
            col.lower().replace(".", "_") for col in df_normalized.columns
        ]

        return df_normalized
