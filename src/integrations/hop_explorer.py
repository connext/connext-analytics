from cmath import log
import time
import random
import asyncio
import httpx
import pandas as pd
import logging
import json
import pandas_gbq
from pprint import pp, pprint
import numpy as np
from src.integrations.utilities import get_raw_from_bq
from datetime import date, datetime, timedelta

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "mainnet-bigq"
URL_HOP_EXPLORER__BASE = "https://explorer-api.hop.exchange/v1"
URL_HOP_EXPLORER__TRANSFERS = "/transfers"


def get_latest_date_from_hop_explorer__transfers():
    try:
        df = get_raw_from_bq(sql_file_name="latest_date_from_hop_explorer__transfers")
        max_date = df["new_start_date"].values[0]
        logging.info(f"Latest date from BQ is {max_date}")
        return datetime.fromisoformat(max_date).date()

    except pandas_gbq.exceptions.GenericGBQException as e:
        if "Reason: 404" in str(e):
            logging.info("No data found in BQ, starting from the beginning")
            return date(2023, 9, 1)
        else:
            raise


def generate_date_logic(start_date):
    """
    Based on the start date generate params to pass
    end date is the same day till_date is today
    """

    till_date = datetime.today().date()

    params = []
    while start_date <= till_date:
        start_date += timedelta(days=1)
        param = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": start_date.strftime("%Y-%m-%d"),
        }
        params.append(param)

    return params


async def get_data(ext_url: str, params: dict) -> list:
    url = URL_HOP_EXPLORER__BASE + ext_url
    try:
        async with httpx.AsyncClient() as client:

            delay = random.uniform(0.2, 10)
            time.sleep(delay)
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()["data"]
            if data:
                for d in data:
                    d.update({"request_url": str(response.url)})
                return data
            else:
                return None
    except httpx.HTTPError as e:
        logging.info(f"HTTP error occurred: {e}")
        logging.info(f"Status code: {response.status_code}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON on {url}: {e}")
        return None
    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        return None


async def get_transfers_data(ext_url=URL_HOP_EXPLORER__TRANSFERS) -> pd.DataFrame:

    transfers_params = generate_date_logic(
        start_date=get_latest_date_from_hop_explorer__transfers()
    )
    logging.info(f"Generating {len(transfers_params)} days of data to pull")

    for param in transfers_params:
        daily_transfers = []
        page = 1
        while True:
            logging.info(f"Processing page {page}")
            param.update({"page": str(page)})
            res = await get_data(ext_url=ext_url, params=param)
            if not res:
                break
            daily_transfers.extend(res)
            page += 1

        df = pd.DataFrame(daily_transfers)
        if not df.empty:
            pprint(df)
            df.to_csv("data/hop_explorer__transfers_jan15.csv", index=False)
            df.columns = df.columns.str.lower()
            df.columns = df.columns.str.replace(".", "_")
            pandas_gbq.to_gbq(
                dataframe=df,
                project_id=PROJECT_ID,
                destination_table="stage.source_hop_explorer__transfers",
                if_exists="append",
                chunksize=100000,
                api_method="load_csv",
            )

            logging.info(f"{len(df)} rows inserted")

    return {"status": "success", "message": "data pull completed"}


# if __name__ == "__main__":

#     sample = asyncio.run(get_transfers_data())
