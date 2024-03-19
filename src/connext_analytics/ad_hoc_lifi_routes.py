import os
import asyncio
from pprint import pprint
import httpx
import pandas as pd
import logging
import json
from jinja2 import Template
import numpy as np
import pandas_gbq
from dotenv import load_dotenv
from itertools import product
from datetime import datetime
from asyncio import Semaphore
from google.cloud import storage
from src.integrations.utilities import (
    get_raw_from_bq,
    nearest_power_of_ten,
    get_secret_gcp_secrete_manager,
    upload_json_to_gcs,
    convert_lists_and_booleans_to_strings,
)
from src.integrations.lifi import convert_routes_payload_to_df

PROJECT_ID = "mainnet-bigq"
bucket_name = "lifi_routes"

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs()
greater_than_date = datetime(2024, 2, 29)

for blob in blobs:
    logging.info(f"Pulling data for: {blob.name}")

    name = os.path.splitext(blob.name)[0]
    dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")

    if dt > greater_than_date:
        data = json.loads(blob.download_as_text())
        print(f"data: {len(data)}")

        # 2. Upload payload data along with this
        logging.info(f"Uploading payload data for:{name} ")
        payload_df = convert_routes_payload_to_df(json_blob=data)
        payload_df["aggregator"] = "lifi"
        payload_df["upload_datetime"] = dt

        print(payload_df)
        # break
        # upload to bq
        pandas_gbq.to_gbq(
            dataframe=convert_lists_and_booleans_to_strings(payload_df),
            project_id=PROJECT_ID,
            destination_table="stage.source__lifi_routes__payloads_logs",
            if_exists="append",
            chunksize=100000,
            api_method="load_csv",
        )
        logging.info(f"Lifi Payloads, {payload_df.shape} rows Added!")
