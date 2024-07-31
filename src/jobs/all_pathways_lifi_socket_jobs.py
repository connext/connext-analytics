import logging
import asyncio
from src.integrations.lifi import (
    main_routes,
)
from src.integrations.socket import (
    get_all_routes,
    convert_socket_routes_steps_json_to_df,
)

from src.integrations.helpers_routes_aggreagators import (
    get_routes_pathways_from_bq,
)

from src.integrations.utilities import (
    upload_json_to_gcs,
)
from google.cloud import storage
from datetime import datetime
import json
import os
import pandas_gbq

logging.basicConfig(level=logging.INFO)
PROJECT_ID = "mainnet-bigq"


async def lifi_pipeline():
    """
    lifi pipeline
    """
    # LIFI
    pathways = get_routes_pathways_from_bq(aggregator="lifi")
    logging.info(f" lifi pathways: {len(pathways)}")

    routes = await main_routes(payloads=pathways, max_concurrency=3)
    upload_json_to_gcs(routes, "lifi_routes")
    return {"lifi_routes_job": "completed"}


async def socket_pipeline():
    """
    socket pipeline
    """
    # Socket
    payloads = get_routes_pathways_from_bq(aggregator="socket")
    logging.info(f"socket pathways, data length: {len(payloads)}")

    routes = await get_all_routes(payloads=payloads, max_concurrency=3)
    upload_json_to_gcs(routes, "socket_routes")
    return {"socket_routes_job": "completed"}


def push_socket_steps_from_cs_to_bq(
    greater_than_date_steps, bucket_name="socket_routes"
):
    """
    push socket steps from cs to bq
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        logging.info(f"Pulling data for: {blob.name}")

        # seperate common parameters
        name = os.path.splitext(blob.name)[0]
        dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")

        # Routes Steps
        if dt > greater_than_date_steps:
            logging.info(f"pulling steps for:{name} ")
            data = json.loads(blob.download_as_text())

            # 1. convert convert_socket_routes_steps_json_to_df
            df_socket_steps = convert_socket_routes_steps_json_to_df(json_blob=data)
            df_socket_steps["upload_datetime"] = dt

            # 2. upload to bq
            pandas_gbq.to_gbq(
                dataframe=df_socket_steps,
                project_id=PROJECT_ID,
                destination_table="raw.source_socket__routes_steps",
                if_exists="append",
                chunksize=10000,
                api_method="load_csv",
            )

            logging.info(
                f"Steps for Socket Routers, {df_socket_steps.shape} rows Added!"
            )

        else:
            logging.info(
                f"Steps for  Socket Routers, {dt} is not greater than {greater_than_date_steps}, Data Already Added!"
            )


async def run_lifi_socket_routes_jobs(reset: bool = False):

    response1, response2 = await asyncio.gather(
        lifi_pipeline(),
        socket_pipeline(),
    )
    return {"lifi_routes_job": response1, "socket_routes_job": response2}


if __name__ == "__main__":
    logging.info("started Routes jobs")
    asyncio.run(run_lifi_socket_routes_jobs())
