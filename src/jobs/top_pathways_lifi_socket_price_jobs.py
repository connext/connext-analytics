import logging
import asyncio
import os
from datetime import datetime
import json
import pandas as pd
import pandas_gbq
from src.integrations.utilities import (
    upload_json_to_gcs,
)

from src.integrations.helpers_routes_aggreagators import (
    get_top_routes_pathways_from_bq,
    get_greater_than_date_from_bq_table,
)
from src.integrations.lifi import main_routes, convert_json_to_df
from src.integrations.socket import (
    get_all_routes,
    convert_socket_routes_json_to_df,
)
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
PROJECT_ID = "mainnet-bigq"
TOP_PATHWAYS_LIFI_BUCKET_NAME = "top_pathways_lifi_routes"
TOP_PATHWAYS_SOCKET_BUCKET_NAME = "top_pathways_socket_routes"


# utility: LIFI upload to gcp from cs
def upload_lifi_socket_to_gcp_from_cs(bucket_name: str, aggregator: str) -> None:
    """
    aggregator: lifi or socket
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    final_df = pd.DataFrame()
    for blob in blobs:
        logging.info(f"Pulling data for: {blob.name}")

        name = os.path.splitext(blob.name)[0]
        dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")

        data = json.loads(blob.download_as_text())
        print(f"data: {len(data)}")

        # 1. depending on aggregator convert the data to df
        if aggregator == "lifi":
            df = convert_json_to_df(json_file=data)
        elif aggregator == "socket":
            df = convert_socket_routes_json_to_df(json_blob=data)
        else:
            raise ValueError(f"Aggregator {aggregator} not supported")

        df["upload_datetime"] = dt
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(".", "_")
        for col in df.columns:
            if df[col].apply(isinstance, args=(list,)).any():
                df[col] = df[col].apply(
                    lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
                )

                df = df.astype(
                    {col: "int" for col in df.select_dtypes(include=[bool]).columns}
                )
        if not df.empty:
            final_df = pd.concat([final_df, df], ignore_index=True)

    # upload to bq
    logging.info(
        f"All data  pulled to Dataframe, Uploading {aggregator} data to BQ, {final_df.shape} rows"
    )
    pandas_gbq.to_gbq(
        dataframe=final_df,
        project_id=PROJECT_ID,
        destination_table=f"ad_hoc.source_{aggregator}__top_pathways_routes",
        if_exists="replace",
        chunksize=100000,
        api_method="load_csv",
    )
    logging.info(f"{aggregator} Routers, {df.shape} rows Added!")


async def lifi_pipeline() -> dict:
    """
    lifi pipeline
    """
    # LIFI

    pathways = get_top_routes_pathways_from_bq(aggregator="lifi")
    logging.info(f" lifi pathways: {len(pathways)}")

    try:

        routes = await main_routes(payloads=pathways)
    except Exception as e:
        logging.info(f"Error in lifi pipeline: {e}")
        return {"lifi_routes_job": "error"}
    upload_json_to_gcs(routes, TOP_PATHWAYS_LIFI_BUCKET_NAME)
    return {"lifi_routes_job": "completed"}


async def socket_pipeline() -> dict:
    """
    socket pipeline
    """
    # Socket

    payloads = get_top_routes_pathways_from_bq(aggregator="socket")
    logging.info(f"socket pathways, data length: {len(payloads)}")
    try:
        routes = await get_all_routes(payloads=payloads)
    except Exception as e:
        logging.info(f"Error in socket pipeline: {e}")
        return {"socket_routes_job": "error"}
    upload_json_to_gcs(routes, TOP_PATHWAYS_SOCKET_BUCKET_NAME)
    return {"socket_routes_job": "completed"}


async def run_lifi_socket_routes_jobs() -> dict:
    """
    run lifi and socket jobs
    """
    response1, response2 = await asyncio.gather(
        lifi_pipeline(),
        socket_pipeline(),
    )
    return {"lifi_routes_job": response1, "socket_routes_job": response2}


async def run_lifi_socket_data_gcs_to_bq() -> dict:
    """
    run lifi and socket data jobs for gcp to bq
    """
    # lifi
    upload_lifi_socket_to_gcp_from_cs(
        bucket_name=TOP_PATHWAYS_LIFI_BUCKET_NAME,
        aggregator="lifi",
    )

    # socket
    upload_lifi_socket_to_gcp_from_cs(
        bucket_name=TOP_PATHWAYS_SOCKET_BUCKET_NAME,
        aggregator="socket",
    )

    return {"message": "Finished uploading data from CS bucket to BQ"}


if __name__ == "__main__":
    logging.info("started Routes jobs")
    # print(asyncio.run(run_lifi_socket_routes_jobs()))
    print(asyncio.run(run_lifi_socket_data_gcs_to_bq()))
