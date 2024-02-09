import json
import logging
import nest_asyncio
import asyncio
import pandas_gbq
import pandas as pd
from fastapi import FastAPI
from src.integrations.lifi import (
    PROJECT_ID,
    get_connections,
    all_chains,
    get_tokens,
    get_tools,
    generate_pathways,
    main_routes,
    get_upload_data_from_lifi_cs_bucket,
    get_greater_than_date_from_bq_lifi_routes,
)
from src.integrations.socket import (
    get_all_routes,
    get_upload_data_from_socket_cs_bucket,
    get_greater_than_date_from_bq_socket_routes,
)
from src.integrations.hop_explorer import get_transfers_data
from src.integrations.utilities import (
    upload_json_to_gcs,
    get_raw_from_bq,
    read_sql_from_file_add_template,
    run_bigquery_query,
)

logging.basicConfig(level=logging.INFO)
nest_asyncio.apply()

app = FastAPI(
    title="LiFi Integration", description="Pipline that run LIFI integrations"
)


@app.get("/")
def start():
    return {"app is running"}


@app.get("/lifi/chains/pipeline")
def lifi_chain_pipeline():
    print("start")
    chains_df = asyncio.run(all_chains())
    if not chains_df.empty:
        pandas_gbq.to_gbq(
            dataframe=chains_df,
            project_id=PROJECT_ID,
            destination_table="stage.source_lifi__chains",
            if_exists="replace",
        )
        return {"message": "lifi chains pipeline finished"}
    else:
        logging.info("No data pulled")
        raise Exception("lifi chains pipeline, TypeError: no data pulled")


@app.get("/lifi/connections/pipeline")
def lifi_connections_pipeline():
    print("start")
    connections = asyncio.run(get_connections())
    pandas_gbq.to_gbq(
        dataframe=connections,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__connections",
        if_exists="replace",
        chunksize=100000,
    )
    return {"message": "lifi connections pipeline finished"}


@app.get("/lifi/tokens/pipeline")
def lifi_tokens_pipeline():
    print("start")
    tokens = asyncio.run(get_tokens())
    print("data pulled successfully")
    print(f"size of data: {tokens.shape} and head: {tokens.head()}")
    pandas_gbq.to_gbq(
        dataframe=tokens,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__tokens",
        if_exists="replace",
        chunksize=100000,
    )
    return {"message": "lifi tokens pipeline finished"}


@app.get("/lifi/tools/pipeline")
def lifi_tools_pipeline():
    print("start")
    tools = asyncio.run(get_tools())
    pandas_gbq.to_gbq(
        dataframe=tools,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__tools",
        if_exists="replace",
        chunksize=100000,
    )
    return {"message": "lifi tools pipeline finished"}


@app.get("/lifi/routes/pipeline")
def lifi_routes_pipeline():
    """
    INPUT
    [
        {
            "fromChainId": 1,
            "fromTokenAddress": "0x0000000000000000000000000000000000000000",
            "fromAddress": "0x32d222E1f6386B3dF7065d639870bE0ef76D3599",
            "toChainId": 10,
            "toTokenAddress": "0x0000000000000000000000000000000000000000",
            "fromAmount": 1e+21,
            "allowDestinationCall": true
        }
    ]
    """
    print("start")

    df_pathways = get_raw_from_bq(sql_file_name="generate_routes_pathways")
    df_pathways["allowDestinationCall"] = True
    df_pathways["fromAmount"] = df_pathways["fromAmount"].apply(lambda x: int(x))
    pathways = df_pathways.to_dict("records")

    routes = asyncio.run(main_routes(payloads=pathways))
    upload_json_to_gcs(routes, "lifi_routes")

    return {"message": "lifi routes pipeline finished"}


@app.get("/lifi/routes/upload_to_bq/")
def lifi_routes_upload_to_bq():

    get_upload_data_from_lifi_cs_bucket(
        greater_than_date=get_greater_than_date_from_bq_lifi_routes()
    )
    return {"message": "Finished uploading data from CS bucket to BQ"}


# -----
# HOP
# -----
@app.get("/hop_explorer/transfers/pipeline")
def hop_explorer__transfers_pipeline():
    asyncio.run(get_transfers_data())
    return {"message": "Finished uploading data to BQ till date"}


# -----
# SOCKET
# -----
@app.get("/socket/routes/pipeline")
def socket_routes_pipeline():

    print("start")

    routes = asyncio.run(get_all_routes())
    upload_json_to_gcs(routes, "socket_routes")

    return {"message": "socket routes pipeline finished"}


@app.get("/socket/routes/upload_to_bq/")
def socket_routes_upload_to_bq():

    output = get_upload_data_from_socket_cs_bucket(
        greater_than_date=get_greater_than_date_from_bq_socket_routes()
    )
    return {"message": f"{output}"}


# -----
# Utils
# -----
@app.get("/utils/drop_duplicate_rows_from_bq/")
def drop_duplicate_rows_from_bq(bq_table_id: str):
    """
    Drop duplicate rows from BQ table
    INPUT
        bq_table_id - Full identifier of the BigQuery table, e.g., "mainnet-bigq.raw.source_socket__bridges"
    """
    logging.info(f"Dropping duplicate rows from {bq_table_id}")

    sql = read_sql_from_file_add_template(
        sql_file_name="drop_duplicate_rows_bq_table",
        template_data={"id": bq_table_id},
    )
    return run_bigquery_query(sql_query=sql)
