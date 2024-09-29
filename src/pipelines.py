import asyncio
import json
import logging
import re
from pprint import pprint

import dlt
import nest_asyncio
import pandas as pd
import pandas_gbq
from fastapi import FastAPI

from src.integrations.arb_distribution_mode_metis_upload_2_bq import \
    main as arb_distribution_mode_metis_upload_2_bq_main
from src.integrations.connext_chains_ninja import get_chaindata_connext_df
from src.integrations.defilamma import defilamma_raw
from src.integrations.dune import dune_bridges, dune_daily_metrics
from src.integrations.helpers_routes_aggreagators import \
    get_greater_than_date_from_bq_table
from src.integrations.hop_explorer import get_transfers_data
from src.integrations.lifi import (PROJECT_ID, all_chains, get_connections,
                                   get_greater_than_date_from_bq_lifi_routes)
from src.integrations.lifi import get_tokens as get_tokens_lifi
from src.integrations.lifi import get_tools as get_tools_lifi
from src.integrations.lifi import get_upload_data_from_lifi_cs_bucket
from src.integrations.prd_ts_metadata import get_prod_mainmet_config_metadata
from src.integrations.socket import get_bridges, get_chains
from src.integrations.socket import get_tokens as get_tokens_socket
from src.integrations.socket import get_upload_data_from_socket_cs_bucket
from src.integrations.utilities import (convert_lists_and_booleans_to_strings,
                                        read_sql_from_file_add_template,
                                        run_bigquery_query)

logging.basicConfig(level=logging.INFO)
nest_asyncio.apply()

app = FastAPI(
    title="CONNEXT DATA Integration", description="Pipline that run data integrations"
)


@app.get("/")
def start():
    return {"app is running"}


# -----
# CONNEXT METADATA
# -----
@app.get("/chaindata_connext_ninja/pipeline")
def get_chaindata_connext_ninja_pipeline():
    """This pipeline, pulls data from nija and upload it to bq table"""
    chaindata_connext_df = asyncio.run(get_chaindata_connext_df())
    if not chaindata_connext_df.empty:
        chaindata_connext_df = convert_lists_and_booleans_to_strings(
            chaindata_connext_df
        )
        pandas_gbq.to_gbq(
            dataframe=chaindata_connext_df,
            project_id=PROJECT_ID,
            destination_table="raw.source_chaindata_nija__metadata",
            if_exists="replace",
        )
        return {"message": "Chain Ninja metadata pipeline finished"}
    else:
        logging.info("No data pulled")
        raise Exception(
            "Chain Ninja metadata pipeline failed, TypeError: no data pulled"
        )


# -----
# LIFI API
# -----
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
    tokens = asyncio.run(get_tokens_lifi())
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
    tools, df_lifi__bridges_exchanges = asyncio.run(get_tools_lifi())
    pandas_gbq.to_gbq(
        dataframe=tools,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__tools",
        if_exists="replace",
        chunksize=100000,
    )

    # Bridges & exchanges
    df_expanded = pd.json_normalize(df_lifi__bridges_exchanges["supportedChains"])
    df_lifi__bridges_exchanges = pd.concat(
        [df_lifi__bridges_exchanges.drop(["supportedChains"], axis=1), df_expanded],
        axis=1,
    )
    pandas_gbq.to_gbq(
        dataframe=df_lifi__bridges_exchanges,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__bridges_exchanges",
        if_exists="replace",
        chunksize=100000,
    )

    return {"message": "lifi tools pipeline finished"}


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


@app.get("/socket/chains/pipeline")
def socket_chain_pipeline():
    msg_output = get_chains()
    return msg_output


@app.get("/socket/tokens/pipeline")
def socket_tokens_pipeline():
    msg_output = get_tokens_socket()
    return msg_output


@app.get("/socket/bridges/pipeline")
def socket_bridge_pipeline():
    msg_output = get_bridges()
    return msg_output


@app.get("/socket/routes/upload_to_bq/")
def socket_routes_upload_to_bq():
    output = get_upload_data_from_socket_cs_bucket(
        greater_than_date_routes=get_greater_than_date_from_bq_table(
            table_id="mainnet-bigq.raw.source_socket__routes",
            date_col="upload_datetime",
        ),
        greater_than_date_steps=get_greater_than_date_from_bq_table(
            table_id="mainnet-bigq.raw.source_socket__routes_steps",
            date_col="upload_datetime",
        ),
    )
    return {"message": f"{output}"}


# -----
# Utils
# -----
@app.get("/utils/drop_duplicate_rows_from_bq/")
def drop_duplicate_rows_from_bq(bq_table_id: str):
    """
    This is a high resource consumption query. Run with care. Make sure data is no bigger than 100K.
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


# -----
# PRODUCTION METADATA
# -----
@app.get("/prod_mainnet_metadata/pipeline")
def prod_mainnet_metadata_pipeline():
    get_prod_mainmet_config_metadata()
    return {"message": "Pipeline completed"}


# -----
# DUNE
# -----


@app.get("/dune/pipeline")
def dune_pipeline():
    logging.info("Running DLT Dune Bridges")
    p = dlt.pipeline(
        pipeline_name="dune",
        destination="bigquery",
        dataset_name="dune",
    )
    p.run(dune_bridges(), loader_file_format="jsonl")
    logging.info("Finished DLT Dune Bridges!")
    return {"message": "Pipeline completed"}


@app.get("/dune/daily_metrics/pipeline")
def dune_daily_metrics_pipeline():
    logging.info("Running DLT Dune Bridges")

    p = dlt.pipeline(
        pipeline_name="dune",
        destination="bigquery",
        dataset_name="dune",
    )

    p.run(dune_daily_metrics(), loader_file_format="jsonl")
    logging.info("Finished DLT Dune Daily Metrics!")
    return {"message": "Pipeline completed"}


# -----
# DEFI LAMMA
# -----


@app.get("/defilamma/pipeline")
def defilamma_pipeline():
    logging.info("Running DLT defilamma")
    p = dlt.pipeline(
        pipeline_name="defilamma",
        destination="bigquery",
        dataset_name="raw",
    )
    p.run(defilamma_raw(), loader_file_format="jsonl")
    logging.info("Finished DLT defilamma")
    return {"message": "Pipeline completed"}


# -----
# ARB DISTRIBUTION MODE & METIS
# -----


@app.get("/arb_distribution_mode_metis/pipeline")
def arb_distribution_mode_metis_pipeline():
    arb_distribution_mode_metis_upload_2_bq_main()
    return {"message": "Pipeline completed"}
