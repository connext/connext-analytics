import json
import logging
from pprint import pprint
import re
import nest_asyncio
import asyncio
import pandas_gbq
import pandas as pd
from fastapi import FastAPI
from src.integrations.lifi import (
    PROJECT_ID,
    get_connections,
    all_chains,
    get_tokens as get_tokens_lifi,
    get_tools as get_tools_lifi,
    main_routes,
    get_upload_data_from_lifi_cs_bucket,
    get_greater_than_date_from_bq_lifi_routes,
)
from src.integrations.socket import (
    get_chains,
    get_tokens as get_tokens_socket,
    get_bridges,
    get_all_routes,
    get_upload_data_from_socket_cs_bucket,
)
from src.integrations.helpers_routes_aggreagators import (
    get_greater_than_date_from_bq_table,
    get_routes_pathways_from_bq,
)
from src.integrations.connext_chains_ninja import get_chaindata_connext_df
from src.integrations.prd_ts_metadata import get_prod_mainmet_config_metadata
from src.integrations.hop_explorer import get_transfers_data
from src.integrations.utilities import (
    upload_json_to_gcs,
    read_sql_from_file_add_template,
    run_bigquery_query,
    convert_lists_and_booleans_to_strings,
)

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


# @app.get("/lifi/generate_alt_pathways_by_chain_ids")
# def lifi_generate_alt_pathways_by_chain_key_inputs():
#     """
#     Adding hard coded chain ids for now
#     [ ] REPLACE ALL these generate Pathways with one SQL to rule all
#     anyway, all possible pathways are aviable in source_lifi__pathways.
#     [ ] Add a integration tag and to success and fail for each of these paths
#     """

#     gp = generate_alt_pathways_by_chain_key_inputs(
#         chain_keys=["era", "bas", "ava", "pze"],
#         tokens=["ETH", "USDT", "DAI", "USDC", "WETH"],
#     )

#     logging.info(f"gp: {len(gp)}")
#     df_gp = pd.DataFrame(gp)

#     pandas_gbq.to_gbq(
#         dataframe=df_gp.astype(str),
#         project_id=PROJECT_ID,
#         destination_table="mainnet-bigq.stage.source_lifi__pathways",
#         if_exists="append",
#         chunksize=100000,
#     )
#     return {"message": "lifi paths added to source_lifi__pathways. pipeline finished"}


# -----
# ALTernative chains Routes: "era", "bas", "ava", "pze"
# -----


# @app.get("/lifi/alt_chains_routes/pipeline")
# def alt_chain_route_pipeline():
#     """Pull Alt chains data and add them to Cloud storage"""
#     gp = generate_alt_pathways_by_chain_key_inputs(
#         # chain_keys=["era", "bas", "ava", "pze"],
#         chain_keys=["mam"],
#         tokens=["ETH", "USDT", "DAI", "USDC", "WETH"],
#     )
#     logging.info(f"gp: {len(gp)}")
#     df_pathways = pd.DataFrame(gp)
#     df_pathways["fromAmount"] = df_pathways["fromAmount"].apply(lambda x: int(x))
#     pprint(df_pathways)
#     pathways = df_pathways.to_dict("records")
#     return pathways
#     # routes = asyncio.run(main_routes(payloads=pathways))
#     # upload_json_to_gcs(routes, "lifi_routes")


# -----
# Routes
# -----


@app.get("/lifi/routes/pipeline")
def lifi_routes_pipeline():
    """
    INPUT

        reset:
            If set as True, All possible pathways combinations will be pulled into GCP Cloud storage,
            on Default, pathways used: mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways
        paylaod sent:
            [
                {
                    "fromChainId": 1,
                    "fromTokenAddress": "0x0000000000000000000000000000000000000000",
                    "fromAddress": "0x32d222E1f6386B3dF7065d639870bE0ef76D3599",
                    "toChainId": 10,
                    "toTokenAddress": "0x0000000000000000000000000000000000000000",
                    "fromAmount": 1e+21,
                    "allowDestinationCall": true,
                    "options": {
                      "integrator": "connext.network"
                      },
                }
            ]
    """

    reset: bool = False
    print(f"start,pathway reset: {reset}")
    pathways = get_routes_pathways_from_bq(aggregator="lifi", reset=reset)
    logging.info(f"pathways: {len(pathways)}")
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


@app.get("/socket/routes/pipeline")
def socket_routes_pipeline():
    """
    INPUT

        reset:
            If set as True, All possible pathways combinations will be pulled into GCP Cloud storage
            On Default, pathways used: mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways.

    """
    reset: bool = False
    print(f"start,pathway reset: {reset}")
    payloads = get_routes_pathways_from_bq(aggregator="socket", reset=reset)
    logging.info(f"payloads pull, data length: {len(payloads)}")

    routes = asyncio.run(get_all_routes(payloads=payloads))
    upload_json_to_gcs(routes, "socket_routes")

    return {"message": "socket routes pipeline finished"}


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
