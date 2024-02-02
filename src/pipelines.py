import os
import json
import dotenv
import nest_asyncio
import asyncio
import pandas_gbq
import pandas as pd
from fastapi import FastAPI
from src.integrations.lifi import (
    PROJECT_ID,
    source_lifi__api_key,
    get_connections,
    all_chains,
    get_tokens,
    get_tools,
    generate_pathways,
    main_routes,
)
from google.oauth2 import service_account
from src.integrations.utilities import upload_json_to_gcs

dotenv.load_dotenv()
nest_asyncio.apply()

app = FastAPI(
    title="LiFi Integration", description="Pipline that run LIFI integrations"
)
BQ_creds = json.loads(os.getenv("BQ_creds"))


@app.get("/")
def start():
    return {"app is running"}


@app.get("/lifi/chains/pipeline")
def lifi_chain_pipeline():
    print("start")
    chains_df = asyncio.run(all_chains())
    print("data pulled successfully")
    print(f"size of data: {chains_df.shape} and head: {chains_df.head()}")
    pandas_gbq.to_gbq(
        dataframe=chains_df,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__chains",
        if_exists="replace",
        credentials=service_account.Credentials.from_service_account_info(BQ_creds),
    )
    return {"message": "lifi chains pipeline finished"}


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
        credentials=service_account.Credentials.from_service_account_info(BQ_creds),
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
        credentials=service_account.Credentials.from_service_account_info(BQ_creds),
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
        credentials=service_account.Credentials.from_service_account_info(BQ_creds),
    )
    return {"message": "lifi tools pipeline finished"}


@app.get("/lifi/routes/pipeline")
def lifi_routes_pipeline():
    print("start")

    tools_df = asyncio.run(get_tools())
    chains_df = asyncio.run(all_chains())
    tokens_df = asyncio.run(get_tokens())

    pathways = generate_pathways(
        connext_chains_ids=tools_df,
        chains=chains_df,
        tokens_df=tokens_df,
        tokens=["ETH", "USDT", "DAI", "USDC", "WETH"],
    )
    pathways_df = pd.DataFrame(pathways).astype(str)

    pandas_gbq.to_gbq(
        dataframe=pathways_df,
        project_id=PROJECT_ID,
        destination_table="stage.source_lifi__pathways",
        if_exists="replace",
        chunksize=100000,
        credentials=service_account.Credentials.from_service_account_info(BQ_creds),
    )
    
    routes = asyncio.run(main_routes(payloads=pathways))
    upload_json_to_gcs(routes, "lifi_routes")

    # pandas_gbq.to_gbq(
    #     dataframe=routes,
    #     project_id=PROJECT_ID,

    #     destination_table="stage.source_lifi__routes",
    #     if_exists="replace",
    #     chunksize=100000,
    # )
    return {"message": "lifi routes pipeline finished"}
