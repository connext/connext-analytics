import nest_asyncio
import asyncio
import pandas_gbq
from fastapi import FastAPI
from src.integrations.lifi import (
    project_id,
    get_connections,
    all_chains,
    get_tokens,
    get_tools,
    generate_pathways,
    main_routes,
)

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
    pandas_gbq.to_gbq(
        dataframe=chains_df,
        project_id=project_id,
        destination_table="stage.source_lifi__chains",
        if_exists="replace",
    )
    return {"message": "lifi chains pipeline finished"}


@app.get("/lifi/connections/pipeline")
def lifi_connections_pipeline():
    print("start")
    connections = asyncio.run(get_connections())
    pandas_gbq.to_gbq(
        dataframe=connections,
        project_id=project_id,
        destination_table="stage.source_lifi__connections",
        if_exists="replace",
        chunksize=100000,
    )
    return {"message": "lifi connections pipeline finished"}


@app.get("/lifi/tokens/pipeline")
def lifi_tokens_pipeline():
    print("start")
    tokens = asyncio.run(get_tokens())
    pandas_gbq.to_gbq(
        dataframe=tokens,
        project_id=project_id,
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
        project_id=project_id,
        destination_table="stage.source_lifi__tools",
        if_exists="replace",
        chunksize=100000,
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
        token_df=tokens_df,
        tokens=["ETH", "USDT", "DAI", "USDC", "WETH"],
    )
    routes = asyncio.run(main_routes(payloads=pathways))

    pandas_gbq.to_gbq(
        dataframe=routes,
        project_id=project_id,
        destination_table="stage.source_lifi__routes",
        if_exists="replace",
        chunksize=100000,
    )
    return {"message": "lifi routes pipeline finished"}
