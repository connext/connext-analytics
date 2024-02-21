import logging
import asyncio
import nest_asyncio
from src.integrations.lifi import (
    main_routes,
)
from src.integrations.socket import (
    get_all_routes,
)

from src.integrations.helpers_routes_aggreagators import (
    get_routes_pathways_from_bq,
)

from src.integrations.utilities import (
    upload_json_to_gcs,
)

logging.basicConfig(level=logging.INFO)
nest_asyncio.apply()


async def lifi_pipeline():
    """
    lifi pipeline
    """
    # LIFI
    reset: bool = True
    logging.info(f"start,pathway reset: {reset}")
    pathways = get_routes_pathways_from_bq(aggregator="lifi", reset=reset)
    logging.info(f" lifi pathways: {len(pathways)}")

    routes = await main_routes(payloads=pathways)
    upload_json_to_gcs(routes, "lifi_routes")
    return {"lifi_routes_job": "completed"}


async def socket_pipeline():
    """
    socket pipeline
    """
    # Socket
    reset: bool = True
    logging.info(f"start,pathway reset: {reset}")
    payloads = get_routes_pathways_from_bq(aggregator="socket", reset=reset)
    logging.info(f"socket pathways, data length: {len(payloads)}")

    routes = await get_all_routes(payloads=payloads)
    upload_json_to_gcs(routes, "socket_routes")
    return {"socket_routes_job": "completed"}


async def run_lifi_socket_routes_jobs():

    response1, response2 = await asyncio.gather(
        lifi_pipeline(),
        socket_pipeline(),
    )
    return {"lifi_routes_job": response1, "socket_routes_job": response2}


if __name__ == "__main__":

    print(asyncio.run(run_lifi_socket_routes_jobs()))
