import logging
import asyncio
from src.integrations.lifi import main_routes
from src.integrations.socket import get_all_routes
from src.integrations.helpers_routes_aggreagators import get_top_routes_pathways_from_bq
from src.integrations.utilities import upload_json_to_gcs

logging.basicConfig(level=logging.INFO)


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
    upload_json_to_gcs(routes, "top_pathways_lifi_routes")
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
    upload_json_to_gcs(routes, "top_pathways_socket_routes")
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


if __name__ == "__main__":
    logging.info("started Routes jobs")
    print(asyncio.run(run_lifi_socket_routes_jobs()))
