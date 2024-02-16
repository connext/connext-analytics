from pprint import pprint
import pandas_gbq
from datetime import datetime
import numpy as np
import logging
from src.integrations.utilities import get_raw_from_bq, read_sql_from_file_add_template


# 1. Greate than date from a big query table
def get_greater_than_date_from_bq_table(table_id, date_col):
    """
    pass table name and date column name
    -- EG:
        -- SELECT max(upload_datetime) AS latest_upload_datetime FROM `mainnet-bigq.stage.source_lifi__routes`
        -- SELECT max(upload_datetime) AS latest_upload_datetime FROM `mainnet-bigq.raw.source_socket__routes`
    """
    try:

        sql = read_sql_from_file_add_template(
            sql_file_name="get_latest_by_bq_table_and_date_col",
            template_data={"date_col": date_col, "table_id": table_id},
        )
        df = pandas_gbq.read_gbq(sql)
        return np.array(df[date_col].dt.to_pydatetime())[0].replace(tzinfo=None)

    except pandas_gbq.exceptions.GenericGBQException as e:
        if "Reason: 404" in str(e):
            return datetime(2024, 1, 1, 1, 1, 1)
        else:
            raise


def get_routes_pathways_from_bq(aggregator, reset):
    """
    reset: used to reset pathway to all possible paths using table selector
        -- OPTIONS:
            -- SELECT * FROM `mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways`
            -- SELECT * FROM `mainnet-bigq.stage.source_lifi__pathways`
    aggregator: socket or lifi
    """
    table_id = "mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways"
    if reset:
        logging.info("Resetting routes to all possible paths")
        table_id = "mainnet-bigq.stage.source_lifi__pathways"

    try:

        sql = read_sql_from_file_add_template(
            sql_file_name="generate_routes_pathways",
            template_data={
                "except_col": "aggregator",
                "aggregator": aggregator,
                "table_id": table_id,
                "reset": reset,
            },
        )
        pprint(sql)

        df = pandas_gbq.read_gbq(sql)
        df["fromAmount"] = df["fromAmount"].apply(lambda x: int(float(x)))
        df["fromChainId"] = df["fromChainId"].astype(float).astype(int)
        df["toChainId"] = df["toChainId"].astype(float).astype(int)

        if aggregator == "socket":
            df["uniqueRoutesPerBridge"] = "false"
            df["sort"] = "output"
            df.rename(
                columns={"fromAddress": "userAddress"},
                inplace=True,
            )

        elif aggregator == "lifi":
            df["allowDestinationCall"] = True

        return df.to_dict(orient="records")

    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        raise


# if __name__ == "__main__":

#     # socket = len(get_routes_pathways_from_bq(aggregator="socket", reset=False))
#     # logging.info(f"socket Done: {socket}")

#     lifi = len(get_routes_pathways_from_bq(aggregator="lifi", reset=False))
#     logging.info(f"lifi Done: {lifi}")
