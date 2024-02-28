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
            -- SELECT * FROM `mainnet-bigq.raw.stg_all_possible_pathways__routes__lifi_socket`
                -- This is created based of LIFI pathway table, which is based of python code.
    aggregator: socket or lifi
    """
    table_id = "mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways"
    if reset:
        logging.info("Resetting routes to all possible paths")
        table_id = "mainnet-bigq.raw.stg_all_possible_pathways__routes__lifi_socket"

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

            df.drop(columns=["aggregator"], inplace=True)
            return df.to_dict(orient="records")

        elif aggregator == "lifi":
            df["allowDestinationCall"] = True
            df.drop(columns=["aggregator"], inplace=True)
            payload = df.to_dict(orient="records")
            # add integrator as connext
            new_key_value = {"options": {"integrator": "connext.network"}}
            for dict_item in payload:
                dict_item.update(new_key_value)
            return payload

    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    data = get_routes_pathways_from_bq(aggregator="lifi", reset=True)
    pprint(data[0])
#     socket = len(data)
#     logging.info(f"socket Done: {socket}")

#     # lifi = len(get_routes_pathways_from_bq(aggregator="lifi", reset=True))
#     # logging.info(f"lifi Done: {lifi}")
# p = get_routes_pathways_from_bq(aggregator="socket", reset=True)
# logging.info(f"socket Done: {len(p)}")
# pprint(p[0])
