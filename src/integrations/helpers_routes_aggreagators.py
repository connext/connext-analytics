import logging
from datetime import datetime
from pprint import pprint

import numpy as np
import pandas_gbq

from src.integrations.utilities import (get_raw_from_bq,
                                        read_sql_from_file_add_template)


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


def get_routes_pathways_from_bq(aggregator):
    """
    stg_all_possible_pathways__routes__lifi_socket: These are approx 4k paths for LIFI, Socket
    """
    try:
        logging.info(
            f"sql_file_name: stg_all_possible_pathways__routes__lifi_socket, aggregator: {aggregator}"
        )
        sql = read_sql_from_file_add_template(
            sql_file_name="stg_all_possible_pathways__routes__lifi_socket",
            template_data={"aggregator": aggregator},
        )

        df = pandas_gbq.read_gbq(sql)
        cols_2_keep = [
            "fromChainId",
            "toChainId",
            "fromAmount",
            "fromTokenAddress",
            "toTokenAddress",
            "fromAddress",
            "aggregator",
        ]
        df = df[cols_2_keep]
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
            new_key_value = {
                "options": {
                    "integrator": "connext.network",
                    "exchanges": {"deny": ["all"]},
                }
            }
            for dict_item in payload:
                dict_item.update(new_key_value)
            return payload

    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        raise


def get_top_routes_pathways_from_bq(aggregator):
    """
    We are pulling data for predefined top routes from DIMA
    aggregator: socket or lifi
    """

    try:
        sql = read_sql_from_file_add_template(
            sql_file_name="top_pathways_lifi_socket_hourly_pull",
            template_data={"aggregator": aggregator},
        )

        df = pandas_gbq.read_gbq(sql)
        cols_2_keep = [
            "fromChainId",
            "toChainId",
            "fromAmount",
            "fromTokenAddress",
            "toTokenAddress",
            "fromAddress",
            "aggregator",
        ]
        df = df[cols_2_keep]
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
            new_key_value = {
                "options": {
                    "integrator": "connext.network",
                    "exchanges": {"deny": ["all"]},
                }
            }
            for dict_item in payload:
                dict_item.update(new_key_value)
            return payload

    except Exception as e:
        logging.info(f"An unexpected error occurred: {e}")
        raise


# if __name__ == "__main__":
#     data = get_top_routes_pathways_from_bq(aggregator="lifi")
