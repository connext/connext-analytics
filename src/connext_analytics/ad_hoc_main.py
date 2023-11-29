import os
import json
from pprint import pprint
from pathlib import Path, PurePath
import pandas as pd
from jinja2 import Template
import pandas_gbq
from google.oauth2 import service_account


PROJECT_ID = "mainnet-bigq"
CURRENT_DIR = Path(__file__).parents[2]


def read_sql_from_file(sql_file_name, template_data) -> str:
    """
    Get sql query from sql file
    """

    data_dir = (
        f"{str(PurePath(CURRENT_DIR))}/src/connext_analytics/sql/{sql_file_name}.sql"
    )

    with open(data_dir, "r") as sql_file:
        file = sql_file.read()
        query = Template(file).render(template_data)
        return query


def get_raw_from_bq(sql_file_name, p) -> pd.DataFrame:
    """
    INPUTS:
    start="2010-08", end="2011-03"

    pipline to get raw data

    Template Data:
    - month to query via jinja
    """

    template_data = {"month": p[0], "year": p[1]}
    print(f"Pulling data for:  ---xxxx----XxXX----{template_data}")
    sql = read_sql_from_file(sql_file_name=sql_file_name, template_data=template_data)
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
    credentials = service_account.Credentials.from_service_account_file(
        f"{str(PurePath(CURRENT_DIR))}/creds/mainnet-bigq-b32efe721225.json",
        scopes=scopes,
    )
    pandas_gbq.context.credentials = credentials
    return pandas_gbq.read_gbq(sql, project_id=PROJECT_ID, credentials=credentials)


def run_storage_pipeline(data_csv_filename, sql_file_name, start, end):
    """INPUTS:
    start="2010-08", end="2011-03", transfers_final_add_on"""

    pr = pd.period_range(start, end, freq="M")
    prTupes = tuple([(period.month, period.year) for period in pr])
    for p in prTupes:
        transfers = get_raw_from_bq(sql_file_name=sql_file_name, p=p)

        pprint(transfers.shape)

        file_name = f"{str(PurePath(CURRENT_DIR))}/data/{data_csv_filename}.csv"
        if not os.path.isfile(file_name):
            transfers.to_csv(
                file_name,
                index=False,
                header=True,
            )
        else:
            transfers.to_csv(
                file_name,
                mode="a",
                index=False,
                header=False,
            )


def upload_df_2_bq(df, table_name):
    schema = get_schema()
    credentials = service_account.Credentials.from_service_account_file(
        f"{str(PurePath(CURRENT_DIR))}/creds/mainnet-bigq-b32efe721225.json"
    )
    pandas_gbq.to_gbq(
        df,
        destination_table=table_name,
        # table_schema=schema,
        project_id=PROJECT_ID,
        if_exists="replace",
        chunksize=50000,
        credentials=credentials,
    )
    print("upload completed!")
    return None


def get_schema():
    return [
        {"name": "transfer_id", "type": "STRING"},
        {"name": "nonce", "type": "STRING"},
        {"name": "to", "type": "STRING"},
        {"name": "call_data", "type": "STRING"},
        {"name": "origin_domain", "type": "STRING"},
        {"name": "destination_domain", "type": "STRING"},
        {"name": "receive_local", "type": "BOOL"},
        {"name": "origin_chain", "type": "STRING"},
        {"name": "origin_transacting_asset", "type": "STRING"},
        {"name": "origin_transacting_amount", "type": "STRING"},
        {"name": "origin_bridged_asset", "type": "STRING"},
        {"name": "origin_bridged_amount", "type": "STRING"},
        {"name": "xcall_caller", "type": "STRING"},
        {"name": "xcall_transaction_hash", "type": "STRING"},
        {"name": "xcall_timestamp", "type": "STRING"},
        {"name": "xcall_gas_price", "type": "STRING"},
        {"name": "xcall_gas_limit", "type": "STRING"},
        {"name": "xcall_block_number", "type": "STRING"},
        {"name": "destination_chain", "type": "STRING"},
        {"name": "destination_transacting_asset", "type": "STRING"},
        {
            "name": "destination_transacting_amount",
            "type": "STRING",
        },
        {"name": "destination_local_asset", "type": "STRING"},
        {"name": "destination_local_amount", "type": "STRING"},
        {"name": "execute_caller", "type": "STRING"},
        {"name": "execute_transaction_hash", "type": "STRING"},
        {"name": "execute_timestamp", "type": "STRING"},
        {"name": "execute_gas_price", "type": "STRING"},
        {"name": "execute_gas_limit", "type": "STRING"},
        {"name": "execute_block_number", "type": "STRING"},
        {"name": "execute_origin_sender", "type": "STRING"},
        {"name": "reconcile_caller", "type": "STRING"},
        {"name": "reconcile_transaction_hash", "type": "STRING"},
        {"name": "reconcile_timestamp", "type": "STRING"},
        {"name": "reconcile_gas_price", "type": "STRING"},
        {"name": "reconcile_gas_limit", "type": "STRING"},
        {"name": "reconcile_block_number", "type": "STRING"},
        {"name": "update_time", "type": "STRING"},
        {"name": "delegate", "type": "STRING"},
        {"name": "message_hash", "type": "STRING"},
        {"name": "canonical_domain", "type": "STRING"},
        {"name": "slippage", "type": "STRING"},
        {"name": "origin_sender", "type": "STRING"},
        {"name": "bridged_amt", "type": "STRING"},
        {"name": "normalized_in", "type": "STRING"},
        {"name": "canonical_id", "type": "STRING"},
        {"name": "router_fee", "type": "STRING"},
        {"name": "xcall_tx_origin", "type": "STRING"},
        {"name": "execute_tx_origin", "type": "STRING"},
        {"name": "reconcile_tx_origin", "type": "STRING"},
        {"name": "relayer_fee", "type": "STRING"},
        {"name": "error_status", "type": "STRING"},
        {"name": "backoff", "type": "STRING"},
        {"name": "next_execution_timestamp", "type": "STRING"},
        {"name": "updated_slippage", "type": "STRING"},
        {"name": "execute_simulation_input", "type": "STRING"},
        {"name": "execute_simulation_from", "type": "STRING"},
        {"name": "execute_simulation_to", "type": "STRING"},
        {"name": "execute_simulation_network", "type": "STRING"},
        {"name": "error_message", "type": "STRING"},
        {"name": "relayer_fees", "type": "JSON"},
        {"name": "message_status", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "execute_tx_nonce", "type": "STRING"},
        {"name": "reconcile_tx_nonce", "type": "STRING"},
        {"name": "price", "type": "STRING"},
        {"name": "closet_price_rank", "type": "STRING"},
    ]


if __name__ == "__main__":
    # run_storage_pipeline(s
    #     data_csv_filename="transfers_final_add_on",
    #     sql_file_name="transfers_raw_only_price_addon",
    #     start="2022-12",
    #     end="2023-11",
    # )

    df = pd.read_csv(
        filepath_or_buffer=f"{str(PurePath(CURRENT_DIR))}/data/transfers_final_add_on.csv"
    )
    df = df.astype("str")
    # df["receive_local"] = df["receive_local"].astype("bool")
    # df.drop(columns=["datastream_metadata"], inplace=True)
    # df = df.astype(
    #     {
    #         "backoff": "str",
    #         "bridged_amt": "str",
    #         "call_data": "str",
    #         "canonical_domain": "str",
    #         "canonical_id": "str",
    #         "closet_price_rank": "str",
    #         "delegate": "str",
    #         "destination_chain": "str",
    #         "destination_domain": "str",
    #         "destination_local_amount": "str",
    #         "destination_local_asset": "str",
    #         "destination_transacting_amount": "str",
    #         "destination_transacting_asset": "str",
    #         "error_message": "str",
    #         "error_status": "str",
    #         "execute_block_number": "str",
    #         "execute_caller": "str",
    #         "execute_gas_limit": "str",
    #         "execute_gas_price": "str",
    #         "execute_origin_sender": "str",
    #         "execute_simulation_from": "str",
    #         "execute_simulation_input": "str",
    #         "execute_simulation_network": "str",
    #         "execute_simulation_to": "str",
    #         "execute_timestamp": "str",
    #         "execute_transaction_hash": "str",
    #         "execute_tx_nonce": "str",
    #         "execute_tx_origin": "str",
    #         "message_hash": "str",
    #         "message_status": "str",
    #         "next_execution_timestamp": "str",
    #         "nonce": "str",
    #         "normalized_in": "str",
    #         "origin_bridged_amount": "str",
    #         "origin_bridged_asset": "str",
    #         "origin_chain": "str",
    #         "origin_domain": "str",
    #         "origin_sender": "str",
    #         "origin_transacting_amount": "str",
    #         "origin_transacting_asset": "str",
    #         "price": "str",
    #         "receive_local": "bool",
    #         "reconcile_block_number": "str",
    #         "reconcile_caller": "str",
    #         "reconcile_gas_limit": "str",
    #         "reconcile_gas_price": "str",
    #         "reconcile_timestamp": "str",
    #         "reconcile_transaction_hash": "str",
    #         "reconcile_tx_nonce": "str",
    #         "reconcile_tx_origin": "str",
    #         "relayer_fee": "str",
    #         "relayer_fees": "str",
    #         "router_fee": "str",
    #         "slippage": "str",
    #         "status": "str",
    #         "to": "str",
    #         "transfer_id": "str",
    #         "update_time": "str",
    #         "updated_slippage": "str",
    #         "xcall_block_number": "str",
    #         "xcall_caller": "str",
    #         "xcall_gas_limit": "str",
    #         "xcall_gas_price": "str",
    #         "xcall_timestamp": "str",
    #         "xcall_transaction_hash": "str",
    #         "xcall_tx_origin": "str",
    #     },
    # )
    # df.to_csv(f"{str(PurePath(CURRENT_DIR))}/data/transfers_final_add_on_v3.csv")

    upload_df_2_bq(
        df, table_name="mainnet-bigq.stage.stg_transfers_raw__approx_hour_price"
    )
