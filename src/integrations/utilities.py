import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq
import pytz
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import bigquery, secretmanager, storage
from jinja2 import Template

logging.basicConfig(level=logging.INFO)


def get_secret_gcp_secrete_manager(secret_name: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/mainnet-bigq/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DeadlineExceeded:
        logging.error("Request to Secret Manager timed out.")
        raise
    except Exception as e:
        logging.info(f"Error accessing secret {secret_name}: {e}")


def upload_to_gcs_via_folder(data, bucket_name, folder_name):
    "uploading within a folder, Name for the upload within that folder is the timestamp of upload"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Generate a filename with the current date and time
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{folder_name}/{current_time}.json"

    # Get the blob (file) where you want to store the JSON data
    blob = bucket.blob(filename)

    blob.upload_from_string(json.dumps(data), content_type="application/json")
    logging.info(f"Uploaded {filename} to {bucket_name}")


def upload_json_to_gcs(data, bucket_name):
    # Convert data to JSON
    json_data = json.dumps(data)

    # Create a GCS client
    storage_client = storage.Client()

    # Get the bucket - if no bucket then create it
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as e:
        logging.info(f"Bucket {bucket_name} not found, creating it. {e}")
        bucket = storage_client.create_bucket(bucket_name)
        logging.info(f"Bucket {bucket_name} created.")
        bucket = storage_client.get_bucket(bucket_name)

    # Generate a filename with the current date and time
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{current_time}.json"

    # Get the blob (file) where you want to store the JSON data
    blob = bucket.blob(filename)

    # Upload the JSON data to the file
    blob.upload_from_string(json_data, content_type="application/json")


def read_sql_from_file_add_template(sql_file_name, template_data: dict) -> str:
    """
    Get SQL query from SQL file and apply Jinja2 templating.
    """
    try:
        sql_dir = os.path.join("src", "sql", f"{sql_file_name}.sql")

        with open(sql_dir) as sql_file:
            file_content = sql_file.read()
            query = Template(file_content).render(template_data)
            return query

    except FileNotFoundError:
        print(f"The file {sql_dir} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def get_raw_from_bq(sql_file_name) -> pd.DataFrame:
    with open(f"src/sql/{sql_file_name}.sql") as file:
        sql = file.read()

    return pandas_gbq.read_gbq(sql)


def pull_data_from_gcp_cs():
    # Old data compare:
    # "gs://lifi_routes/2024-02-05_20-50-22.json"
    # storage_client = storage.Client()
    # bucket = storage_client.get_bucket("lifi_routes")
    # blob = bucket.blob("2024-02-05_20-50-22.json")
    # data = json.loads(blob.download_as_text())
    # df = convert_json_to_df(json_file=data)
    # print(df)
    pass


def run_bigquery_query(sql_query: str):
    """
    Run a SQL query in BigQuery using ADC.

    Args:
        sql_query (str): The SQL query to execute.

    Returns:
        List[Dict]: A list of dictionaries representing the rows returned by the query.
    """
    try:
        client = bigquery.Client()
        query_job = client.query(sql_query)
        query_job.result()
        logging.info("Query completed successfully.")

        return {"message": "Query completed successfully."}

    except Exception as e:
        logging.info(f"An error occurred while running the query: {e}")
        return {"message": f"An error occurred while running the query: {e}"}


def nearest_power_of_ten(value):
    log_value = np.log10(float(value))
    rounded_log = np.round(log_value)
    power_of_ten = np.power(10, rounded_log)
    # formatted_result = format(float(power_of_ten), ".15f")
    return power_of_ten


def convert_lists_and_booleans_to_strings(df):
    "Also convert to lower and . to _"
    for col in df.columns:
        # Check if the column contains lists
        if df[col].apply(type).eq(list).any():
            df[col] = df[col].astype(str)
        # Check if the column contains booleans
        elif df[col].apply(type).eq(bool).any():
            df[col] = df[col].astype(str)

    df.columns = [col.lower().replace(".", "_") for col in df.columns]
    return df


def get_latest_value_from_bq_table_by_col(
    table_id: str, col: int, base_val: int
) -> datetime:
    """
    base_val: int = 1704067200
        THis is start of 2024 in unix time
    The sql used is same as routes Helper module: get_latest_by_bq_table_and_date_col
    """
    try:
        sql = read_sql_from_file_add_template(
            sql_file_name="get_latest_by_bq_table_and_date_col",
            template_data={"date_col": col, "table_id": table_id},
        )
        logging.info(f"Running query: {sql}")
        df = pandas_gbq.read_gbq(sql)
        final_start_date = np.array(df[col])[0]

        # if value is int then return, if date convert to int and return
        if isinstance(final_start_date, np.int64):
            logging.info(f"Instace is int, {final_start_date}")
            return final_start_date
        elif isinstance(final_start_date, str):
            dt = datetime.strptime(final_start_date, "%Y-%m-%d %H:%M:%S.%f UTC")
            utc_dt = pytz.utc.localize(dt)  # Explicitly localize the datetime to UTC
            logging.info(f"Instance is string, {utc_dt}, {int(utc_dt.timestamp())}")
            return int(utc_dt.timestamp())
        else:
            raise ValueError(f"Invalid type: {type(final_start_date)}")

    except pandas_gbq.exceptions.GenericGBQException as e:
        if "Reason: 404" in str(e):
            logging.info(f"The table {table_id} was not found.")
            logging.info(f"The base value {base_val} was returned.")
            return base_val
        else:
            raise

    # ValueError: NaTType does not support timestamp
    except ValueError as e:
        logging.info(f"ValueError: {e}")
        return datetime.fromtimestamp(base_val).astimezone(pytz.UTC).timestamp()


def to_snake_case(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def remove_duplicate_rows_by_col(table_id: str, col: list):
    """
    Remove duplicate rows from a dataframe by a specific column.
    """

    columns_str = ", ".join(col)
    data = {"table_id": table_id, "columns": columns_str}
    sql = f"SELECT DISTINCT {columns_str} FROM `{table_id}`"
    query = Template(sql).render(data)
    df = pandas_gbq.read_gbq(query)
    pandas_gbq.to_gbq(df, table_id, project_id="mainnet-bigq", if_exists="replace")
    logging.info(f"{df.shape} rows adjusted in {table_id}")


def pydantic_schema_to_list(schema):
    result = []
    properties = schema.get("properties", {})

    for field_name, field_info in properties.items():
        field_type = "STRING"  # Default type

        if "anyOf" in field_info:
            types = [t.get("type") for t in field_info["anyOf"] if "type" in t]
            formats = [t.get("format") for t in field_info["anyOf"] if "format" in t]

            if "integer" in types:
                field_type = "INTEGER"
            elif "number" in types:
                field_type = "FLOAT"
            elif "boolean" in types:
                field_type = "BOOLEAN"
            elif "date-time" in formats:
                field_type = "TIMESTAMP"

        elif "type" in field_info:
            if field_info["type"] == "integer":
                field_type = "INTEGER"
            elif field_info["type"] == "number":
                field_type = "FLOAT"
            elif field_info["type"] == "boolean":
                field_type = "BOOLEAN"
            elif field_info.get("format") == "date-time":
                field_type = "TIMESTAMP"

        result.append({"name": field_name, "type": field_type})

    return result


# if __name__ == "__main__":
#     print(
#         remove_duplicate_rows_by_col(
#             table_id="mainnet-bigq.raw.source_all_bridge_explorer_transfers",
#             col=[
#                 "id",
#                 "status",
#                 "timestamp",
#                 "from_chain_symbol",
#                 "to_chain_symbol",
#                 "from_amount",
#                 "stable_fee",
#                 "from_token_address",
#                 "to_token_address",
#                 "from_address",
#                 "to_address",
#                 "messaging_type",
#                 "partner_id",
#                 "from_gas",
#                 "to_gas",
#                 "relayer_fee_in_native",
#                 "relayer_fee_in_tokens",
#                 "send_transaction_hash",
#                 "receive_transaction_hash",
#                 "api_url",
#             ],
#         )
#     )
