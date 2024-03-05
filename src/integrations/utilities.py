import os
import logging
import numpy as np
import pandas as pd
import pandas_gbq
from google.cloud import secretmanager
from google.cloud import storage
import json
from datetime import datetime
from jinja2 import Template
from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)


def get_secret_gcp_secrete_manager(secret_name: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/mainnet-bigq/secrets/{secret_name}/versions/1"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def upload_json_to_gcs(data, bucket_name):
    # Convert data to JSON
    json_data = json.dumps(data)

    # Create a GCS client
    storage_client = storage.Client()

    # Get the bucket
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

        with open(sql_dir, "r") as sql_file:
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

    with open(f"src/sql/{sql_file_name}.sql", "r") as file:
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
    table_id: str, col: int, base_val: int = 1704067200
):
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
        df = pandas_gbq.read_gbq(sql)
        return np.array(df[col])[0]

    except pandas_gbq.exceptions.GenericGBQException as e:
        if "Reason: 404" in str(e):
            return base_val
        else:
            raise
