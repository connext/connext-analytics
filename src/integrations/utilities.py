import os
import pandas as pd
import pandas_gbq
from google.cloud import secretmanager
from google.cloud import storage
import json
from datetime import datetime
from jinja2 import Template
from google.oauth2 import service_account


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


def read_sql_from_file_add_template(sql_file_name, template_data) -> str:
    """
    Get sql query from sql file
    """

    sql_dir = f"src/sql/{sql_file_name}.sql"

    with open(sql_dir, "r") as sql_file:
        file = sql_file.read()
        query = Template(file).render(template_data)
        return query


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
