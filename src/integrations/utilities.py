from google.cloud import secretmanager
from google.cloud import storage
import json
from datetime import datetime


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
