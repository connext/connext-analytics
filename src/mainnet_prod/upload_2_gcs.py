from google.cloud import storage
import os


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


if __name__ == "__main__":
    bucket_name = os.getenv("GCS_BUCKET")
    source_file_name = "/app/output.json"
    destination_blob_name = "prod-config/config.json"

    upload_blob(bucket_name, source_file_name, destination_blob_name)
