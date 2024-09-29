import logging

from google.api_core.exceptions import DeadlineExceeded
from google.cloud import secretmanager

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
