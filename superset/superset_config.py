import logging
import os

from google.api_core.exceptions import DeadlineExceeded
from google.cloud import secretmanager


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


# Database Configuration
SQLALCHEMY_DATABASE_URI = get_secret_gcp_secrete_manager(
    "SUPERSET_METATDATA_DATABASE_URL"
)

# Superset Configuration
SECRET_KEY = get_secret_gcp_secrete_manager("SUPERSET_SECRET_KEY")
AUTH_USER_REGISTRATION_ROLE = get_secret_gcp_secrete_manager(
    "SUPERSET_AUTH_USER_REGISTRATION_ROLE"
)
ADMIN_PASSWORD = get_secret_gcp_secrete_manager("SUPERSET_ADMIN_PASSWORD")
# Additional configurations for production
ENABLE_PROXY_FIX = True
