import requests
import re
import json

from src.integrations.utilities import (
    upload_json_to_gcs,
)


def fetch_and_extract_allowlist_with_comments(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        typescript_code = response.text

        pattern = r"routers:\s*{\s*allowlist:\s*\[([^\]]+)\]"

        allowlist_match = re.search(pattern, typescript_code, re.DOTALL)

        if allowlist_match:
            allowlist_data = allowlist_match.group(1).strip()
            # Match addresses and optional comments
            entries = re.findall(
                r'"(0x[a-fA-F0-9]{40})",?\s*(//\s*(.*))?', allowlist_data
            )

            routers = {}
            for entry in entries:
                address, _, comment = entry
                # Use the address as key and comment as value; if no comment, use an empty string
                routers[address] = comment.strip() if comment else ""

            return routers
        else:
            print("Allowlist array not found in the TypeScript code.")
            return None
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")


def get_routes_allowed_list_with_names():

    url = "https://raw.githubusercontent.com/connext/monorepo/main/packages/deployments/contracts/src/cli/init/config/mainnet/production.ts"
    allowlist_with_comments = fetch_and_extract_allowlist_with_comments(url)
    if allowlist_with_comments is not None:
        return allowlist_with_comments


def fetch_and_extract_mainnet_production_init_config(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        typescript_code = response.text

        # Updated pattern to be more flexible with spaces and line breaks
        pattern = r"MAINNET_PRODUCTION_INIT_CONFIG: InitConfig\s*=\s*({[\s\S]*?});"

        config_match = re.search(pattern, typescript_code)

        if config_match:
            config_data = config_match.group(1).strip()
            # Attempt to directly parse the JSON, replacing single quotes with double quotes
            # This might still fail if the object contains comments or trailing commas
            config_dict = json.loads(config_data.replace("'", '"'))
            return config_dict
        else:
            print(
                "MAINNET_PRODUCTION_INIT_CONFIG variable not found in the TypeScript code."
            )
            return None
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")


def get_mainnet_production_init_config():
    url = "https://raw.githubusercontent.com/connext/monorepo/main/packages/deployments/contracts/src/cli/init/config/mainnet/production.ts"
    mainnet_production_init_config = fetch_and_extract_mainnet_production_init_config(
        url
    )
    if mainnet_production_init_config is not None:
        return mainnet_production_init_config


def upload_blob():

    source_file_name = "/app/output.json"
    with open(source_file_name, "r") as file:
        config = json.load(file)

    upload_json_to_gcs(config, "connex-monorepo")


if __name__ == "__main__":
    upload_blob()
