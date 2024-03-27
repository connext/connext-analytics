import requests
import re
import json
import pandas as pd
import pandas_gbq
import logging

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "mainnet-bigq"
url = "https://raw.githubusercontent.com/connext/monorepo/main/packages/deployments/contracts/src/cli/init/config/mainnet/production.ts"


def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
        return response.text
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None


def fetch_and_extract_allowlist_with_comments(url):
    try:

        typescript_code = fetch_url_content(url)

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


def fetch_and_extract_mainnet_production_init_config(url):
    try:
        typescript_code = fetch_url_content(url)

        pattern = r"assets:\s*\[([\s\S]*?)\]"

        assets_match = re.search(pattern, typescript_code, re.DOTALL)

        if assets_match:
            assets_data = assets_match.group(1).strip()
            # Remove "cap:" elements and their values
            assets_data_cleaned = re.sub(
                r'\bcap:\s*utils\.parseUnits\("[^"]+",\s*\d+\)\.toString\(\),?',
                "",
                assets_data,
            )
            # Remove comments
            assets_data_no_comments = re.sub(r"//.*", "", assets_data_cleaned)
            # Convert keys to strings (assuming keys are simple words)
            assets_data_json_compatible = re.sub(
                r"(\s*)(\w+)(\s*:\s*)", r'\1"\2"\3', assets_data_no_comments
            )
            # Remove trailing commas before closing braces
            assets_data_json_compatible = re.sub(
                r",(\s*})", r"\1", assets_data_json_compatible
            )

            # # Remove trailing commas before closing braces and brackets
            # assets_data_json_compatible = re.sub(
            #     r",(\s*[}\]])", r"\1", assets_data_json_compatible
            # )
            # assets_data_json_compatible = re.sub(
            #     r",(\s*})", r"\1", assets_data_json_compatible
            # )
            assets_data_json_compatible = assets_data_json_compatible[:-1]

            # Parse the JSON-compatible string to a Python dictionary
            assets_list = json.loads(f"[{assets_data_json_compatible}]")
            return assets_list
        else:
            print("Assets list not found in the TypeScript code.")
            return None
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
        return None


def fetch_and_transform_supported_domains(url):
    try:

        content = fetch_url_content(url)

        # Adjusted pattern to match the supportedDomains section more accurately
        pattern = r"supportedDomains:\s*\[\s*((?:\"?\d+\"?,?\s*//\s*\w+.*\n\s*)+)]"

        match = re.search(pattern, content, re.MULTILINE | re.DOTALL)

        if match:
            domains_section = match.group(1)

            # Adjusted pattern to match each domain and its comment (chain name)
            domain_pattern = r'"?(\d+)"?,?\s*//\s*(\w+.*)'

            # Transform each matched domain into the desired JSON-like object format
            transformed_domains = []
            for domain_id, chain_name in re.findall(domain_pattern, domains_section):
                # Clean up the chain name by removing any trailing spaces or comments
                chain_name_cleaned = chain_name.strip()
                transformed_domains.append(
                    {"id": domain_id, "chain": chain_name_cleaned}
                )

            # Convert the list of domain objects into a JSON string for display or use
            transformed_json = json.dumps(transformed_domains, indent=2)
            return transformed_json
        else:
            print("supportedDomains section not found.")
            return None
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None
    except Exception as err:
        print(f"An error occurred: {err}")
        return None


def get_routes_allowed_list_with_names():

    allowlist_with_comments = fetch_and_extract_allowlist_with_comments(url)
    if allowlist_with_comments is not None:
        return allowlist_with_comments


def get_mainnet_production_init_config():

    mainnet_production_init_config = fetch_and_extract_mainnet_production_init_config(
        url
    )
    if mainnet_production_init_config is not None:
        return mainnet_production_init_config


def get_supported_domains():

    supported_domains = fetch_and_transform_supported_domains(url)
    if supported_domains is not None:
        return json.loads(supported_domains)


def get_prod_mainmet_config_metadata():

    # -------
    # Routers
    # -------
    prod_mainnnet_routes_config = get_routes_allowed_list_with_names()
    clean_prod_mainnnet_routes_config = [
        {"address": key, "name": value}
        for key, value in prod_mainnnet_routes_config.items()
    ]

    df_prod_mainnet_routes_config = pd.DataFrame(clean_prod_mainnnet_routes_config)
    df_prod_mainnet_routes_config["upload_timestamp"] = pd.to_datetime("now", utc=True)
    pandas_gbq.to_gbq(
        dataframe=df_prod_mainnet_routes_config,
        project_id=PROJECT_ID,
        destination_table="raw.source_monorepo__prod_mainnet_routes_config",
        if_exists="append",
        chunksize=100000,
        api_method="load_csv",
    )
    logging.info("Data loaded to BigQuery for prod_mainnet_routes_config")

    # Supported Domains
    # ----------------------------
    supported_domains = get_supported_domains()
    df_supported_domains = pd.DataFrame(supported_domains)
    df_supported_domains["upload_timestamp"] = pd.to_datetime("now", utc=True)
    pandas_gbq.to_gbq(
        dataframe=df_supported_domains,
        project_id=PROJECT_ID,
        destination_table="raw.source_monorepo__prod_mainnet_supported_domains",
        if_exists="append",
        chunksize=100000,
        api_method="load_csv",
    )
    logging.info("Data loaded to BigQuery for prod_mainnet_supported_domains")

    # Assets
    # ----------------------------
    assets = get_mainnet_production_init_config()

    # Flatten the representations into rows
    flattened_data = []
    for asset in assets:

        for k, v in asset["representations"].items():
            # Combine asset name and canonical information with each representation
            flattened_row = {
                "name": asset["name"],
                "canonical_domain": asset["canonical"]["domain"],
                "canonical_address": asset["canonical"]["address"],
                "canonical_decimals": asset["canonical"]["decimals"],
                "representation_domain": k,
                "representation_local": v["local"],
                "representation_adopted": v["adopted"],
            }

            flattened_data.append(flattened_row)
    df = pd.DataFrame(flattened_data)
    # df.to_csv("data/source_monorepo__prod_mainnet_assets.csv", index=False)
    df["upload_timestamp"] = pd.to_datetime("now", utc=True)
    pandas_gbq.to_gbq(
        dataframe=df,
        project_id=PROJECT_ID,
        destination_table="raw.source_monorepo__prod_mainnet_assets",
        if_exists="append",
        chunksize=100000,
        api_method="load_csv",
    )
    logging.info("Data loaded to BigQuery for prod_mainnet_assets")

    logging.info("Pipeline completed")
