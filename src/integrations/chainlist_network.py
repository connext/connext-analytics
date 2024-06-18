import requests
import pandas as pd
import pandas_gbq as gbq
import logging

PROJECT_ID = "mainnet-bigq"
logging.basicConfig(level=logging.INFO)


def fetch_data():
    # URL of the JSON data
    url = "https://chainid.network/page-data/sq/d/2609099887.json"

    # Fetch the JSON data
    response = requests.get(url)
    data = response.json()

    # Extract the relevant data from the JSON
    chains = data["data"]["allChain"]["nodes"]

    # Create a list to store the flattened data
    flattened_data = []

    for chain in chains:
        flattened_chain = {
            "id": chain.get("id"),
            "name": chain.get("name"),
            "chain": chain.get("chain"),
            "chainId": chain.get("chainId"),
            "rpc": ", ".join(chain.get("rpc", [])),
            "icon": (
                chain.get("icon", {}).get("publicURL") if chain.get("icon") else None
            ),
            "nativeCurrency_decimals": chain.get("nativeCurrency", {}).get("decimals"),
            "nativeCurrency_name": chain.get("nativeCurrency", {}).get("name"),
            "nativeCurrency_symbol": chain.get("nativeCurrency", {}).get("symbol"),
            "explorers": (
                ", ".join(
                    [explorer.get("url") for explorer in chain.get("explorers", [])]
                )
                if chain.get("explorers")
                else None
            ),
            "status": chain.get("status"),
            "faucets": ", ".join(chain.get("faucets", [])),
        }
        flattened_data.append(flattened_chain)

    return flattened_data


def main():
    data = fetch_data()
    df = pd.DataFrame(data)
    logging.info(f"Pushing {len(df)} chains to BQ")
    gbq.to_gbq(
        dataframe=df,
        project_id=PROJECT_ID,
        destination_table="raw.source_chainlist_network__chains",
        if_exists="replace",
    )


if __name__ == "__main__":
    main()
