# this pipeline pull data from url: https://api.everclear.org, invoice_url_ext = "/invoices"
# It uses httpx, pandas, and async to pull data from the API and load it into a pandas dataframe

import asyncio
import pandas as pd
import httpx
from typing import List, Dict, Optional, Tuple

# Constants
API_BASE_URL = "https://api.everclear.org"
INVOICE_ENDPOINT = "/invoices"


class EverclearAPIClient:
    """
    A client for interacting with the Everclear API.

    Attributes:
        client (httpx.AsyncClient): The HTTP client for making requests.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(base_url=API_BASE_URL)

    async def fetch_data(
        self,
        endpoint: str,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict], Optional[str], int]:
        """
        Fetch data from the specified API endpoint using a GET request with pagination.

        Args:
            endpoint (str): The API endpoint to fetch data from.
            cursor (Optional[str]): The cursor for pagination. Defaults to None.

        Returns:
            Tuple[List[Dict], Optional[str], int]:
                - A list of data dictionaries from the response.
                - The next cursor if available, otherwise None.
                - The maximum count of items per page.
        """
        params = {}
        if cursor:
            params["cursor"] = cursor

        try:
            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            invoices = data.get("invoices", [])
            next_cursor = data.get("nextCursor")

            return invoices, next_cursor
        except httpx.HTTPError as http_err:
            print(f"HTTP error occurred while fetching {endpoint}: {http_err}")
        except Exception as err:
            print(f"An unexpected error occurred while fetching {endpoint}: {err}")
        return [], None

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()


def load_data_into_dataframe(data: List[Dict]) -> pd.DataFrame:
    """
    Load data into a pandas DataFrame.

    Args:
        data (List[Dict]): A list of data dictionaries.

    Returns:
        pd.DataFrame: A DataFrame containing the data.
    """
    return pd.DataFrame(data)


async def fetch_all_invoices(client: EverclearAPIClient) -> List[Dict]:
    """
    Fetch all invoices by handling pagination using nextCursor.

    Args:
        client (EverclearAPIClient): The API client instance.

    Returns:
        List[Dict]: A list containing all invoice data.
    """
    all_invoices = []
    cursor = None

    while True:
        invoices, next_cursor = await client.fetch_data(INVOICE_ENDPOINT, cursor=cursor)
        all_invoices.extend(invoices)
        print(f"Fetched {len(invoices)} invoices. Total so far: {len(all_invoices)}")

        if not next_cursor:
            print("No more pages to fetch.")
            break

        cursor = next_cursor

    return all_invoices


async def get_all_active_invoices(save_to_csv: bool = False) -> pd.DataFrame:
    """
    Main function to execute the invoice fetching and loading pipeline with pagination.
    """
    client = EverclearAPIClient()
    try:
        all_invoices = await fetch_all_invoices(client)
        if all_invoices:
            df = load_data_into_dataframe(all_invoices)
            print("Loaded all invoices into DataFrame:")
            if save_to_csv:
                df.to_csv("data/invoices.csv", index=False)
            return df
        else:
            print("No invoices fetched.")
    finally:
        await client.close()
