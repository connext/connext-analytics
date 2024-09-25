import requests
import pandas as pd
from bs4 import BeautifulSoup


class ChainsAssetsMetadata:
    def __init__(self, url: str):
        self.url = url

    def pull_registered_assets_data(self) -> pd.DataFrame:
        """
        Fetches and parses the registered assets table from the specified URL.

        Returns:
            pd.DataFrame: A DataFrame containing the registered assets metadata.
        """
        # Step 1: Fetch the webpage content
        response = requests.get(self.url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        content = response.content

        # Step 2: Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(content, "html.parser")

        # Step 3: Locate the 'Registered Assets' section and find the table
        registered_assets_header = soup.find("h2", id="registered-assets")
        if not registered_assets_header:
            raise ValueError(
                "Couldn't find the 'Registered Assets' section in the HTML."
            )

        # Assuming the table is immediately after the header
        table = registered_assets_header.find_next("table")
        if not table:
            raise ValueError(
                "Couldn't find the table under the 'Registered Assets' section."
            )

        # Step 4: Loop through the rows and columns of the table to gather the data
        table_data = []
        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if cols:  # Ensure the row has data cells
                cols_text = [col.get_text(strip=True) for col in cols]
                table_data.append(cols_text)

        # Step 5: Convert the data to a pandas DataFrame
        df = pd.DataFrame(table_data)
        df.columns = [
            "asset_name",
            "symbol",
            "decimals",
            "domain_id",
            "address",
            "faucet",
            "faucet_limit",
        ]

        return df

    def save_to_csv(self, df: pd.DataFrame, filepath: str):
        """
        Saves the DataFrame to a specified CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            filepath (str): The path where the CSV will be saved.
        """
        df.to_csv(filepath, index=False)
        print(f"Data has been saved to '{filepath}'.")


# if __name__ == "__main__":
#     metadata_scraper = ChainsAssetsMetadata(
#         url="https://docs.everclear.org/resources/contracts/mainnet"
#     )
#     df_assets = metadata_scraper.pull_registered_assets_data()
#     print(df_assets)
