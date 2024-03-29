import json
import requests


def fetch_all_data(api_url):
    # Initialize pagination variables
    limit = 10
    offset = 0
    total = None

    # Loop to fetch all pages of data
    while total is None or offset < total:
        # Construct the URL with the current offset
        paginated_url = f"{api_url}&limit={limit}&offset={offset}"

        # Make the request
        response = requests.get(paginated_url)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()

            # Process the data (example: print it)
            print(json.dumps(data, indent=4))

            # Update pagination variables
            offset += limit
            if total is None:
                total = data.get("total", 0)

        else:
            print(f"Failed to fetch data: {response.status_code}")
            break


# Example usage
api_url = "https://public.api.across.to/deposits/tx-page?status=filled&skipOldUnprofitable=true&limit=10&offset=0&orderBy=status"
fetch_all_data(api_url)
