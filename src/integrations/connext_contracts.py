import requests
import re
import json


# Fetch the TypeScript file content
def fetch_ts_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Error fetching file: {response.status_code}")
        return None


# Extract 'assets' list using regular expressions
def extract_assets_list(ts_code):
    pattern = r"assets\s*=\s*\[(.*?)\];"
    match = re.search(pattern, ts_code, re.DOTALL)
    if match:
        assets_str = match.group(1)
        # Assuming the assets are strings or simple objects, convert to JSON
        assets_list = json.loads(f"[{assets_str}]")
        return assets_list
    else:
        print("Assets list not found.")
        return None


# Example usage
ts_file_url = "https://raw.githubusercontent.com/connext/monorepo/main/packages/deployments/contracts/src/cli/init/config/mainnet/production.ts"
ts_code = fetch_ts_file(ts_file_url)
print(ts_code)
if ts_code:
    assets_list = extract_assets_list(ts_code)
    if assets_list:
        print("Assets list found:")
        print(json.dumps(assets_list, indent=2))
    else:
        print("Assets list not found.")
else:
    print("Failed to fetch TypeScript file.")
