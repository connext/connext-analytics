import itertools
import json
from pprint import pprint

import pandas as pd
from google.cloud import storage

from src.integrations.utilities import convert_lists_and_booleans_to_strings


def get_data():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("socket_routes")
    blob = bucket.get_blob("2024-02-16_02-24-08.json")
    data = json.loads(blob.download_as_text())
    with open("data/ad_hoc_socket_routes.json", "w") as f:
        json.dump(data, f)
    return data


if __name__ == "__main__":
    print("Running ad_hoc_socket_routes.py")
    # json_blob = get_data()

    with open("data/ad_hoc_socket_routes.json") as f:
        json_blob = json.load(f)

    all_steps = []
    for r in json_blob:
        if "routes" in r["result"]:
            routes = r["result"]["routes"]
            for r in routes:
                if "userTxs" in r:
                    for u in r["userTxs"]:
                        if "steps" in u:
                            steps = u["steps"]
                            step_counter = 0
                            for step in steps:
                                step["step_id"] = step_counter
                                step["route_id"] = r["routeId"]
                                step["routePath"] = u["routePath"]
                                step["userTxIndex"] = u["userTxIndex"]
                                all_steps.append(step)
                                step_counter += 1

    steps_df = pd.json_normalize(all_steps)
    steps_df = convert_lists_and_booleans_to_strings(steps_df)
    steps_df.to_csv("data/ad_hoc_socket_routes_steps_norm.csv", index=False)
