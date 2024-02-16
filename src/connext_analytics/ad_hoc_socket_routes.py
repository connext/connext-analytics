from pprint import pprint
import pandas as pd
from src.integrations.utilities import get_raw_from_bq


import itertools


def get_all_combinations():
    # Generate permutations for b and d
    b_perms = list(itertools.permutations(range(1, 12)))
    d_perms = list(itertools.permutations(range(1, 12)))

    # Iterate over all permutations for b and d
    for b_perm in b_perms:
        for d_perm in d_perms:
            # Ensure b and d are not the same permutation
            if b_perm != d_perm:
                # Iterate over all values for a and c
                for a in range(1, 7):
                    for c in range(1, 6):
                        # Create the combination tuple
                        combination = (a, b_perm, c, d_perm)


if __name__ == "__main__":
    print("Running ad_hoc_socket_routes.py")
    # df_all_socket_routes = get_raw_from_bq(sql_file_name="all_socket_routes")
    # df_all_socket_routes.to_csv("data/all_socket_routes.csv", index=False)

    # Pull daa from csv file above to a dataframe
    # df_all_socket_routes = pd.read_csv("data/cs_all_socket_routes.csv")
    # exploded = df_all_socket_routes["usertxs"].explode().to_frame()
    # exploded.reset_index(inplace=True)
    # exploded.rename(columns={"index": "org_index"}, inplace=True)
    # df_expanded = pd.json_normalize(exploded["usertxs"])
    # df_combined = exploded.join(df_expanded).add_prefix("asset_")

    # # df_expanded.to_csv("data/cs_all_socket_routes_exploded.csv", index=False)

    # pprint(df_expanded.columns)
    # pprint(df_expanded.shape)
    pprint(len(get_all_combinations()))
