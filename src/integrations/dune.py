import json
import pandas as pd
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from src.integrations.utilities import get_secret_gcp_secrete_manager

dune = DuneClient(api_key=get_secret_gcp_secrete_manager("source_DUNE_API_KEY_2"))
dune_query_id_native_evm_bridges_daily = 3537139


def get_result_by_query_id(id: int, from_date: str = "2024-03-01", new: bool = False):

    query = QueryBase(
        name="native EVM bridges ETH",
        query_id=id,
        params=[QueryParameter.text_type(name="from_date", value=from_date)],
    )
    if new:
        results = dune.run_query(query).get_rows()
    else:
        results = dune.get_latest_result(query).get_rows()

    with open(f"data/dune_query_result_{id}.json", "w") as f:
        json.dump(results, f, indent=4)

    return results[0]


if __name__ == "__main__":
    print(
        get_result_by_query_id(
            dune_query_id_native_evm_bridges_daily, from_date="2024-03-01", new=True
        )
    )

    with open("data/dune_query_result_3537139.json", "r") as f:
        results = json.load(f)

    df = pd.DataFrame(results)
    print(df.dtypes)
    print(df.shape)
