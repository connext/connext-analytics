import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from setup import ALL_CONNEXT_TXS, apply_universal_sidebar_filters


@st.cache_data(ttl=86400)
def batch_txs(data: pd.DataFrame) -> pd.DataFrame:
    def create_batches(df: pd.DataFrame) -> pd.DataFrame:
        # Initialize variables
        batch_id = 1
        batches = []
        current_batch = []
        current_batch_start_time = df.iloc[0]["date"]

        # Group transactions into batches
        for idx, row in df.iterrows():
            if len(current_batch) < 20 and (
                row["date"] - current_batch_start_time
            ) <= timedelta(hours=3):
                current_batch.append((row["date"], batch_id))
            else:
                for timestamp, b_id in current_batch:
                    batches.append((timestamp, b_id))
                batch_id += 1
                current_batch = [(row["date"], batch_id)]
                current_batch_start_time = row["date"]

        # Append the last batch
        for timestamp, b_id in current_batch:
            batches.append((timestamp, b_id))

        # Create batches DataFrame
        batch_df = pd.DataFrame(batches, columns=["date", "batch_id"])

        # Merge the batch IDs back to the original DataFrame
        df = df.merge(batch_df, on="date")

        return df

    # Convert xcall_timestamp to datetime
    data["date"] = pd.to_datetime(data["date"])

    # Sort DataFrame by xcall_timestamp
    data = data.sort_values("date").reset_index(drop=True)

    # Group by chain and asset, then apply batching
    grouped = data.groupby(["chain", "asset"])
    batched_dfs = [create_batches(group) for _, group in grouped]

    # Concatenate all batched DataFrames
    result_df = pd.concat(batched_dfs).reset_index(drop=True)

    return result_df


def aggregate_batched_tx(data: pd.DataFrame, bs: int, nw: str):
    """
    INPUTS:
        data: pd.DataFrame
        bs: int
        nw: str
    OUTPUTS:
        df: pd.DataFrame

    Convert the raw tx data into aggregated data based on the batch size and netting window
    """
    data["tx_count"] = 1
    df_agg = (
        data.groupby(["batch_id", "chain", "asset", "asset_group"])
        .agg(
            first_tx_date=("date", "min"),
            last_tx_date=("date", "max"),
            volume=("amount", "sum"),
            volume_usd=("amount_usd", "sum"),
            tx_count=("tx_count", "sum"),
        )
        .reset_index()
        .sort_values(["first_tx_date", "last_tx_date"])
    )
    # time diff-> netting window hours(first date - last date)
    df_agg["netting_window_in_hours"] = df_agg["last_tx_date"] - df_agg["first_tx_date"]
    return df_agg


def keep_clean_data(df: pd.DataFrame):
    """
    INPUTS:
        df: pd.DataFrame
    OUTPUTS:
        df: pd.DataFrame
    """
    # remove rows with null values
    current_date = datetime.now()
    clean_df = df[df["first_tx_date"].dt.date < current_date.date()]
    # cols to keep:
    cols_to_keep = [
        "batch_id",
        "chain",
        "asset",
        "asset_group",
        "first_tx_date",
        "last_tx_date",
        "volume",
        "volume_usd",
        "tx_count",
    ]

    return clean_df[cols_to_keep]


def main():
    st.title("Batched Connext Data")
    netting_window = st.sidebar.select_slider(
        label="**Netting Window:**",
        options=["1-Hour", "3-Hour", "6-Hour", "12-Hour", "1-Day"],
        value="3-Hour",
    )

    batch_size = st.sidebar.select_slider(
        label="**Batch Size(# of Tx per batch):**",
        options=[
            i for i in range(10, 101, 10)
        ],  # Options from 10 to 100 with increments of 10
        value=20,  # Default value set to 20
    )

    st.subheader(
        "Batches of Tx aggreagte based on selected batch size and netting window"
    )

    # batch the txs -> adding cache to avoid re-running the function
    # filtered aggregated data
    raw_txs = apply_universal_sidebar_filters(df=ALL_CONNEXT_TXS, date_col="date")
    batched_txs = batch_txs(raw_txs)
    batched_txs_agg = aggregate_batched_tx(
        data=batched_txs, bs=batch_size, nw=netting_window
    )

    # showcase data
    st.markdown(
        """
    ### Aggregated Txs. by Batch
    **Columns:**
    - batch_id: unique id for each batch for every token <> chain pair
    - chain: chain of the tx
    - asset: asset of the tx
    - asset_group: asset group of the tx(for simplicity, we group same assets into one group)
    - first_tx_date: date of the first tx in the batch
    - last_tx_date: date of the last tx in the batch
    - volume: volume of the tx in the batch
    - volume_usd: volume of the tx in the batch in usd
    - tx_count: number of tx in the batch(**max 20 per batch**)

    *Note: Batch start date is the first tx date for the token chain pair.
    This is used after the filter is applied from life sidebar.*
    """
    )
    st.data_editor(keep_clean_data(batched_txs_agg), height=800, width=1500)


if __name__ == "__main__":
    main()
