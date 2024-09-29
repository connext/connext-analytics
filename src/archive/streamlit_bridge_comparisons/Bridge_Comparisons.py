import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from setup import bridge_comparisons_raw


def format_millions(value):
    return f"{value / 1_000_000:.2f}M"


def apply_filters(df):
    st.sidebar.title("Filters")
    # Create a date range for the slider
    start_date = pd.to_datetime("2024-01-01").to_pydatetime()
    end_date = pd.to_datetime("2024-07-01").to_pydatetime()

    # Create a monthly slider
    selected_date = st.sidebar.slider(
        "Select a Month:",
        min_value=start_date,
        max_value=end_date,
        value=pd.to_datetime("2024-07-01").to_pydatetime(),
        format="MMM YYYY",
    )
    st.markdown(f"**Showing data for month of {selected_date.strftime('%B %Y')}**")
    # Filter the dataframe based on the selected date
    df["date"] = pd.to_datetime(df["date"])
    filter_df = df[
        df["date"].dt.to_period("M") == pd.to_datetime(selected_date).to_period("M")
    ]

    # Add a filter for the origin chain
    origin_chain = st.sidebar.multiselect(
        "Select an Origin Chain:", df["origin_chain"].unique()
    )
    if origin_chain:
        filter_df = filter_df[filter_df["origin_chain"].isin(origin_chain)]

    # Add a filter for the destination chain
    destination_chain = st.sidebar.multiselect(
        "Select a Destination Chain:", df["destination_chain"].unique()
    )
    if destination_chain:
        filter_df = filter_df[filter_df["destination_chain"].isin(destination_chain)]

    # bridges
    bridges = st.sidebar.multiselect("Select a Bridges:", df["bridge"].unique())
    if bridges:
        filter_df = filter_df[filter_df["bridge"].isin(bridges)]
    return filter_df


def get_clean_df(df):
    df.rename(
        columns={
            "currency_symbol": "token",
            "source_chain_name": "origin_chain",
            "destination_chain_name": "destination_chain",
        },
        inplace=True,
    )
    return df


def aggregate_flow(df):
    df_agg = (
        df.groupby(["origin_chain", "destination_chain", "token"])
        .agg(
            {
                "total_txs": "sum",
                "tx_count_10k_txs": "sum",
                "volume_10k_txs": "sum",
                "avg_volume_10k_txs": "mean",
                "avg_volume": "mean",
                "total_volume": "sum",
            }
        )
        .sort_values(by="total_volume", ascending=False)
    )

    return df_agg.reset_index().round(0)


def show_sankey_plot(df, metric):
    """How:
    for a selected metric, show three level sankey using
    - origin chain
    - token
    - destination chain
    """
    # Define the nodes
    # Add prefixes to origin_chain and destination_chain
    df["from"] = "from_" + df["origin_chain"].astype(str)
    df["to"] = "to_" + df["destination_chain"].astype(str)

    origin_nodes = list(set(df["from"]))
    token_nodes = list(set(df["token"]))
    destination_nodes = list(set(df["to"]))

    all_nodes = origin_nodes + token_nodes + destination_nodes
    node_indices = {node: idx for idx, node in enumerate(all_nodes)}

    # Define the links
    links = {"source": [], "target": [], "value": []}

    for _, row in df.iterrows():
        origin_idx = node_indices[row["from"]]
        token_idx = node_indices[row["token"]]
        destination_idx = node_indices[row["to"]]

        # Origin to Token
        links["source"].append(origin_idx)
        links["target"].append(token_idx)
        links["value"].append(row[metric])

        # Token to Destination
        links["source"].append(token_idx)
        links["target"].append(destination_idx)
        links["value"].append(row[metric])

    # Create the Sankey diagram
    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=all_nodes,
                ),
                link=dict(
                    source=links["source"], target=links["target"], value=links["value"]
                ),
            )
        ]
    )

    fig.update_layout(
        title_text="Sankey Diagram of Bridge Pathways",
        font_size=10,
        height=1200,
    )

    st.plotly_chart(fig)  # Make the plot bigger


def main() -> None:
    st.title("Bridge Pathway Comparisons")
    clean_df = get_clean_df(df=bridge_comparisons_raw)
    filtered_df = apply_filters(df=clean_df)
    agg_df = aggregate_flow(df=filtered_df)
    st.dataframe(
        agg_df,
        height=500,
        width=1500,
        column_config={
            "total_volume": st.column_config.NumberColumn(
                format="$ %d",
            )
        },
    )

    st.markdown("#### Select Metrics for Flow")
    metric = st.selectbox(
        "",
        options=[
            "total_txs",
            "tx_count_10k_txs",
            "volume_10k_txs",
            "avg_volume_10k_txs",
            "avg_volume",
            "total_volume",
        ],
    )

    show_sankey_plot(df=agg_df, metric=metric)
    return None


if __name__ == "__main__":
    main()
