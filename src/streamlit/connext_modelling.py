import streamlit as st
import plotly.express as px
from utility import get_raw_data_from_bq_df

st.set_page_config(layout="wide")
st.title("Connext Modelling Service")


# Example of adding Markdown
st.markdown(
    """
    ## Question to Answer

    """
)


# Move this code to Pages:

raw_data_cannonical_bridges_hourly = get_raw_data_from_bq_df(
    "raw_data_cannonical_bridges_hourly"
)
st.markdown("### Raw data:")
st.data_editor(raw_data_cannonical_bridges_hourly.head(100))

# Calculate mean and median of value_usd
mean_value_usd = raw_data_cannonical_bridges_hourly["value_usd"].mean()
median_value_usd = raw_data_cannonical_bridges_hourly["value_usd"].median()

st.write(f"Mean of value_usd: {mean_value_usd}")
st.write(f"Median of value_usd: {median_value_usd}")

# Scatter plot for value_usd against src_chain and dst_chain using Plotly
fig = px.scatter(
    raw_data_cannonical_bridges_hourly,
    x="src_chain",
    y="value_usd",
    color="dst_chain",
    labels={
        "src_chain": "Source Chain",
        "dst_chain": "Destination Chain",
        "value_usd": "Value (USD)",
    },
    title="Value USD by Source and Destination Chains",
)
st.plotly_chart(fig)
                        