# Adding the streamlit pages to the sidebar
import json
import os
import numpy as np
import pytz
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from google.cloud import secretmanager
from google.api_core.exceptions import DeadlineExceeded
import logging
from jinja2 import Template
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)


def generate_test_chain_mapping():
    chains = [
        {
            "Chain": "Everclear-Sepolia",
            "ChainID": 6398,
            "DomainID": 6398,
            "Role": "Hub",
        },
        {
            "Chain": "Sepolia",
            "ChainID": 11155111,
            "DomainID": 11155111,
            "Role": "Spoke",
        },
        {
            "Chain": "Chapel (BNB Testnet)",
            "ChainID": 97,
            "DomainID": 97,
            "Role": "Spoke",
        },
        {
            "Chain": "Optimism Sepolia",
            "ChainID": 11155420,
            "DomainID": 11155420,
            "Role": "Spoke",
        },
        {
            "Chain": "Arbitrum Sepolia",
            "ChainID": 421614,
            "DomainID": 421614,
            "Role": "Spoke",
        },
    ]

    return json.dumps(chains, indent=4)


def generate_test_token_mapping():
    tokens = [
        {
            "Asset Name": "Test Token",
            "Symbol": "TEST",
            "Decimals": 18,
            "DomainID": 11155111,
            "Address": "0x000000000000000000000000d26e3540A0A368845B234736A0700E0a5A821bBA",
            "Faucet": "(open mint)",
            "Faucet Limit": "N/A",
        },
        {
            "Asset Name": "Test Token",
            "Symbol": "TEST",
            "Decimals": 18,
            "DomainID": 97,
            "Address": "0x0000000000000000000000005f921E4DE609472632CEFc72a3846eCcfbed4ed8",
            "Faucet": "(open mint)",
            "Faucet Limit": "N/A",
        },
        {
            "Asset Name": "Test Token",
            "Symbol": "TEST",
            "Decimals": 18,
            "DomainID": 11155420,
            "Address": "0x0000000000000000000000007Fa13D6CB44164ea09dF8BCc673A8849092D435b",
            "Faucet": "(open mint)",
            "Faucet Limit": "N/A",
        },
        {
            "Asset Name": "Test Token",
            "Symbol": "TEST",
            "Decimals": 18,
            "DomainID": 421614,
            "Address": "0x000000000000000000000000aBF282c88DeD3e386701a322e76456c062468Ac2",
            "Faucet": "(open mint)",
            "Faucet Limit": "N/A",
        },
        {
            "Asset Name": "DecimalsTestToken",
            "Symbol": "DTT",
            "Decimals": 6,
            "DomainID": 11155111,
            "Address": "0x000000000000000000000000d18C5E22E67947C8f3E112C622036E65a49773ab",
            "Faucet": "0x277b67ce20c83e1ad9825c152762ba2b9b297aa6",
            "Faucet Limit": "100 * 1e6 per day",
        },
        {
            "Asset Name": "DecimalsTestToken",
            "Symbol": "DTT",
            "Decimals": 18,
            "DomainID": 97,
            "Address": "0x000000000000000000000000def63AA35372780f8F92498a94CD0fA30A9beFbB",
            "Faucet": "0xca7c45a3e5bdf9d6db5dae64c41204195879042f",
            "Faucet Limit": "100 * 1e18 per day",
        },
        {
            "Asset Name": "DecimalsTestToken",
            "Symbol": "DTT",
            "Decimals": 6,
            "DomainID": 11155420,
            "Address": "0x000000000000000000000000294FD6cfb1AB97Ad5EA325207fF1d0E85b9C693f",
            "Faucet": "0x1f150e59d87e53b6543cdaee964b7f4f074e2867",
            "Faucet Limit": "100 * 1e6 per day",
        },
        {
            "Asset Name": "DecimalsTestToken",
            "Symbol": "DTT",
            "Decimals": 6,
            "DomainID": 421614,
            "Address": "0x000000000000000000000000DFEA0bb49bcdCaE920eb39F48156B857e817840F",
            "Faucet": "0xc194c96430a43ebcd6a19c13813343f019492e5b",
            "Faucet Limit": "100 * 1e6 per day",
        },
        {
            "Asset Name": "TestxERC20",
            "Symbol": "xTEST",
            "Decimals": 18,
            "DomainID": 11155111,
            "Address": "0x0000000000000000000000008F936120b6c5557e7Cd449c69443FfCb13005751",
            "Faucet": "0x2de7cc4291078d1e49b41ee382ec702f5e29b6ff",
            "Faucet Limit": "1000000 * 1e18 per day",
        },
        {
            "Asset Name": "TestxERC20",
            "Symbol": "xTEST",
            "Decimals": 18,
            "DomainID": 97,
            "Address": "0x0000000000000000000000009064cD072D5cEfe70f854155d1b23171013be5c7",
            "Faucet": "0x3bb905a81de6928002e79cb2dd22badca5e78e2c",
            "Faucet Limit": "1000000 * 1e18 per day",
        },
        {
            "Asset Name": "TestxERC20",
            "Symbol": "xTEST",
            "Decimals": 18,
            "DomainID": 11155420,
            "Address": "0x000000000000000000000000D3D4c6845e297e99e219BD8e3CaC84CA013c0770",
            "Faucet": "0x4902cd7bacda179cd8b08f824124821c62a493fc",
            "Faucet Limit": "1000000 * 1e18 per day",
        },
        {
            "Asset Name": "TestxERC20",
            "Symbol": "xTEST",
            "Decimals": 18,
            "DomainID": 421614,
            "Address": "0x000000000000000000000000d6dF5E67e2DEF6b1c98907d9a09c18b2b7cd32C3",
            "Faucet": "0xb6abe199f9256c598df0646ca9c073276d412c90",
            "Faucet Limit": "1000000 * 1e18 per day",
        },
    ]

    return json.dumps(tokens, indent=4)


def get_secret_gcp_secrete_manager(secret_name: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/mainnet-bigq/secrets/{secret_name}/versions/latest"
    try:
        logging.info(f"Accessing secret {secret_name}")
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DeadlineExceeded:
        logging.error("Request to Secret Manager timed out.")
        raise
    except Exception as e:
        logging.info(f"Error accessing secret {secret_name}: {e}")


def get_db_url(mode="prod"):
    """GCP Secret Manager to get the DB URL"""
    if mode == "prod":
        return get_secret_gcp_secrete_manager("AWS_DB_URL_EVERCLEAR_MAINNET")
    else:
        return get_secret_gcp_secrete_manager("AWS_DB_URL_EVERCLEAR_TESTNET")


# cache data for an hour
@st.cache_data(ttl=3600)
def get_raw_data_from_postgres_by_sql(sql_file_name, mode="prod") -> pd.DataFrame:
    """
    Fetch raw data from PostgreSQL by executing a SQL file.

    Parameters:
    - sql_file_name: Name of the SQL file (without extension) located in src/streamlit_everclear/sql/
    - mode: 'prod' for production database, 'test' for test database

    Returns:
    - DataFrame containing the query results
    """
    # Read SQL file
    with open(f"src/streamlit_everclear/sql/{sql_file_name}.sql", "r") as file:
        sql = file.read()

    # Database connection settings
    if mode == "prod":
        db_url = get_db_url(mode="prod")
    else:
        db_url = get_db_url(mode="test")

    # Create a database connection
    engine = create_engine(db_url)

    # Execute the query and return the result as a DataFrame
    with engine.connect() as connection:
        df = pd.read_sql_query(sql, connection)

    return df


def apply_universal_sidebar_filters(df, date_col="date"):
    """
    Apply universal sidebar filters to the dataframe
    Filters applied and columns needed in dataframe:
    - asset_group
    - bridge
    - chain
    - date
    """
    st.sidebar.header("Filters")

    selected_asset = st.sidebar.multiselect(
        "Tokens/Assets:",
        options=df["asset_group"].unique(),
        default=["WETH", "USDC", "USDT", "DAI"],
        key="asset",
    )

    selected_bridges = st.sidebar.multiselect(
        "Bridges:", options=df["bridge"].unique(), default=[], key="bridge"
    )

    st.sidebar.subheader("Time Range Picker")

    # last 30 days
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=31),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    from_date = st.sidebar.date_input(
        "Start Date", value=default_start, max_value=default_end, key="start_date"
    )
    to_date = st.sidebar.date_input(
        "End Date",
        value=default_end,
        min_value=default_start,
        max_value=default_end,
        key="end_date",
    )

    if from_date and to_date:
        start_date, end_date = from_date, to_date
        if df[date_col].dtype == "O":
            df["datetime"] = pd.to_datetime(df[date_col])
        else:
            df["datetime"] = df[date_col]
        df["day"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df = df[(df["day"] >= start_date) & (df["day"] <= end_date)]

    selected_chain = st.sidebar.multiselect(
        "Chains:", options=df["chain"].unique(), default=[], key="chain"
    )

    if selected_chain:
        df = df[df["chain"].isin(selected_chain)]
    if selected_bridges:
        df = df[df["bridge"].isin(selected_bridges)]
    if selected_asset:
        df = df[df["asset_group"].isin(selected_asset)]

    return df


def get_latest_value_by_date(df, date_col="date"):
    """
    Get the latest value by date -> filter the df by date and get the value of the metric_col
    """
    return df.loc[df[date_col] == df[date_col].max()]


def sql_template_filter_date(sql_file_name, date_filter: dict) -> str:
    """
    Get SQL query from SQL file and apply Jinja2 templating.
    date_filter -> x -> number of days to filter the data by
    """

    logging.info(f"Applying date filter to agg query {sql_file_name}")
    sql_file_path = f"src/streamlit_everclear/sql/agg/{sql_file_name}.sql"

    try:
        with open(sql_file_path, "r") as sql_file:
            file_content = sql_file.read()
            query = Template(file_content).render(date_filter)
            return query
    except FileNotFoundError:
        print(f"The file {sql_file_path} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


@st.cache_data(ttl=3600)
def get_agg_data_from_sql_template(
    sql_file_name: str, date_filter: dict, mode: str = "prod"
) -> pd.DataFrame:
    """
    Retrieve aggregated data by applying a SQL template with Jinja2 templating.

    Parameters:
        sql_file_name (str): Name of the SQL template file (without extension) located in src/streamlit_everclear/sql/agg/.
        date_filter (dict): Dictionary containing date filtering parameters, e.g., {"from_date": "2024-01-01", "to_date": "2024-01-31"}.
        mode (str, optional): Database mode, either "prod" for production or "test" for testing. Defaults to "prod".

    Returns:
        pd.DataFrame: DataFrame containing the query results.
    """
    try:
        sql = sql_template_filter_date(sql_file_name, date_filter)
        logging.info(f"Generated SQL: {sql}")
        db_url = get_db_url(mode)
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df = pd.read_sql_query(sql, connection)
        return df
    except SQLAlchemyError as e:
        logging.error(f"Database query failed: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def apply_date_filter_to_df(df, date_col="day", from_date=None, to_date=None):
    """
    Apply universal sidebar filters to the dataframe
    Filters applied and columns needed in dataframe:
    - asset_group
    - bridge
    - chain
    - date
    """

    if from_date and to_date:
        start_date, end_date = from_date, to_date
        if df[date_col].dtype == "O":
            df["datetime"] = pd.to_datetime(df[date_col])
        else:
            df["datetime"] = df[date_col]
        df["day_part"] = df["datetime"].dt.date

        df = df[(df["day_part"] >= start_date) & (df["day_part"] <= end_date)]
    return df


# if __name__ == "__main__":
#     print(
#         get_agg_data_from_sql_template(
#             "agg_metric_1_Settlement_Rate_24h",
#             {"from_date": "2024-01-01", "to_date": "2024-01-31"},
#             mode="test",
#         )
#     )
