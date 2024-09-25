# Adding the streamlit pages to the sidebar
import json
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
from sqlalchemy.util import immutabledict
from chains_assets_metadata import ChainsAssetsMetadata

logging.basicConfig(level=logging.INFO)


def get_secret_gcp_secrete_manager(secret_name: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/mainnet-bigq/secrets/{secret_name}/versions/latest"
    try:
        # logging.info(f"Accessing secret {secret_name}")
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


# cache data for a day
@st.cache_data(ttl=86400)
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
    try:
        logging.info(f"Fetching raw data for {sql_file_name}")
        with open(f"src/streamlit_everclear/sql/{sql_file_name}.sql", "r") as file:
            sql = file.read()
    except FileNotFoundError:
        logging.error(f"The file {sql_file_name}.sql was not found.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

    # Database connection settings
    if mode == "prod":
        db_url = get_db_url(mode="prod")
    else:
        db_url = get_db_url(mode="test")

    # Create a database connection
    engine = create_engine(db_url)

    # Execute the query and return the result as a DataFrame
    with engine.connect() as connection:
        if isinstance(connection, immutabledict):
            connection = dict(connection)
        df = pd.read_sql_query(sql, connection)

    logging.info(f"Raw data fetched for {sql_file_name}")
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
    sql_file_path = f"src/streamlit_everclear/sql/{sql_file_name}.sql"

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
        # logging.info(f"Generated SQL: {sql}")
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
        # return based on null or not
        if df[date_col].isnull().all():
            return df
        else:
            df = df[(df["day_part"] >= start_date) & (df["day_part"] <= end_date)]
            return df


def convert_to_token_address(padded_address: str) -> str:
    """
    Convert a 32-byte padded address to a standard 20-byte ERC-20 token address.

    Args:
        padded_address (str): The padded address string starting with '0x'.

    Returns:
        str: The standard ERC-20 token address.
    """
    if padded_address.startswith("0x"):
        padded_address = padded_address[2:]
    # Extract the last 40 characters (20 bytes)
    token_address = "0x" + padded_address[-40:]
    return token_address


@st.cache_data(ttl=86400)
def get_chains_assets_metadata():
    """
    Get the chains and assets metadata
    """

    # Initialize the class with the target URL
    metadata_scraper = ChainsAssetsMetadata(
        url="https://docs.everclear.org/resources/contracts/mainnet"
    )

    # Pull the registered assets data into a DataFrame
    df_assets = metadata_scraper.pull_registered_assets_data()
    print(df_assets)
    return df_assets
