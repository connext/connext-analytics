# Adding the streamlit pages to the sidebar
import logging
import os
import pandas as pd
import pandas_gbq as gbq
import streamlit as st
from chains_assets_metadata import ChainsAssetsMetadata
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import secretmanager
from jinja2 import Template
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.util import immutabledict

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
        # if timeout pull from env file
        return os.getenv(secret_name)
    except Exception as e:
        logging.info(f"Error accessing secret {secret_name}: {e}")
        return os.getenv(secret_name)


@st.cache_data(ttl=86400)
def get_db_url(mode="prod"):
    """GCP Secret Manager to get the DB URL"""
    if mode == "prod":
        return get_secret_gcp_secrete_manager("AWS_DB_URL_EVERCLEAR_MAINNET")
    else:
        return get_secret_gcp_secrete_manager("AWS_DB_URL_EVERCLEAR_TESTNET")


# cache data for a day
@st.cache_data(ttl=86400)
def get_raw_data_from_postgres_by_sql(sql_file_name, db_url=None) -> pd.DataFrame:
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
        with open(f"src/streamlit_everclear/sql/{sql_file_name}.sql") as file:
            sql = file.read()
    except FileNotFoundError:
        logging.error(f"The file {sql_file_name}.sql was not found.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

    # Create a database connection
    engine = create_engine(db_url)

    # Execute the query and return the result as a DataFrame
    with engine.connect() as connection:
        if isinstance(connection, immutabledict):
            connection = dict(connection)
        df = pd.read_sql_query(sql, connection)

    logging.info(f"Raw data fetched for {sql_file_name}")
    return df


# get data for invoices every 30 mins``
@st.cache_data(ttl=1800)
def get_raw_data_from_postgres_by_sql_for_invoices(
    sql_file_name, mode="prod"
) -> pd.DataFrame:
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
        with open(f"src/streamlit_everclear/sql/{sql_file_name}.sql") as file:
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
        with open(sql_file_path) as sql_file:
            file_content = sql_file.read()
            query = Template(file_content).render(date_filter)
            return query
    except FileNotFoundError:
        print(f"The file {sql_file_path} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


@st.cache_data(ttl=86400)
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


def apply_sidebar_filters(
    old_df: pd.DataFrame, selected_chain, selected_asset, is_agg, from_date, to_date
):
    """
    Apply sidebar filters to the DataFrame based on selected chains, assets, and date range.
    """
    df = old_df.copy()
    try:
        if not is_agg:
            # add a datepart col
            df["datepart"] = pd.to_datetime(df["day"]).dt.date
            df = df[(df["datepart"] >= from_date) & (df["datepart"] <= to_date)]
        # Apply chain and asset filters if necessary
        if selected_chain:
            df = df[
                df["from_chain_name"].isin(selected_chain)
                | df["to_chain_name"].isin(selected_chain)
            ]

        if selected_asset:
            df = df[
                df["from_asset_symbol"].isin(selected_asset)
                | df["to_asset_symbol"].isin(selected_asset)
            ]

        # st.write(df)

        return df
    except Exception as e:
        logging.error(f"Error in apply_sidebar_filters: {e}")


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
    return df_assets


@st.cache_data(ttl=86400)
def get_raw_data_from_bq_df(sql_file_name) -> pd.DataFrame:
    """
    Get raw data from BigQuery
    Cols included are
    - date
    - router_address
    - chain
    - asset
    - tvl
    - daily_fee_earned
    - total_fee_earned
    - daily_liquidity_added
    - router_locked_total
    - calculated_router_locked_total
    - total_balance
    - daily_apr
    """
    with open(f"src/streamlit_everclear/sql/{sql_file_name}.sql") as file:
        sql = file.read()
    return gbq.read_gbq(sql)


def get_pricing_data_from_bq():
    """
    Get the pricing data from BigQuery
    """
    df = get_raw_data_from_bq_df("daily_price")
    return df


def get_chain_id_to_chain_name_data_from_bq():
    """
    Get the chain id to chain name data from BigQuery
    """
    df = get_raw_data_from_bq_df("chainids_metadata")
    return df
