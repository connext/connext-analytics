import os
import time
import asyncio
import pandas as pd
import asyncpg
import pandas_gbq as gbq
import logging
import dotenv

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
# Database connection details
DATABASE_URL = os.getenv("AWS_DB_URL_EVERCLEAR_MAINNET")

# List of tables to query
TABLES = ["invoices", "intents", "messages"]


async def fetch_table_data(conn, table):
    logging.info(f"Fetching sql for {table}")
    sql_file_path = f"src/sql/everclear/prod/{table}.sql"

    with open(sql_file_path, "r") as sql_file:
        query = sql_file.read().strip()

    # Execute the query
    logging.info(f"Executing query for {table}")
    rows = await conn.fetch(query)
    logging.info(f"Fetched {len(rows)} rows from {table}")

    # Get column names from the result
    if rows:
        column_names = list(rows[0].keys())
    else:
        raise Exception(f"No rows fetched for {table}")
    # Create DataFrame with proper column names
    df = pd.DataFrame(rows, columns=column_names)

    return df


async def push_data_to_bigquery(dataframe, table_name):
    """table_name: name_of_dataset.table_name"""
    logging.info(
        f"Pushing data to bigquery for {table_name} into everclear_prod_db.{table_name}"
    )
    if not dataframe.empty:
        logging.info(dataframe.head())
        dataframe["uploaded_at"] = pd.Timestamp.now(tz="UTC")
        gbq.to_gbq(
            dataframe=dataframe,
            project_id="mainnet-bigq",
            destination_table=f"everclear_prod_db.{table_name}",
            if_exists="replace",
            api_method="load_csv",
        )
        logging.info(f"Successful, {table_name} data pushed to bigquery")
    else:
        logging.info(f"Dataframe for {table_name} is empty")


async def pull_and_push_pipeline(database_url, tables):
    async def fetch_single_table(table):
        logging.info(f"Fetching data from {table}")
        conn = await asyncpg.connect(database_url)
        try:
            return await fetch_table_data(conn, table)
        finally:
            await conn.close()

    dataframes = await asyncio.gather(*[fetch_single_table(table) for table in tables])
    all_dataframes = dict(zip(tables, dataframes))

    for name, df in all_dataframes.items():
        logging.info(f"Data from {name}:")
        await push_data_to_bigquery(dataframe=df, table_name=name)


async def intent_pipeline():
    """
    -- logic:
        -- for intents that are not in final state. -> merge them.
            -- final state logic -> origin_status IN ('DISPATCHED') AND hub_status IN ('DISPATCHED', 'DISPATCHED_UNSUPPORTED')
        -- get new intents based on max timestamp of origin_timestamp in BQ and append them
            -- PULL MAX TIMESTAMP FROM BQ
            -- USING THIS AS FILTER IN WHERE CALUSE ALONG WITH STATUS OF FINAL STATE -> origin_status IN ('DISPATCHED') AND hub_status IN ('DISPATCHED', 'DISPATCHED_UNSUPPORTED'),
            -- PULL ALL THOSE TXS AND APPEND TO BQ TABLE
    """
    pass


async def everclear_pg_2_bq():
    start_time = time.time()
    await pull_and_push_pipeline(DATABASE_URL, TABLES)
    end_time = time.time()
    logging.info(f"Total time taken: {end_time - start_time} seconds")
