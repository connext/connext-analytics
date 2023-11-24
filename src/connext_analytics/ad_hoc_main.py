from pprint import pprint
from pathlib import Path, PurePath
import pandas as pd
from jinja2 import Template
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

# If you don't specify credentials when constructing the client, the
# client library will look for credentials in the environment.


PROJECT_ID = "mainnet-bigq"
CURRENT_DIR = Path(__file__).parents[1]

def read_sql_from_file(sql_file_name, template_data) -> str:

    """
    Get sql query from sql file
    """

    data_dir = f"{str(PurePath(CURRENT_DIR))}/connext_analytics/sql/{sql_file_name}.sql"

    with open(data_dir, 'r') as sql_file:
        file = sql_file.read()
        query = Template(file).render(template_data)
        return query


def get_raw_from_bq(sql_file_name) -> pd.DataFrame:

    """
    pipline to get raw data
    
    Template Data:
    - month to query via jinja
    """
    
    template_data = {
    }

    sql = read_sql_from_file(
        sql_file_name=sql_file_name, 
        template_data=template_data
    )
    
    scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    ]
    credentials = service_account.Credentials.from_service_account_file(f"{str(PurePath(CURRENT_DIR))}/creds/mainnet-bigq-b32efe721225.json", scopes=scopes)
    pandas_gbq.context.credentials = credentials
    return pandas_gbq.read_gbq(sql, project_id= PROJECT_ID, credentials=credentials)
    

def pipeline():
    transfers = get_raw_from_bq(
        sql_file_name= "transfers_raw_v2"
    )
    pprint(transfers.shape)
    pprint(transfers.head())
    
    transfers.to_csv(f"{str(PurePath(CURRENT_DIR))}/data/transfers_final_v2.csv", 
                    mode='a', 
                    index=False,
                     header=True
    )

def compare_transfer_data():
    df_transfers['date_xcall_timestamp'] = pd.to_datetime(df_transfers['xcall_timestamp'], unit='s')
    df_transfers['xcall_month'] = df_transfers['date_xcall_timestamp'].dt.month
    df_transfers['xcall_year'] = df_transfers['date_xcall_timestamp'].dt.year

    pprint(
        df_transfers.groupby(
            ['xcall_month', 'xcall_year']
        )['transfer_id'].count().reset_index()
    )


    pprint(df_transfers.shape)

def upload_df_2_bq(df, table_name):
    credentials = service_account.Credentials.from_service_account_file(f"{str(PurePath(CURRENT_DIR))}/creds/mainnet-bigq-b32efe721225.json")
    pandas_gbq.to_gbq(df, table_name, project_id= PROJECT_ID, if_exists='replace', chunksize= 50000, credentials = credentials)
    print("upload completed!")
    return None


if __name__ == "__main__":

    # [X] get connected to big query via service account
    # [X] Pull data by month
    # [X] save to csv/df and push to new table in same schema

    pipeline()

    # [X] manually pull data for historical events
    # df_transfers = pd.read_csv(f"{str(PurePath(CURRENT_DIR))}/data/transfers_final_v1.csv")
    # pprint(
    #     df_transfers.shape
    # )
    # df_transfers_final = df_transfers.drop(
    #     columns= ["date_xcall_timestamp", "xcall_month", "xcall_year"],
    # )
    # pprint(
    #     df_transfers_final.dtypes
    # )

    # pprint(
    #     df_transfers_final.shape
    # )

    # df_transfers['execute_timestamp'] = pd.to_datetime(df_transfers['execute_timestamp'])
    # df_transfers['reconcile_timestamp'] = pd.to_datetime(df_transfers['reconcile_timestamp'])

    # drop 10 and 11 for 2023
    # df_transfers['date_xcall_timestamp'] = pd.to_datetime(df_transfers['xcall_timestamp'], unit='s')
    # df_transfers['xcall_month'] = df_transfers['date_xcall_timestamp'].dt.month
    # df_transfers['xcall_year'] = df_transfers['date_xcall_timestamp'].dt.year
    # logic = (df_transfers['xcall_month'] >= 10) & (df_transfers['xcall_year'] == 2023)
    # edit_df_transfers = df_transfers[~ logic]
    # pprint(
    #     edit_df_transfers.groupby(
    #         ['xcall_month', 'xcall_year']
    #     )['transfer_id'].count().reset_index()
    # )
    # pprint(edit_df_transfers.shape)
    # edit_df_transfers.to_csv(f"{str(PurePath(CURRENT_DIR))}/data/transfers_final_v1.csv", 
    #                 mode='a', 
    #                 index=False,
    #                 #  header=False
    # )


    # compare_transfer_data()

    # # Test: 
    # logic_amount = (~ df_transfers['d_bridged_amt'].isna() & df_transfers['usd_bridged_amt'].isna())
    # pprint(df_transfers[logic_amount]['origin_transacting_asset'].unique())

    # [X] Push to big query
    # upload_df_2_bq(df= df_transfers_final, table_name="mainnet-bigq.public.transfers_in_usd")
