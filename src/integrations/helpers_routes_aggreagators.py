import pandas_gbq
from datetime import datetime
import numpy as np
from src.integrations.utilities import get_raw_from_bq, read_sql_from_file_add_template


# 1. Greate than date from a big query table
def get_greater_than_date_from_bq_table(table_id, date_col):
    """
    pass table name and date column name
    -- EG:
        -- SELECT max(upload_datetime) AS latest_upload_datetime FROM `mainnet-bigq.stage.source_lifi__routes`
        -- SELECT max(upload_datetime) AS latest_upload_datetime FROM `mainnet-bigq.raw.source_socket__routes`
    """
    try:

        sql = read_sql_from_file_add_template(
            sql_file_name="get_latest_by_bq_table_and_date_col",
            template_data={"date_col": date_col, "table_id": table_id},
        )
        df = pandas_gbq.read_gbq(sql)
        return np.array(df[date_col].dt.to_pydatetime())[0].replace(tzinfo=None)

    except pandas_gbq.exceptions.GenericGBQException as e:
        if "Reason: 404" in str(e):
            return datetime(2024, 1, 1, 1, 1, 1)
        else:
            raise


if __name__ == "__main__":
    print("LIFI")
    print(
        get_greater_than_date_from_bq_table(
            table_id="mainnet-bigq.stage.source_lifi__routes",
            date_col="upload_datetime",
        )
    )

    print("SOCKET")
    print(
        get_greater_than_date_from_bq_table(
            table_id="mainnet-bigq.raw.source_socket__routes",
            date_col="upload_datetime",
        )
    )
