from google.api_core import exceptions as google_exceptions
import pandas_gbq as gbq
import pandas as pd
import logging


def get_raw_data_from_bq_df(sql) -> pd.DataFrame:
    """
    Fetch data from BigQuery and return as a pandas DataFrame.

    Exceptions handled:
    1. google.api_core.exceptions.NotFound: Data not available in BigQuery
    2. google.api_core.exceptions.BadRequest: Error in fetching data from BigQuery
    3. google.api_core.exceptions.GoogleAPICallError: General API errors (including 404 and 500)
    4. Exception: Any other unexpected errors
    """
    try:
        return gbq.read_gbq(sql)
    except google_exceptions.NotFound as e:
        logging.error(f"Data not available in BigQuery: {e}")
    except google_exceptions.BadRequest as e:
        logging.error(f"Error in fetching data from BigQuery: {e}")
    except google_exceptions.GoogleAPICallError as e:
        if e.code == 404:
            logging.error(f"404 Error: Resource not found: {e}")
        elif e.code == 500:
            logging.error(f"500 Error: Internal server error: {e}")
        else:
            logging.error(f"Google API call error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching data from BigQuery: {e}")

    return pd.DataFrame()  # Return an empty DataFrame if an error occurs


def metrics_daily():
    sql = """
        SELECT
            *
        FROM `everclear.mtr_okrs__daily`
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")


def metric_rebalancing_fee_usd():
    sql = """
        SELECT
            day,
            rebalancing_fee_usd AS rebalancing_fee_usd
        FROM `everclear.mtr_okrs__daily`
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")


def metric_volume_usd():
    sql = """
        SELECT
            day,
            volume_usd AS volume_usd
        FROM `everclear.mtr_okrs__daily`
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")


def metric_avg_rebalancing_fee_bps():
    sql = """
        SELECT
            day,
            avg_rebalancing_fee_bps AS avg_rebalancing_fee_bps
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")


def metric_avg_settlement_time_hrs():
    sql = """
        SELECT
            day,
            avg_settlement_time_hrs AS avg_settlement_time_hrs
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")


def metric_pct_intents_settled_within_6hrs():
    sql = """
        SELECT
            day,
            pct_intents_settled_within_6hrs AS pct_intents_settled_within_6hrs
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")


def metric_pct_intents_netted_in_24hrs():
    sql = """
        SELECT
            day,
            pct_intents_netted_in_24hrs AS pct_intents_netted_in_24hrs
    """
    return get_raw_data_from_bq_df(sql).to_dict(orient="records")
