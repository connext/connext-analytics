import json
import logging
import pprint
from ast import Dict
from datetime import datetime, timedelta
from typing import Iterator, Sequence

import dlt
import pandas as pd
import pytz
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

from src.integrations.models.dune import (AcrossAggregatorDaily,
                                          ArbWethDepositTransaction,
                                          BridgesAggregateFlowsDaily,
                                          BridgesNativeEvmEth,
                                          BridgesTokensEvmEth,
                                          CannonicalBridgesFlowsNativeHourly,
                                          CannonicalBridgesFlowsTokensHourly,
                                          HourlyTokenPricingBlockchainEth,
                                          StargateBridgesDailyAgg)
from src.integrations.utilities import (get_latest_value_from_bq_table_by_col,
                                        get_secret_gcp_secrete_manager)

# Logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DUNE_START_DATE = 1703980800  # dec 31st 2023- only this year

# Dune Client
dune = DuneClient(api_key=get_secret_gcp_secrete_manager("source_DUNE_API_KEY_1"))


def epoch_date_before_today_utc() -> int:
    end_date = datetime.now(pytz.UTC) - timedelta(days=1)
    return int(
        (
            end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)
        ).timestamp()
    )


def get_result_by_query_id(id: int, start_date: int, end_date: int = None):
    """
    get_result_by_query_id _summary_

    _extended_summary_

    Args:
        id (int): _description_
        start_date (int): _description_
        end_date (int, optional): _description_. Defaults to None.
        new (bool, optional): Get Data after running query to get new data or get old executed data. Defaults to False.
        new= False: to use for local deployment, run the query in UI and query the result via API

    Yields:
        _type_: _description_
    """
    #

    logging.info(
        f""" Query ID: {id}, \n 
        start_date: {start_date}, \n 
        end_date: {end_date}
        """
    )
    query = QueryBase(
        name="DUNE PIPELINE QUERY",
        query_id=id,
        params=[
            QueryParameter.number_type(name="start_date", value=start_date),
            QueryParameter.number_type(name="end_date", value=end_date),
        ],
    )
    results = dune.run_query(query, ping_frequency=10, batch_size=1000)
    results_resp = results.result.rows

    # # write dict of list results_resp to file
    # with open("data/dune_results.json", "w") as f:
    #     json.dump(results_resp, f)

    yield results_resp


def get_start_end_date(
    table_id: str,
    start_date: int,
    end_date: int = int(datetime.now(pytz.UTC).timestamp()),
) -> Dict:
    """
    Input:
        start_date (epoch timestamp): Data to pull. Defaults to DUNE_START_DATE. or pulls from database
        end_date (epoch timestamp): Date till data to pull. Defaults to epoch_date_before_today_utc().

    Output:
        _type_: _description_
     _summary_

    _extended_summary_

    Returns:
        _type_: _description_
    """

    final_start_date = get_latest_value_from_bq_table_by_col(
        table_id=table_id,
        col="date",
        base_val=start_date,
    )
    return {
        "start_date": int(final_start_date),
        "end_date": int(end_date),
    }


# Bridges
@dlt.resource(
    table_name="source_native_evm_eth_bridges",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(BridgesNativeEvmEth),
)
def get_native_evm_eth__bridges(
    native_evm_eth__bridges_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    # df = pd.read_csv("data/native_evM_bridge_txs_daily_1_jan_march_24.csv")
    # df["date"] = pd.to_datetime(df["date"])
    # yield df.to_dict("records")

    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_native_evm_eth_bridges",
        start_date=DUNE_START_DATE,
    )

    if date_param["start_date"] < date_param["end_date"]:
        yield get_result_by_query_id(
            native_evm_eth__bridges_query_id,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        )
    else:
        logging.info("Data uptodate in the DB")


@dlt.resource(
    table_name="source_tokens_evm_eth_bridges",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(BridgesTokensEvmEth),
)
def get_tokens_evm_eth__bridges(
    tokens_evm_eth__bridges_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    # df = pd.read_csv("data/native_evM_bridge_txs_daily_1_jan_march_24.csv")
    # df["date"] = pd.to_datetime(df["date"])
    # yield df.to_dict("records")

    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_tokens_evm_eth_bridges",
        start_date=DUNE_START_DATE,
    )

    if date_param["start_date"] < date_param["end_date"]:
        yield get_result_by_query_id(
            tokens_evm_eth__bridges_query_id,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        )
    else:
        logging.info("Data uptodate in the DB")


@dlt.resource(
    table_name="source_stargate_bridges",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(StargateBridgesDailyAgg),
)
def get_stargate_bridges_daily_agg(
    stargate_daily_agg_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_stargate_bridges",
        start_date=DUNE_START_DATE,
    )
    if date_param["start_date"] < date_param["end_date"]:
        while date_param["start_date"] < date_param["end_date"]:
            start_date = date_param["start_date"]
            end_date = start_date + 86400
            print(f"pair of dates: {start_date}, {end_date}")
            yield get_result_by_query_id(
                stargate_daily_agg_query_id,
                start_date=start_date,
                end_date=end_date,
            )
            date_param["start_date"] = end_date
    else:
        logging.info("Data uptodate in the DB")


# across_daily_agg_query_id
@dlt.resource(
    table_name="source_across_aggregator_daily",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(AcrossAggregatorDaily),
)
def get_across__aggregator_daily(
    across_aggregator_daily_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_across_aggregator_daily",
        start_date=DUNE_START_DATE,
    )
    if date_param["start_date"] < date_param["end_date"]:
        yield get_result_by_query_id(
            across_aggregator_daily_query_id,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        )
    else:
        logging.info("Data uptodate in the DB")


# hourly_token_pricing_query_id1
@dlt.resource(
    table_name="source_hourly_token_pricing_blockchain_eth",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(HourlyTokenPricingBlockchainEth),
)
def get_hourly_token_pricing_blockchain_eth(
    hourly_token_pricing_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth",
        start_date=1609459200,  # 2021-01-01
    )

    # end date is latest timestamp
    date_param["end_date"] = int(datetime.now(pytz.UTC).timestamp())

    if date_param["start_date"] < date_param["end_date"]:
        for result in get_result_by_query_id(
            hourly_token_pricing_query_id,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        ):
            for record in result:  # Iterate over each dictionary in the list
                yield HourlyTokenPricingBlockchainEth(
                    **record
                )  # Unpack each dictionary
    else:
        logging.info("Data uptodate in the DB")


# ARB Distributions
@dlt.resource(
    table_name="source_arb_weth_deposit_transactions",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(ArbWethDepositTransaction),
)
def get_arb_weth_deposit_transactions(
    arb_weth_deposit_transactions_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_arb_weth_deposit_transactions",
        start_date=1718668800,
    )

    # date_param = {}
    # date_param["start_date"] = 1718668800

    # end date is latest timestamp
    date_param["end_date"] = int(datetime.now(pytz.UTC).timestamp())

    if date_param["start_date"] < date_param["end_date"]:
        for result in get_result_by_query_id(
            arb_weth_deposit_transactions_query_id,
            start_date=date_param["start_date"],
            end_date=date_param["end_date"],
        ):
            for record in result:  # Iterate over each dictionary in the list
                yield ArbWethDepositTransaction(**record)  # Unpack each dictionary
    else:
        logging.info("Data uptodate in the DB")


# TOKENS
@dlt.resource(
    table_name="source_cannonical_bridges_flows_tokens_hourly",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(CannonicalBridgesFlowsTokensHourly),
)
def get_hourly_cannonical_bridges_hourly_flows_tokens(
    cannonical_bridges_hourly_flows_tokens_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_cannonical_bridges_flows_tokens_hourly",
        start_date=DUNE_START_DATE,
    )
    if date_param["start_date"] < date_param["end_date"]:
        for result in get_result_by_query_id(
            cannonical_bridges_hourly_flows_tokens_query_id,
            start_date=date_param["start_date"],
        ):
            for record in result:  # Iterate over each dictionary in the list
                yield CannonicalBridgesFlowsTokensHourly(
                    **record
                )  # Unpack each dictionary
    else:
        logging.info("Data uptodate in the DB")


# NATIVE
@dlt.resource(
    table_name="source_cannonical_bridges_flows_native_hourly",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(CannonicalBridgesFlowsNativeHourly),
)
def get_hourly_cannonical_bridges_hourly_flows_native(
    cannonical_bridges_hourly_flows_native_query_id=dlt.config.value,
) -> Iterator[TDataItems]:
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_cannonical_bridges_flows_native_hourly",
        start_date=DUNE_START_DATE,
    )
    if date_param["start_date"] < date_param["end_date"]:
        for result in get_result_by_query_id(
            cannonical_bridges_hourly_flows_native_query_id,
            start_date=date_param["start_date"],
        ):
            for record in result:  # Iterate over each dictionary in the list
                yield CannonicalBridgesFlowsNativeHourly(
                    **record
                )  # Unpack each dictionary
    else:
        logging.info("Data uptodate in the DB")


# NATIVE
@dlt.resource(
    table_name="source_bridges_aggregate_flows_daily",
    write_disposition="append",
    columns=pydantic_to_table_schema_columns(BridgesAggregateFlowsDaily),
)
def get_bridges_aggregate_flows_daily(
    bridges_aggregate_flows_daily_query_id_list=dlt.config.value,
) -> Iterator[TDataItems]:
    """
    get_bridges_aggregate_flows_monthly

    Args:
        bridges_aggregate_flows_monthly_query_id_list (list, optional): Loop over all query ids and get data for each,
        append to the result and store to the BQ
        The overall data ETL is on replace basis, so if the data is up to date, it will replace the old data in BQ

        Notes:
        - provide a start date 1 day before the desired date.
        - end date is latest timestamp - but not needed given its a monthly agg.

    QUERY IDS:
        3908167: circle CCTP- bridge_circle_cctp_flow_aggregate
        3909186: Native Bridges Tokens- bridge_native_cannonical_token_flow_aggregate
        3909641: Native Bridges ETH- bridge_native_cannonical_eth_flow_aggregate
        3908667: Across- bridge_across_flow_aggregate
        3908109: LayerZero- bridge_layerzero_flow_aggregate

    URL PARAMETER: # ?end_date_t6c1ea=1722038400&start_date_t6c1ea=1703980800

    Yields:
        Iterator[TDataItems]: _description_
    TODO
    - [X] replace start date from start of year to dynamic date
    - [X] replace to append for type of disposition
    """
    date_param = get_start_end_date(
        table_id="mainnet-bigq.dune.source_bridges_aggregate_flows_daily",
        start_date=DUNE_START_DATE,
        end_date=epoch_date_before_today_utc(),
    )
    if date_param["start_date"] < date_param["end_date"]:
        for query_id in bridges_aggregate_flows_daily_query_id_list:
            for result in get_result_by_query_id(
                query_id,
                start_date=date_param["start_date"],
                end_date=date_param["end_date"],
            ):
                for record in result:  # Iterate over each dictionary in the list
                    # add bridge name based on query if to the records
                    yield BridgesAggregateFlowsDaily(**record)
    else:
        logging.info("Data uptodate in the DB")


# Sources
@dlt.source(
    max_table_nesting=0,
)
def dune_bridges() -> Sequence[DltResource]:
    return [
        # new:
        get_hourly_token_pricing_blockchain_eth,
        get_arb_weth_deposit_transactions,
    ]


# Sources
@dlt.source(
    max_table_nesting=0,
)
def dune_daily_metrics() -> Sequence[DltResource]:
    return [
        # testing
        get_bridges_aggregate_flows_daily
    ]
